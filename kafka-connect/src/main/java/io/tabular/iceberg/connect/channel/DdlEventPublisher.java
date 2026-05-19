/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.tabular.iceberg.connect.channel;

import io.tabular.iceberg.connect.IcebergSinkConfig;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.apache.avro.generic.GenericRecord;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.Tasks;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.confluent.kafka.serializers.KafkaAvroSerializer;

class DdlEventPublisher implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(DdlEventPublisher.class);

  static final String LAST_EMITTED_SCHEMA_ID_PROP = "kafka.connect.last-emitted-schema-id";
  static final int PROPERTY_UPDATE_RETRIES = 2; // 3 total attempts

  private final String topic;
  private final Producer<String, Object> producer;
  private volatile boolean terminated;

  DdlEventPublisher(IcebergSinkConfig config) {
    this(config.ddlEventsTopic(), createProducer(config));
  }

  // VisibleForTesting
  DdlEventPublisher(String topic, Producer<String, Object> producer) {
    this.topic = topic;
    this.producer = producer;
  }

  private static Producer<String, Object> createProducer(IcebergSinkConfig config) {
    Map<String, Object> props = Maps.newHashMap();
    props.putAll(config.kafkaProps());
    Coordinator.inheritFromValueConverter(props, config.kafkaProps());
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
    props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
    props.put(ProducerConfig.ACKS_CONFIG, "all");
    return new KafkaProducer<>(props);
  }

  void publish(Map<TableIdentifier, Table> tables) {
    if (terminated || topic == null || tables == null || tables.isEmpty()) {
      return;
    }

    List<Pending> sent = new ArrayList<>();
    for (Map.Entry<TableIdentifier, Table> entry : tables.entrySet()) {
      try {
        Pending pending = computeAndSend(entry.getKey(), entry.getValue());
        if (pending != null) {
          sent.add(pending);
        }
      } catch (Exception e) {
        LOG.warn("Failed to compute DDL events for table {}", entry.getKey(), e);
      }
    }

    if (sent.isEmpty()) {
      return;
    }

    try {
      producer.flush();
    } catch (Exception e) {
      LOG.warn("Failed to flush DDL event producer", e);
    }

    for (Pending pending : sent) {
      boolean allAcked = true;
      for (Future<RecordMetadata> f : pending.futures) {
        try {
          f.get();
        } catch (ExecutionException | InterruptedException ex) {
          LOG.error(
              "Failed to deliver DDL event for table {}",
              pending.tableId,
              ex);
          allAcked = false;
        }
      }
      if (allAcked) {
        try {
          writeLastEmittedSchemaId(pending.table, pending.sentThroughSchemaId);
        } catch (Exception e) {
          LOG.warn(
              "Failed to update last-emitted-schema-id={} for table {} (events were sent; will retry next cycle)",
              pending.sentThroughSchemaId, pending.tableId, e);
        }
      }
    }
  }

  private Pending computeAndSend(TableIdentifier tableId, Table table) {
    Schema currentSchema = table.schema();
    int current = currentSchema.schemaId();
    Integer lastEmit = readLastEmittedSchemaId(table);
    Map<Integer, Schema> available = table.schemas();

    String db = String.join(".", tableId.namespace().levels());
    String tableName = tableId.name();
    String tableUuid = String.valueOf(table.uuid());
    String key = tableUuid;

    List<Future<RecordMetadata>> futures = new ArrayList<>();

    if (lastEmit == null) {
      long occurredAt = resolveSchemaCommitTime(table, Set.of(current))
          .getOrDefault(current, ((HasTableOperations) table).operations().current().lastUpdatedMillis());
      GenericRecord event = DdlEvent.tableCreated(
          db, tableName, tableUuid, occurredAt,
          current,
          SchemaDiff.columnsFor(currentSchema),
          SchemaDiff.identifierFieldIds(currentSchema),
          SchemaDiff.identifierFieldNames(currentSchema),
          null);
      futures.add(send(key, event));
      LOG.info("Emitting TABLE_CREATED for {} schema={}", tableId, current);
      return new Pending(tableId, table, futures, current);
    }

    if (current == lastEmit) {
      return null;
    }

    if (current < lastEmit) {
      long occurredAt = resolveSchemaCommitTime(table, Set.of(current))
          .getOrDefault(current, ((HasTableOperations) table).operations().current().lastUpdatedMillis());
      GenericRecord event = DdlEvent.tableCreated(
          db, tableName, tableUuid, occurredAt,
          current,
          SchemaDiff.columnsFor(currentSchema),
          SchemaDiff.identifierFieldIds(currentSchema),
          SchemaDiff.identifierFieldNames(currentSchema),
          DdlEvent.incompleteHistory(lastEmit, DdlEvent.REASON_SCHEMA_ID_REUSED));
      futures.add(send(key, event));
      LOG.info(
          "Emitting TABLE_CREATED (schema_id_reused) for {} schema={} previous={}",
          tableId, current, lastEmit);
      return new Pending(tableId, table, futures, current);
    }

    if (!available.containsKey(lastEmit) || !chainAvailable(available, lastEmit, current)) {
      long occurredAt = resolveSchemaCommitTime(table, Set.of(current))
          .getOrDefault(current, ((HasTableOperations) table).operations().current().lastUpdatedMillis());
      GenericRecord event = DdlEvent.tableCreated(
          db, tableName, tableUuid, occurredAt,
          current,
          SchemaDiff.columnsFor(currentSchema),
          SchemaDiff.identifierFieldIds(currentSchema),
          SchemaDiff.identifierFieldNames(currentSchema),
          DdlEvent.incompleteHistory(lastEmit, DdlEvent.REASON_HISTORY_GAP));
      futures.add(send(key, event));
      LOG.info(
          "Emitting TABLE_CREATED (history_gap) for {} schema={} previous={}",
          tableId, current, lastEmit);
      return new Pending(tableId, table, futures, current);
    }

    // happy chain: resolve commit time per target schemaId in (lastEmit, current]
    Set<Integer> targets = new HashSet<>();
    for (int i = lastEmit + 1; i <= current; i++) {
      targets.add(i);
    }
    Map<Integer, Long> commitTimes = resolveSchemaCommitTime(table, targets);
    long fallback = ((HasTableOperations) table).operations().current().lastUpdatedMillis();

    for (int i = lastEmit; i < current; i++) {
      Schema before = available.get(i);
      Schema after = available.get(i + 1);
      List<GenericRecord> changes = SchemaDiff.diff(before, after);
      long occurredAt = commitTimes.getOrDefault(i + 1, fallback);
      GenericRecord event = DdlEvent.schemaChanged(
          db, tableName, tableUuid, occurredAt, i + 1, changes);
      futures.add(send(key, event));
    }
    LOG.info(
        "Emitting SCHEMA_CHANGED for {} {} → {}",
        tableId, lastEmit, current);
    return new Pending(tableId, table, futures, current);
  }

  // Walks back through the table's previous-metadata log to find when each target
  // schema id first became `currentSchemaId`. Reads previous metadata.json files lazily
  // and stops once all targets are resolved (or the log is exhausted). For typical
  // use cases the schema change is recent and 1-2 files are read.
  private static Map<Integer, Long> resolveSchemaCommitTime(Table table, Set<Integer> targets) {
    Map<Integer, Long> result = new HashMap<>();
    if (targets.isEmpty()) {
      return result;
    }
    TableMetadata metadata = ((HasTableOperations) table).operations().current();
    FileIO io = ((HasTableOperations) table).operations().io();
    List<TableMetadata.MetadataLogEntry> log = metadata.previousFiles();

    int blockSchemaId = metadata.currentSchemaId();
    long blockStartTs = metadata.lastUpdatedMillis();

    for (int i = log.size() - 1; i >= 0; i--) {
      TableMetadata.MetadataLogEntry entry = log.get(i);
      TableMetadata prev;
      try {
        prev = TableMetadataParser.read(io, entry.file());
      } catch (Exception e) {
        LOG.warn("Failed to read previous metadata file {} while resolving schema commit time", entry.file(), e);
        break;
      }
      if (prev.currentSchemaId() == blockSchemaId) {
        // still inside the same block; push earliest-known timestamp earlier
        blockStartTs = entry.timestampMillis();
      } else {
        // boundary: blockSchemaId first became current at blockStartTs
        if (targets.contains(blockSchemaId)) {
          result.put(blockSchemaId, blockStartTs);
          if (result.keySet().containsAll(targets)) {
            return result;
          }
        }
        blockSchemaId = prev.currentSchemaId();
        blockStartTs = entry.timestampMillis();
      }
    }
    // exhausted the log; record the oldest block if it's a target
    if (targets.contains(blockSchemaId) && !result.containsKey(blockSchemaId)) {
      result.put(blockSchemaId, blockStartTs);
    }
    return result;
  }

  private static boolean chainAvailable(Map<Integer, Schema> available, int from, int to) {
    for (int i = from; i <= to; i++) {
      if (!available.containsKey(i)) {
        return false;
      }
    }
    return true;
  }

  private Future<RecordMetadata> send(String key, GenericRecord event) {
    return producer.send(new ProducerRecord<>(topic, key, event));
  }

  private static Integer readLastEmittedSchemaId(Table table) {
    String raw = table.properties().get(LAST_EMITTED_SCHEMA_ID_PROP);
    if (raw == null) {
      return null;
    }
    try {
      return Integer.parseInt(raw);
    } catch (NumberFormatException e) {
      LOG.warn(
          "Invalid value '{}' for property {} on table {}; treating as null",
          raw, LAST_EMITTED_SCHEMA_ID_PROP, table.name());
      return null;
    }
  }

  // Captures `sentThrough` as a final value — Tasks.retry must NOT recompute it
  // from a freshly-refreshed schema id, otherwise events that arrive between
  // our send and the property commit retry are silently skipped.
  private void writeLastEmittedSchemaId(Table table, int sentThrough) {
    Tasks.range(1)
        .retry(PROPERTY_UPDATE_RETRIES)
        .run(notUsed -> {
          table.refresh();
          table.updateProperties()
              .set(LAST_EMITTED_SCHEMA_ID_PROP, Integer.toString(sentThrough))
              .commit();
        });
  }

  @Override
  public void close() {
    this.terminated = true;
    if (producer != null) {
      try {
        producer.flush();
      } catch (Exception e) {
        LOG.warn("Error flushing DDL event producer", e);
      }
      try {
        producer.close(Duration.ofSeconds(30));
      } catch (Exception e) {
        LOG.warn("Error closing DDL event producer", e);
      }
    }
  }

  private static final class Pending {
    final TableIdentifier tableId;
    final Table table;
    final List<Future<RecordMetadata>> futures;
    final int sentThroughSchemaId;

    Pending(TableIdentifier tableId, Table table, List<Future<RecordMetadata>> futures, int sentThroughSchemaId) {
      this.tableId = tableId;
      this.table = table;
      this.futures = futures;
      this.sentThroughSchemaId = sentThroughSchemaId;
    }
  }
}
