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

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.tabular.iceberg.connect.IcebergSinkConfig;
import io.tabular.iceberg.connect.events.DataOffsetsPayload;
import io.tabular.iceberg.connect.events.EventType;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.Collection;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.Tasks;
import org.apache.iceberg.util.ThreadPools;
import org.apache.iceberg.connect.events.Event;
import org.apache.iceberg.connect.events.StartCommit;
import org.apache.iceberg.connect.events.CommitComplete;
import org.apache.iceberg.connect.events.CommitToTable;
import org.apache.iceberg.connect.events.TableReference;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Coordinator extends Channel implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(Coordinator.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String CONTROL_TOPIC_OFFSETS_PROP_FMT = "kafka.connect.offsets.%s.%s";
  private static final String COMMIT_ID_SNAPSHOT_PROP = "kafka.connect.commit-id";
  private static final String VTTS_SNAPSHOT_PROP = "kafka.connect.vtts";
  private static final String DATA_OFFSETS_SNAPSHOT_PROP = "kafka.connect.data-offsets";
  private static final Duration POLL_DURATION = Duration.ofMillis(1000);

  private final Catalog catalog;
  private final IcebergSinkConfig config;
  private final int totalPartitionCount;
  private final String controlTopicOffsetsProp;
  private final ExecutorService exec;
  private final CommitState commitState;
  private final String watermarkTopic;
  private final Producer<String, Object> watermarkProducer;
  private final TableTopicResolver tableTopicResolver;
  private volatile boolean terminated;

  public Coordinator(
      Catalog catalog,
      IcebergSinkConfig config,
      Collection<MemberDescription> members,
      KafkaClientFactory clientFactory) {
    // pass consumer group ID to which we commit low watermark offsets
    super("coordinator", config.controlGroupId() + "-coord", config, clientFactory);

    this.catalog = catalog;
    this.config = config;
    this.totalPartitionCount =
        members.stream().mapToInt(desc -> desc.assignment().topicPartitions().size()).sum();
    this.controlTopicOffsetsProp =
        String.format(CONTROL_TOPIC_OFFSETS_PROP_FMT, config.controlTopic(), config.controlGroupId());
    this.exec = ThreadPools.newWorkerPool("iceberg-committer", config.commitThreads());
    this.commitState = new CommitState(config);

    this.watermarkTopic = config.watermarkTopic();
    if (watermarkTopic != null) {
      this.watermarkProducer = createWatermarkProducer(config);
      this.tableTopicResolver = new TableTopicResolver(admin(), config);
      LOG.info("Watermark publishing enabled for topic '{}'", watermarkTopic);
    } else {
      this.watermarkProducer = null;
      this.tableTopicResolver = null;
    }

    // initial poll with longer duration so the consumer will initialize...
    consumeAvailable(Duration.ofMillis(1000), this::receive);
  }

  private static Producer<String, Object> createWatermarkProducer(IcebergSinkConfig config) {
    Map<String, Object> props = Maps.newHashMap();
    props.putAll(config.kafkaProps());
    // KafkaAvroSerializer reads `schema.registry.url` etc. without a prefix, but Kafka Connect
    // workers expose these as `value.converter.<key>` in their properties file. Carry them over
    // unless they were explicitly set under `iceberg.kafka.*`.
    inheritFromValueConverter(props, config.kafkaProps());
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
    props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
    props.put(ProducerConfig.ACKS_CONFIG, "all");
    return new KafkaProducer<>(props);
  }

  private static final String VALUE_CONVERTER_PREFIX = "value.converter.";

  static void inheritFromValueConverter(Map<String, Object> props, Map<String, String> source) {
    source.forEach(
        (k, v) -> {
          if (k.startsWith(VALUE_CONVERTER_PREFIX) && k.length() > VALUE_CONVERTER_PREFIX.length()) {
            String stripped = k.substring(VALUE_CONVERTER_PREFIX.length());
            if (!props.containsKey(stripped)) {
              props.put(stripped, v);
              LOG.debug("Inherited '{}' from '{}' for watermark producer", stripped, k);
            }
          }
        });
  }

  public void process() {
    if (commitState.isCommitIntervalReached()) {
      // send out begin commit
      commitState.startNewCommit();
      LOG.info("Started new commit with commit-id={}", commitState.currentCommitId().toString());
      Event event =
          new Event(config.controlGroupId(), new StartCommit(commitState.currentCommitId()));
      send(event);
      LOG.info("Sent workers commit trigger with commit-id={}", commitState.currentCommitId().toString());

    }

    consumeAvailable(POLL_DURATION, this::receive);

    if (commitState.isCommitTimedOut()) {
      commit(true);
    }
  }

  private boolean receive(Envelope envelope) {
    // Handle local events (DATA_OFFSETS, etc.)
    if (envelope.isLocalEvent()) {
      if (envelope.localEventType() == EventType.DATA_OFFSETS) {
        commitState.addDataOffsets(envelope);
        return true;
      }
      return false;
    }

    // Handle standard Iceberg events
    switch (envelope.event().type()) {
      case DATA_WRITTEN:
        commitState.addResponse(envelope);
        return true;
      case DATA_COMPLETE:
        commitState.addReady(envelope);
        if (commitState.isCommitReady(totalPartitionCount)) {
          commit(false);
        }
        return true;
    }
    return false;
  }

  private void commit(boolean partialCommit) {
    try {
      LOG.info("Processing commit after responses for {}, isPartialCommit {}",commitState.currentCommitId(), partialCommit);
      doCommit(partialCommit);
    } catch (Exception e) {
      LOG.warn("Commit failed, will try again next cycle", e);
    } finally {
      commitState.endCurrentCommit();
    }
  }

  private void doCommit(boolean partialCommit) {
    Map<TableIdentifier, List<List<Envelope>>> commitMap = commitState.tableCommitMap();

    OffsetDateTime vtts = commitState.vtts(partialCommit);

    List<Envelope> dataOffsetsSnapshot = Lists.newArrayList(commitState.dataOffsetsBuffer());

    Tasks.foreach(commitMap.entrySet())
        .executeWith(exec)
        .stopOnFailure()
        .run(
            entry -> {
              commitToTableBatch(entry.getKey(), entry.getValue(), vtts);
            });

    // we should only get here if all tables committed successfully...
    commitConsumerOffsets();
    commitState.clearResponses();

    Event event =
        new Event(config.controlGroupId(), new CommitComplete(commitState.currentCommitId(), vtts));
    send(event);

    if (!partialCommit) {
      Map<TableIdentifier, Snapshot> committedSnapshots =
          captureCommittedSnapshots(commitMap.keySet());
      publishWatermarks(dataOffsetsSnapshot, committedSnapshots);
    }

    LOG.info(
        "Commit {} complete, committed to {} table(s), vtts {}",
        commitState.currentCommitId(),
        commitMap.size(),
        vtts);
  }

  private Map<TableIdentifier, Snapshot> captureCommittedSnapshots(Set<TableIdentifier> tables) {
    String currentCommitId = commitState.currentCommitId().toString();
    Map<TableIdentifier, Snapshot> result = Maps.newHashMap();
    for (TableIdentifier tableId : tables) {
      try {
        Table t = catalog.loadTable(tableId);
        Snapshot found = findOurSnapshot(t, currentCommitId);
        if (found != null) {
          result.put(tableId, found);
        }
      } catch (Exception e) {
        LOG.warn("Failed to capture iceberg snapshot for {} (skipping in watermark)", tableId, e);
      }
    }
    return result;
  }

  // Find the snapshot that THIS commit cycle produced for the table by scanning all snapshots
  // and matching by `kafka.connect.commit-id` in the snapshot summary. Iterating instead of
  // taking currentSnapshot() so a concurrent writer that pushed a newer snapshot on top of ours
  // doesn't make us lose our snapshot reference. Returns null if our snapshot was skipped (empty
  // after dedup) or already expired.
  private static Snapshot findOurSnapshot(Table table, String currentCommitId) {
    for (Snapshot snapshot : table.snapshots()) {
      if (currentCommitId.equals(snapshot.summary().get(COMMIT_ID_SNAPSHOT_PROP))) {
        return snapshot;
      }
    }
    return null;
  }

  void publishWatermarks(
      List<Envelope> dataOffsetsSnapshot, Map<TableIdentifier, Snapshot> committedSnapshots) {
    if (terminated
        || watermarkProducer == null
        || watermarkTopic == null
        || tableTopicResolver == null) {
      return;
    }
    try {
      Map<TableIdentifier, Map<TopicPartition, io.tabular.iceberg.connect.events.TopicPartitionOffset>>
          activeByTable = collectActiveOffsets(dataOffsetsSnapshot);
      Map<TableIdentifier, TopicPartition> tablesToPublish =
          mergeKnownAndActive(tableTopicResolver.resolve(), activeByTable);
      if (tablesToPublish.isEmpty()) {
        return;
      }

      Set<TopicPartition> allTps = Sets.newHashSet(tablesToPublish.values());
      Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> latestResult =
          listOffsets(allTps, OffsetSpec.latest());
      Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> maxTsResult =
          listOffsets(allTps, OffsetSpec.maxTimestamp());

      long commitTime = commitState.getStartTime();
      String commitId = commitState.currentCommitId().toString();

      List<TableIdentifier> sentTables = new ArrayList<>(tablesToPublish.size());
      List<Future<RecordMetadata>> sentFutures = new ArrayList<>(tablesToPublish.size());
      for (Map.Entry<TableIdentifier, TopicPartition> entry : tablesToPublish.entrySet()) {
        sentTables.add(entry.getKey());
        sentFutures.add(
            sendWatermarkRecord(
                entry.getKey(),
                entry.getValue(),
                commitId,
                commitTime,
                activeByTable.get(entry.getKey()),
                latestResult.get(entry.getValue()),
                maxTsResult.get(entry.getValue()),
                committedSnapshots.get(entry.getKey())));
      }
      // wait for the broker to ack every send so silent serializer / SR / broker failures
      // surface here instead of being lost in the producer's internal callback queue
      watermarkProducer.flush();
      for (int i = 0; i < sentFutures.size(); i++) {
        try {
          sentFutures.get(i).get();
        } catch (ExecutionException | InterruptedException ex) {
          LOG.error(
              "Failed to deliver watermark for table {} (commit {})",
              sentTables.get(i),
              commitState.currentCommitId(),
              ex);
        }
      }
    } catch (Throwable t) {
      LOG.error("Failed to publish watermarks for commit {}", commitState.currentCommitId(), t);
    }
  }

  static Map<TableIdentifier, TopicPartition> mergeKnownAndActive(
      Map<TableIdentifier, TopicPartition> known,
      Map<TableIdentifier, Map<TopicPartition, io.tabular.iceberg.connect.events.TopicPartitionOffset>>
          activeByTable) {
    Map<TableIdentifier, TopicPartition> result = Maps.newHashMap(known);
    activeByTable.forEach(
        (tableId, perTp) -> {
          if (!result.containsKey(tableId) && !perTp.isEmpty()) {
            result.put(tableId, perTp.keySet().iterator().next());
          }
        });
    return result;
  }

  private Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> listOffsets(
      Set<TopicPartition> tps, OffsetSpec spec) throws Exception {
    Map<TopicPartition, OffsetSpec> req = Maps.newHashMap();
    tps.forEach(tp -> req.put(tp, spec));
    return admin().listOffsets(req).all().get();
  }

  private Future<RecordMetadata> sendWatermarkRecord(
      TableIdentifier tableId,
      TopicPartition tp,
      String commitId,
      long commitTime,
      Map<TopicPartition, io.tabular.iceberg.connect.events.TopicPartitionOffset> tableActive,
      ListOffsetsResult.ListOffsetsResultInfo latest,
      ListOffsetsResult.ListOffsetsResultInfo maxTs,
      Snapshot icebergSnapshot) {
    String db = String.join(".", tableId.namespace().levels());
    String table = tableId.name();

    io.tabular.iceberg.connect.events.TopicPartitionOffset tpo = pickActiveOffset(tableActive, tp);
    Long lastConsumedOffset = tpo == null ? null : tpo.offset();
    Long lastConsumedEventTime = tpo == null ? null : tpo.timestamp();

    Long lastKafkaOffset = computeLastKafkaOffset(latest, maxTs);
    Long lastKafkaEventTime = computeLastKafkaEventTime(maxTs);

    Long icebergSnapshotId = icebergSnapshot == null ? null : icebergSnapshot.snapshotId();
    Long icebergCommittedAt = icebergSnapshot == null ? null : icebergSnapshot.timestampMillis();

    org.apache.avro.generic.GenericRecord record =
        TableWatermark.build(
            db,
            table,
            commitId,
            commitTime,
            tp.topic(),
            lastConsumedOffset,
            lastConsumedEventTime,
            lastKafkaOffset,
            lastKafkaEventTime,
            icebergSnapshotId,
            icebergCommittedAt);

    String key = db + "." + table;
    return watermarkProducer.send(new ProducerRecord<>(watermarkTopic, key, record));
  }

  // Treats the partition as "empty right now" when maxTimestamp returns -1 (KIP-734) — either
  // never written to or retention deleted everything. In that case latest.offset() still reports
  // the log-end-offset, but it points at a non-existent record, so we publish null for both
  // fields. Downstream sees "Kafka has nothing here" rather than a phantom offset.
  static Long computeLastKafkaOffset(
      ListOffsetsResult.ListOffsetsResultInfo latest,
      ListOffsetsResult.ListOffsetsResultInfo maxTs) {
    if (maxTs == null || maxTs.timestamp() < 0) {
      return null;
    }
    if (latest == null || latest.offset() <= 0) {
      return null;
    }
    return latest.offset() - 1;
  }

  static Long computeLastKafkaEventTime(ListOffsetsResult.ListOffsetsResultInfo maxTs) {
    if (maxTs == null || maxTs.timestamp() < 0) {
      return null;
    }
    return maxTs.timestamp();
  }

  static io.tabular.iceberg.connect.events.TopicPartitionOffset pickActiveOffset(
      Map<TopicPartition, io.tabular.iceberg.connect.events.TopicPartitionOffset> tableActive,
      TopicPartition tp) {
    if (tableActive == null || tableActive.isEmpty()) {
      return null;
    }
    io.tabular.iceberg.connect.events.TopicPartitionOffset tpo = tableActive.get(tp);
    return tpo != null ? tpo : tableActive.values().iterator().next();
  }

  static Map<
          TableIdentifier, Map<TopicPartition, io.tabular.iceberg.connect.events.TopicPartitionOffset>>
      collectActiveOffsets(List<Envelope> dataOffsetsSnapshot) {
    Map<TableIdentifier, Map<TopicPartition, io.tabular.iceberg.connect.events.TopicPartitionOffset>>
        result = Maps.newHashMap();
    for (Envelope env : dataOffsetsSnapshot) {
      DataOffsetsPayload payload = (DataOffsetsPayload) env.localEvent().payload();
      TableIdentifier tableId = payload.tableName().toIdentifier();
      Map<TopicPartition, io.tabular.iceberg.connect.events.TopicPartitionOffset> perTable =
          result.computeIfAbsent(tableId, k -> Maps.newHashMap());
      for (io.tabular.iceberg.connect.events.TopicPartitionOffset tpo : payload.dataOffsets()) {
        TopicPartition tp = new TopicPartition(tpo.topic(), tpo.partition());
        io.tabular.iceberg.connect.events.TopicPartitionOffset existing = perTable.get(tp);
        // when several DATA_OFFSETS come for the same (table, tp) within a cycle, keep the one
        // with the highest offset — that is the latest record we wrote
        if (existing == null
            || (tpo.offset() != null
                && (existing.offset() == null || tpo.offset() > existing.offset()))) {
          perTable.put(tp, tpo);
        }
      }
    }
    return result;
  }

  private Pair<Table, Optional<String>> getTableAndBranch(TableIdentifier tableIdentifier) {
    Table table;
    try {
      table = catalog.loadTable(tableIdentifier);
      Optional<String> branch = config.tableConfig(tableIdentifier.toString()).commitBranch();
      return Pair.of(table, branch);
    } catch (NoSuchTableException e) {
      LOG.warn("Table not found, skipping commit: {}", tableIdentifier);
      return null;
    }
  }

  /**
   * This method takes the tokenized Envelope list and calls commitToTable for each batch. In each
   * batch(except the last one) the maxOffset of the last Envelope in the List is resolved with the
   * offset committed in the previous commit and committed to the snapshot summary. The last batch
   * takes the offsetJson from the control topic and commits it.
   *
   * @param tableIdentifier Iceberg TableIdentifier
   * @param tokenizedEnvelopeList Tokenized Envelop List of Events
   * @param offsetsJson offsetsJson from control topic
   * @param vtts valid-through timestamp
   */
  private void commitToTableBatch(
      TableIdentifier tableIdentifier,
      List<List<Envelope>> tokenizedEnvelopeList,
      OffsetDateTime vtts) {
    Pair<Table, Optional<String>> tableBranch = getTableAndBranch(tableIdentifier);
    if (tableBranch != null) {
      for (int i = 0; i < tokenizedEnvelopeList.size(); i++) {
        List<Envelope> envelopeList = tokenizedEnvelopeList.get(i);
        commitToTable(
            tableIdentifier,
            tableBranch,
            envelopeList,
            vtts);
      }
  }
}

  private void commitToTable(
      TableIdentifier tableIdentifier,
      Pair<Table, Optional<String>> tableBranch,
      List<Envelope> envelopeList,
      OffsetDateTime vtts) {
    Table table;
    if (tableBranch == null) {
      return;
    } else {
      table = tableBranch.first();
    }
    Optional<String> branch = tableBranch.second();

    SnapshotCommitMetadata commitMetadata = lastSnapshotCommitMetadata(table, branch.orElse(null));
    Map<Integer, Long> lastControlTopicOffsets = commitMetadata.controlTopicOffsets;
    Map<String, Map<Integer, CommitState.OffsetRange>> lastDataOffsets = commitMetadata.dataOffsets;

    // Filter DATA_WRITTEN by control topic offset
    List<Envelope> filteredEnvelopeList = envelopeList.stream()
      .filter(envelope -> {
        Long minOffset = lastControlTopicOffsets.get(envelope.partition());
        return minOffset == null || envelope.offset() > minOffset;
      })
      .collect(toList());

    // Filter DATA_OFFSETS by control topic offset (same logic as DATA_WRITTEN)
    List<Envelope> filteredDataOffsets = commitState.dataOffsetsBuffer().stream()
      .filter(envelope -> {
        Long minOffset = lastControlTopicOffsets.get(envelope.partition());
        return minOffset == null || envelope.offset() > minOffset;
      })
      .collect(toList());

    Map<Integer, Long> currentOffsets = filteredEnvelopeList.stream()
    .collect(groupingBy(
        Envelope::partition,
        Collectors.mapping(
            Envelope::offset,
            Collectors.maxBy(Long::compareTo))
    )).entrySet().stream()
    .collect(toMap(
        Map.Entry::getKey,
        e -> e.getValue().get()
    ));

    // Merge last committed offsets with current offsets, taking max for conflicts
    Map<Integer, Long> mergedOffsets =
        java.util.stream.Stream.of(lastControlTopicOffsets, currentOffsets)
            .flatMap(map -> map.entrySet().stream())
            .collect(toMap(
                Map.Entry::getKey,
                Map.Entry::getValue,
                Long::max));

    String offsetsJson;
    try {
      offsetsJson = MAPPER.writeValueAsString(mergedOffsets);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }

    // Aggregate DATA_OFFSETS for this table
    Map<String, Map<Integer, CommitState.OffsetRange>> currentDataOffsets =
        aggregateDataOffsets(filteredDataOffsets, tableIdentifier);
    Map<String, Map<Integer, CommitState.OffsetRange>> mergedDataOffsets =
        mergeDataOffsets(lastDataOffsets, currentDataOffsets);
    String dataOffsetsJson = serializeDataOffsets(mergedDataOffsets);

    List<DataFile> dataFiles =
        Deduplicated.dataFiles(commitState.currentCommitId(), tableIdentifier, filteredEnvelopeList)
            .stream()
            .filter(dataFile -> dataFile.recordCount() > 0)
            .collect(toList());

    List<DeleteFile> deleteFiles =
        Deduplicated.deleteFiles(
                commitState.currentCommitId(), tableIdentifier, filteredEnvelopeList)
            .stream()
            .filter(deleteFile -> deleteFile.recordCount() > 0)
            .collect(toList());

    if (terminated) {
      throw new ConnectException("Coordinator is terminated, commit aborted");
    }

    if (dataFiles.isEmpty() && deleteFiles.isEmpty()) {
      LOG.info("Nothing to commit to table {}, skipping", tableIdentifier);
    } else {
      if (deleteFiles.isEmpty()) {
        Transaction transaction = table.newTransaction();

        Map<Integer, List<DataFile>> filesBySpec =
            dataFiles.stream()
                .collect(Collectors.groupingBy(DataFile::specId, Collectors.toList()));

        List<List<DataFile>> list = Lists.newArrayList(filesBySpec.values());
        int lastIdx = list.size() - 1;
        for (int i = 0; i <= lastIdx; i++) {
          AppendFiles appendOp = transaction.newAppend();
          branch.ifPresent(appendOp::toBranch);

          list.get(i).forEach(appendOp::appendFile);
          appendOp.set(COMMIT_ID_SNAPSHOT_PROP, commitState.currentCommitId().toString());
          if (i == lastIdx) {
            appendOp.set(controlTopicOffsetsProp, offsetsJson);
            if (dataOffsetsJson != null) {
              appendOp.set(DATA_OFFSETS_SNAPSHOT_PROP, dataOffsetsJson);
            }
            if (vtts != null) {
              appendOp.set(VTTS_SNAPSHOT_PROP, Long.toString(vtts.toInstant().toEpochMilli()));
            }
          }

          appendOp.commit();
        }

        transaction.commitTransaction();
      } else {
        RowDelta deltaOp = table.newRowDelta();
        branch.ifPresent(deltaOp::toBranch);
        deltaOp.set(controlTopicOffsetsProp, offsetsJson);
        deltaOp.set(COMMIT_ID_SNAPSHOT_PROP, commitState.currentCommitId().toString());
        if (dataOffsetsJson != null) {
          deltaOp.set(DATA_OFFSETS_SNAPSHOT_PROP, dataOffsetsJson);
        }
        if (vtts != null) {
          deltaOp.set(VTTS_SNAPSHOT_PROP, Long.toString(vtts.toInstant().toEpochMilli()));
        }
        dataFiles.forEach(deltaOp::addRows);
        deleteFiles.forEach(deltaOp::addDeletes);
        deltaOp.commit();
      }

      Long snapshotId = latestSnapshot(table, branch.orElse(null)).snapshotId();
      Event event =
          new Event(
              config.controlGroupId(),
              new CommitToTable(
                  commitState.currentCommitId(),
                  TableReference.of(config.catalogName(), tableIdentifier),
                  snapshotId,
                  vtts));
      send(event);

      LOG.info(
          "Commit complete to table {}, snapshot {}, commit ID {}, vtts {}",
          tableIdentifier,
          snapshotId,
          commitState.currentCommitId(),
          vtts);
    }
  }

  private Snapshot latestSnapshot(Table table, String branch) {
    if (branch == null) {
      return table.currentSnapshot();
    }
    return table.snapshot(branch);
  }

  /**
   * Aggregates DATA_OFFSETS for a specific table. Merges non-overlapping ranges,
   * throws on intersecting ranges.
   */
  Map<String, Map<Integer, CommitState.OffsetRange>> aggregateDataOffsets(
      List<Envelope> filteredDataOffsets, TableIdentifier tableIdentifier) {
    Map<String, Map<Integer, CommitState.OffsetRange>> result = Maps.newHashMap();

    for (Envelope envelope : filteredDataOffsets) {
      DataOffsetsPayload payload = (DataOffsetsPayload) envelope.localEvent().payload();
      if (!payload.tableName().toIdentifier().equals(tableIdentifier)) {
        continue;
      }

      for (io.tabular.iceberg.connect.events.TopicPartitionOffset tpo : payload.dataOffsets()) {
        String topic = tpo.topic();
        Integer partition = tpo.partition();
        CommitState.OffsetRange incoming =
            new CommitState.OffsetRange(
                tpo.startOffset() != null ? tpo.startOffset() : tpo.offset(), tpo.offset());

        Map<Integer, CommitState.OffsetRange> partitionMap =
            result.computeIfAbsent(topic, k -> Maps.newHashMap());
        CommitState.OffsetRange existing = partitionMap.get(partition);

        if (existing != null) {
          // Check for intersection: [a, b] and [c, d] intersect if !(b < c || d < a)
          boolean intersects = !(existing.end() < incoming.start() || incoming.end() < existing.start());
          if (intersects) {
            throw new IllegalStateException(
                String.format(
                    "Intersecting offset ranges for topic=%s partition=%d: existing=[%d,%d] incoming=[%d,%d]",
                    topic,
                    partition,
                    existing.start(),
                    existing.end(),
                    incoming.start(),
                    incoming.end()));
          }
          // Merge non-overlapping ranges
          partitionMap.put(
              partition,
              new CommitState.OffsetRange(
                  Math.min(existing.start(), incoming.start()),
                  Math.max(existing.end(), incoming.end())));
        } else {
          partitionMap.put(partition, incoming);
        }
      }
    }

    return result;
  }

  /**
   * Merges source data offsets from previous snapshot with current offsets. Takes all entries from
   * current and adds entries from previous that are not in current (propagation).
   */
  private Map<String, Map<Integer, CommitState.OffsetRange>> mergeDataOffsets(
      Map<String, Map<Integer, CommitState.OffsetRange>> previous,
      Map<String, Map<Integer, CommitState.OffsetRange>> current) {
    Map<String, Map<Integer, CommitState.OffsetRange>> result = Maps.newHashMap();

    // Add all current entries
    for (Map.Entry<String, Map<Integer, CommitState.OffsetRange>> entry : current.entrySet()) {
      result.put(entry.getKey(), Maps.newHashMap(entry.getValue()));
    }

    // Propagate missing (topic, partition) from previous
    for (Map.Entry<String, Map<Integer, CommitState.OffsetRange>> entry : previous.entrySet()) {
      String topic = entry.getKey();
      Map<Integer, CommitState.OffsetRange> partitionMap =
          result.computeIfAbsent(topic, k -> Maps.newHashMap());
      for (Map.Entry<Integer, CommitState.OffsetRange> partitionEntry :
          entry.getValue().entrySet()) {
        // Only add if not present in current
        partitionMap.putIfAbsent(partitionEntry.getKey(), partitionEntry.getValue());
      }
    }

    return result;
  }

  /**
   * Serializes data offsets to JSON format: {"topic": {"partition": {"start": X, "end": Y}, ...}}
   */
  private String serializeDataOffsets(
      Map<String, Map<Integer, CommitState.OffsetRange>> dataOffsets) {
    if (dataOffsets.isEmpty()) {
      return null;
    }
    Map<String, Map<Integer, Map<String, Long>>> serializable = Maps.newHashMap();
    for (Map.Entry<String, Map<Integer, CommitState.OffsetRange>> topicEntry :
        dataOffsets.entrySet()) {
      Map<Integer, Map<String, Long>> partitionMap = Maps.newHashMap();
      for (Map.Entry<Integer, CommitState.OffsetRange> partitionEntry :
          topicEntry.getValue().entrySet()) {
        CommitState.OffsetRange range = partitionEntry.getValue();
        Map<String, Long> rangeMap = Maps.newHashMap();
        rangeMap.put("start", range.start());
        rangeMap.put("end", range.end());
        partitionMap.put(partitionEntry.getKey(), rangeMap);
      }
      serializable.put(topicEntry.getKey(), partitionMap);
    }
    try {
      return MAPPER.writeValueAsString(serializable);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static class SnapshotCommitMetadata {
    final UUID commitId;
    final Map<Integer, Long> controlTopicOffsets;
    final Map<String, Map<Integer, CommitState.OffsetRange>> dataOffsets;

    SnapshotCommitMetadata(
        UUID commitId,
        Map<Integer, Long> controlTopicOffsets,
        Map<String, Map<Integer, CommitState.OffsetRange>> dataOffsets) {
      this.commitId = commitId;
      this.controlTopicOffsets = controlTopicOffsets;
      this.dataOffsets = dataOffsets;
    }
  }

  private SnapshotCommitMetadata lastSnapshotCommitMetadata(Table table, String branch) {
    Snapshot snapshot = latestSnapshot(table, branch);
    while (snapshot != null) {
      Map<String, String> summary = snapshot.summary();
      if (hasCommitInfo(summary)) {
        return parseSnapshotCommitMetadata(summary);
      }
      Long parentSnapshotId = snapshot.parentId();
      snapshot = parentSnapshotId != null ? table.snapshot(parentSnapshotId) : null;
    }
    return new SnapshotCommitMetadata(null, ImmutableMap.of(), Maps.newHashMap());
  }

  private boolean hasCommitInfo(Map<String, String> summary) {
    return summary.get(COMMIT_ID_SNAPSHOT_PROP) != null
        || summary.get(controlTopicOffsetsProp) != null
        || summary.get(DATA_OFFSETS_SNAPSHOT_PROP) != null;
  }

  private SnapshotCommitMetadata parseSnapshotCommitMetadata(Map<String, String> summary) {
    String commitIdValue = summary.get(COMMIT_ID_SNAPSHOT_PROP);
    UUID commitId = commitIdValue != null ? UUID.fromString(commitIdValue) : null;
    Map<Integer, Long> controlTopicOffsets = parseControlTopicOffsets(summary.get(controlTopicOffsetsProp));
    Map<String, Map<Integer, CommitState.OffsetRange>> dataOffsets =
        parseDataOffsets(summary.get(DATA_OFFSETS_SNAPSHOT_PROP));
    return new SnapshotCommitMetadata(commitId, controlTopicOffsets, dataOffsets);
  }

  private Map<Integer, Long> parseControlTopicOffsets(String json) {
    if (json == null) {
      return ImmutableMap.of();
    }
    try {
      TypeReference<Map<Integer, Long>> typeRef = new TypeReference<Map<Integer, Long>>() {};
      return MAPPER.readValue(json, typeRef);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private Map<String, Map<Integer, CommitState.OffsetRange>> parseDataOffsets(String json) {
    if (json == null) {
      return Maps.newHashMap();
    }
    try {
      TypeReference<Map<String, Map<Integer, Map<String, Long>>>> typeRef =
          new TypeReference<Map<String, Map<Integer, Map<String, Long>>>>() {};
      Map<String, Map<Integer, Map<String, Long>>> parsed = MAPPER.readValue(json, typeRef);

      Map<String, Map<Integer, CommitState.OffsetRange>> result = Maps.newHashMap();
      for (Map.Entry<String, Map<Integer, Map<String, Long>>> topicEntry : parsed.entrySet()) {
        Map<Integer, CommitState.OffsetRange> partitionMap = Maps.newHashMap();
        for (Map.Entry<Integer, Map<String, Long>> partitionEntry :
            topicEntry.getValue().entrySet()) {
          Map<String, Long> range = partitionEntry.getValue();
          partitionMap.put(
              partitionEntry.getKey(),
              new CommitState.OffsetRange(range.get("start"), range.get("end")));
        }
        result.put(topicEntry.getKey(), partitionMap);
      }
      return result;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public void close() throws IOException {
    this.terminated = true;

    exec.shutdownNow();

    // wait for coordinator termination, else cause the sink task to fail
    try {
      if (!exec.awaitTermination(1, TimeUnit.MINUTES)) {
        throw new ConnectException("Timed out waiting for coordinator shutdown");
      }
    } catch (InterruptedException e) {
      throw new ConnectException("Interrupted while waiting for coordinator shutdown", e);
    }

    stop();
  }

  @Override
  public void stop() {
    if (watermarkProducer != null) {
      try {
        watermarkProducer.flush();
      } catch (Exception e) {
        LOG.warn("Error flushing watermark producer", e);
      }
      try {
        watermarkProducer.close(Duration.ofSeconds(30));
      } catch (Exception e) {
        LOG.warn("Error closing watermark producer", e);
      }
    }
    super.stop();
  }
}
