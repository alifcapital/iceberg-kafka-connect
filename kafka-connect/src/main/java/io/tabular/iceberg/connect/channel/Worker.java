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

import static java.util.stream.Collectors.toList;

import io.tabular.iceberg.connect.IcebergSinkConfig;
import io.tabular.iceberg.connect.data.IcebergWriterFactory;
import io.tabular.iceberg.connect.data.Offset;
import io.tabular.iceberg.connect.data.RecordConversionException;
import io.tabular.iceberg.connect.data.RecordWriter;
import io.tabular.iceberg.connect.data.Utilities;
import io.tabular.iceberg.connect.data.WriteComplete;
import io.tabular.iceberg.connect.data.WriterResult;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO: rename to WriterImpl later, minimize changes for clearer commit history for now
class Worker implements Writer, AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(Worker.class);
  private final IcebergSinkConfig config;
  private final IcebergWriterFactory writerFactory;
  private final Map<String, RecordWriter> writers;
  private final Map<TopicPartition, Offset> dataOffsets;
  private RecordConversionException deferredFailure;
  private boolean madeProgress;
  private int tablesWrittenForRecord;
  // the route field split into its path, once for as long as the route field stays the same
  private String splitRouteField;
  private String[] splitRouteFieldPath;

  Worker(IcebergSinkConfig config, Catalog catalog) {
    this(config, new IcebergWriterFactory(catalog, config));
  }

  @VisibleForTesting
  Worker(IcebergSinkConfig config, IcebergWriterFactory writerFactory) {
    this.config = config;
    this.writerFactory = writerFactory;
    this.writers = Maps.newHashMap();
    this.dataOffsets = Maps.newHashMap();
  }

  @Override
  public Committable committable() {
    List<WriteComplete> writeCompletes =
        writers.values().stream().map(RecordWriter::complete).collect(toList());

    List<WriterResult> writeResults =
        writeCompletes.stream()
            .flatMap(wc -> wc.writerResults().stream())
            .collect(toList());

    // Aggregate offsets per table instead of merging into flat map
    Map<TableIdentifier, Map<TopicPartition, Offset>> dataOffsetsByTable = Maps.newHashMap();
    for (WriteComplete wc : writeCompletes) {
      if (wc.tableIdentifier() != null && !wc.dataOffsets().isEmpty()) {
        dataOffsetsByTable
            .computeIfAbsent(wc.tableIdentifier(), k -> Maps.newHashMap())
            .putAll(wc.dataOffsets());
      }
    }

    Map<TopicPartition, Offset> offsets = Maps.newHashMap(dataOffsets);

    writers.clear();
    dataOffsets.clear();
    madeProgress = false;

    return new Committable(offsets, writeResults, dataOffsetsByTable);
  }

  @Override
  public void close() throws IOException {
    writers.values().forEach(RecordWriter::close);
    writers.clear();
    dataOffsets.clear();
  }

  @Override
  public void write(Collection<SinkRecord> sinkRecords) {
    if (sinkRecords == null) {
      return;
    }
    for (SinkRecord record : sinkRecords) {
      if (deferredFailure != null) {
        // the failed record and everything after it is consumed again after the restart
        return;
      }
      save(record);
    }
  }

  @Override
  public RuntimeException deferredFailure() {
    return deferredFailure;
  }

  @Override
  public boolean madeProgress() {
    return madeProgress;
  }

  private void save(SinkRecord record) {
    TopicPartition topicPartition = new TopicPartition(record.topic(), record.kafkaPartition());
    tablesWrittenForRecord = 0;
    try {
      if (config.dynamicTablesEnabled()) {
        routeRecordDynamically(record);
      } else {
        routeRecordStatically(record);
      }
    } catch (RecordConversionException e) {
      if (tablesWrittenForRecord > 0) {
        // the record is already in another table, consuming it again would duplicate it there
        throw e;
      }
      deferredFailure = e;
      // the failed record is the next one to consume, so it is retried after the restart
      dataOffsets.put(topicPartition, new Offset(record.kafkaOffset(), record.timestamp()));
      return;
    }

    // the consumer stores the offsets that corresponds to the next record to consume,
    // so increment the record offset by one
    dataOffsets.put(topicPartition, new Offset(record.kafkaOffset() + 1, record.timestamp()));
    madeProgress = true;
  }

  private void routeRecordStatically(SinkRecord record) {
    String routeField = config.tablesRouteField();

    if (routeField == null) {
      // route to all tables
      config
          .tables()
          .forEach(
              tableName -> {
                writeToTable(tableName, record, false);
              });

    } else {
      String routeValue = extractRouteValue(record.value(), routeField);
      if (routeValue != null) {
        config
            .tables()
            .forEach(
                tableName ->
                    config
                        .tableConfig(tableName)
                        .routeRegex()
                        .ifPresent(
                            regex -> {
                              if (regex.matcher(routeValue).matches()) {
                                writeToTable(tableName, record, false);
                              }
                            }));
      }
    }
  }

  private void routeRecordDynamically(SinkRecord record) {
    String routeField = config.tablesRouteField();
    if (routeField == null) {
      // formatted only when thrown, not for every record
      throw new NullPointerException(
          String.format(
              "Route field cannot be null with dynamic routing at topic: %s, partition: %d, offset: %d",
              record.topic(), record.kafkaPartition(), record.kafkaOffset()));
    }

    String routeValue = extractRouteValue(record.value(), routeField);
    if (routeValue != null) {
      String tableName = routeValue.toLowerCase();
      writeToTable(tableName, record, true);
    }
  }

  private String extractRouteValue(Object recordValue, String routeField) {
    if (recordValue == null) {
      return null;
    }
    Object routeValue = Utilities.extractFromRecordValue(recordValue, routeFieldPath(routeField));
    return routeValue == null ? null : routeValue.toString();
  }

  private String[] routeFieldPath(String field) {
    if (!field.equals(splitRouteField)) {
      splitRouteFieldPath = Utilities.fieldPath(field);
      splitRouteField = field;
    }
    return splitRouteFieldPath;
  }

  private void writeToTable(String tableName, SinkRecord record, boolean ignoreMissingTable) {
    writerForTable(tableName, record, ignoreMissingTable).write(record);
    tablesWrittenForRecord++;
  }

  private RecordWriter writerForTable(
      String tableName, SinkRecord sample, boolean ignoreMissingTable) {
    return writers.computeIfAbsent(
        tableName, notUsed -> writerFactory.createWriter(tableName, sample, ignoreMissingTable));
  }
}
