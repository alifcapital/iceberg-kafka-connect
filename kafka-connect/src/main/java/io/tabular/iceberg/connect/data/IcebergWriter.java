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
package io.tabular.iceberg.connect.data;

import io.tabular.iceberg.connect.IcebergSinkConfig;
import io.tabular.iceberg.connect.data.SchemaUpdate.Consumer;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IcebergWriter implements RecordWriter {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergWriter.class);
  private static final String[] BEFORE_IMAGE_PATH = {"_before_image"};

  private final Table table;
  private final String tableName;
  private final IcebergSinkConfig config;
  private final boolean hasReliablePk;
  private final List<WriterResult> writerResults;
  private final Map<TopicPartition, Offset> dataOffsets;

  // Bounded cache scoped to the current table schema and this writer's immutable config.
  private final Map<Schema, Boolean> validatedSchemas = new LinkedHashMap<>();
  // The instances already found in validatedSchemas. A Schema is hashed and compared field by
  // field, which is wasted on every record of a schema the converter hands out as one instance.
  // Cleared together with validatedSchemas.
  private final Map<Schema, Boolean> validatedSchemaInstances = new IdentityHashMap<>();
  private Schema validatedKeySchema;
  private int validatedTableSchemaId = -1;
  private final Map<List<Object>, CompactKeyMap> pendingKeys = Maps.newHashMap();
  private RecordConverter recordConverter;
  private TaskWriter<Record> writer;
  // the CDC field split into its path, once for as long as the CDC field stays the same
  private String splitCdcField;
  private String[] splitCdcFieldPath;

  public IcebergWriter(
      Table table, String tableName, IcebergSinkConfig config, boolean hasReliablePk) {
    this.table = table;
    this.tableName = tableName;
    this.config = config;
    this.hasReliablePk = hasReliablePk;
    this.writerResults = Lists.newArrayList();
    this.dataOffsets = Maps.newHashMap();
    initNewWriter();
  }

  private void initNewWriter() {
    validatedSchemas.clear();
    validatedSchemaInstances.clear();
    validatedKeySchema = null;
    validatedTableSchemaId = table.schema().schemaId();
    this.writer = Utilities.createTableWriter(table, tableName, config);
    if (writer instanceof CompactDeltaTaskWriter) {
      ((CompactDeltaTaskWriter) writer).usePendingKeys(pendingKeys);
    }
    // In append mode without reliable PK, write _before_image as _cdc_before_image to Iceberg
    boolean writeBeforeImageToIceberg = config.tablesCdcField() == null && !hasReliablePk;
    this.recordConverter = new RecordConverter(table, config, writeBeforeImageToIceberg);
  }

  @Override
  public void write(SinkRecord record) {
    // TODO: config to handle tombstones instead of always ignoring?
    if (record.value() == null) {
      return;
    }
    String cdcField = config.tablesCdcField();
    Record row;
    Operation op = null;
    Record before = null;
    try {
      row = convertToRow(record);
      if (cdcField != null) {
        op = extractCdcOperation(record.value(), cdcField);
        before = extractBeforeImage(record.value());
      }
    } catch (WriterStateException e) {
      throw new DataException(errorMessage(record), e.getCause());
    } catch (Exception e) {
      throw new RecordConversionException(errorMessage(record), e);
    }
    try {
      if (cdcField == null) {
        writer.write(row);
      } else {
        writer.write(new RecordWrapper(row, op, before));
      }
      trackDataOffset(record);
    } catch (Exception e) {
      throw new DataException(errorMessage(record), e);
    }
  }

  private static String errorMessage(SinkRecord record) {
    return String.format(
        "An error occurred converting record, topic: %s, partition, %d, offset: %d",
        record.topic(), record.kafkaPartition(), record.kafkaOffset());
  }

  /** Completing or reopening the task writer failed, so its state is unknown. */
  private static class WriterStateException extends RuntimeException {
    WriterStateException(Throwable cause) {
      super(cause);
    }
  }

  // Completes the current file, runs the schema change and opens a writer for the resulting table
  // schema. A failed schema change still leaves a consistent writer behind.
  private void reopenWriter(Runnable schemaChange) {
    try {
      flush();
    } catch (RuntimeException e) {
      throw new WriterStateException(e);
    }
    RuntimeException schemaChangeFailure = null;
    try {
      schemaChange.run();
    } catch (RuntimeException e) {
      schemaChangeFailure = e;
    }
    try {
      initNewWriter();
    } catch (RuntimeException e) {
      if (schemaChangeFailure != null) {
        e.addSuppressed(schemaChangeFailure);
      }
      throw new WriterStateException(e);
    }
    if (schemaChangeFailure != null) {
      throw schemaChangeFailure;
    }
  }

  private void trackDataOffset(SinkRecord record) {
    TopicPartition tp = new TopicPartition(record.topic(), record.kafkaPartition());
    Offset existing = dataOffsets.get(tp);
    if (existing == null) {
      // First record: start=current, end=current → range [100, 100]
      dataOffsets.put(tp, new Offset(record.kafkaOffset(), record.timestamp(), record.kafkaOffset()));
    } else {
      // Subsequent: keep start, update end → range [100, 105]
      dataOffsets.put(tp, new Offset(record.kafkaOffset(), record.timestamp(), existing.startOffset()));
    }
  }

  private Record convertToRow(SinkRecord record) {
    // Table metadata can also be refreshed by another owner of the Table instance.
    if (validatedTableSchemaId != table.schema().schemaId()) {
      reopenWriter(() -> {});
    }
    validateKey(record.keySchema());
    if (record.valueSchema() != null) {
      if (!isValidated(record.valueSchema())) {
        boolean cdcWithoutKey =
            !config.tableConfig(tableName).appendOnly()
                && (config.tablesCdcField() != null || config.upsertModeEnabled())
                && table.schema().identifierFieldIds().isEmpty();
        SchemaUpdate.Consumer planned =
            recordConverter.planSchema(record.valueSchema(), cdcWithoutKey);
        if (!planned.empty()) {
          if (!config.evolveSchemaEnabled()) {
            throw new DataException("Schema evolution is disabled for table " + tableName);
          }
          reopenWriter(() -> SchemaUtils.applySchemaUpdates(table, planned));
          // Recheck refreshed metadata, including changes committed by other writers.
          if (!recordConverter.planSchema(record.valueSchema(), cdcWithoutKey).empty()) {
            throw new DataException(
                "Table schema changed concurrently; retry record for " + tableName);
          }
        }
        if (validatedSchemas.size() >= 64) {
          validatedSchemas.clear();
          validatedSchemaInstances.clear();
        }
        validatedSchemas.put(record.valueSchema(), Boolean.TRUE);
        rememberValidated(record.valueSchema());
      }
      return recordConverter.convert(record.value());
    }
    if (!config.evolveSchemaEnabled()) {
      return recordConverter.convert(record.value());
    }

    SchemaUpdate.Consumer updates = new Consumer();
    Record row = recordConverter.convert(record.value(), updates);

    if (!updates.empty()) {
      // complete the current file, apply the schema updates (this will refresh the table)
      // and initialize a new writer with the new schema
      reopenWriter(() -> SchemaUtils.applySchemaUpdates(table, updates));
      LOG.info("Table schema evolution on table {} caused by record at topic: {}, partition: {}, offset: {}", table.name(), record.topic(), record.kafkaPartition(), record.kafkaOffset());
      // convert the row again, this time using the new table schema
      row = recordConverter.convert(record.value(), null);
    }

    return row;
  }

  private boolean isValidated(Schema schema) {
    // a ConnectSchema never changes, a SchemaBuilder might
    if (schema instanceof ConnectSchema) {
      if (validatedSchemaInstances.containsKey(schema)) {
        return true;
      }
      if (validatedSchemas.containsKey(schema)) {
        rememberValidated(schema);
        return true;
      }
      return false;
    }
    return validatedSchemas.containsKey(schema);
  }

  private void rememberValidated(Schema schema) {
    if (schema instanceof ConnectSchema) {
      if (validatedSchemaInstances.size() >= 64) {
        validatedSchemaInstances.clear();
      }
      validatedSchemaInstances.put(schema, Boolean.TRUE);
    }
  }

  private void validateKey(Schema keySchema) {
    if (table.schema().identifierFieldIds().isEmpty()
        || !config.tableConfig(tableName).idColumns().isEmpty()) {
      return;
    }
    if (keySchema == null || keySchema.type() != Schema.Type.STRUCT) {
      throw new DataException("Missing structured Kafka key for identifier fields of " + tableName);
    }
    if (keySchema.equals(validatedKeySchema)) {
      return;
    }
    Set<String> names = keySchema.fields().stream().map(f -> f.name()).collect(Collectors.toSet());
    if (!names.equals(table.schema().identifierFieldNames())) {
      throw new DataException(
          "Kafka key fields changed for "
              + tableName
              + ": expected "
              + table.schema().identifierFieldNames()
              + ", received "
              + names);
    }
    for (Field field : keySchema.fields()) {
      if (field.schema().isOptional()) {
        throw new DataException("Nullable Kafka key field: " + field.name());
      }
      Type incoming = SchemaUtils.toIcebergType(field.schema(), config);
      Type current = table.schema().findType(field.name());
      if (!compatibleKeyTypes(incoming, current)) {
        throw new DataException(
            "Incompatible Kafka key type at "
                + field.name()
                + ": incoming "
                + incoming
                + ", Iceberg "
                + current);
      }
    }
    validatedKeySchema = keySchema;
  }

  private static boolean compatibleKeyTypes(Type incoming, Type current) {
    return incoming.isPrimitiveType() && current.isPrimitiveType()
        && (TypeUtil.isPromotionAllowed(incoming, current.asPrimitiveType())
            || TypeUtil.isPromotionAllowed(current, incoming.asPrimitiveType()));
  }

  private Operation extractCdcOperation(Object recordValue, String field) {
    if (!field.equals(splitCdcField)) {
      splitCdcFieldPath = Utilities.fieldPath(field);
      splitCdcField = field;
    }
    Object opValue = Utilities.extractFromRecordValue(recordValue, splitCdcFieldPath);

    if (opValue == null) {
      return Operation.INSERT;
    }

    String opStr = opValue.toString().trim().toUpperCase();
    if (opStr.isEmpty()) {
      return Operation.INSERT;
    }

    // TODO: define value mapping in config?

    switch (opStr.charAt(0)) {
      case 'U':
        return Operation.UPDATE;
      case 'D':
        return Operation.DELETE;
      default:
        return Operation.INSERT;
    }
  }

  private Record extractBeforeImage(Object recordValue) {
    // Check if field exists before extracting (only present for UPDATE operations)
    if (recordValue instanceof Struct) {
      Struct struct = (Struct) recordValue;
      if (struct.schema().field("_before_image") == null) {
        return null;
      }
    } else if (recordValue instanceof Map) {
      @SuppressWarnings("unchecked")
      Map<String, Object> map = (Map<String, Object>) recordValue;
      if (!map.containsKey("_before_image")) {
        return null;
      }
    }

    Object beforeImage = Utilities.extractFromRecordValue(recordValue, BEFORE_IMAGE_PATH);
    if (beforeImage == null) {
      return null;
    }
    return recordConverter.convert(beforeImage);
  }

  private void flush() {
    WriteResult writeResult;
    try {
      writeResult = writer.complete();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }

    writerResults.add(
        new WriterResult(
            TableIdentifier.parse(tableName),
            Arrays.asList(writeResult.dataFiles()),
            Arrays.asList(writeResult.deleteFiles()),
            table.spec().partitionType()));
  }

  @Override
  public synchronized WriteComplete complete() {
    flush();

    List<WriterResult> normalized =
        writerResults.stream()
            .map(
                files ->
                    new WriterResult(
                        files.tableIdentifier(),
                        files.dataFiles().stream()
                            .map(file -> PendingFileNormalizer.normalize(table, file))
                            .collect(Collectors.toList()),
                        files.deleteFiles().stream()
                            .map(file -> PendingFileNormalizer.normalize(table, file))
                            .collect(Collectors.toList()),
                        table.spec().partitionType()))
            .collect(Collectors.toList());
    WriteComplete result =
        new WriteComplete(
            TableIdentifier.parse(tableName), normalized, Maps.newHashMap(dataOffsets));
    writerResults.clear();
    dataOffsets.clear();
    pendingKeys.values().forEach(CompactKeyMap::clear);
    pendingKeys.clear();

    return result;
  }

  @Override
  public void close() {
    try {
      writer.close();
      pendingKeys.clear();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
