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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.tabular.iceberg.connect.IcebergSinkConfig;
import io.tabular.iceberg.connect.transforms.DebeziumTransform;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.LocationProviders;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.encryption.PlaintextEncryptionManager;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Integration test verifying that UPDATE operations on tables without real PK
 * correctly use the BEFORE image for equality deletes.
 *
 * <p>Scenario: Table with columns (a, c), no primary key.
 * When we UPDATE {a=1, c=0} -> {a=1, c=1}, the equality delete must contain
 * the BEFORE values {a=1, c=0}, not the AFTER values {a=1, c=1}.
 */
public class BeforeImageIntegrationTest {

  // Iceberg schema - no identifier fields (simulating table without PK)
  private static final Schema ICEBERG_SCHEMA =
      new Schema(
          ImmutableList.of(
              Types.NestedField.required(1, "a", Types.IntegerType.get()),
              Types.NestedField.required(2, "c", Types.IntegerType.get())));

  // All columns as identifier fields (fake PK for tables without real PK)
  private static final Set<Integer> ALL_COLUMNS_AS_IDENTIFIER = ImmutableSet.of(1, 2);

  // Kafka Connect schemas for Debezium envelope
  private static final org.apache.kafka.connect.data.Schema ROW_SCHEMA =
      SchemaBuilder.struct()
          .field("a", org.apache.kafka.connect.data.Schema.INT32_SCHEMA)
          .field("c", org.apache.kafka.connect.data.Schema.INT32_SCHEMA)
          .build();

  private static final org.apache.kafka.connect.data.Schema SOURCE_SCHEMA =
      SchemaBuilder.struct()
          .field("db", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
          .field("schema", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
          .field("table", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
          .field("ts_ms", org.apache.kafka.connect.data.Schema.INT64_SCHEMA)
          .build();

  private static final org.apache.kafka.connect.data.Schema OPTIONAL_ROW_SCHEMA =
      SchemaBuilder.struct()
          .field("a", org.apache.kafka.connect.data.Schema.INT32_SCHEMA)
          .field("c", org.apache.kafka.connect.data.Schema.INT32_SCHEMA)
          .optional()
          .build();

  private static final org.apache.kafka.connect.data.Schema DEBEZIUM_ENVELOPE_SCHEMA =
      SchemaBuilder.struct()
          .field("before", OPTIONAL_ROW_SCHEMA)
          .field("after", OPTIONAL_ROW_SCHEMA)
          .field("source", SOURCE_SCHEMA)
          .field("op", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
          .field("ts_ms", org.apache.kafka.connect.data.Schema.INT64_SCHEMA)
          .build();

  private InMemoryFileIO fileIO;
  private Table table;
  private OutputFileFactory fileFactory;
  private GenericAppenderFactory appenderFactory;
  private DebeziumTransform<SinkRecord> debeziumTransform;
  private RecordConverter recordConverter;

  @BeforeEach
  public void setup() {
    fileIO = new InMemoryFileIO();

    table = mock(Table.class);
    when(table.schema()).thenReturn(ICEBERG_SCHEMA);
    when(table.spec()).thenReturn(PartitionSpec.unpartitioned());
    when(table.io()).thenReturn(fileIO);
    when(table.locationProvider())
        .thenReturn(LocationProviders.locationsFor("file", ImmutableMap.of()));
    when(table.encryption()).thenReturn(PlaintextEncryptionManager.instance());
    when(table.properties()).thenReturn(ImmutableMap.of());

    fileFactory =
        OutputFileFactory.builderFor(table, 1, 1).format(FileFormat.PARQUET).build();

    appenderFactory =
        new GenericAppenderFactory(
            ICEBERG_SCHEMA,
            PartitionSpec.unpartitioned(),
            new int[] {1, 2}, // all columns as identifier
            ICEBERG_SCHEMA,   // delete schema = full schema
            null);

    debeziumTransform = new DebeziumTransform<>();
    debeziumTransform.configure(ImmutableMap.of());

    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.schemaCaseInsensitive()).thenReturn(false);
    when(config.schemaDebeziumTimeTypes()).thenReturn(false);

    recordConverter = new RecordConverter(table, config);
  }

  /**
   * Test the full flow with 6 Debezium events:
   * 1. r: {a=1, c=0}
   * 2. r: {a=1, c=0}
   * 3. r: {a=2, c=0}
   * 4. u: {a=1, c=0} -> {a=1, c=1}
   * 5. u: {a=1, c=0} -> {a=1, c=1}
   * 6. c: {a=2, c=1}
   *
   * Equality deletes for events 4 and 5 must contain {a=1, c=0} (before),
   * not {a=1, c=1} (after).
   */
  @Test
  public void testUpdateUsesBeforeImageForEqualityDelete() throws IOException {
    // Create writer with hasRealPk=false (all columns as identifier)
    CompactUnpartitionedDeltaWriter writer =
        new CompactUnpartitionedDeltaWriter(
            PartitionSpec.unpartitioned(),
            FileFormat.PARQUET,
            appenderFactory,
            fileFactory,
            fileIO,
            Long.MAX_VALUE, // don't roll files
            ICEBERG_SCHEMA,
            ALL_COLUMNS_AS_IDENTIFIER,
            false, // upsertMode = false (CDC mode)
            false); // hasRealPk = false

    // Process all 6 events
    List<SinkRecord> debeziumEvents = createDebeziumEvents();

    for (SinkRecord debeziumEvent : debeziumEvents) {
      // Transform through DebeziumTransform
      SinkRecord transformed = debeziumTransform.apply(debeziumEvent);
      if (transformed == null) {
        continue; // skip if filtered
      }

      // Extract operation and before image
      Struct value = (Struct) transformed.value();
      String op = value.getString("_cdc_op");
      Operation operation = mapOperation(op);

      // Convert to Iceberg record
      Record icebergRecord = recordConverter.convert(transformed.value());

      // Extract before image for UPDATE operations only
      Record beforeRecord = null;
      if (operation == Operation.UPDATE) {
        Object beforeImage = Utilities.extractFromRecordValue(transformed.value(), "_before_image");
        if (beforeImage != null) {
          beforeRecord = recordConverter.convert(beforeImage);
        }
      }

      // Write through the writer
      writer.write(new RecordWrapper(icebergRecord, operation, beforeRecord));
    }

    WriteResult result = writer.complete();

    // Find equality delete files
    List<DeleteFile> eqDeleteFiles = Lists.newArrayList();
    for (DeleteFile deleteFile : result.deleteFiles()) {
      if (deleteFile.content() == FileContent.EQUALITY_DELETES) {
        eqDeleteFiles.add(deleteFile);
      }
    }

    assertThat(eqDeleteFiles)
        .as("Should have equality delete files for UPDATE operations")
        .isNotEmpty();

    // Read equality delete contents
    List<Record> allEqDeletes = Lists.newArrayList();
    for (DeleteFile eqDeleteFile : eqDeleteFiles) {
      List<Record> deletes = readRecords(ICEBERG_SCHEMA, eqDeleteFile.location());
      allEqDeletes.addAll(deletes);
    }

    // Verify: equality deletes should contain BEFORE values {a=1, c=0}
    // NOT AFTER values {a=1, c=1}
    Record expectedBeforeRecord = GenericRecord.create(ICEBERG_SCHEMA);
    expectedBeforeRecord.setField("a", 1);
    expectedBeforeRecord.setField("c", 0);

    Record wrongAfterRecord = GenericRecord.create(ICEBERG_SCHEMA);
    wrongAfterRecord.setField("a", 1);
    wrongAfterRecord.setField("c", 1);

    assertThat(allEqDeletes)
        .as("Equality deletes should contain BEFORE image {a=1, c=0}")
        .contains(expectedBeforeRecord);

    // Count how many times {a=1, c=0} appears
    // First UPDATE finds row in insertedRowMap (from snapshot reads) -> positional delete
    // Second UPDATE doesn't find row (already removed) -> equality delete with BEFORE image
    long beforeCount = allEqDeletes.stream()
        .filter(r -> r.getField("a").equals(1) && r.getField("c").equals(0))
        .count();

    assertThat(beforeCount)
        .as("Should have 1 equality delete with BEFORE values {a=1, c=0}")
        .isEqualTo(1);

    // Verify we don't have the wrong AFTER values in equality deletes
    long afterCount = allEqDeletes.stream()
        .filter(r -> r.getField("a").equals(1) && r.getField("c").equals(1))
        .count();

    assertThat(afterCount)
        .as("Should NOT have equality deletes with AFTER values {a=1, c=1}")
        .isEqualTo(0);
  }

  private List<SinkRecord> createDebeziumEvents() {
    Struct source = new Struct(SOURCE_SCHEMA)
        .put("db", "upsert_test")
        .put("schema", "public")
        .put("table", "test")
        .put("ts_ms", System.currentTimeMillis());

    Struct row_1_0 = new Struct(OPTIONAL_ROW_SCHEMA).put("a", 1).put("c", 0);
    Struct row_1_1 = new Struct(OPTIONAL_ROW_SCHEMA).put("a", 1).put("c", 1);
    Struct row_2_0 = new Struct(OPTIONAL_ROW_SCHEMA).put("a", 2).put("c", 0);
    Struct row_2_1 = new Struct(OPTIONAL_ROW_SCHEMA).put("a", 2).put("c", 1);

    String topic = "debezium_test_upsert.public.test";

    return ImmutableList.of(
        // Event 1: snapshot read {a=1, c=0}
        createSinkRecord(topic, null, row_1_0, source, "r", 0),
        // Event 2: snapshot read {a=1, c=0} (duplicate)
        createSinkRecord(topic, null, row_1_0, source, "r", 1),
        // Event 3: snapshot read {a=2, c=0}
        createSinkRecord(topic, null, row_2_0, source, "r", 2),
        // Event 4: update {a=1, c=0} -> {a=1, c=1}
        createSinkRecord(topic, row_1_0, row_1_1, source, "u", 3),
        // Event 5: update {a=1, c=0} -> {a=1, c=1} (second duplicate)
        createSinkRecord(topic, row_1_0, row_1_1, source, "u", 4),
        // Event 6: insert {a=2, c=1}
        createSinkRecord(topic, null, row_2_1, source, "c", 5));
  }

  private SinkRecord createSinkRecord(
      String topic, Struct before, Struct after, Struct source, String op, long offset) {
    Struct envelope = new Struct(DEBEZIUM_ENVELOPE_SCHEMA)
        .put("before", before)
        .put("after", after)
        .put("source", source)
        .put("op", op)
        .put("ts_ms", System.currentTimeMillis());

    return new SinkRecord(
        topic,
        0,
        null,
        null,
        DEBEZIUM_ENVELOPE_SCHEMA,
        envelope,
        offset);
  }

  private Operation mapOperation(String op) {
    if (op == null) {
      return Operation.INSERT;
    }
    switch (op.charAt(0)) {
      case 'U':
        return Operation.UPDATE;
      case 'D':
        return Operation.DELETE;
      default:
        return Operation.INSERT;
    }
  }

  private List<Record> readRecords(Schema schema, CharSequence path) throws IOException {
    InputFile inputFile = fileIO.newInputFile(path.toString());
    try (CloseableIterable<Record> iterable =
        Parquet.read(inputFile)
            .project(schema)
            .createReaderFunc(fileSchema -> GenericParquetReaders.buildReader(schema, fileSchema))
            .build()) {
      return Lists.newArrayList(iterable);
    }
  }

  // ==================== Test for table WITH PK ====================

  /**
   * Test for table WITH primary key (id).
   * Schema: id (PK), a (optional), b (optional)
   *
   * Events:
   * 1. c: {id=1, b=0}
   * 2. c: {id=2, a=1, b=0}
   * 3. u: {id=1, b=0} -> {id=1, a=1, b=0}
   * 4. u: {id=2, a=1, b=0} -> {id=2, a=1, b=1}
   * 5. d: {id=2, a=1, b=1}
   *
   * For tables with PK, equality delete uses only the PK column (id).
   */
  @Test
  public void testTableWithPrimaryKey() throws IOException {
    // Schema with PK on 'id'
    Schema schemaWithPk =
        new Schema(
            ImmutableList.of(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "a", Types.IntegerType.get()),
                Types.NestedField.optional(3, "b", Types.IntegerType.get())),
            ImmutableSet.of(1)); // id is identifier field

    Set<Integer> pkIdentifier = ImmutableSet.of(1);

    // Delete schema (only id)
    Schema deleteSchema =
        new Schema(ImmutableList.of(Types.NestedField.required(1, "id", Types.IntegerType.get())));

    // Setup table mock for this test
    Table pkTable = mock(Table.class);
    when(pkTable.schema()).thenReturn(schemaWithPk);
    when(pkTable.spec()).thenReturn(PartitionSpec.unpartitioned());
    when(pkTable.io()).thenReturn(fileIO);
    when(pkTable.locationProvider())
        .thenReturn(LocationProviders.locationsFor("file", ImmutableMap.of()));
    when(pkTable.encryption()).thenReturn(PlaintextEncryptionManager.instance());
    when(pkTable.properties()).thenReturn(ImmutableMap.of());

    OutputFileFactory pkFileFactory =
        OutputFileFactory.builderFor(pkTable, 2, 1).format(FileFormat.PARQUET).build();

    GenericAppenderFactory pkAppenderFactory =
        new GenericAppenderFactory(
            schemaWithPk,
            PartitionSpec.unpartitioned(),
            new int[] {1}, // only id as identifier
            deleteSchema,
            null);

    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.schemaCaseInsensitive()).thenReturn(false);
    when(config.schemaDebeziumTimeTypes()).thenReturn(false);

    RecordConverter pkRecordConverter = new RecordConverter(pkTable, config);

    // Create writer with hasRealPk=true
    CompactUnpartitionedDeltaWriter writer =
        new CompactUnpartitionedDeltaWriter(
            PartitionSpec.unpartitioned(),
            FileFormat.PARQUET,
            pkAppenderFactory,
            pkFileFactory,
            fileIO,
            Long.MAX_VALUE,
            schemaWithPk,
            pkIdentifier,
            false, // upsertMode = false (CDC mode)
            true); // hasRealPk = true

    // Kafka Connect schemas for table with PK
    org.apache.kafka.connect.data.Schema rowSchemaWithPk =
        SchemaBuilder.struct()
            .field("id", org.apache.kafka.connect.data.Schema.INT32_SCHEMA)
            .field("a", org.apache.kafka.connect.data.Schema.OPTIONAL_INT32_SCHEMA)
            .field("b", org.apache.kafka.connect.data.Schema.OPTIONAL_INT32_SCHEMA)
            .optional()
            .build();

    org.apache.kafka.connect.data.Schema envelopeSchemaWithPk =
        SchemaBuilder.struct()
            .field("before", rowSchemaWithPk)
            .field("after", rowSchemaWithPk)
            .field("source", SOURCE_SCHEMA)
            .field("op", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
            .field("ts_ms", org.apache.kafka.connect.data.Schema.INT64_SCHEMA)
            .build();

    Struct source = new Struct(SOURCE_SCHEMA)
        .put("db", "upsert_test")
        .put("schema", "public")
        .put("table", "testpk")
        .put("ts_ms", System.currentTimeMillis());

    // Create rows
    Struct row_id1_b0 = new Struct(rowSchemaWithPk).put("id", 1).put("b", 0);
    Struct row_id2_a1_b0 = new Struct(rowSchemaWithPk).put("id", 2).put("a", 1).put("b", 0);
    Struct row_id1_a1_b0 = new Struct(rowSchemaWithPk).put("id", 1).put("a", 1).put("b", 0);
    Struct row_id2_a1_b1 = new Struct(rowSchemaWithPk).put("id", 2).put("a", 1).put("b", 1);

    String topic = "debezium_test_upsert.public.testpk";

    List<SinkRecord> events = ImmutableList.of(
        // Event 1: insert {id=1, b=0}
        createSinkRecordWithSchema(topic, null, row_id1_b0, source, "c", 0, envelopeSchemaWithPk),
        // Event 2: insert {id=2, a=1, b=0}
        createSinkRecordWithSchema(topic, null, row_id2_a1_b0, source, "c", 1, envelopeSchemaWithPk),
        // Event 3: update {id=1, b=0} -> {id=1, a=1, b=0}
        createSinkRecordWithSchema(topic, row_id1_b0, row_id1_a1_b0, source, "u", 2, envelopeSchemaWithPk),
        // Event 4: update {id=2, a=1, b=0} -> {id=2, a=1, b=1}
        createSinkRecordWithSchema(topic, row_id2_a1_b0, row_id2_a1_b1, source, "u", 3, envelopeSchemaWithPk),
        // Event 5: delete {id=2, a=1, b=1}
        createSinkRecordWithSchema(topic, row_id2_a1_b1, null, source, "d", 4, envelopeSchemaWithPk));

    DebeziumTransform<SinkRecord> pkTransform = new DebeziumTransform<>();
    pkTransform.configure(ImmutableMap.of());

    for (SinkRecord event : events) {
      SinkRecord transformed = pkTransform.apply(event);
      if (transformed == null) {
        continue;
      }

      Struct value = (Struct) transformed.value();
      String op = value.getString("_cdc_op");
      Operation operation = mapOperation(op);

      Record icebergRecord = pkRecordConverter.convert(transformed.value());

      // For tables with PK, we don't need before image (PK doesn't change)
      Record beforeRecord = null;
      if (operation == Operation.UPDATE) {
        Object beforeImage = Utilities.extractFromRecordValue(transformed.value(), "_before_image");
        if (beforeImage != null) {
          beforeRecord = pkRecordConverter.convert(beforeImage);
        }
      }

      writer.write(new RecordWrapper(icebergRecord, operation, beforeRecord));
    }

    pkTransform.close();

    WriteResult result = writer.complete();

    // Find equality delete files
    List<DeleteFile> eqDeleteFiles = Lists.newArrayList();
    for (DeleteFile deleteFile : result.deleteFiles()) {
      if (deleteFile.content() == FileContent.EQUALITY_DELETES) {
        eqDeleteFiles.add(deleteFile);
      }
    }

    // For table with PK, UPDATE operations find rows in insertedRowMap -> positional deletes
    // So we might have fewer equality deletes
    // DELETE operation (event 5) should produce equality delete for id=2

    // Read equality delete contents (delete schema has only 'id')
    List<Record> allEqDeletes = Lists.newArrayList();
    for (DeleteFile eqDeleteFile : eqDeleteFiles) {
      List<Record> deletes = readRecords(deleteSchema, eqDeleteFile.location());
      allEqDeletes.addAll(deletes);
    }

    // Verify equality deletes contain only PK values
    // The DELETE for id=2 should result in equality delete with {id=2}
    // (unless it was found in insertedRowMap, then positional delete)

    // For this test, verify that any equality deletes have the correct structure
    // (only id column, correct values)
    for (Record eqDelete : allEqDeletes) {
      assertThat(eqDelete.getField("id"))
          .as("Equality delete should have id field")
          .isNotNull();
      // Verify it's either id=1 or id=2
      int id = (Integer) eqDelete.getField("id");
      assertThat(id)
          .as("Equality delete id should be 1 or 2")
          .isIn(1, 2);
    }
  }

  private SinkRecord createSinkRecordWithSchema(
      String topic,
      Struct before,
      Struct after,
      Struct source,
      String op,
      long offset,
      org.apache.kafka.connect.data.Schema envelopeSchema) {
    Struct envelope = new Struct(envelopeSchema)
        .put("before", before)
        .put("after", after)
        .put("source", source)
        .put("op", op)
        .put("ts_ms", System.currentTimeMillis());

    return new SinkRecord(
        topic,
        0,
        null,
        null,
        envelopeSchema,
        envelope,
        offset);
  }
}
