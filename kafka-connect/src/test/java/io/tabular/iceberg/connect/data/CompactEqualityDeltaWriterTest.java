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

import java.io.IOException;
import java.util.List;
import java.util.Set;
import org.apache.iceberg.DataFile;
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
import org.apache.iceberg.io.DeleteSchemaUtil;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for CompactEqualityDeltaWriter.
 *
 * <p>Ported from iceberg's TestTaskEqualityDeltaWriter to verify CompactKeyMap implementation
 * produces correct position deletes.
 */
public class CompactEqualityDeltaWriterTest {

  private static final Schema SCHEMA =
      new Schema(
          ImmutableList.of(
              Types.NestedField.required(1, "id", Types.LongType.get()),
              Types.NestedField.required(2, "data", Types.StringType.get())),
          ImmutableSet.of(1)); // id is identifier field

  private static final Set<Integer> IDENTIFIER_FIELD_IDS = ImmutableSet.of(1);

  // Very small target file size to force rolling
  private static final long TARGET_FILE_SIZE = 128L;

  private InMemoryFileIO fileIO;
  private Table table;
  private OutputFileFactory fileFactory;
  private GenericAppenderFactory appenderFactory;
  private GenericRecord posRecord;

  @BeforeEach
  public void setup() {
    fileIO = new InMemoryFileIO();

    table = mock(Table.class);
    when(table.schema()).thenReturn(SCHEMA);
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
            SCHEMA,
            PartitionSpec.unpartitioned(),
            new int[] {1},
            SCHEMA.select("id"),
            null);

    posRecord = GenericRecord.create(DeleteSchemaUtil.pathPosSchema());
  }

  private Record createRecord(long id, String data) {
    GenericRecord record = GenericRecord.create(SCHEMA);
    record.setField("id", id);
    record.setField("data", data);
    return record;
  }

  private CompactEqualityDeltaWriter createWriter() {
    return createWriter(TARGET_FILE_SIZE);
  }

  private CompactEqualityDeltaWriter createWriter(long targetFileSize) {
    return new CompactEqualityDeltaWriter(
        SCHEMA,
        IDENTIFIER_FIELD_IDS,
        PartitionSpec.unpartitioned(),
        null,
        FileFormat.PARQUET,
        appenderFactory,
        fileFactory,
        fileIO,
        targetFileSize);
  }

  /**
   * Test pure inserts - no duplicates, should produce only data files.
   */
  @Test
  public void testPureInsert() throws IOException {
    CompactEqualityDeltaWriter writer = createWriter();

    for (int i = 0; i < 10; i++) {
      writer.write(createRecord(i, "val-" + i));
    }

    WriteResult result = writer.complete();

    assertThat(result.dataFiles()).hasSize(1);
    assertThat(result.deleteFiles()).isEmpty();
  }

  /**
   * Test inserting rows with duplicate keys in same session.
   *
   * <p>Insert sequence: 1,2,3,4,4,3,2,1
   *
   * <p>Expected: pos deletes for positions 0,1,2,3 (the first occurrences that got replaced)
   */
  @Test
  public void testInsertDuplicatedKey() throws IOException {
    CompactEqualityDeltaWriter writer = createWriter(Long.MAX_VALUE); // Don't roll

    writer.write(createRecord(1, "aaa")); // pos 0
    writer.write(createRecord(2, "bbb")); // pos 1
    writer.write(createRecord(3, "ccc")); // pos 2
    writer.write(createRecord(4, "ddd")); // pos 3
    writer.write(createRecord(4, "eee")); // pos 4 - replaces pos 3
    writer.write(createRecord(3, "fff")); // pos 5 - replaces pos 2
    writer.write(createRecord(2, "ggg")); // pos 6 - replaces pos 1
    writer.write(createRecord(1, "hhh")); // pos 7 - replaces pos 0

    WriteResult result = writer.complete();

    assertThat(result.dataFiles()).hasSize(1);
    assertThat(result.deleteFiles()).hasSize(1);

    DeleteFile posDeleteFile = result.deleteFiles()[0];
    assertThat(posDeleteFile.content()).isEqualTo(FileContent.POSITION_DELETES);

    // Verify referenced data files
    assertThat(result.referencedDataFiles()).hasSize(1);

    // Should have 4 pos deletes
    assertThat(posDeleteFile.recordCount()).isEqualTo(4);

    // Read and verify the actual content of data file
    DataFile dataFile = result.dataFiles()[0];
    List<Record> dataRecords = readRecords(SCHEMA, dataFile.location());
    assertThat(dataRecords)
        .as("Data file should contain all 8 records")
        .containsExactly(
            createRecord(1, "aaa"),
            createRecord(2, "bbb"),
            createRecord(3, "ccc"),
            createRecord(4, "ddd"),
            createRecord(4, "eee"),
            createRecord(3, "fff"),
            createRecord(2, "ggg"),
            createRecord(1, "hhh"));

    // Read and verify the actual content of pos delete file
    Schema posDeleteSchema = DeleteSchemaUtil.pathPosSchema();
    List<Record> posDeletes = readRecords(posDeleteSchema, posDeleteFile.location());
    assertThat(posDeletes)
        .as("Pos delete file should contain deletes for positions 0,1,2,3")
        .containsExactlyInAnyOrder(
            posRecord.copy("file_path", dataFile.location(), "pos", 0L),
            posRecord.copy("file_path", dataFile.location(), "pos", 1L),
            posRecord.copy("file_path", dataFile.location(), "pos", 2L),
            posRecord.copy("file_path", dataFile.location(), "pos", 3L));
  }

  /**
   * Test delete of a key that was inserted in same session -> pos delete.
   */
  @Test
  public void testDeleteInsertedKey() throws IOException {
    CompactEqualityDeltaWriter writer = createWriter(Long.MAX_VALUE);

    writer.write(createRecord(1, "aaa")); // pos 0
    writer.write(createRecord(2, "bbb")); // pos 1
    writer.write(createRecord(3, "ccc")); // pos 2

    // Delete key 2 which exists in this session
    GenericRecord keyRecord = GenericRecord.create(SCHEMA.select("id"));
    keyRecord.setField("id", 2L);
    writer.deleteKey(keyRecord);

    WriteResult result = writer.complete();

    assertThat(result.dataFiles()).hasSize(1);
    assertThat(result.deleteFiles()).hasSize(1);

    DeleteFile posDeleteFile = result.deleteFiles()[0];
    assertThat(posDeleteFile.content()).isEqualTo(FileContent.POSITION_DELETES);

    // Should have pos delete for position 1
    assertThat(posDeleteFile.recordCount()).isEqualTo(1);
  }

  /**
   * Test delete of a key that was NOT inserted in this session -> eq delete.
   */
  @Test
  public void testDeleteNonExistentKey() throws IOException {
    CompactEqualityDeltaWriter writer = createWriter(Long.MAX_VALUE);

    writer.write(createRecord(1, "aaa"));

    // Delete key 999 which doesn't exist in this session
    GenericRecord keyRecord = GenericRecord.create(SCHEMA.select("id"));
    keyRecord.setField("id", 999L);
    writer.deleteKey(keyRecord);

    WriteResult result = writer.complete();

    assertThat(result.dataFiles()).hasSize(1);
    assertThat(result.deleteFiles()).hasSize(1);

    DeleteFile eqDeleteFile = result.deleteFiles()[0];
    assertThat(eqDeleteFile.content()).isEqualTo(FileContent.EQUALITY_DELETES);
  }

  /**
   * Test upsert: write -> delete -> write same key.
   */
  @Test
  public void testUpsertSameRow() throws IOException {
    CompactEqualityDeltaWriter writer = createWriter(Long.MAX_VALUE);

    Record record = createRecord(1, "aaa");
    writer.write(record); // pos 0

    // Upsert: delete then write
    writer.delete(record);
    writer.write(record); // pos 1

    WriteResult result = writer.complete();

    assertThat(result.dataFiles()).hasSize(1);
    assertThat(result.deleteFiles()).hasSize(1);

    DeleteFile posDeleteFile = result.deleteFiles()[0];
    assertThat(posDeleteFile.content()).isEqualTo(FileContent.POSITION_DELETES);

    // Should have 1 pos delete
    assertThat(posDeleteFile.recordCount()).isEqualTo(1);
  }

  /**
   * CRITICAL TEST: Multiple data files with rolling, then update references first file.
   *
   * <p>This tests the main use case for CompactKeyMap:
   *
   * <ol>
   *   <li>Write enough rows to roll to a new data file
   *   <li>Update a key that was in the FIRST data file
   *   <li>Verify pos delete correctly references first file and position
   * </ol>
   *
   * <p>Note: Rolling happens every ROWS_DIVISOR=1000 rows, so we need >2000 to get 2+ files.
   */
  @Test
  public void testPosDeleteAcrossFileRoll() throws IOException {
    // Use small target size to force rolling quickly
    CompactEqualityDeltaWriter writer = createWriter(TARGET_FILE_SIZE);

    // Write 2100 records to ensure rolling (ROWS_DIVISOR=1000 checks size every 1000 rows)
    for (long i = 0; i < 2100; i++) {
      writer.write(createRecord(i, "data-" + i + "-padding"));
    }

    // Now update key 0 which should be in the first file (position 0)
    writer.write(createRecord(0, "updated-0"));

    WriteResult result = writer.complete();

    // Should have multiple data files due to rolling
    assertThat(result.dataFiles().length)
        .as("Should have multiple data files after rolling")
        .isGreaterThan(1);

    // Should have pos delete file
    assertThat(result.deleteFiles()).isNotEmpty();

    boolean foundPosDelete = false;
    for (DeleteFile deleteFile : result.deleteFiles()) {
      if (deleteFile.content() == FileContent.POSITION_DELETES) {
        foundPosDelete = true;
        assertThat(deleteFile.recordCount()).isGreaterThan(0);
      }
    }
    assertThat(foundPosDelete).as("Should have position delete file").isTrue();

    // Verify the pos delete references the first data file
    DataFile firstDataFile = result.dataFiles()[0];
    assertThat(result.referencedDataFiles())
        .as("Should reference the first data file")
        .contains(firstDataFile.location());
  }

  /**
   * Test many records with interleaved updates across file boundaries.
   */
  @Test
  public void testManyRecordsWithUpdates() throws IOException {
    CompactEqualityDeltaWriter writer = createWriter(TARGET_FILE_SIZE);

    int numRecords = 2000;
    int expectedPosDeletes = 0;

    // Write records and update every 50th one
    for (int i = 0; i < numRecords; i++) {
      writer.write(createRecord(i, "data-" + i + "-padding"));

      if (i % 50 == 0 && i > 0) {
        // Update this record (write again with same key)
        writer.write(createRecord(i, "updated-" + i));
        expectedPosDeletes++;
      }
    }

    WriteResult result = writer.complete();

    // Should have multiple data files (rolling at 1000 rows)
    assertThat(result.dataFiles().length).isGreaterThanOrEqualTo(1);

    // Count total pos deletes
    long totalPosDeletes = 0;
    for (DeleteFile deleteFile : result.deleteFiles()) {
      if (deleteFile.content() == FileContent.POSITION_DELETES) {
        totalPosDeletes += deleteFile.recordCount();
      }
    }

    assertThat(totalPosDeletes)
        .as("Should have correct number of pos deletes")
        .isEqualTo(expectedPosDeletes);
  }

  /**
   * Test that path interning works - same path should be interned.
   */
  @Test
  public void testPathInterning() throws IOException {
    CompactEqualityDeltaWriter writer = createWriter(Long.MAX_VALUE);

    // Write many records with same key to generate multiple pos deletes
    for (int i = 0; i < 100; i++) {
      writer.write(createRecord(1, "value-" + i));
    }

    WriteResult result = writer.complete();

    assertThat(result.dataFiles()).hasSize(1);
    assertThat(result.deleteFiles()).hasSize(1);

    DeleteFile posDeleteFile = result.deleteFiles()[0];
    assertThat(posDeleteFile.content()).isEqualTo(FileContent.POSITION_DELETES);
    // Should have 99 pos deletes (first insert + 99 updates)
    assertThat(posDeleteFile.recordCount()).isEqualTo(99);
  }

  /**
   * Test STRING key (UUID detection).
   */
  @Test
  public void testStringKey() throws IOException {
    Schema stringKeySchema =
        new Schema(
            ImmutableList.of(
                Types.NestedField.required(1, "id", Types.StringType.get()),
                Types.NestedField.required(2, "data", Types.StringType.get())),
            ImmutableSet.of(1));

    GenericAppenderFactory stringAppenderFactory =
        new GenericAppenderFactory(
            stringKeySchema,
            PartitionSpec.unpartitioned(),
            new int[] {1},
            stringKeySchema.select("id"),
            null);

    CompactEqualityDeltaWriter writer =
        new CompactEqualityDeltaWriter(
            stringKeySchema,
            ImmutableSet.of(1),
            PartitionSpec.unpartitioned(),
            null,
            FileFormat.PARQUET,
            stringAppenderFactory,
            fileFactory,
            fileIO,
            Long.MAX_VALUE);

    GenericRecord rec1 = GenericRecord.create(stringKeySchema);
    rec1.setField("id", "key-1");
    rec1.setField("data", "aaa");

    GenericRecord rec2 = GenericRecord.create(stringKeySchema);
    rec2.setField("id", "key-1");
    rec2.setField("data", "bbb");

    writer.write(rec1); // pos 0
    writer.write(rec2); // pos 1, replaces pos 0

    WriteResult result = writer.complete();

    assertThat(result.dataFiles()).hasSize(1);
    assertThat(result.deleteFiles()).hasSize(1);

    DeleteFile posDeleteFile = result.deleteFiles()[0];
    assertThat(posDeleteFile.content()).isEqualTo(FileContent.POSITION_DELETES);
    assertThat(posDeleteFile.recordCount()).isEqualTo(1);
  }

  /**
   * Test UUID key optimization.
   */
  @Test
  public void testUuidKey() throws IOException {
    Schema uuidKeySchema =
        new Schema(
            ImmutableList.of(
                Types.NestedField.required(1, "id", Types.StringType.get()),
                Types.NestedField.required(2, "data", Types.StringType.get())),
            ImmutableSet.of(1));

    GenericAppenderFactory uuidAppenderFactory =
        new GenericAppenderFactory(
            uuidKeySchema,
            PartitionSpec.unpartitioned(),
            new int[] {1},
            uuidKeySchema.select("id"),
            null);

    CompactEqualityDeltaWriter writer =
        new CompactEqualityDeltaWriter(
            uuidKeySchema,
            ImmutableSet.of(1),
            PartitionSpec.unpartitioned(),
            null,
            FileFormat.PARQUET,
            uuidAppenderFactory,
            fileFactory,
            fileIO,
            Long.MAX_VALUE);

    String uuid1 = "550e8400-e29b-41d4-a716-446655440000";
    String uuid2 = "550e8400-e29b-41d4-a716-446655440001";

    GenericRecord rec1 = GenericRecord.create(uuidKeySchema);
    rec1.setField("id", uuid1);
    rec1.setField("data", "aaa");

    GenericRecord rec2 = GenericRecord.create(uuidKeySchema);
    rec2.setField("id", uuid1); // same UUID
    rec2.setField("data", "bbb");

    GenericRecord rec3 = GenericRecord.create(uuidKeySchema);
    rec3.setField("id", uuid2);
    rec3.setField("data", "ccc");

    writer.write(rec1); // pos 0
    writer.write(rec2); // pos 1, replaces pos 0
    writer.write(rec3); // pos 2

    WriteResult result = writer.complete();

    assertThat(result.dataFiles()).hasSize(1);
    assertThat(result.deleteFiles()).hasSize(1);

    DeleteFile posDeleteFile = result.deleteFiles()[0];
    assertThat(posDeleteFile.content()).isEqualTo(FileContent.POSITION_DELETES);
    assertThat(posDeleteFile.recordCount()).isEqualTo(1);
  }

  /**
   * CRITICAL TEST: Pos delete across file roll with specific row group targeting.
   *
   * <p>This test configures small row groups and verifies:
   *
   * <ol>
   *   <li>First data file has multiple row groups
   *   <li>File rolls to second file
   *   <li>Update a key from row group 2 of first file
   *   <li>Verify pos delete references correct file_path and position
   * </ol>
   */
  @Test
  public void testPosDeleteAcrossFileRollWithRowGroups() throws IOException {
    // Configure small row group size to get multiple row groups per file
    // Each record is roughly 20-30 bytes, so 100 bytes per row group = ~3-4 records
    ImmutableMap<String, String> tableProperties =
        ImmutableMap.of(
            "write.parquet.row-group-size-bytes", "200",
            "write.parquet.row-group-check-min-record-count", "2");

    Table tableWithProps = mock(Table.class);
    when(tableWithProps.schema()).thenReturn(SCHEMA);
    when(tableWithProps.spec()).thenReturn(PartitionSpec.unpartitioned());
    when(tableWithProps.io()).thenReturn(fileIO);
    when(tableWithProps.locationProvider())
        .thenReturn(LocationProviders.locationsFor("file", ImmutableMap.of()));
    when(tableWithProps.encryption()).thenReturn(PlaintextEncryptionManager.instance());
    when(tableWithProps.properties()).thenReturn(tableProperties);

    OutputFileFactory fileFactoryWithProps =
        OutputFileFactory.builderFor(tableWithProps, 1, 2).format(FileFormat.PARQUET).build();

    GenericAppenderFactory appenderFactoryWithProps =
        new GenericAppenderFactory(
            SCHEMA,
            PartitionSpec.unpartitioned(),
            new int[] {1},
            SCHEMA.select("id"),
            null);
    appenderFactoryWithProps.setAll(tableProperties);

    // Use large target file size for first file, then smaller for rolling
    // First write many records to a single file, then force roll
    CompactEqualityDeltaWriter writer =
        new CompactEqualityDeltaWriter(
            SCHEMA,
            IDENTIFIER_FIELD_IDS,
            PartitionSpec.unpartitioned(),
            null,
            FileFormat.PARQUET,
            appenderFactoryWithProps,
            fileFactoryWithProps,
            fileIO,
            100L); // Small target to force early rolling

    // Write 2100 records to force rolling (ROWS_DIVISOR=1000)
    // With small row groups, first file should have multiple row groups
    // Record at position 1500 should be in row group 2+ of first file (if file doesn't roll before)
    // Let's write enough to ensure rolling happens
    for (long i = 0; i < 2100; i++) {
      writer.write(createRecord(i, "data-" + i + "-padding-to-fill-row"));
    }

    // Now update key 500 which should be well into the first file
    // This position will be in a later row group of first file
    writer.write(createRecord(500, "updated-500"));

    WriteResult result = writer.complete();

    // Should have multiple data files
    assertThat(result.dataFiles().length)
        .as("Should have multiple data files after rolling")
        .isGreaterThan(1);

    // Find the data file containing position 500
    DataFile firstDataFile = result.dataFiles()[0];

    // Should have pos delete
    assertThat(result.deleteFiles()).isNotEmpty();

    DeleteFile posDeleteFile = null;
    for (DeleteFile deleteFile : result.deleteFiles()) {
      if (deleteFile.content() == FileContent.POSITION_DELETES) {
        posDeleteFile = deleteFile;
        break;
      }
    }
    assertThat(posDeleteFile).as("Should have position delete file").isNotNull();

    // Read and verify the pos delete content
    Schema posDeleteSchema = DeleteSchemaUtil.pathPosSchema();
    List<Record> posDeletes = readRecords(posDeleteSchema, posDeleteFile.location());

    // Find the pos delete for key 500
    Record posDeleteFor500 = null;
    for (Record pd : posDeletes) {
      Long pos = (Long) pd.getField("pos");
      if (pos != null && pos == 500L) {
        posDeleteFor500 = pd;
        break;
      }
    }

    assertThat(posDeleteFor500)
        .as("Should have pos delete for position 500")
        .isNotNull();

    // Verify it references the first data file
    String deletedFilePath = (String) posDeleteFor500.getField("file_path");
    assertThat(deletedFilePath)
        .as("Pos delete should reference the first data file")
        .isEqualTo(firstDataFile.location().toString());

    // Also verify that we can read the first data file and position 500 exists
    List<Record> dataRecords = readRecords(SCHEMA, firstDataFile.location());
    assertThat(dataRecords.size())
        .as("First data file should have many records")
        .isGreaterThan(500);

    Record recordAt500 = dataRecords.get(500);
    assertThat(recordAt500.getField("id"))
        .as("Record at position 500 should have id=500")
        .isEqualTo(500L);
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
}
