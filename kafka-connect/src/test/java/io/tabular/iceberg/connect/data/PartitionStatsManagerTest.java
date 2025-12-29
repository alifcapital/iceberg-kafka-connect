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

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.InternalData;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionStatisticsFile;
import org.apache.iceberg.Partitioning;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class PartitionStatsManagerTest {

  private InMemoryCatalog catalog;
  private Table table;

  private static final Namespace NAMESPACE = Namespace.of("db");
  private static final String TABLE_NAME = "tbl";
  private static final TableIdentifier TABLE_IDENTIFIER =
      TableIdentifier.of(NAMESPACE, TABLE_NAME);
  private static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.LongType.get()),
          required(2, "date", Types.StringType.get()));
  private static final PartitionSpec SPEC =
      PartitionSpec.builderFor(SCHEMA).identity("date").build();

  @BeforeEach
  public void before() {
    catalog = new InMemoryCatalog();
    catalog.initialize(null, ImmutableMap.of());
    catalog.createNamespace(NAMESPACE);
    table = catalog.createTable(TABLE_IDENTIFIER, SCHEMA, SPEC);
  }

  @AfterEach
  public void after() throws IOException {
    catalog.close();
  }

  @Test
  public void testIncrementalMatchesFullRecomputation() throws IOException {
    // Step 1: Create initial data and compute stats from scratch
    DataFile file1 = createDataFile("2024-01-01", 100, 1000);
    DataFile file2 = createDataFile("2024-01-02", 200, 2000);
    table.newAppend().appendFile(file1).appendFile(file2).commit();
    Snapshot snapshot1 = table.currentSnapshot();

    PartitionStatisticsFile initialStats =
        PartitionStatsManager.computeFullStats(table, snapshot1);
    assertThat(initialStats).isNotNull();

    // Register initial stats with table
    table.updatePartitionStatistics().setPartitionStatistics(initialStats).commit();

    // Step 2: Add more data
    DataFile file3 = createDataFile("2024-01-01", 150, 1500); // same partition as file1
    DataFile file4 = createDataFile("2024-01-03", 300, 3000); // new partition
    table.newAppend().appendFile(file3).appendFile(file4).commit();
    Snapshot snapshot2 = table.currentSnapshot();

    // Step 3: Compute stats incrementally
    PartitionStatisticsFile incrementalStats =
        PartitionStatsManager.computeAndWriteStatsFile(
            table, snapshot2, ImmutableList.of(file3, file4), ImmutableList.of());
    assertThat(incrementalStats).isNotNull();

    // Step 4: Compute stats from scratch for comparison
    PartitionStatisticsFile fullStats =
        PartitionStatsManager.computeFullStats(table, snapshot2);
    assertThat(fullStats).isNotNull();

    // Step 5: Compare - both should have same partition stats
    Map<String, PartitionStatsRecord> incrementalMap = readStatsFile(table, incrementalStats);
    Map<String, PartitionStatsRecord> fullMap = readStatsFile(table, fullStats);

    assertThat(incrementalMap.keySet()).isEqualTo(fullMap.keySet());

    for (String partition : incrementalMap.keySet()) {
      PartitionStatsRecord incr = incrementalMap.get(partition);
      PartitionStatsRecord full = fullMap.get(partition);

      assertThat(incr.dataRecordCount)
          .as("dataRecordCount for partition " + partition)
          .isEqualTo(full.dataRecordCount);
      assertThat(incr.dataFileCount)
          .as("dataFileCount for partition " + partition)
          .isEqualTo(full.dataFileCount);
      assertThat(incr.totalDataFileSizeInBytes)
          .as("totalDataFileSizeInBytes for partition " + partition)
          .isEqualTo(full.totalDataFileSizeInBytes);
    }
  }

  @Test
  public void testIncrementalWithDeleteFiles() throws IOException {
    // Step 1: Create initial data
    DataFile file1 = createDataFile("2024-01-01", 100, 1000);
    table.newAppend().appendFile(file1).commit();
    Snapshot snapshot1 = table.currentSnapshot();

    PartitionStatisticsFile initialStats =
        PartitionStatsManager.computeFullStats(table, snapshot1);
    table.updatePartitionStatistics().setPartitionStatistics(initialStats).commit();

    // Step 2: Add data and delete files
    DataFile file2 = createDataFile("2024-01-01", 200, 2000);
    DeleteFile deleteFile = createDeleteFile("2024-01-01", 10, 100);
    table.newRowDelta().addRows(file2).addDeletes(deleteFile).commit();
    Snapshot snapshot2 = table.currentSnapshot();

    // Step 3: Compute incrementally
    PartitionStatisticsFile incrementalStats =
        PartitionStatsManager.computeAndWriteStatsFile(
            table, snapshot2, ImmutableList.of(file2), ImmutableList.of(deleteFile));
    assertThat(incrementalStats).isNotNull();

    // Step 4: Compute from scratch
    PartitionStatisticsFile fullStats =
        PartitionStatsManager.computeFullStats(table, snapshot2);

    // Step 5: Compare
    Map<String, PartitionStatsRecord> incrementalMap = readStatsFile(table, incrementalStats);
    Map<String, PartitionStatsRecord> fullMap = readStatsFile(table, fullStats);

    assertThat(incrementalMap.keySet()).isEqualTo(fullMap.keySet());

    for (String partition : incrementalMap.keySet()) {
      PartitionStatsRecord incr = incrementalMap.get(partition);
      PartitionStatsRecord full = fullMap.get(partition);

      assertThat(incr.dataRecordCount).isEqualTo(full.dataRecordCount);
      assertThat(incr.dataFileCount).isEqualTo(full.dataFileCount);
      assertThat(incr.positionDeleteRecordCount).isEqualTo(full.positionDeleteRecordCount);
      assertThat(incr.positionDeleteFileCount).isEqualTo(full.positionDeleteFileCount);
    }
  }

  @Test
  public void testReturnsNullWithoutExistingStatsFile() {
    DataFile file1 = createDataFile("2024-01-01", 100, 1000);
    table.newAppend().appendFile(file1).commit();
    Snapshot snapshot = table.currentSnapshot();

    // Should return null because no partition stats file exists
    PartitionStatisticsFile statsFile =
        PartitionStatsManager.computeAndWriteStatsFile(
            table, snapshot, ImmutableList.of(file1), ImmutableList.of());

    assertThat(statsFile).isNull();
  }

  @Test
  public void testReturnsNullForUnpartitionedTable() throws IOException {
    TableIdentifier unpartId = TableIdentifier.of(NAMESPACE, "unpart");
    Table unpartTable = catalog.createTable(unpartId, SCHEMA, PartitionSpec.unpartitioned());

    DataFile dataFile =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath(UUID.randomUUID() + ".parquet")
            .withFormat(FileFormat.PARQUET)
            .withRecordCount(100)
            .withFileSizeInBytes(1000)
            .build();

    unpartTable.newAppend().appendFile(dataFile).commit();
    Snapshot snapshot = unpartTable.currentSnapshot();

    assertThat(PartitionStatsManager.computeFullStats(unpartTable, snapshot)).isNull();
  }

  private Map<String, PartitionStatsRecord> readStatsFile(
      Table tbl, PartitionStatisticsFile statsFile) throws IOException {
    Types.StructType partitionType = Partitioning.partitionType(tbl);
    Schema statsSchema = PartitionStatsManager.schema(partitionType);
    FileFormat format = FileFormat.fromFileName(statsFile.path());

    Map<String, PartitionStatsRecord> result = new HashMap<>();

    try (CloseableIterable<StructLike> records =
        InternalData.read(format, tbl.io().newInputFile(statsFile.path()))
            .project(statsSchema)
            .build()) {

      for (StructLike record : records) {
        StructLike partition = record.get(0, StructLike.class);
        String partitionKey = partition.toString();

        PartitionStatsRecord stats = new PartitionStatsRecord();
        stats.dataRecordCount = record.get(2, Long.class);
        stats.dataFileCount = record.get(3, Integer.class);
        stats.totalDataFileSizeInBytes = record.get(4, Long.class);
        stats.positionDeleteRecordCount = record.get(5, Long.class);
        stats.positionDeleteFileCount = record.get(6, Integer.class);
        stats.equalityDeleteRecordCount = record.get(7, Long.class);
        stats.equalityDeleteFileCount = record.get(8, Integer.class);

        result.put(partitionKey, stats);
      }
    }

    return result;
  }

  private static class PartitionStatsRecord {
    Long dataRecordCount;
    Integer dataFileCount;
    Long totalDataFileSizeInBytes;
    Long positionDeleteRecordCount;
    Integer positionDeleteFileCount;
    Long equalityDeleteRecordCount;
    Integer equalityDeleteFileCount;
  }

  private DataFile createDataFile(String dateValue, long recordCount, long fileSizeInBytes) {
    return DataFiles.builder(SPEC)
        .withPath(UUID.randomUUID() + ".parquet")
        .withFormat(FileFormat.PARQUET)
        .withRecordCount(recordCount)
        .withFileSizeInBytes(fileSizeInBytes)
        .withPartitionPath("date=" + dateValue)
        .build();
  }

  private DeleteFile createDeleteFile(String dateValue, long recordCount, long fileSizeInBytes) {
    return FileMetadata.deleteFileBuilder(SPEC)
        .ofPositionDeletes()
        .withPath(UUID.randomUUID() + ".parquet")
        .withFormat(FileFormat.PARQUET)
        .withRecordCount(recordCount)
        .withFileSizeInBytes(fileSizeInBytes)
        .withPartitionPath("date=" + dateValue)
        .build();
  }
}
