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
import java.util.List;
import java.util.UUID;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionStatisticsFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
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
  public void testComputeStatsForNewPartitions() {
    // Create data files for different partitions
    DataFile file1 = createDataFile("2024-01-01", 100, 1000);
    DataFile file2 = createDataFile("2024-01-02", 200, 2000);
    DataFile file3 = createDataFile("2024-01-01", 150, 1500); // same partition as file1

    List<DataFile> dataFiles = ImmutableList.of(file1, file2, file3);
    List<DeleteFile> deleteFiles = ImmutableList.of();

    // Commit files to get a snapshot
    table.newAppend().appendFile(file1).appendFile(file2).appendFile(file3).commit();
    Snapshot snapshot = table.currentSnapshot();

    // Compute partition stats
    PartitionStatisticsFile statsFile =
        PartitionStatsManager.computeAndWriteStatsFile(
            table, snapshot.snapshotId(), snapshot, dataFiles, deleteFiles);

    assertThat(statsFile).isNotNull();
    assertThat(statsFile.snapshotId()).isEqualTo(snapshot.snapshotId());
    assertThat(statsFile.path()).contains("partition-stats");
    assertThat(statsFile.fileSizeInBytes()).isGreaterThan(0);
  }

  @Test
  public void testComputeStatsWithDeleteFiles() {
    // Create data file and delete file for same partition
    DataFile dataFile = createDataFile("2024-01-01", 100, 1000);
    DeleteFile deleteFile = createDeleteFile("2024-01-01", 10, 100);

    List<DataFile> dataFiles = ImmutableList.of(dataFile);
    List<DeleteFile> deleteFiles = ImmutableList.of(deleteFile);

    // Commit files
    table.newRowDelta().addRows(dataFile).addDeletes(deleteFile).commit();
    Snapshot snapshot = table.currentSnapshot();

    // Compute partition stats
    PartitionStatisticsFile statsFile =
        PartitionStatsManager.computeAndWriteStatsFile(
            table, snapshot.snapshotId(), snapshot, dataFiles, deleteFiles);

    assertThat(statsFile).isNotNull();
  }

  @Test
  public void testReturnsNullForUnpartitionedTable() {
    // Create unpartitioned table
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

    // Should return null for unpartitioned table
    PartitionStatisticsFile statsFile =
        PartitionStatsManager.computeAndWriteStatsFile(
            unpartTable,
            snapshot.snapshotId(),
            snapshot,
            ImmutableList.of(dataFile),
            ImmutableList.of());

    assertThat(statsFile).isNull();
  }

  @Test
  public void testReturnsNullForEmptyFiles() {
    DataFile dataFile = createDataFile("2024-01-01", 100, 1000);
    table.newAppend().appendFile(dataFile).commit();
    Snapshot snapshot = table.currentSnapshot();

    // Empty files list should return null
    PartitionStatisticsFile statsFile =
        PartitionStatsManager.computeAndWriteStatsFile(
            table, snapshot.snapshotId(), snapshot, ImmutableList.of(), ImmutableList.of());

    assertThat(statsFile).isNull();
  }

  @Test
  public void testIncrementalUpdate() {
    // First commit
    DataFile file1 = createDataFile("2024-01-01", 100, 1000);
    table.newAppend().appendFile(file1).commit();
    Snapshot snapshot1 = table.currentSnapshot();

    // Compute initial stats
    PartitionStatisticsFile statsFile1 =
        PartitionStatsManager.computeAndWriteStatsFile(
            table, snapshot1.snapshotId(), snapshot1, ImmutableList.of(file1), ImmutableList.of());

    assertThat(statsFile1).isNotNull();

    // Register stats file with table
    table.updatePartitionStatistics().setPartitionStatistics(statsFile1).commit();

    // Second commit - add more data to existing partition
    DataFile file2 = createDataFile("2024-01-01", 200, 2000);
    table.newAppend().appendFile(file2).commit();
    Snapshot snapshot2 = table.currentSnapshot();

    // Compute incremental stats - should merge with existing
    PartitionStatisticsFile statsFile2 =
        PartitionStatsManager.computeAndWriteStatsFile(
            table, snapshot2.snapshotId(), snapshot2, ImmutableList.of(file2), ImmutableList.of());

    assertThat(statsFile2).isNotNull();
    assertThat(statsFile2.snapshotId()).isEqualTo(snapshot2.snapshotId());
    // The file size should reflect accumulated stats
    assertThat(statsFile2.fileSizeInBytes()).isGreaterThan(0);
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
