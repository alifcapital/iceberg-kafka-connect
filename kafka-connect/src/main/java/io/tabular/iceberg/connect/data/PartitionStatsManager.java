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

import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT_DEFAULT;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.ImmutableGenericPartitionStatisticsFile;
import org.apache.iceberg.InternalData;
import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionStatisticsFile;
import org.apache.iceberg.PartitionStats;
import org.apache.iceberg.Partitioning;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Types.IntegerType;
import org.apache.iceberg.types.Types.LongType;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StructType;
import org.apache.iceberg.util.PartitionMap;
import org.apache.iceberg.util.PartitionUtil;
import org.apache.iceberg.util.SnapshotUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages incremental partition statistics updates during commits.
 *
 * <p>Logic:
 * <ul>
 *   <li>If no previous stats file exists - log warning and skip (run compute_partition_stats first)
 *   <li>If stats file is from direct parent snapshot - use delta from dataFiles/deleteFiles (fast path)
 *   <li>If stats file is from older snapshot - incremental compute via manifests between snapshots
 * </ul>
 */
public class PartitionStatsManager {

  private static final Logger LOG = LoggerFactory.getLogger(PartitionStatsManager.class);

  // Schema field IDs for partition stats (matching Iceberg spec)
  private static final int PARTITION_FIELD_ID = 1;
  private static final String PARTITION_FIELD_NAME = "partition";
  private static final NestedField SPEC_ID = NestedField.required(2, "spec_id", IntegerType.get());
  private static final NestedField DATA_RECORD_COUNT =
      NestedField.required(3, "data_record_count", LongType.get());
  private static final NestedField DATA_FILE_COUNT =
      NestedField.required(4, "data_file_count", IntegerType.get());
  private static final NestedField TOTAL_DATA_FILE_SIZE_IN_BYTES =
      NestedField.required(5, "total_data_file_size_in_bytes", LongType.get());
  private static final NestedField POSITION_DELETE_RECORD_COUNT =
      NestedField.optional(6, "position_delete_record_count", LongType.get());
  private static final NestedField POSITION_DELETE_FILE_COUNT =
      NestedField.optional(7, "position_delete_file_count", IntegerType.get());
  private static final NestedField EQUALITY_DELETE_RECORD_COUNT =
      NestedField.optional(8, "equality_delete_record_count", LongType.get());
  private static final NestedField EQUALITY_DELETE_FILE_COUNT =
      NestedField.optional(9, "equality_delete_file_count", IntegerType.get());
  private static final NestedField TOTAL_RECORD_COUNT =
      NestedField.optional(10, "total_record_count", LongType.get());
  private static final NestedField LAST_UPDATED_AT =
      NestedField.optional(11, "last_updated_at", LongType.get());
  private static final NestedField LAST_UPDATED_SNAPSHOT_ID =
      NestedField.optional(12, "last_updated_snapshot_id", LongType.get());

  private PartitionStatsManager() {}

  /**
   * Generates the partition stats schema.
   */
  public static Schema schema(StructType partitionType) {
    return new Schema(
        NestedField.required(PARTITION_FIELD_ID, PARTITION_FIELD_NAME, partitionType),
        SPEC_ID,
        DATA_RECORD_COUNT,
        DATA_FILE_COUNT,
        TOTAL_DATA_FILE_SIZE_IN_BYTES,
        POSITION_DELETE_RECORD_COUNT,
        POSITION_DELETE_FILE_COUNT,
        EQUALITY_DELETE_RECORD_COUNT,
        EQUALITY_DELETE_FILE_COUNT,
        TOTAL_RECORD_COUNT,
        LAST_UPDATED_AT,
        LAST_UPDATED_SNAPSHOT_ID);
  }

  /**
   * Computes and writes partition statistics file.
   *
   * @param table the table to update stats for
   * @param currentSnapshot the current snapshot (after data commit)
   * @param dataFiles data files added in this commit (used for fast path)
   * @param deleteFiles delete files added in this commit (used for fast path)
   * @return the new partition statistics file, or null if skipped
   */
  public static PartitionStatisticsFile computeAndWriteStatsFile(
      Table table,
      Snapshot currentSnapshot,
      List<DataFile> dataFiles,
      List<DeleteFile> deleteFiles) {

    if (!Partitioning.isPartitioned(table)) {
      LOG.debug("Table {} is not partitioned, skipping partition stats", table.name());
      return null;
    }

    if (dataFiles.isEmpty() && deleteFiles.isEmpty()) {
      LOG.debug("No files to commit for table {}, skipping partition stats", table.name());
      return null;
    }

    long currentSnapshotId = currentSnapshot.snapshotId();

    // Find latest stats file in ancestry
    StatsFileInfo statsInfo = findLatestStatsFile(table, currentSnapshotId);
    if (statsInfo == null) {
      LOG.warn(
          "No partition stats file found for table {}. "
              + "Run CALL system.compute_partition_stats('{}') to initialize.",
          table.name(),
          table.name());
      return null;
    }

    StructType partitionType = Partitioning.partitionType(table);
    PartitionMap<PartitionStats> statsMap = PartitionMap.create(table.specs());

    // Load existing stats
    loadExistingStats(table, statsInfo.statsFile, partitionType, statsMap);

    // Apply updates based on whether stats file is from direct parent or older
    if (statsInfo.isDirectParent) {
      // Fast path: stats file is from parent snapshot, just add our delta
      LOG.debug("Using fast path - stats file is from direct parent snapshot");
      applyFileDelta(table, statsMap, partitionType, dataFiles, deleteFiles, currentSnapshot);
    } else {
      // Slow path: need to scan manifests between stats snapshot and current
      LOG.debug(
          "Using incremental path - scanning manifests from snapshot {} to {}",
          statsInfo.statsFile.snapshotId(),
          currentSnapshotId);
      applyIncrementalFromManifests(
          table, statsMap, partitionType, statsInfo.statsFile.snapshotId(), currentSnapshot);
    }

    if (statsMap.isEmpty()) {
      return null;
    }

    // Sort and write
    List<PartitionStats> sortedStats = sortStats(statsMap.values(), partitionType);
    Schema statsSchema = schema(partitionType);

    try {
      return writeStatsFile(table, currentSnapshotId, statsSchema, sortedStats);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to write partition stats file", e);
    }
  }

  private static class StatsFileInfo {
    final PartitionStatisticsFile statsFile;
    final boolean isDirectParent;

    StatsFileInfo(PartitionStatisticsFile statsFile, boolean isDirectParent) {
      this.statsFile = statsFile;
      this.isDirectParent = isDirectParent;
    }
  }

  private static StatsFileInfo findLatestStatsFile(Table table, long currentSnapshotId) {
    List<PartitionStatisticsFile> statsFiles = table.partitionStatisticsFiles();
    if (statsFiles.isEmpty()) {
      return null;
    }

    Map<Long, PartitionStatisticsFile> statsById =
        statsFiles.stream()
            .collect(Collectors.toMap(PartitionStatisticsFile::snapshotId, f -> f, (a, b) -> a));

    Snapshot currentSnapshot = table.snapshot(currentSnapshotId);
    Long parentId = currentSnapshot != null ? currentSnapshot.parentId() : null;

    // Check if stats file exists for direct parent (fast path)
    if (parentId != null && statsById.containsKey(parentId)) {
      return new StatsFileInfo(statsById.get(parentId), true);
    }

    // Search ancestry for stats file
    for (Snapshot snap : SnapshotUtil.ancestorsOf(currentSnapshotId, table::snapshot)) {
      if (statsById.containsKey(snap.snapshotId())) {
        return new StatsFileInfo(statsById.get(snap.snapshotId()), false);
      }
    }

    return null;
  }

  private static void loadExistingStats(
      Table table,
      PartitionStatisticsFile statsFile,
      StructType partitionType,
      PartitionMap<PartitionStats> statsMap) {

    Schema statsSchema = schema(partitionType);
    FileFormat format = FileFormat.fromFileName(statsFile.path());

    if (format == null) {
      LOG.warn("Unable to determine format of stats file: {}", statsFile.path());
      return;
    }

    try (CloseableIterable<StructLike> records =
        InternalData.read(format, table.io().newInputFile(statsFile.path()))
            .project(statsSchema)
            .build()) {

      for (StructLike record : records) {
        PartitionStats stats = recordToPartitionStats(record);
        statsMap.put(stats.specId(), stats.partition(), stats);
      }

      LOG.debug(
          "Loaded {} existing partition stats entries from {}",
          statsMap.size(),
          statsFile.path());

    } catch (Exception e) {
      LOG.warn(
          "Failed to load existing partition stats from {}: {}",
          statsFile.path(),
          e.getMessage());
      throw new UncheckedIOException(
          new IOException("Failed to load partition stats", e));
    }
  }

  private static PartitionStats recordToPartitionStats(StructLike record) {
    int pos = 0;
    PartitionStats stats =
        new PartitionStats(
            record.get(pos++, StructLike.class), // partition
            record.get(pos++, Integer.class)); // spec id

    // Set remaining fields
    for (; pos < record.size(); pos++) {
      stats.set(pos, record.get(pos, Object.class));
    }

    return stats;
  }

  /**
   * Fast path: apply delta directly from known files.
   */
  private static void applyFileDelta(
      Table table,
      PartitionMap<PartitionStats> statsMap,
      StructType partitionType,
      List<DataFile> dataFiles,
      List<DeleteFile> deleteFiles,
      Snapshot snapshot) {

    for (DataFile file : dataFiles) {
      applyFileToStats(table, statsMap, partitionType, file, snapshot);
    }

    for (DeleteFile file : deleteFiles) {
      applyFileToStats(table, statsMap, partitionType, file, snapshot);
    }
  }

  /**
   * Slow path: scan manifests between snapshots to compute incremental stats.
   */
  private static void applyIncrementalFromManifests(
      Table table,
      PartitionMap<PartitionStats> statsMap,
      StructType partitionType,
      long fromSnapshotId,
      Snapshot toSnapshot) {

    FileIO io = table.io();
    Map<Integer, PartitionSpec> specs = table.specs();

    // Get snapshots between fromSnapshot (exclusive) and toSnapshot (inclusive)
    Iterable<Snapshot> snapshots =
        SnapshotUtil.ancestorsBetween(toSnapshot.snapshotId(), fromSnapshotId, table::snapshot);

    // Collect manifests added by each snapshot in the range
    List<ManifestFile> manifests = Lists.newArrayList();
    for (Snapshot snapshot : snapshots) {
      for (ManifestFile manifest : snapshot.allManifests(io)) {
        // Only include manifests added by this snapshot
        if (manifest.snapshotId() != null && manifest.snapshotId().equals(snapshot.snapshotId())) {
          manifests.add(manifest);
        }
      }
    }

    for (ManifestFile manifest : manifests) {
      if (manifest.content() == ManifestContent.DATA) {
        try (ManifestReader<DataFile> reader =
            ManifestFiles.read(manifest, io, specs).select(Lists.newArrayList("*"))) {
          for (DataFile file : reader) {
            applyFileToStats(table, statsMap, partitionType, file, toSnapshot);
          }
        } catch (IOException e) {
          throw new UncheckedIOException("Failed to read manifest: " + manifest.path(), e);
        }
      } else {
        try (ManifestReader<DeleteFile> reader =
            ManifestFiles.readDeleteManifest(manifest, io, specs)
                .select(Lists.newArrayList("*"))) {
          for (DeleteFile file : reader) {
            applyFileToStats(table, statsMap, partitionType, file, toSnapshot);
          }
        } catch (IOException e) {
          throw new UncheckedIOException("Failed to read delete manifest: " + manifest.path(), e);
        }
      }
    }
  }

  private static void applyFileToStats(
      Table table,
      PartitionMap<PartitionStats> statsMap,
      StructType partitionType,
      ContentFile<?> file,
      Snapshot snapshot) {

    int specId = file.specId();
    PartitionSpec spec = table.specs().get(specId);

    StructLike coercedPartition =
        PartitionUtil.coercePartition(partitionType, spec, file.partition());

    PartitionStats stats =
        statsMap.computeIfAbsent(
            specId,
            ((PartitionData) file.partition()).copy(),
            () -> new PartitionStats(coercedPartition, specId));

    FileContent content = file.content();
    switch (content) {
      case DATA:
        stats.set(2, stats.dataRecordCount() + file.recordCount());
        stats.set(3, stats.dataFileCount() + 1);
        stats.set(4, stats.totalDataFileSizeInBytes() + file.fileSizeInBytes());
        break;

      case POSITION_DELETES:
        stats.set(5, stats.positionDeleteRecordCount() + file.recordCount());
        stats.set(6, stats.positionDeleteFileCount() + 1);
        break;

      case EQUALITY_DELETES:
        stats.set(7, stats.equalityDeleteRecordCount() + file.recordCount());
        stats.set(8, stats.equalityDeleteFileCount() + 1);
        break;

      default:
        LOG.warn("Unknown file content type: {}", content);
    }

    if (snapshot != null) {
      Long currentLastUpdated = stats.lastUpdatedAt();
      if (currentLastUpdated == null || currentLastUpdated < snapshot.timestampMillis()) {
        stats.set(10, snapshot.timestampMillis());
        stats.set(11, snapshot.snapshotId());
      }
    }
  }

  /**
   * Computes partition stats from scratch by scanning all manifests.
   * For testing only - verifies incremental computation matches full recomputation.
   */
  @VisibleForTesting
  public static PartitionStatisticsFile computeFullStats(Table table, Snapshot snapshot)
      throws IOException {
    if (!Partitioning.isPartitioned(table)) {
      return null;
    }

    if (snapshot == null) {
      return null;
    }

    StructType partitionType = Partitioning.partitionType(table);
    PartitionMap<PartitionStats> statsMap = PartitionMap.create(table.specs());
    FileIO io = table.io();
    Map<Integer, PartitionSpec> specs = table.specs();

    // Scan all manifests in the snapshot
    for (ManifestFile manifest : snapshot.allManifests(io)) {
      if (manifest.content() == ManifestContent.DATA) {
        try (ManifestReader<DataFile> reader =
            ManifestFiles.read(manifest, io, specs).select(Lists.newArrayList("*"))) {
          for (DataFile file : reader) {
            applyFileToStats(table, statsMap, partitionType, file, snapshot);
          }
        }
      } else {
        try (ManifestReader<DeleteFile> reader =
            ManifestFiles.readDeleteManifest(manifest, io, specs)
                .select(Lists.newArrayList("*"))) {
          for (DeleteFile file : reader) {
            applyFileToStats(table, statsMap, partitionType, file, snapshot);
          }
        }
      }
    }

    if (statsMap.isEmpty()) {
      return null;
    }

    List<PartitionStats> sortedStats = sortStats(statsMap.values(), partitionType);
    Schema statsSchema = schema(partitionType);
    return writeStatsFile(table, snapshot.snapshotId(), statsSchema, sortedStats);
  }

  private static List<PartitionStats> sortStats(
      Collection<PartitionStats> stats, StructType partitionType) {
    List<PartitionStats> sorted = Lists.newArrayList(stats);
    sorted.sort(
        Comparator.comparing(PartitionStats::partition, Comparators.forType(partitionType)));
    return sorted;
  }

  private static PartitionStatisticsFile writeStatsFile(
      Table table, long snapshotId, Schema schema, Iterable<PartitionStats> stats)
      throws IOException {

    FileFormat format =
        FileFormat.fromString(
            table.properties().getOrDefault(DEFAULT_FILE_FORMAT, DEFAULT_FILE_FORMAT_DEFAULT));

    OutputFile outputFile = newStatsFile(table, format, snapshotId);

    try (FileAppender<StructLike> writer =
        InternalData.write(format, outputFile).schema(schema).build()) {
      for (PartitionStats stat : stats) {
        writer.add(stat);
      }
    }

    LOG.info("Wrote partition stats file {} for snapshot {}", outputFile.location(), snapshotId);

    return ImmutableGenericPartitionStatisticsFile.builder()
        .snapshotId(snapshotId)
        .path(outputFile.location())
        .fileSizeInBytes(outputFile.toInputFile().getLength())
        .build();
  }

  private static OutputFile newStatsFile(Table table, FileFormat format, long snapshotId) {
    if (!(table instanceof HasTableOperations)) {
      throw new IllegalArgumentException(
          "Table must have operations to retrieve metadata location");
    }

    String fileName =
        format.addExtension(
            String.format(Locale.ROOT, "partition-stats-%d-%s", snapshotId, UUID.randomUUID()));

    return table
        .io()
        .newOutputFile(((HasTableOperations) table).operations().metadataFileLocation(fileName));
  }
}
