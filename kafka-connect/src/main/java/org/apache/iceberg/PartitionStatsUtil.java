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
package org.apache.iceberg;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Queue;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Queues;
import org.apache.iceberg.types.Types.StructType;
import org.apache.iceberg.util.PartitionMap;
import org.apache.iceberg.util.PartitionUtil;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.iceberg.util.Tasks;
import org.apache.iceberg.util.ThreadPools;

/**
 * Utility class to access package-private PartitionStats methods.
 * This class must be in org.apache.iceberg package to access package-private API.
 *
 * Copied from Iceberg's PartitionStatsHandler for incremental partition stats computation.
 */
public class PartitionStatsUtil {

  private PartitionStatsUtil() {}

  /**
   * Computes incremental stats diff between two snapshots.
   * Exactly matches Iceberg's PartitionStatsHandler.computeStatsDiff logic.
   */
  public static PartitionMap<PartitionStats> computeStatsDiff(
      Table table, Snapshot fromSnapshot, Snapshot toSnapshot) {
    Iterable<Snapshot> snapshots =
        SnapshotUtil.ancestorsBetween(
            toSnapshot.snapshotId(), fromSnapshot.snapshotId(), table::snapshot);
    // DELETED manifest entries are not carried over to subsequent snapshots.
    // So, for incremental computation, gather the manifests added by each snapshot
    // instead of relying solely on those from the latest snapshot.
    List<ManifestFile> manifests =
        StreamSupport.stream(snapshots.spliterator(), false)
            .flatMap(
                snapshot ->
                    snapshot.allManifests(table.io()).stream()
                        .filter(file -> file.snapshotId().equals(snapshot.snapshotId())))
            .collect(Collectors.toList());

    return computeStats(table, manifests, true /* incremental */);
  }

  /**
   * Computes stats from manifests.
   * Exactly matches Iceberg's PartitionStatsHandler.computeStats logic.
   */
  public static PartitionMap<PartitionStats> computeStats(
      Table table, List<ManifestFile> manifests, boolean incremental) {
    StructType partitionType = Partitioning.partitionType(table);
    Queue<PartitionMap<PartitionStats>> statsByManifest = Queues.newConcurrentLinkedQueue();
    Tasks.foreach(manifests)
        .stopOnFailure()
        .throwFailureWhenFinished()
        .executeWith(ThreadPools.getWorkerPool())
        .run(
            manifest ->
                statsByManifest.add(
                    collectStatsForManifest(table, manifest, partitionType, incremental)));

    PartitionMap<PartitionStats> statsMap = PartitionMap.create(table.specs());
    for (PartitionMap<PartitionStats> stats : statsByManifest) {
      mergePartitionMap(stats, statsMap);
    }

    return statsMap;
  }

  private static PartitionMap<PartitionStats> collectStatsForManifest(
      Table table, ManifestFile manifest, StructType partitionType, boolean incremental) {
    List<String> projection = BaseScan.scanColumns(manifest.content());
    try (ManifestReader<?> reader = ManifestFiles.open(manifest, table.io()).select(projection)) {
      PartitionMap<PartitionStats> statsMap = PartitionMap.create(table.specs());
      int specId = manifest.partitionSpecId();
      PartitionSpec spec = table.specs().get(specId);
      PartitionData keyTemplate = new PartitionData(partitionType);

      for (ManifestEntry<?> entry : reader.entries()) {
        ContentFile<?> file = entry.file();
        StructLike coercedPartition =
            PartitionUtil.coercePartition(partitionType, spec, file.partition());
        StructLike key = keyTemplate.copyFor(coercedPartition);
        Snapshot snapshot = table.snapshot(entry.snapshotId());
        PartitionStats stats =
            statsMap.computeIfAbsent(
                specId,
                ((PartitionData) file.partition()).copy(),
                () -> new PartitionStats(key, specId));
        if (entry.isLive()) {
          // Live can have both added and existing entries. Consider only added entries for
          // incremental compute as existing entries was already included in previous compute.
          if (!incremental || entry.status() == ManifestEntry.Status.ADDED) {
            stats.liveEntry(file, snapshot);
          }
        } else {
          if (incremental) {
            stats.deletedEntryForIncrementalCompute(file, snapshot);
          } else {
            stats.deletedEntry(snapshot);
          }
        }
      }

      return statsMap;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static void mergePartitionMap(
      PartitionMap<PartitionStats> fromMap, PartitionMap<PartitionStats> toMap) {
    fromMap.forEach(
        (key, value) ->
            toMap.merge(
                key,
                value,
                (existingEntry, newEntry) -> {
                  existingEntry.appendStats(newEntry);
                  return existingEntry;
                }));
  }

  /**
   * Reads partition stats from a stats file.
   * Uses InternalData reader for format-agnostic reading.
   */
  public static CloseableIterable<PartitionStats> readPartitionStatsFile(
      Schema schema, Table table, String path) {
    FileFormat fileFormat = FileFormat.fromFileName(path);
    CloseableIterable<StructLike> records =
        InternalData.read(fileFormat, table.io().newInputFile(path)).project(schema).build();
    return CloseableIterable.transform(records, PartitionStatsUtil::recordToPartitionStats);
  }

  private static PartitionStats recordToPartitionStats(StructLike record) {
    int pos = 0;
    PartitionStats stats =
        new PartitionStats(
            record.get(pos++, StructLike.class), // partition
            record.get(pos++, Integer.class)); // spec id
    for (; pos < record.size(); pos++) {
      stats.set(pos, record.get(pos, Object.class));
    }

    return stats;
  }
}
