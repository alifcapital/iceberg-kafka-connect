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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.UnpartitionedWriter;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.Tasks;
import org.apache.iceberg.util.ThreadPools;
import org.apache.iceberg.connect.events.Event;
import org.apache.iceberg.connect.events.StartCommit;
import org.apache.iceberg.connect.events.CommitComplete;
import org.apache.iceberg.connect.events.CommitToTable;
import org.apache.iceberg.connect.events.DataWritten;
import org.apache.iceberg.connect.events.TableReference;
import org.apache.kafka.clients.admin.MemberDescription;
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

    // initial poll with longer duration so the consumer will initialize...
    consumeAvailable(Duration.ofMillis(1000), this::receive);
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
    String db = null;
    if (!commitMap.isEmpty()) {
        db = commitMap.entrySet().iterator().next().getKey().namespace().toString();
    }

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

    if (db != null && !partialCommit) {
      logWatermark(db);
    }

    LOG.info(
        "Commit {} complete, committed to {} table(s), vtts {}",
        commitState.currentCommitId(),
        commitMap.size(),
        vtts);
  }

  private void logWatermark(String db) {
    if (db == null) {
        LOG.warn("No database name available, skipping watermark logging");
        return;
    }

    UnpartitionedWriter<Record> writer = null;
    boolean succeeded = false;

    try {
        TableIdentifier watermarkTable = TableIdentifier.of("meta", "watermarks");
        Table table;

        try {
            table = catalog.loadTable(watermarkTable);
            LOG.debug("Found existing watermarks table");
        } catch (NoSuchTableException e) {
            LOG.info("Creating watermarks table in meta namespace");
            org.apache.iceberg.Schema schema = new org.apache.iceberg.Schema(
                Types.NestedField.required(1, "db", Types.StringType.get()),
                Types.NestedField.required(2, "commit_start_time", Types.LongType.get()),
                Types.NestedField.required(3, "commit_id", Types.StringType.get())
            );

            table = catalog.createTable(
                watermarkTable,
                schema,
                PartitionSpec.unpartitioned(),
                ImmutableMap.of()
            );
        }

        OutputFileFactory fileFactory = OutputFileFactory.builderFor(table, 1, System.currentTimeMillis())
            .defaultSpec(table.spec())
            .operationId(UUID.randomUUID().toString())
            .format(FileFormat.PARQUET)
            .build();

        GenericAppenderFactory appenderFactory = new GenericAppenderFactory(table.schema());
        writer = new UnpartitionedWriter<>(
            table.spec(),
            FileFormat.PARQUET,
            appenderFactory,
            fileFactory,
            table.io(),
            2*1024*1024
        );

        Record record = GenericRecord.create(table.schema());
        record.setField("db", db);
        record.setField("commit_start_time", commitState.getStartTime());
        record.setField("commit_id", commitState.currentCommitId().toString());

        writer.write(record);
        DataFile[] dataFiles = writer.dataFiles();
        writer.close();
        writer = null;  // prevent double-cleanup in finally block

        AppendFiles appendFiles = table.newAppend();
        for (DataFile dataFile : dataFiles) {
            appendFiles.appendFile(dataFile);
        }
        appendFiles.commit();

        succeeded = true;
        LOG.info("Successfully logged watermark for db={} commit_id={} commit_start_time={}",
            db, commitState.currentCommitId(), commitState.getStartTime());

    } catch (Throwable t) {
        LOG.error("Failed to write watermark for db={} commit_id={}",
            db, commitState.currentCommitId(), t);
    } finally {
        if (writer != null) {
            try {
                if (succeeded) {
                    writer.close();
                } else {
                    writer.abort();
                }
            } catch (IOException e) {
                LOG.warn("Failed to cleanup writer", e);
            }
        }
    }
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
}
