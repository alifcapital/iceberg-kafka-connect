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

import io.tabular.iceberg.connect.channel.UUIDv7;
import io.tabular.iceberg.connect.IcebergSinkConfig;
import java.time.OffsetDateTime;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.UUID;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.events.DataComplete;
import org.apache.iceberg.connect.events.DataWritten;
import org.apache.iceberg.connect.events.TopicPartitionOffset;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommitState {
  private static final Logger LOG = LoggerFactory.getLogger(CommitState.class);

  private final List<Envelope> commitBuffer = new LinkedList<>();
  private final List<DataComplete> readyBuffer = new LinkedList<>();
  private long startTime;
  private UUID currentCommitId;
  private final IcebergSinkConfig config;

  private final Comparator<OffsetDateTime> dateTimeComparator =
          OffsetDateTime::compareTo;

  public CommitState(IcebergSinkConfig config) {
    this.config = config;
  }

  public void addResponse(Envelope envelope) {
    commitBuffer.add(envelope);
    if (!isCommitInProgress()) {
      LOG.debug(
          "Received data written with commit-id={} when no commit in progress, this can happen during recovery",
          ((DataWritten) envelope.event().payload()).commitId());
    }
  }

  public void addReady(Envelope envelope) {
    readyBuffer.add((DataComplete) envelope.event().payload());
    if (!isCommitInProgress()) {
      LOG.debug(
          "Received data complete for commit-id={} when no commit in progress, this can happen during recovery",
          ((DataComplete) envelope.event().payload()).commitId());
    }
  }

  public long getStartTime() {
    return startTime;
  }
  public UUID currentCommitId() {
    return currentCommitId;
  }

  public boolean isCommitInProgress() {
    return currentCommitId != null;
  }

  public boolean isCommitIntervalReached() {
    if (startTime == 0) {
      // add delay to avoid all nodes starting a commit at the same time
      startTime = System.currentTimeMillis() - (long)(Math.random() * config.commitIntervalMs());
    }

    return (!isCommitInProgress()
        && System.currentTimeMillis() - startTime >= config.commitIntervalMs());
  }

  public void startNewCommit() {
    currentCommitId = UUIDv7.randomUUID();
    startTime = System.currentTimeMillis();
  }

  public void endCurrentCommit() {
    readyBuffer.clear();
    currentCommitId = null;
  }

  public void clearResponses() {
    commitBuffer.clear();
  }

  public boolean isCommitTimedOut() {
    if (!isCommitInProgress()) {
      return false;
    }

    long currentTime = System.currentTimeMillis();
    if (currentTime - startTime > config.commitTimeoutMs()) {
      LOG.info("Commit timeout reached. Now: {}, start: {}, timeout: {}", currentTime, startTime, config.commitTimeoutMs());
      return true;
    }
    return false;
  }

  public boolean isCommitReady(int expectedPartitionCount) {
    if (!isCommitInProgress()) {
      return false;
    }

    int receivedPartitionCount =
        readyBuffer.stream()
            .filter(payload -> payload.commitId().equals(currentCommitId))
            .mapToInt(payload -> payload.assignments().size())
            .sum();

    if (receivedPartitionCount >= expectedPartitionCount) {
      LOG.info(
          "Commit {} ready, received responses for all {} partitions",
          currentCommitId,
          receivedPartitionCount);
      return true;
    }

    LOG.info(
        "Commit {} not ready, received responses for {} of {} partitions, waiting for more",
        currentCommitId,
        receivedPartitionCount,
        expectedPartitionCount);

    return false;
  }

  /**
   * Tokenize the input envelope list when we get an envelope with an Equality Delete File. This
   * will make sure that one commit will have at max one Envelope with Equality Delete. Used to
   * handle iceberg commit issues arising from multiple envelopes having CDC records of the same
   * original record.If an equality delete in Envelope E1 has a corresponding Insert record in E0 we
   * don't want them to be part of same commit because in Iceberg, an Equality delete D1's range for
   * finding insert records is commits which are strictly earlier than the commit of D1.
   *
   * @param list Envelope List
   * @return List of Envelope Lists
   */
  public List<List<Envelope>> tokenize(List<Envelope> list) {
    List<List<Envelope>> tokenized = Lists.newArrayList();
    List<Envelope> tempList = new LinkedList<>();
    list.forEach(
        envelope -> {
          boolean checkEqualityDeletes =
              ((DataWritten) envelope.event().payload())
                  .deleteFiles().stream()
                      .anyMatch(x -> x.content() == FileContent.EQUALITY_DELETES);
          if (checkEqualityDeletes) {
            // create a list containing only one envelope having eq delete files
            if (!tempList.isEmpty()) {
              tokenized.add(List.copyOf(tempList));
              tempList.clear();
            }
            tempList.add(envelope);
            tokenized.add(List.copyOf(tempList));
            tempList.clear();
            return;
          }
          tempList.add(envelope);
        });
    if (!tempList.isEmpty()) {
      tokenized.add(List.copyOf(tempList));
    }
    return tokenized;
  }

  public Map<TableIdentifier, List<List<Envelope>>> tableCommitMap() {
    Map<TableIdentifier, List<Envelope>> tempCommitMap =
        commitBuffer.stream()
            .collect(
                groupingBy(
                    envelope ->
                      ((DataWritten) envelope.event().payload())
                          .tableReference()
                          .identifier()));
    Map<TableIdentifier, List<List<Envelope>>> commitMap = Maps.newHashMap();
    tempCommitMap.forEach((k, v) -> commitMap.put(k, tokenize(v)));
    return commitMap;
  }

  public OffsetDateTime vtts(boolean partialCommit) {
    boolean validVtts =
        !partialCommit
            && readyBuffer.stream()
                .flatMap(event -> event.assignments().stream())
                .allMatch(offset -> offset.timestamp() != null);

      OffsetDateTime result;
      if (validVtts) {
          Optional<OffsetDateTime> maybeResult =
                  readyBuffer.stream()
                          .flatMap(event -> event.assignments().stream())
                          .map(TopicPartitionOffset::timestamp)
                          .min(dateTimeComparator);
          if (maybeResult.isPresent()) {
              result = maybeResult.get();
          } else {
              throw new NoSuchElementException("no vtts found");
          }
    } else {
      result = null;
    }
    return result;
  }
}

