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

import static org.assertj.core.api.Assertions.assertThat;

import io.tabular.iceberg.connect.events.DataOffsetsPayload;
import io.tabular.iceberg.connect.events.Event;
import io.tabular.iceberg.connect.events.EventType;
import io.tabular.iceberg.connect.events.TableName;
import io.tabular.iceberg.connect.events.TopicPartitionOffset;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

public class CoordinatorHelpersTest {

  private static final TableIdentifier T1 = TableIdentifier.parse("db.t1");
  private static final TableIdentifier T2 = TableIdentifier.parse("db.t2");
  private static final TopicPartition TP1 = new TopicPartition("topic1", 0);
  private static final TopicPartition TP2 = new TopicPartition("topic2", 0);

  @Test
  public void mergeKnownAndActiveAddsActiveOnlyTables() {
    Map<TableIdentifier, TopicPartition> known = ImmutableMap.of(T1, TP1);
    Map<TableIdentifier, Map<TopicPartition, TopicPartitionOffset>> active =
        ImmutableMap.of(T2, ImmutableMap.of(TP2, tpo(TP2, 5L, 1000L)));

    Map<TableIdentifier, TopicPartition> result = Coordinator.mergeKnownAndActive(known, active);

    assertThat(result).containsEntry(T1, TP1).containsEntry(T2, TP2).hasSize(2);
  }

  @Test
  public void mergeKnownAndActiveKeepsKnownTpForTablesAlreadyKnown() {
    Map<TableIdentifier, TopicPartition> known = ImmutableMap.of(T1, TP1);
    Map<TableIdentifier, Map<TopicPartition, TopicPartitionOffset>> active =
        ImmutableMap.of(T1, ImmutableMap.of(TP1, tpo(TP1, 9L, 2000L)));

    Map<TableIdentifier, TopicPartition> result = Coordinator.mergeKnownAndActive(known, active);

    assertThat(result).containsExactlyEntriesOf(ImmutableMap.of(T1, TP1));
  }

  @Test
  public void mergeKnownAndActiveSkipsActiveTableWithEmptyOffsets() {
    Map<TableIdentifier, Map<TopicPartition, TopicPartitionOffset>> active =
        ImmutableMap.of(T1, Collections.emptyMap());

    Map<TableIdentifier, TopicPartition> result =
        Coordinator.mergeKnownAndActive(Collections.emptyMap(), active);

    assertThat(result).isEmpty();
  }

  @Test
  public void pickActiveOffsetReturnsExactTpMatch() {
    TopicPartitionOffset expected = tpo(TP1, 100L, 1700_000L);
    Map<TopicPartition, TopicPartitionOffset> tableActive = ImmutableMap.of(TP1, expected);

    assertThat(Coordinator.pickActiveOffset(tableActive, TP1)).isSameAs(expected);
  }

  @Test
  public void pickActiveOffsetReturnsNullWhenActiveMapEmptyOrNull() {
    assertThat(Coordinator.pickActiveOffset(null, TP1)).isNull();
    assertThat(Coordinator.pickActiveOffset(Collections.emptyMap(), TP1)).isNull();
  }

  @Test
  public void pickActiveOffsetFallsBackToFirstWhenTpMissing() {
    TopicPartitionOffset other = tpo(TP2, 100L, 1700_000L);
    Map<TopicPartition, TopicPartitionOffset> tableActive = ImmutableMap.of(TP2, other);

    assertThat(Coordinator.pickActiveOffset(tableActive, TP1)).isSameAs(other);
  }

  @Test
  public void collectActiveOffsetsBuildsPerTablePerTpMap() {
    Envelope env =
        envelopeWith(
            UUID.randomUUID(),
            T1,
            ImmutableList.of(tpo(TP1, 10L, 1000L), tpo(TP2, 20L, 2000L)));

    Map<TableIdentifier, Map<TopicPartition, TopicPartitionOffset>> result =
        Coordinator.collectActiveOffsets(ImmutableList.of(env));

    assertThat(result).containsOnlyKeys(T1);
    assertThat(result.get(T1)).containsOnlyKeys(TP1, TP2);
    assertThat(result.get(T1).get(TP1).offset()).isEqualTo(10L);
    assertThat(result.get(T1).get(TP2).offset()).isEqualTo(20L);
  }

  @Test
  public void collectActiveOffsetsKeepsHighestOffsetForRepeatedTp() {
    UUID commitId = UUID.randomUUID();
    Envelope a = envelopeWith(commitId, T1, ImmutableList.of(tpo(TP1, 10L, 1000L)));
    Envelope b = envelopeWith(commitId, T1, ImmutableList.of(tpo(TP1, 25L, 2500L)));
    Envelope c = envelopeWith(commitId, T1, ImmutableList.of(tpo(TP1, 15L, 1500L)));

    Map<TableIdentifier, Map<TopicPartition, TopicPartitionOffset>> result =
        Coordinator.collectActiveOffsets(ImmutableList.of(a, b, c));

    TopicPartitionOffset chosen = result.get(T1).get(TP1);
    assertThat(chosen.offset()).isEqualTo(25L);
    assertThat(chosen.timestamp()).isEqualTo(2500L);
  }

  @Test
  public void collectActiveOffsetsReturnsEmptyForEmptyInput() {
    assertThat(Coordinator.collectActiveOffsets(Collections.emptyList())).isEmpty();
  }

  @Test
  public void kafkaFieldsBothNullWhenTopicEmptyOrExpired() {
    // Retention deleted everything: log-end-offset still reports the next-offset-to-write,
    // but maxTimestamp comes back as -1 because no records remain.
    ListOffsetsResult.ListOffsetsResultInfo latest = listOffsetsInfo(227_030_227L, 0L);
    ListOffsetsResult.ListOffsetsResultInfo maxTs = listOffsetsInfo(-1L, -1L);

    assertThat(Coordinator.computeLastKafkaOffset(latest, maxTs)).isNull();
    assertThat(Coordinator.computeLastKafkaEventTime(maxTs)).isNull();
  }

  @Test
  public void kafkaFieldsBothFilledForActiveTopic() {
    ListOffsetsResult.ListOffsetsResultInfo latest = listOffsetsInfo(1_000L, 0L);
    ListOffsetsResult.ListOffsetsResultInfo maxTs = listOffsetsInfo(999L, 1_700_000_000L);

    assertThat(Coordinator.computeLastKafkaOffset(latest, maxTs)).isEqualTo(999L);
    assertThat(Coordinator.computeLastKafkaEventTime(maxTs)).isEqualTo(1_700_000_000L);
  }

  @Test
  public void kafkaFieldsBothNullForBrandNewEmptyTopic() {
    // Topic created but never written: latest=0, maxTs=-1.
    ListOffsetsResult.ListOffsetsResultInfo latest = listOffsetsInfo(0L, 0L);
    ListOffsetsResult.ListOffsetsResultInfo maxTs = listOffsetsInfo(-1L, -1L);

    assertThat(Coordinator.computeLastKafkaOffset(latest, maxTs)).isNull();
    assertThat(Coordinator.computeLastKafkaEventTime(maxTs)).isNull();
  }

  @Test
  public void kafkaFieldsHandleNullInputs() {
    assertThat(Coordinator.computeLastKafkaOffset(null, null)).isNull();
    assertThat(Coordinator.computeLastKafkaEventTime(null)).isNull();
  }

  private static ListOffsetsResult.ListOffsetsResultInfo listOffsetsInfo(long offset, long ts) {
    return new ListOffsetsResult.ListOffsetsResultInfo(offset, ts, Optional.empty());
  }

  private static TopicPartitionOffset tpo(TopicPartition tp, long offset, long timestamp) {
    return new TopicPartitionOffset(tp.topic(), tp.partition(), offset, timestamp);
  }

  private static Envelope envelopeWith(
      UUID commitId, TableIdentifier tableId, List<TopicPartitionOffset> offsets) {
    Event localEvent =
        new Event(
            "test-group",
            EventType.DATA_OFFSETS,
            new DataOffsetsPayload(commitId, TableName.of(tableId), offsets));
    return new Envelope(localEvent, 0, 0L);
  }
}
