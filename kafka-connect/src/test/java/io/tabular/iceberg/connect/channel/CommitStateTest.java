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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.tabular.iceberg.connect.IcebergSinkConfig;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.events.DataComplete;
import org.apache.iceberg.connect.events.DataWritten;
import org.apache.iceberg.connect.events.Event;
import org.apache.iceberg.connect.events.Payload;
import org.apache.iceberg.connect.events.TableReference;
import org.apache.iceberg.connect.events.TopicPartitionOffset;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.Pair;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class CommitStateTest {

  private OffsetDateTime offsetDateTime(Long ts) {
    return OffsetDateTime.ofInstant(Instant.ofEpochMilli(ts), ZoneOffset.UTC);
  }

  @Test
  public void testIsCommitReady() {
    TopicPartitionOffset tp = mock(TopicPartitionOffset.class);

    CommitState commitState = new CommitState(mock(IcebergSinkConfig.class));
    commitState.startNewCommit();

    DataComplete payload1 = mock(DataComplete.class);
    when(payload1.commitId()).thenReturn(commitState.currentCommitId());
    when(payload1.assignments()).thenReturn(ImmutableList.of(tp, tp));

    DataComplete payload2 = mock(DataComplete.class);
    when(payload2.commitId()).thenReturn(commitState.currentCommitId());
    when(payload2.assignments()).thenReturn(ImmutableList.of(tp));

    DataComplete payload3 = mock(DataComplete.class);
    when(payload3.commitId()).thenReturn(UUID.randomUUID());
    when(payload3.assignments()).thenReturn(ImmutableList.of(tp));

    commitState.addReady(wrapInEnvelope(payload1));
    commitState.addReady(wrapInEnvelope(payload2));
    commitState.addReady(wrapInEnvelope(payload3));

    assertThat(commitState.isCommitReady(3)).isTrue();
    assertThat(commitState.isCommitReady(4)).isFalse();
  }

  @Test
  public void testGetVtts() {
    DataComplete payload1 = mock(DataComplete.class);
    TopicPartitionOffset tp1 = mock(TopicPartitionOffset.class);
    when(tp1.timestamp()).thenReturn(offsetDateTime(3L));
    TopicPartitionOffset tp2 = mock(TopicPartitionOffset.class);
    when(tp2.timestamp()).thenReturn(offsetDateTime(2L));
    when(payload1.assignments()).thenReturn(ImmutableList.of(tp1, tp2));

    DataComplete payload2 = mock(DataComplete.class);
    TopicPartitionOffset tp3 = mock(TopicPartitionOffset.class);
    when(tp3.timestamp()).thenReturn(offsetDateTime(1L));
    when(payload2.assignments()).thenReturn(ImmutableList.of(tp3));

    CommitState commitState = new CommitState(mock(IcebergSinkConfig.class));
    commitState.startNewCommit();

    commitState.addReady(wrapInEnvelope(payload1));
    commitState.addReady(wrapInEnvelope(payload2));

    assertThat(commitState.vtts(false)).isEqualTo(offsetDateTime(1L));
    assertThat(commitState.vtts(true)).isNull();

    // null timestamp for one, so should not set a vtts
    DataComplete payload3 = mock(DataComplete.class);
    TopicPartitionOffset tp4 = mock(TopicPartitionOffset.class);
    when(tp4.timestamp()).thenReturn(null);
    when(payload3.assignments()).thenReturn(ImmutableList.of(tp4));

    commitState.addReady(wrapInEnvelope(payload3));

    assertThat(commitState.vtts(false)).isNull();
    assertThat(commitState.vtts(true)).isNull();
  }

  @ParameterizedTest
  @MethodSource("envelopeListProvider")
  public void testTokenize(Pair<List<Envelope>, List<List<Envelope>>> input) {
    CommitState commitState = new CommitState(mock(IcebergSinkConfig.class));
    List<List<Envelope>> actual = commitState.tokenize(input.first());

    assertThat(actual).isEqualTo(input.second());
  }

  @Test
  public void testTableCommitMap() {
    CommitState commitState = new CommitState(mock(IcebergSinkConfig.class));
    List<Envelope> envelopeList =
        Arrays.asList(
            wrapDataWrittenEnvelope(ImmutableList.of(FileContent.DATA, FileContent.POSITION_DELETES), 0L),
            wrapDataWrittenEnvelope(ImmutableList.of(FileContent.DATA, FileContent.EQUALITY_DELETES), 1L),
            wrapDataWrittenEnvelope(ImmutableList.of(FileContent.DATA), 2L),
            wrapDataWrittenEnvelope(ImmutableList.of(FileContent.DATA, FileContent.EQUALITY_DELETES), 3L),
            wrapDataWrittenEnvelope(ImmutableList.of(FileContent.DATA), 4L),
            wrapDataWrittenEnvelope(ImmutableList.of(FileContent.DATA, FileContent.EQUALITY_DELETES), 5L));

    envelopeList.forEach(commitState::addResponse);

    List<Long> expected = Lists.newArrayList(0L, 1L, 2L, 3L, 4L, 5L);

    List<Long> actual = Lists.newArrayList();

    Map<TableIdentifier, List<List<Envelope>>> tableCommitMap = commitState.tableCommitMap();

    tableCommitMap.forEach(
        (key, value) -> value.forEach(x -> x.forEach(y -> actual.add(y.offset()))));

    assertThat(actual).isEqualTo(expected);
  }

  private static Stream<Pair<List<Envelope>, List<List<Envelope>>>> envelopeListProvider() {
    Envelope posDelete = wrapDataWrittenEnvelope(ImmutableList.of(FileContent.POSITION_DELETES), 0L);
    Envelope eqDelete = wrapDataWrittenEnvelope(ImmutableList.of(FileContent.EQUALITY_DELETES), 0L);
    Envelope data = wrapDataWrittenEnvelope(ImmutableList.of(FileContent.DATA), 0L);
    return Stream.of(
        Pair.of(Arrays.asList(data, eqDelete), Arrays.asList(List.of(data), List.of(eqDelete))),
        Pair.of(
            Arrays.asList(posDelete, eqDelete, posDelete),
            Arrays.asList(List.of(posDelete), List.of(eqDelete), List.of(posDelete))),
        Pair.of(
            Arrays.asList(posDelete, posDelete, posDelete),
            List.of(Arrays.asList(posDelete, posDelete, posDelete))),
        Pair.of(
            Arrays.asList(posDelete, eqDelete, posDelete, eqDelete),
            Arrays.asList(
                List.of(posDelete), List.of(eqDelete), List.of(posDelete), List.of(eqDelete))),
        Pair.of(
            Arrays.asList(eqDelete, eqDelete, eqDelete, posDelete),
            Arrays.asList(
                List.of(eqDelete), List.of(eqDelete), List.of(eqDelete), List.of(posDelete))),
        Pair.of(
            Arrays.asList(posDelete, eqDelete, eqDelete, eqDelete),
            Arrays.asList(
                List.of(posDelete), List.of(eqDelete), List.of(eqDelete), List.of(eqDelete))));
  }

  private static Envelope wrapDataWrittenEnvelope(List<FileContent> fileContents, Long offset) {
    TableIdentifier tableIdentifier = TableIdentifier.of("db", "tbl");
    TableReference tableRef = mock(TableReference.class);
    when(tableRef.identifier()).thenReturn(tableIdentifier);

    List<DeleteFile> deleteFiles = Lists.newLinkedList();

    for (FileContent content : fileContents) {
      if (content == FileContent.EQUALITY_DELETES || content == FileContent.POSITION_DELETES) {
        DeleteFile deleteFile = mock(DeleteFile.class);
        when(deleteFile.content()).thenReturn(content);
        deleteFiles.add(deleteFile);
      }
    }

    DataWritten dataWritten = mock(DataWritten.class);
    when(dataWritten.deleteFiles()).thenReturn(deleteFiles);
    when(dataWritten.tableReference()).thenReturn(tableRef);

    Event event = mock(Event.class);
    when(event.payload()).thenReturn(dataWritten);

    return new Envelope(event, 0, offset);
  }

  private Envelope wrapInEnvelope(Payload payload) {
    Event event = mock(Event.class);
    when(event.payload()).thenReturn(payload);
    return new Envelope(event, 0, 0);
  }
}
