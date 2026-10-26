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

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.tabular.iceberg.connect.IcebergSinkConfig;
import io.tabular.iceberg.connect.data.RecordConversionException;
import java.util.UUID;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TaskImplTest {
  private static final TopicPartition SRC_TP = new TopicPartition("src-topic", 0);
  private static final RecordConversionException FAILURE =
      new RecordConversionException("phone: Cannot convert to long", null);

  private SinkTaskContext context;
  private Writer writer;
  private Committer committer;

  @BeforeEach
  void before() {
    context = mock(SinkTaskContext.class);
    when(context.assignment()).thenReturn(ImmutableSet.of(SRC_TP));
    writer = mock(Writer.class);
    committer = mock(Committer.class);
  }

  private TaskImpl task(int commitIntervalMs) {
    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.commitIntervalMs()).thenReturn(commitIntervalMs);
    when(config.commitTimeoutMs()).thenReturn(0);
    return new TaskImpl(context, config, null, writer, committer);
  }

  @Test
  void failsAfterCommitThatFollowsTheFailure() {
    UUID beforeFailure = UUID.randomUUID();
    UUID afterFailure = UUID.randomUUID();
    when(writer.deferredFailure()).thenReturn(FAILURE);
    when(writer.madeProgress()).thenReturn(true);
    when(committer.lastResponseCommitId()).thenReturn(beforeFailure);
    when(committer.isCommitComplete(beforeFailure)).thenReturn(true);
    TaskImpl task = task(60_000);

    // a commit answered before the failure does not contain the written prefix
    assertThatCode(() -> task.put(ImmutableList.of())).doesNotThrowAnyException();
    verify(context).pause(SRC_TP);

    when(committer.lastResponseCommitId()).thenReturn(afterFailure);
    assertThatCode(() -> task.put(ImmutableList.of())).doesNotThrowAnyException();

    when(committer.isCommitComplete(afterFailure)).thenReturn(true);
    assertThatThrownBy(() -> task.put(ImmutableList.of())).isSameAs(FAILURE);
    // records are not written again while waiting for the commit
    verify(writer, times(1)).write(any());
    verify(committer, times(3)).commit(writer);
  }

  @Test
  void failsImmediatelyWhenNothingWasWrittenBeforeTheFailure() {
    when(writer.deferredFailure()).thenReturn(FAILURE);
    when(writer.madeProgress()).thenReturn(false);
    TaskImpl task = task(60_000);

    assertThatThrownBy(() -> task.put(ImmutableList.of())).isSameAs(FAILURE);
    verify(context, never()).pause(any());
    verify(committer, never()).commit(any());
  }

  @Test
  void failsWhenNoCommitCompletesInTime() {
    when(writer.deferredFailure()).thenReturn(FAILURE);
    when(writer.madeProgress()).thenReturn(true);
    TaskImpl task = task(0);

    assertThatThrownBy(() -> task.put(ImmutableList.of())).isSameAs(FAILURE);
    verify(committer).commit(writer);
  }

  @Test
  void keepsWritingWithoutFailure() {
    TaskImpl task = task(60_000);

    task.put(ImmutableList.of());
    task.put(ImmutableList.of());

    verify(writer, times(2)).write(any());
    verify(context, never()).pause(any());
  }
}
