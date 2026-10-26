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
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.tabular.iceberg.connect.IcebergSinkConfig;
import io.tabular.iceberg.connect.data.IcebergWriter;
import io.tabular.iceberg.connect.data.IcebergWriterFactory;
import io.tabular.iceberg.connect.data.RecordConversionException;
import io.tabular.iceberg.connect.data.WriteComplete;
import io.tabular.iceberg.connect.data.WriterResult;
import io.tabular.iceberg.connect.events.EventTestUtil;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types.StructType;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

public class WorkerTest {
  private static final String SRC_TOPIC_NAME = "src-topic";
  private static final String TABLE_NAME = "db.tbl";
  private static final String FIELD_NAME = "fld";
  private static final TopicPartition SRC_TP = new TopicPartition(SRC_TOPIC_NAME, 0);

  @Test
  public void testStaticRoute() {
    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.tables()).thenReturn(ImmutableList.of(TABLE_NAME));
    when(config.catalogName()).thenReturn("catalog");
    Map<String, Object> value = ImmutableMap.of(FIELD_NAME, "val");
    workerTest(config, value);
  }

  @Test
  public void testDynamicRoute() {
    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.dynamicTablesEnabled()).thenReturn(true);
    when(config.tablesRouteField()).thenReturn(FIELD_NAME);
    when(config.catalogName()).thenReturn("catalog");

    Map<String, Object> value = ImmutableMap.of(FIELD_NAME, TABLE_NAME);
    workerTest(config, value);
  }

  @Test
  public void testIncompatibleRecordDoesNotDiscardWrittenPrefix() {
    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.tables()).thenReturn(ImmutableList.of(TABLE_NAME));
    Writer worker = new Worker(config, failingAtOffsetWriterFactory(1259221L));

    // 1259160..1259220 are compatible, 1259221 carries an incompatible schema
    assertThatCode(() -> worker.write(records(1259160L, 1259221L)))
        .doesNotThrowAnyException();

    Committable committable = worker.committable();
    assertThat(committable.writerResults()).hasSize(1);
    assertThat(worker.deferredFailure()).isInstanceOf(RecordConversionException.class);
    // next offset to consume must be the incompatible record, so it is retried
    assertThat(committable.offsetsByTopicPartition().get(SRC_TP).offset()).isEqualTo(1259221L);
  }

  @Test
  public void testFailedRecordOffsetIsNotMarkedConsumed() {
    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.tables()).thenReturn(ImmutableList.of(TABLE_NAME));
    Writer worker = new Worker(config, failingAtOffsetWriterFactory(1259221L));

    try {
      worker.write(records(1259160L, 1259221L));
    } catch (DataException e) {
      // the offset must hold whether the failure is deferred or not
    }

    // committing offset 1259222 would silently skip the incompatible record
    assertThat(worker.committable().offsetsByTopicPartition().get(SRC_TP).offset())
        .isEqualTo(1259221L);
  }

  @Test
  public void testRecordAlreadyWrittenToAnotherTableFailsImmediately() {
    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.tables()).thenReturn(ImmutableList.of("db.ok", TABLE_NAME));
    IcebergWriterFactory failing = failingAtOffsetWriterFactory(1259221L);
    IcebergWriterFactory writerFactory = mock(IcebergWriterFactory.class);
    when(writerFactory.createWriter(eq("db.ok"), any(), anyBoolean()))
        .thenReturn(mock(IcebergWriter.class));
    when(writerFactory.createWriter(eq(TABLE_NAME), any(), anyBoolean()))
        .thenAnswer(inv -> failing.createWriter(TABLE_NAME, inv.getArgument(1), false));
    Writer worker = new Worker(config, writerFactory);

    // retrying the record would duplicate it in db.ok
    assertThatThrownBy(() -> worker.write(records(1259220L, 1259221L)))
        .isInstanceOf(RecordConversionException.class);
  }

  private static List<SinkRecord> records(long fromOffset, long toOffset) {
    List<SinkRecord> records = Lists.newArrayList();
    for (long offset = fromOffset; offset <= toOffset; offset++) {
      records.add(
          new SinkRecord(
              SRC_TOPIC_NAME, 0, null, "key", null, ImmutableMap.of(FIELD_NAME, "val"), offset));
    }
    return records;
  }

  private static IcebergWriterFactory failingAtOffsetWriterFactory(long badOffset) {
    WriterResult writeResult =
        new WriterResult(
            TableIdentifier.parse(TABLE_NAME),
            ImmutableList.of(EventTestUtil.createDataFile()),
            ImmutableList.of(),
            StructType.of());
    IcebergWriter writer = mock(IcebergWriter.class);
    when(writer.complete())
        .thenReturn(
            new WriteComplete(
                TableIdentifier.parse(TABLE_NAME),
                ImmutableList.of(writeResult),
                ImmutableMap.of()));
    doThrow(new RecordConversionException("phone: Cannot convert to long: java.lang.String", null))
        .when(writer)
        .write(argThat(rec -> rec != null && rec.kafkaOffset() == badOffset));

    IcebergWriterFactory writerFactory = mock(IcebergWriterFactory.class);
    when(writerFactory.createWriter(any(), any(), anyBoolean())).thenReturn(writer);
    return writerFactory;
  }

  private void workerTest(IcebergSinkConfig config, Map<String, Object> value) {
    WriterResult writeResult =
        new WriterResult(
            TableIdentifier.parse(TABLE_NAME),
            ImmutableList.of(EventTestUtil.createDataFile()),
            ImmutableList.of(),
            StructType.of());
    WriteComplete writeComplete =
        new WriteComplete(
            TableIdentifier.parse(TABLE_NAME), ImmutableList.of(writeResult), ImmutableMap.of());
    IcebergWriter writer = mock(IcebergWriter.class);
    when(writer.complete()).thenReturn(writeComplete);

    IcebergWriterFactory writerFactory = mock(IcebergWriterFactory.class);
    when(writerFactory.createWriter(any(), any(), anyBoolean())).thenReturn(writer);

    Writer worker = new Worker(config, writerFactory);

    // save a record
    SinkRecord rec = new SinkRecord(SRC_TOPIC_NAME, 0, null, "key", null, value, 0L);
    worker.write(ImmutableList.of(rec));

    Committable committable = worker.committable();

    assertThat(committable.offsetsByTopicPartition()).hasSize(1);
    // offset should be one more than the record offset
    assertThat(
            committable
                .offsetsByTopicPartition()
                .get(committable.offsetsByTopicPartition().keySet().iterator().next())
                .offset())
        .isEqualTo(1L);
  }
}
