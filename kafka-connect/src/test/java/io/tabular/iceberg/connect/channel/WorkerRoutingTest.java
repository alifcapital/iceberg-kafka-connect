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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.tabular.iceberg.connect.IcebergSinkConfig;
import io.tabular.iceberg.connect.TableSinkConfig;
import io.tabular.iceberg.connect.data.IcebergWriter;
import io.tabular.iceberg.connect.data.IcebergWriterFactory;
import io.tabular.iceberg.connect.data.WriteComplete;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class WorkerRoutingTest {

  private static final Schema CDC_SCHEMA =
      SchemaBuilder.struct().optional().field("target", Schema.OPTIONAL_STRING_SCHEMA).build();
  private static final Schema VALUE_SCHEMA =
      SchemaBuilder.struct()
          .field("id", Schema.INT64_SCHEMA)
          .field("_cdc", CDC_SCHEMA)
          .field("kind", Schema.OPTIONAL_STRING_SCHEMA)
          .build();

  private IcebergWriterFactory writerFactory;
  private Map<String, IcebergWriter> writers;

  @BeforeEach
  public void before() {
    writers = Maps.newLinkedHashMap();
    writerFactory = mock(IcebergWriterFactory.class);
    when(writerFactory.createWriter(any(), any(), anyBoolean()))
        .thenAnswer(inv -> writers.computeIfAbsent(inv.getArgument(0), k -> newWriter(k)));
  }

  private static IcebergWriter newWriter(String tableName) {
    IcebergWriter writer = mock(IcebergWriter.class);
    when(writer.complete())
        .thenReturn(
            new WriteComplete(
                TableIdentifier.parse(tableName), ImmutableList.of(), ImmutableMap.of()));
    return writer;
  }

  private static IcebergSinkConfig dynamicConfig(String routeField) {
    return new IcebergSinkConfig(
        ImmutableMap.of(
            "topics", "src",
            "iceberg.catalog.type", "hadoop",
            "iceberg.tables.dynamic-enabled", "true",
            "iceberg.tables.route-field", routeField));
  }

  private static Struct value(long id, String target) {
    return new Struct(VALUE_SCHEMA)
        .put("id", id)
        .put("_cdc", target == null ? null : new Struct(CDC_SCHEMA).put("target", target));
  }

  private static SinkRecord record(Object value, long offset) {
    Schema schema = value instanceof Struct ? VALUE_SCHEMA : null;
    return new SinkRecord("src", 0, null, "key", schema, value, offset);
  }

  @Test
  public void testDynamicRouteByNestedStructField() {
    Worker worker = new Worker(dynamicConfig("_cdc.target"), writerFactory);
    worker.write(
        ImmutableList.of(
            record(value(1, "Db.MixedCase"), 0),
            record(value(2, "db.mixedcase"), 1),
            record(value(3, "other.Table"), 2)));

    assertThat(writers.keySet()).containsExactly("db.mixedcase", "other.table");
    verify(writers.get("db.mixedcase"), times(2)).write(any());
    verify(writers.get("other.table")).write(any());
  }

  @Test
  public void testDynamicRouteByNestedMapField() {
    Worker worker = new Worker(dynamicConfig("_cdc.target"), writerFactory);
    worker.write(
        ImmutableList.of(
            record(ImmutableMap.of("_cdc", ImmutableMap.of("target", "Db.T1")), 0),
            record(ImmutableMap.of("_cdc", ImmutableMap.of("target", "db.t2")), 1)));

    assertThat(writers.keySet()).containsExactly("db.t1", "db.t2");
  }

  @Test
  public void testRecordsWithoutRouteValueAreSkippedButConsumed() {
    Worker worker = new Worker(dynamicConfig("_cdc.target"), writerFactory);
    worker.write(
        ImmutableList.of(
            record(value(1, null), 0),
            record(ImmutableMap.of("id", 2L), 1),
            new SinkRecord("src", 0, null, "key", null, null, 2),
            record(value(4, "db.t"), 3)));

    assertThat(writers.keySet()).containsExactly("db.t");
    assertThat(
            worker.committable().offsetsByTopicPartition().values().iterator().next().offset())
        .isEqualTo(4L);
  }

  @Test
  public void testDynamicRouteWithoutRouteFieldFailsWithTheRecordPosition() {
    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.dynamicTablesEnabled()).thenReturn(true);
    Worker worker = new Worker(config, writerFactory);

    assertThatThrownBy(() -> worker.write(ImmutableList.of(record(value(1, "db.t"), 17))))
        .isInstanceOf(NullPointerException.class)
        .hasMessage(
            "Route field cannot be null with dynamic routing at topic: src, partition: 0, offset: 17");
    assertThat(writers).isEmpty();
  }

  @Test
  public void testChangedRouteFieldIsSplitAgain() {
    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.dynamicTablesEnabled()).thenReturn(true);
    Worker worker = new Worker(config, writerFactory);
    Map<String, Object> value =
        ImmutableMap.of("a", ImmutableMap.of("b", "db.nested"), "a.b", "db.flat", "c", "db.top");

    when(config.tablesRouteField()).thenReturn("a.b");
    worker.write(ImmutableList.of(record(value, 0)));
    when(config.tablesRouteField()).thenReturn("c");
    worker.write(ImmutableList.of(record(value, 1)));
    when(config.tablesRouteField()).thenReturn("a.b");
    worker.write(ImmutableList.of(record(value, 2)));

    assertThat(ImmutableList.copyOf(writers.keySet())).containsExactly("db.nested", "db.top");
    verify(writers.get("db.nested"), times(2)).write(any());
  }

  @Test
  public void testStaticRouteByNestedFieldAndRegex() {
    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.tables()).thenReturn(ImmutableList.of("db.a", "db.b", "db.c"));
    when(config.tablesRouteField()).thenReturn("_cdc.target");
    when(config.tableConfig("db.a")).thenReturn(tableConfig("x\\..*"));
    when(config.tableConfig("db.b")).thenReturn(tableConfig("y\\..*"));
    when(config.tableConfig("db.c")).thenReturn(tableConfig(null));
    Worker worker = new Worker(config, writerFactory);

    worker.write(ImmutableList.of(record(value(1, "x.t"), 0), record(value(2, "z.t"), 1)));

    assertThat(writers.keySet()).containsExactly("db.a");
    verify(writers.get("db.a")).write(any());
  }

  private static TableSinkConfig tableConfig(String routeRegex) {
    return new TableSinkConfig(
        routeRegex == null ? null : Pattern.compile(routeRegex),
        ImmutableList.of(),
        ImmutableList.of(),
        null,
        false,
        null);
  }
}
