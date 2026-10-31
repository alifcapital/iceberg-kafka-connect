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
package io.tabular.iceberg.connect;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Function;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class IcebergSinkConfigTest {

  @Test
  public void testGetVersion() {
    String version = IcebergSinkConfig.version();
    assertThat(version).isNotNull();
  }

  @Test
  public void testInvalid() {
    Map<String, String> props =
        ImmutableMap.of(
            "topics", "source-topic",
            "iceberg.catalog.type", "rest",
            "iceberg.tables", "db.landing",
            "iceberg.tables.dynamic-enabled", "true");
    assertThatThrownBy(() -> new IcebergSinkConfig(props))
        .isInstanceOf(ConfigException.class)
        .hasMessage("Cannot specify both static and dynamic table names");
  }

  @Test
  public void testGetDefault() {
    Map<String, String> props =
        ImmutableMap.of(
            "iceberg.catalog.type", "rest",
            "topics", "source-topic",
            "iceberg.tables", "db.landing");
    IcebergSinkConfig config = new IcebergSinkConfig(props);
    assertThat(config.commitIntervalMs()).isEqualTo(300_000);
  }

  @ParameterizedTest
  @ValueSource(strings = {"", "true", "false", "TRUE", "False"})
  public void testRecordPathSettingsAreTheConfiguredValues(String flag) {
    Map<String, String> props = new HashMap<>();
    props.put("iceberg.catalog.type", "rest");
    props.put("topics", "source-topic");
    props.put("iceberg.tables.dynamic-enabled", "true");
    props.put("iceberg.tables.route-field", "_cdc.target");
    props.put("iceberg.tables.cdc-field", "_cdc.op");
    if (!flag.isEmpty()) {
      props.put("iceberg.tables.upsert-mode-enabled", flag);
      props.put("iceberg.tables.evolve-schema-enabled", flag);
      props.put("iceberg.tables.schema-variable-decimal-as-string", flag);
      props.put("iceberg.tables.schema-force-optional", flag);
      props.put("iceberg.tables.schema-case-insensitive", flag);
      props.put("iceberg.tables.schema-debezium-time-types", flag);
    }
    IcebergSinkConfig config = new IcebergSinkConfig(props);

    assertThat(config.dynamicTablesEnabled()).isTrue();
    assertThat(config.dynamicTablesEnabled())
        .isEqualTo(config.getBoolean("iceberg.tables.dynamic-enabled"));
    assertThat(config.tablesRouteField()).isEqualTo("_cdc.target");
    assertThat(config.tablesCdcField()).isEqualTo("_cdc.op");
    assertThat(config.tables()).isEqualTo(config.getList("iceberg.tables")).isNull();
    assertBooleanSetting(config, "iceberg.tables.upsert-mode-enabled", IcebergSinkConfig::upsertModeEnabled);
    assertBooleanSetting(config, "iceberg.tables.evolve-schema-enabled", IcebergSinkConfig::evolveSchemaEnabled);
    assertBooleanSetting(
        config,
        "iceberg.tables.schema-variable-decimal-as-string",
        IcebergSinkConfig::schemaVariableDecimalAsString);
    assertBooleanSetting(config, "iceberg.tables.schema-force-optional", IcebergSinkConfig::schemaForceOptional);
    assertBooleanSetting(config, "iceberg.tables.schema-case-insensitive", IcebergSinkConfig::schemaCaseInsensitive);
    assertBooleanSetting(
        config, "iceberg.tables.schema-debezium-time-types", IcebergSinkConfig::schemaDebeziumTimeTypes);
    if (flag.isEmpty()) {
      // unset settings keep their default
      assertThat(config.upsertModeEnabled()).isFalse();
      assertThat(config.schemaCaseInsensitive()).isFalse();
    } else {
      assertThat(config.schemaCaseInsensitive()).isEqualTo(Boolean.parseBoolean(flag));
    }
  }

  private static void assertBooleanSetting(
      IcebergSinkConfig config, String name, Function<IcebergSinkConfig, Boolean> getter) {
    assertThat(getter.apply(config)).as(name).isEqualTo(config.getBoolean(name));
  }

  @Test
  public void testStaticTablesAndUnsetRecordPathSettings() {
    IcebergSinkConfig config =
        new IcebergSinkConfig(
            ImmutableMap.of(
                "iceberg.catalog.type", "rest",
                "topics", "source-topic",
                "iceberg.tables", "db.a, db.b"));
    assertThat(config.tables()).containsExactly("db.a", "db.b");
    assertThat(config.tables()).isEqualTo(config.getList("iceberg.tables"));
    assertThat(config.dynamicTablesEnabled()).isFalse();
    assertThat(config.tablesRouteField()).isNull();
    assertThat(config.tablesCdcField()).isNull();
  }

  @Test
  public void testTableConfigIsBuiltOncePerTable() throws Exception {
    Map<String, String> props = new HashMap<>();
    props.put("iceberg.catalog.type", "rest");
    props.put("topics", "source-topic");
    props.put("iceberg.tables", "db.a,db.b");
    props.put("iceberg.tables.default-id-columns", "id");
    props.put("iceberg.table.db.a.route-regex", "a.*");
    props.put("iceberg.table.db.a.partition-by", "bucket(id, 4), day(ts)");
    props.put("iceberg.table.db.b.id-columns", "x, y");
    props.put("iceberg.table.db.b.append-only", "true");
    IcebergSinkConfig config = new IcebergSinkConfig(props);

    TableSinkConfig a = config.tableConfig("db.a");
    assertThat(config.tableConfig("db.a")).isSameAs(a);
    assertThat(a.routeRegex()).isPresent();
    assertThat(a.routeRegex().get().pattern()).isEqualTo("a.*");
    assertThat(a.idColumns()).containsExactly("id");
    assertThat(a.partitionBy()).containsExactly("bucket(id, 4)", "day(ts)");
    assertThat(a.appendOnly()).isFalse();
    TableSinkConfig b = config.tableConfig("db.b");
    assertThat(b).isNotSameAs(a);
    assertThat(b.routeRegex()).isEmpty();
    assertThat(b.idColumns()).containsExactly("x", "y");
    assertThat(b.appendOnly()).isTrue();

    // several threads asking for the same new tables all get one instance of each
    ExecutorService executor = Executors.newFixedThreadPool(4);
    try {
      List<Future<List<TableSinkConfig>>> futures = new ArrayList<>();
      for (int t = 0; t < 4; t++) {
        futures.add(
            executor.submit(
                () -> {
                  List<TableSinkConfig> found = new ArrayList<>();
                  for (int i = 0; i < 500; i++) {
                    found.add(config.tableConfig("db.dynamic" + i));
                  }
                  return found;
                }));
      }
      List<TableSinkConfig> first = futures.get(0).get();
      for (Future<List<TableSinkConfig>> future : futures) {
        List<TableSinkConfig> found = future.get();
        for (int i = 0; i < found.size(); i++) {
          assertThat(found.get(i)).isSameAs(first.get(i));
        }
      }
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void testStringToList() {
    List<String> result = IcebergSinkConfig.stringToList(null, ",");
    assertThat(result).isEmpty();

    result = IcebergSinkConfig.stringToList("", ",");
    assertThat(result).isEmpty();

    result = IcebergSinkConfig.stringToList("one ", ",");
    assertThat(result).contains("one");

    result = IcebergSinkConfig.stringToList("one, two", ",");
    assertThat(result).contains("one", "two");

    result = IcebergSinkConfig.stringToList("bucket(id, 4)", ",");
    assertThat(result).contains("bucket(id", "4)");

    result =
        IcebergSinkConfig.stringToList("bucket(id, 4)", IcebergSinkConfig.COMMA_NO_PARENS_REGEX);
    assertThat(result).contains("bucket(id, 4)");

    result =
        IcebergSinkConfig.stringToList(
            "bucket(id, 4), type", IcebergSinkConfig.COMMA_NO_PARENS_REGEX);
    assertThat(result).contains("bucket(id, 4)", "type");
  }

  @Test
  public void testStringWithParensToList() {}

  @Test
  public void testWatermarkTopicDefaultsToNull() {
    Map<String, String> props =
        ImmutableMap.of(
            "iceberg.catalog.type", "rest",
            "topics", "t",
            "iceberg.tables", "db.t");
    IcebergSinkConfig config = new IcebergSinkConfig(props);
    assertThat(config.watermarkTopic()).isNull();
  }

  @Test
  public void testWatermarkTopicSet() {
    Map<String, String> props =
        ImmutableMap.of(
            "iceberg.catalog.type", "rest",
            "topics", "t",
            "iceberg.tables", "db.t",
            "iceberg.watermark.topic", "cdc.watermarks");
    IcebergSinkConfig config = new IcebergSinkConfig(props);
    assertThat(config.watermarkTopic()).isEqualTo("cdc.watermarks");
  }

  @Test
  public void testTopicsList() {
    Map<String, String> props =
        ImmutableMap.of(
            "iceberg.catalog.type", "rest",
            "topics", "t1, t2 ,t3",
            "iceberg.tables", "db.t");
    IcebergSinkConfig config = new IcebergSinkConfig(props);
    assertThat(config.topicsList()).containsExactly("t1", "t2", "t3");
    assertThat(config.topicsRegex()).isEmpty();
  }

  @Test
  public void testTopicsRegex() {
    Map<String, String> props =
        ImmutableMap.of(
            "iceberg.catalog.type", "rest",
            "topics.regex", "debezium_.*",
            "iceberg.tables", "db.t");
    IcebergSinkConfig config = new IcebergSinkConfig(props);
    assertThat(config.topicsRegex()).isPresent();
    assertThat(config.topicsRegex().get().matcher("debezium_x.public.t").matches()).isTrue();
    assertThat(config.topicsRegex().get().matcher("other_x").matches()).isFalse();
    assertThat(config.topicsList()).isEmpty();
  }

  @Test
  public void testDebeziumTransformPatternFound() {
    Map<String, String> props =
        ImmutableMap.<String, String>builder()
            .put("iceberg.catalog.type", "rest")
            .put("topics", "t")
            .put("iceberg.tables", "db.t")
            .put("transforms", "dbz")
            .put("transforms.dbz.type", "io.tabular.iceberg.connect.transforms.DebeziumTransform")
            .put("transforms.dbz.cdc.target.pattern", "landing.{table}")
            .build();
    IcebergSinkConfig config = new IcebergSinkConfig(props);
    assertThat(config.debeziumTransformPattern()).contains("landing.{table}");
  }

  @Test
  public void testDebeziumTransformPatternMissing() {
    Map<String, String> props =
        ImmutableMap.of(
            "iceberg.catalog.type", "rest",
            "topics", "t",
            "iceberg.tables", "db.t");
    IcebergSinkConfig config = new IcebergSinkConfig(props);
    assertThat(config.debeziumTransformPattern()).isEmpty();
  }

  @Test
  public void testDebeziumTransformPatternWrongType() {
    Map<String, String> props =
        ImmutableMap.<String, String>builder()
            .put("iceberg.catalog.type", "rest")
            .put("topics", "t")
            .put("iceberg.tables", "db.t")
            .put("transforms", "x")
            .put("transforms.x.type", "org.apache.kafka.connect.transforms.InsertField$Value")
            .put("transforms.x.cdc.target.pattern", "landing.{table}")
            .build();
    IcebergSinkConfig config = new IcebergSinkConfig(props);
    assertThat(config.debeziumTransformPattern()).isEmpty();
  }

  @Test
  public void testDebeziumTransformPatternMultipleTransformsFindsDebezium() {
    Map<String, String> props =
        ImmutableMap.<String, String>builder()
            .put("iceberg.catalog.type", "rest")
            .put("topics", "t")
            .put("iceberg.tables", "db.t")
            .put("transforms", "ins, dbz")
            .put("transforms.ins.type", "org.apache.kafka.connect.transforms.InsertField$Value")
            .put("transforms.dbz.type", "io.tabular.iceberg.connect.transforms.DebeziumTransform")
            .put("transforms.dbz.cdc.target.pattern", "landing.{table}")
            .build();
    IcebergSinkConfig config = new IcebergSinkConfig(props);
    assertThat(config.debeziumTransformPattern()).contains("landing.{table}");
  }
}
