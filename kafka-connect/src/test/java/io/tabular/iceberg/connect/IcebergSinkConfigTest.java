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

import java.util.List;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;

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
