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
package io.tabular.iceberg.connect.data;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

import io.tabular.iceberg.connect.IcebergSinkConfig;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.hadoop.Configurable;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class UtilitiesTest {

  private static final String HADOOP_CONF_TEMPLATE =
      "<configuration><property><name>%s</name><value>%s</value></property></configuration>";

  @TempDir private Path tempDir;

  public static class TestCatalog extends InMemoryCatalog implements Configurable<Configuration> {
    private Configuration conf;

    @Override
    public void setConf(Configuration conf) {
      this.conf = conf;
    }
  }

  @Test
  public void testLoadCatalogNoHadoopDir() {
    Map<String, String> props =
        ImmutableMap.of(
            "topics",
            "mytopic",
            "iceberg.tables",
            "mytable",
            "iceberg.hadoop.conf-prop",
            "conf-value",
            "iceberg.catalog.catalog-impl",
            TestCatalog.class.getName());
    IcebergSinkConfig config = new IcebergSinkConfig(props);
    Catalog result = Utilities.loadCatalog(config);

    assertThat(result).isInstanceOf(TestCatalog.class);

    Configuration conf = ((TestCatalog) result).conf;
    assertThat(conf).isNotNull();

    // check that the sink config property was added
    assertThat(conf.get("conf-prop")).isEqualTo("conf-value");

    // check that core-site.xml was loaded
    assertThat(conf.get("foo")).isEqualTo("bar");
  }

  @ParameterizedTest
  @ValueSource(strings = {"core-site.xml", "hdfs-site.xml", "hive-site.xml"})
  public void testLoadCatalogWithHadoopDir(String confFile) throws IOException {
    Path path = tempDir.resolve(confFile);
    String xml = String.format(HADOOP_CONF_TEMPLATE, "file-prop", "file-value");
    Files.write(path, xml.getBytes(StandardCharsets.UTF_8));

    Map<String, String> props =
        ImmutableMap.of(
            "topics",
            "mytopic",
            "iceberg.tables",
            "mytable",
            "iceberg.hadoop-conf-dir",
            tempDir.toString(),
            "iceberg.hadoop.conf-prop",
            "conf-value",
            "iceberg.catalog.catalog-impl",
            TestCatalog.class.getName());
    IcebergSinkConfig config = new IcebergSinkConfig(props);
    Catalog result = Utilities.loadCatalog(config);

    assertThat(result).isInstanceOf(TestCatalog.class);

    Configuration conf = ((TestCatalog) result).conf;
    assertThat(conf).isNotNull();

    // check that the sink config property was added
    assertThat(conf.get("conf-prop")).isEqualTo("conf-value");

    // check that the config file was loaded
    assertThat(conf.get("file-prop")).isEqualTo("file-value");

    // check that core-site.xml was loaded
    assertThat(conf.get("foo")).isEqualTo("bar");
  }

  @Test
  public void testExtractFromRecordValueStruct() {
    Schema valSchema = SchemaBuilder.struct().field("key", Schema.INT64_SCHEMA).build();
    Struct val = new Struct(valSchema).put("key", 123L);
    Object result = Utilities.extractFromRecordValue(val, "key");
    assertThat(result).isEqualTo(123L);
  }

  @Test
  public void testExtractFromRecordValueStructNested() {
    Schema idSchema = SchemaBuilder.struct().field("key", Schema.INT64_SCHEMA).build();
    Schema dataSchema = SchemaBuilder.struct().field("id", idSchema).build();
    Schema valSchema = SchemaBuilder.struct().field("data", dataSchema).build();

    Struct id = new Struct(idSchema).put("key", 123L);
    Struct data = new Struct(dataSchema).put("id", id);
    Struct val = new Struct(valSchema).put("data", data);

    Object result = Utilities.extractFromRecordValue(val, "data.id.key");
    assertThat(result).isEqualTo(123L);
  }

  @Test
  public void testExtractFromRecordValueMap() {
    Map<String, Object> val = ImmutableMap.of("key", 123L);
    Object result = Utilities.extractFromRecordValue(val, "key");
    assertThat(result).isEqualTo(123L);
  }

  @Test
  public void testExtractFromRecordValueMapNested() {
    Map<String, Object> id = ImmutableMap.of("key", 123L);
    Map<String, Object> data = ImmutableMap.of("id", id);
    Map<String, Object> val = ImmutableMap.of("data", data);

    Object result = Utilities.extractFromRecordValue(val, "data.id.key");
    assertThat(result).isEqualTo(123L);
  }

  @Test
  public void testCollectEqualityDeleteFieldIds_simplePrimitives() {
    org.apache.iceberg.Schema schema =
        new org.apache.iceberg.Schema(
            required(1, "id", Types.LongType.get()),
            optional(2, "name", Types.StringType.get()),
            optional(3, "count", Types.IntegerType.get()));

    Set<Integer> fieldIds = Utilities.collectEqualityDeleteFieldIds(schema, "test_table");

    assertThat(fieldIds).containsExactlyInAnyOrder(1, 2, 3);
  }

  @Test
  public void testCollectEqualityDeleteFieldIds_nestedStruct() {
    org.apache.iceberg.Schema schema =
        new org.apache.iceberg.Schema(
            required(1, "id", Types.LongType.get()),
            optional(
                2,
                "address",
                Types.StructType.of(
                    required(3, "city", Types.StringType.get()),
                    optional(4, "zip", Types.StringType.get()))));

    Set<Integer> fieldIds = Utilities.collectEqualityDeleteFieldIds(schema, "test_table");

    // Should include id (1), city (3), zip (4) - all primitives
    assertThat(fieldIds).containsExactlyInAnyOrder(1, 3, 4);
  }

  @Test
  public void testCollectEqualityDeleteFieldIds_skipsCdcStruct() {
    org.apache.iceberg.Schema schema =
        new org.apache.iceberg.Schema(
            required(1, "id", Types.LongType.get()),
            optional(2, "data", Types.StringType.get()),
            optional(
                3,
                "_cdc",
                Types.StructType.of(
                    required(4, "op", Types.StringType.get()),
                    optional(5, "ts", Types.LongType.get()))));

    Set<Integer> fieldIds = Utilities.collectEqualityDeleteFieldIds(schema, "test_table");

    // Should only include id (1) and data (2), not _cdc struct or its children
    assertThat(fieldIds).containsExactlyInAnyOrder(1, 2);
  }

  @Test
  public void testCollectEqualityDeleteFieldIds_includesFloatDouble() {
    org.apache.iceberg.Schema schema =
        new org.apache.iceberg.Schema(
            required(1, "id", Types.LongType.get()),
            optional(2, "price", Types.FloatType.get()),
            optional(3, "amount", Types.DoubleType.get()));

    Set<Integer> fieldIds = Utilities.collectEqualityDeleteFieldIds(schema, "test_table");

    // Float and double should be included (with warning logged)
    assertThat(fieldIds).containsExactlyInAnyOrder(1, 2, 3);
  }

  @Test
  public void testCollectEqualityDeleteFieldIds_failsOnMap() {
    org.apache.iceberg.Schema schema =
        new org.apache.iceberg.Schema(
            required(1, "id", Types.LongType.get()),
            optional(
                2,
                "tags",
                Types.MapType.ofRequired(3, 4, Types.StringType.get(), Types.StringType.get())));

    assertThatThrownBy(() -> Utilities.collectEqualityDeleteFieldIds(schema, "test_table"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("MAP column")
        .hasMessageContaining("tags");
  }

  @Test
  public void testCollectEqualityDeleteFieldIds_failsOnList() {
    org.apache.iceberg.Schema schema =
        new org.apache.iceberg.Schema(
            required(1, "id", Types.LongType.get()),
            optional(2, "items", Types.ListType.ofRequired(3, Types.StringType.get())));

    assertThatThrownBy(() -> Utilities.collectEqualityDeleteFieldIds(schema, "test_table"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("LIST column")
        .hasMessageContaining("items");
  }

  @Test
  public void testCollectEqualityDeleteFieldIds_deeplyNested() {
    org.apache.iceberg.Schema schema =
        new org.apache.iceberg.Schema(
            required(1, "id", Types.LongType.get()),
            optional(
                2,
                "level1",
                Types.StructType.of(
                    required(3, "a", Types.StringType.get()),
                    optional(
                        4,
                        "level2",
                        Types.StructType.of(
                            required(5, "b", Types.IntegerType.get()),
                            optional(6, "c", Types.LongType.get()))))));

    Set<Integer> fieldIds = Utilities.collectEqualityDeleteFieldIds(schema, "test_table");

    // Should include all primitives: id (1), a (3), b (5), c (6)
    assertThat(fieldIds).containsExactlyInAnyOrder(1, 3, 5, 6);
  }
}
