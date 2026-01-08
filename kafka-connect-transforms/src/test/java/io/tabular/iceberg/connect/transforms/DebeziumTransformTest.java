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
package io.tabular.iceberg.connect.transforms;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

public class DebeziumTransformTest {

  private static final Schema KEY_SCHEMA =
      SchemaBuilder.struct().field("account_id", Schema.INT64_SCHEMA).build();

  private static final Schema ROW_SCHEMA =
      SchemaBuilder.struct()
          .field("account_id", Schema.INT64_SCHEMA)
          .field("balance", Decimal.schema(2))
          .field("last_updated", Schema.STRING_SCHEMA)
          .build();

  private static final Schema SOURCE_SCHEMA =
      SchemaBuilder.struct()
          .field("db", Schema.STRING_SCHEMA)
          .field("schema", Schema.STRING_SCHEMA)
          .field("table", Schema.STRING_SCHEMA)
          .field("ts_ms", Schema.INT64_SCHEMA)
          .build();

  private static final Schema VALUE_SCHEMA =
      SchemaBuilder.struct()
          .field("op", Schema.STRING_SCHEMA)
          .field("ts_ms", Schema.INT64_SCHEMA)
          .field("source", SOURCE_SCHEMA)
          .field("before", ROW_SCHEMA)
          .field("after", ROW_SCHEMA)
          .build();

  @Test
  public void testDmsTransformNull() {
    try (DmsTransform<SinkRecord> smt = new DmsTransform<>()) {
      SinkRecord record = new SinkRecord("topic", 0, null, null, null, null, 0);
      SinkRecord result = smt.apply(record);
      assertThat(result.value()).isNull();
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testDebeziumTransformSchemaless() {
    try (DebeziumTransform<SinkRecord> smt = new DebeziumTransform<>()) {
      smt.configure(ImmutableMap.of("cdc.target.pattern", "{db}_x.{table}_x"));

      Map<String, Object> event = createDebeziumEventMap("u");
      Map<String, Object> key = ImmutableMap.of("account_id", 1L);
      SinkRecord record = new SinkRecord("topic", 0, null, key, null, event, 0);

      SinkRecord result = smt.apply(record);
      assertThat(result.value()).isInstanceOf(Map.class);
      Map<String, Object> value = (Map<String, Object>) result.value();

      assertThat(value.get("account_id")).isEqualTo(1);

      Map<String, Object> cdcMetadata = (Map<String, Object>) value.get("_cdc");
      assertThat(cdcMetadata.get("op")).isEqualTo("U");
      assertThat(cdcMetadata.get("source")).isEqualTo("schema.tbl");
      assertThat(cdcMetadata.get("target")).isEqualTo("schema_x.tbl_x");
      assertThat(cdcMetadata.get("key")).isInstanceOf(Map.class);
    }
  }

  @Test
  public void testDebeziumTransformWithSchema() {
    try (DebeziumTransform<SinkRecord> smt = new DebeziumTransform<>()) {
      smt.configure(ImmutableMap.of("cdc.target.pattern", "{db}_x.{table}_x"));

      Struct event = createDebeziumEventStruct("u");
      Struct key = new Struct(KEY_SCHEMA).put("account_id", 1L);
      SinkRecord record = new SinkRecord("server.schema.tbl", 0, KEY_SCHEMA, key, VALUE_SCHEMA, event, 0);

      SinkRecord result = smt.apply(record);
      assertThat(result.value()).isInstanceOf(Struct.class);
      Struct value = (Struct) result.value();

      assertThat(value.get("account_id")).isEqualTo(1L);

      Struct cdcMetadata = value.getStruct("_cdc");
      assertThat(cdcMetadata.get("op")).isEqualTo("U");
      assertThat(cdcMetadata.get("source")).isEqualTo("schema.tbl");
      assertThat(cdcMetadata.get("target")).isEqualTo("schema_x.tbl_x");
      assertThat(cdcMetadata.get("key")).isInstanceOf(Struct.class);
    }
  }

  private Map<String, Object> createDebeziumEventMap(String operation) {
    Map<String, Object> source =
        ImmutableMap.of(
            "db", "db",
            "schema", "schema",
            "table", "tbl");

    Map<String, Object> data =
        ImmutableMap.of(
            "account_id", 1,
            "balance", 100,
            "last_updated", Instant.now().toString());

    return ImmutableMap.of(
        "op", operation,
        "ts_ms", System.currentTimeMillis(),
        "source", source,
        "before", data,
        "after", data);
  }

  private Struct createDebeziumEventStruct(String operation) {
    Struct source =
        new Struct(SOURCE_SCHEMA).put("db", "db").put("schema", "schema").put("table", "tbl").put("ts_ms", System.currentTimeMillis());

    Struct data =
        new Struct(ROW_SCHEMA)
            .put("account_id", 1L)
            .put("balance", BigDecimal.valueOf(100))
            .put("last_updated", Instant.now().toString());

    return new Struct(VALUE_SCHEMA)
        .put("op", operation)
        .put("ts_ms", System.currentTimeMillis())
        .put("source", source)
        .put("before", data)
        .put("after", data);
  }

  @Test
  public void testMysqlSourceCoordinatesWithSchema() {
    Schema mysqlSourceSchema =
        SchemaBuilder.struct()
            .field("db", Schema.STRING_SCHEMA)
            .field("table", Schema.STRING_SCHEMA)
            .field("ts_ms", Schema.INT64_SCHEMA)
            .field("gtid", Schema.OPTIONAL_STRING_SCHEMA)
            .field("file", Schema.STRING_SCHEMA)
            .field("pos", Schema.INT64_SCHEMA)
            .build();

    Schema mysqlValueSchema =
        SchemaBuilder.struct()
            .field("op", Schema.STRING_SCHEMA)
            .field("ts_ms", Schema.INT64_SCHEMA)
            .field("source", mysqlSourceSchema)
            .field("before", ROW_SCHEMA)
            .field("after", ROW_SCHEMA)
            .build();

    try (DebeziumTransform<SinkRecord> smt = new DebeziumTransform<>()) {
      smt.configure(ImmutableMap.of());

      Struct source = new Struct(mysqlSourceSchema)
          .put("db", "testdb")
          .put("table", "users")
          .put("ts_ms", System.currentTimeMillis())
          .put("gtid", "dcfd2fb8-7bf6-11eb-8f35-005056a48b59:156855132")
          .put("file", "mysql-bin.000893")
          .put("pos", 1839458L);

      Struct data = new Struct(ROW_SCHEMA)
          .put("account_id", 1L)
          .put("balance", BigDecimal.valueOf(100))
          .put("last_updated", Instant.now().toString());

      Struct event = new Struct(mysqlValueSchema)
          .put("op", "c")
          .put("ts_ms", System.currentTimeMillis())
          .put("source", source)
          .put("before", data)
          .put("after", data);

      SinkRecord record = new SinkRecord("server.testdb.users", 0, null, null, mysqlValueSchema, event, 0);
      SinkRecord result = smt.apply(record);

      Struct value = (Struct) result.value();
      Struct cdcMetadata = value.getStruct("_cdc");

      assertThat(cdcMetadata.get("gtid")).isEqualTo("dcfd2fb8-7bf6-11eb-8f35-005056a48b59:156855132");
      assertThat(cdcMetadata.get("binlog_file")).isEqualTo("mysql-bin.000893");
      assertThat(cdcMetadata.get("binlog_pos")).isEqualTo(1839458L);
      assertThat(cdcMetadata.get("pgtxid")).isNull();
      assertThat(cdcMetadata.get("lsn")).isNull();
    }
  }

  @Test
  public void testPostgresSourceCoordinatesWithSchema() {
    Schema pgSourceSchema =
        SchemaBuilder.struct()
            .field("db", Schema.STRING_SCHEMA)
            .field("schema", Schema.STRING_SCHEMA)
            .field("table", Schema.STRING_SCHEMA)
            .field("ts_ms", Schema.INT64_SCHEMA)
            .field("txId", Schema.OPTIONAL_INT64_SCHEMA)
            .field("lsn", Schema.OPTIONAL_INT64_SCHEMA)
            .build();

    Schema pgValueSchema =
        SchemaBuilder.struct()
            .field("op", Schema.STRING_SCHEMA)
            .field("ts_ms", Schema.INT64_SCHEMA)
            .field("source", pgSourceSchema)
            .field("before", ROW_SCHEMA)
            .field("after", ROW_SCHEMA)
            .build();

    try (DebeziumTransform<SinkRecord> smt = new DebeziumTransform<>()) {
      smt.configure(ImmutableMap.of());

      Struct source = new Struct(pgSourceSchema)
          .put("db", "mobi_device")
          .put("schema", "public")
          .put("table", "device_info")
          .put("ts_ms", System.currentTimeMillis())
          .put("txId", 2906313417L)
          .put("lsn", 22261756615688L);

      Struct data = new Struct(ROW_SCHEMA)
          .put("account_id", 1L)
          .put("balance", BigDecimal.valueOf(100))
          .put("last_updated", Instant.now().toString());

      Struct event = new Struct(pgValueSchema)
          .put("op", "c")
          .put("ts_ms", System.currentTimeMillis())
          .put("source", source)
          .put("before", data)
          .put("after", data);

      SinkRecord record = new SinkRecord("server.public.device_info", 0, null, null, pgValueSchema, event, 0);
      SinkRecord result = smt.apply(record);

      Struct value = (Struct) result.value();
      Struct cdcMetadata = value.getStruct("_cdc");

      assertThat(cdcMetadata.get("gtid")).isNull();
      assertThat(cdcMetadata.get("binlog_file")).isNull();
      assertThat(cdcMetadata.get("binlog_pos")).isNull();
      assertThat(cdcMetadata.get("pgtxid")).isEqualTo(2906313417L);
      assertThat(cdcMetadata.get("lsn")).isEqualTo(22261756615688L);
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testMysqlSourceCoordinatesSchemaless() {
    try (DebeziumTransform<SinkRecord> smt = new DebeziumTransform<>()) {
      smt.configure(ImmutableMap.of());

      Map<String, Object> source = ImmutableMap.<String, Object>builder()
          .put("db", "testdb")
          .put("table", "users")
          .put("gtid", "dcfd2fb8-7bf6-11eb-8f35-005056a48b59:156855132")
          .put("file", "mysql-bin.000893")
          .put("pos", 1839458L)
          .build();

      Map<String, Object> data = ImmutableMap.of(
          "account_id", 1,
          "balance", 100,
          "last_updated", Instant.now().toString());

      Map<String, Object> event = ImmutableMap.of(
          "op", "c",
          "ts_ms", System.currentTimeMillis(),
          "source", source,
          "before", data,
          "after", data);

      SinkRecord record = new SinkRecord("topic", 0, null, null, null, event, 0);
      SinkRecord result = smt.apply(record);

      Map<String, Object> value = (Map<String, Object>) result.value();
      Map<String, Object> cdcMetadata = (Map<String, Object>) value.get("_cdc");

      assertThat(cdcMetadata.get("gtid")).isEqualTo("dcfd2fb8-7bf6-11eb-8f35-005056a48b59:156855132");
      assertThat(cdcMetadata.get("binlog_file")).isEqualTo("mysql-bin.000893");
      assertThat(cdcMetadata.get("binlog_pos")).isEqualTo(1839458L);
      assertThat(cdcMetadata.get("pgtxid")).isNull();
      assertThat(cdcMetadata.get("lsn")).isNull();
    }
  }
}
