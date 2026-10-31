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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The DebeziumTransform caches the schemas it builds. Every result is compared with the one of the
 * transform before the cache, which built both schemas again for every record.
 */
public class DebeziumTransformSchemaCacheTest {

  private static final Schema KEY_SCHEMA =
      SchemaBuilder.struct().field("id", Schema.INT64_SCHEMA).build();

  private static final Schema ADDRESS_SCHEMA =
      SchemaBuilder.struct()
          .optional()
          .field("street", Schema.OPTIONAL_STRING_SCHEMA)
          .field("zip", Schema.OPTIONAL_INT32_SCHEMA)
          .build();

  private static final Schema ROW_V1 =
      SchemaBuilder.struct()
          .name("server.public.customers.Value")
          .optional()
          .field("id", Schema.INT64_SCHEMA)
          .field("name", Schema.OPTIONAL_STRING_SCHEMA)
          .field("balance", Decimal.builder(2).optional().build())
          .field("address", ADDRESS_SCHEMA)
          .build();

  private static final Schema ROW_V2 =
      SchemaBuilder.struct()
          .name("server.public.customers.Value")
          .optional()
          .field("id", Schema.INT64_SCHEMA)
          .field("name", Schema.OPTIONAL_STRING_SCHEMA)
          .field("balance", Decimal.builder(2).optional().build())
          .field("address", ADDRESS_SCHEMA)
          .field("email", Schema.OPTIONAL_STRING_SCHEMA)
          .build();

  private static final Schema PG_SOURCE =
      SchemaBuilder.struct()
          .field("db", Schema.STRING_SCHEMA)
          .field("schema", Schema.STRING_SCHEMA)
          .field("table", Schema.STRING_SCHEMA)
          .field("ts_ms", Schema.INT64_SCHEMA)
          .field("txId", Schema.OPTIONAL_INT64_SCHEMA)
          .field("lsn", Schema.OPTIONAL_INT64_SCHEMA)
          .field("snapshot", Schema.OPTIONAL_STRING_SCHEMA)
          .build();

  private static final Schema MYSQL_SOURCE =
      SchemaBuilder.struct()
          .field("db", Schema.STRING_SCHEMA)
          .field("table", Schema.STRING_SCHEMA)
          .field("ts_ms", Schema.INT64_SCHEMA)
          .field("gtid", Schema.OPTIONAL_STRING_SCHEMA)
          .field("file", Schema.STRING_SCHEMA)
          .field("pos", Schema.INT64_SCHEMA)
          .build();

  private DebeziumTransform<SinkRecord> transform;
  private LegacyDebeziumTransform<SinkRecord> legacy;

  @BeforeEach
  public void before() {
    transform = new DebeziumTransform<>();
    legacy = new LegacyDebeziumTransform<>();
    transform.configure(ImmutableMap.of("cdc.target.pattern", "{db}_x.{table}_x"));
    legacy.configure(ImmutableMap.of("cdc.target.pattern", "{db}_x.{table}_x"));
  }

  @AfterEach
  public void after() {
    transform.close();
    legacy.close();
  }

  @Test
  public void testAllOperationsMatchTheUncachedTransform() {
    Schema pgEnvelope = envelope(ROW_V1, PG_SOURCE);
    Schema mysqlEnvelope = envelope(ROW_V1, MYSQL_SOURCE);

    long offset = 0;
    for (int round = 0; round < 3; round++) {
      for (String op : new String[] {"c", "r", "u", "d"}) {
        assertSameResult(
            record("srv.public.customers", KEY_SCHEMA, key(1L), pgEnvelope, pgEvent(pgEnvelope, op, 1L), offset++));
        assertSameResult(
            record("srv.shop.customers", KEY_SCHEMA, key(2L), mysqlEnvelope, mysqlEvent(mysqlEnvelope, op, 2L), offset++));
        // no key at all
        assertSameResult(
            record("srv.public.customers", null, null, pgEnvelope, pgEvent(pgEnvelope, op, 3L), offset++));
      }
    }
  }

  @Test
  public void testUpdateWithoutBeforeImageAndMessageEvents() {
    Schema envelope = envelope(ROW_V1, PG_SOURCE);

    Struct update = pgEvent(envelope, "u", 1L);
    update.put("before", null);
    assertSameResult(record("srv.public.customers", KEY_SCHEMA, key(1L), envelope, update, 0));
    Struct update2 = pgEvent(envelope, "u", 1L);
    assertSameResult(record("srv.public.customers", KEY_SCHEMA, key(1L), envelope, update2, 1));

    // the same transform skips message events and passes tombstones on
    SinkRecord message =
        record("srv.public.customers", KEY_SCHEMA, key(1L), envelope, pgEvent(envelope, "m", 1L), 2);
    assertThat(transform.apply(message)).isNull();
    assertThat(legacy.apply(message)).isNull();
    SinkRecord tombstone = new SinkRecord("srv.public.customers", 0, KEY_SCHEMA, key(1L), null, null, 3);
    assertThat(transform.apply(tombstone)).isSameAs(tombstone);
  }

  @Test
  public void testSchemaChangeMidStream() {
    Schema envelopeV1 = envelope(ROW_V1, PG_SOURCE);
    Schema envelopeV2 = envelope(ROW_V2, PG_SOURCE);

    SinkRecord v1 = apply(record("srv.public.customers", KEY_SCHEMA, key(1L), envelopeV1, pgEvent(envelopeV1, "u", 1L), 0));
    SinkRecord v2 = apply(record("srv.public.customers", KEY_SCHEMA, key(1L), envelopeV2, pgEvent(envelopeV2, "u", 1L), 1));
    SinkRecord v1Again = apply(record("srv.public.customers", KEY_SCHEMA, key(1L), envelopeV1, pgEvent(envelopeV1, "u", 1L), 2));

    assertThat(v1.valueSchema().field("email")).isNull();
    assertThat(v2.valueSchema().field("email")).isNotNull();
    assertThat(v1Again.valueSchema().field("email")).isNull();
    assertThat(v1Again.valueSchema()).isSameAs(v1.valueSchema());
    assertThat(v2.valueSchema()).isNotSameAs(v1.valueSchema());
    assertThat(v2.valueSchema().field("_cdc").schema()).isSameAs(v1.valueSchema().field("_cdc").schema());
  }

  @Test
  public void testKeySchemaChange() {
    Schema compositeKey =
        SchemaBuilder.struct().field("id", Schema.INT64_SCHEMA).field("tenant", Schema.STRING_SCHEMA).build();
    Schema envelope = envelope(ROW_V1, PG_SOURCE);

    SinkRecord first = apply(record("srv.public.customers", KEY_SCHEMA, key(1L), envelope, pgEvent(envelope, "c", 1L), 0));
    Struct composite = new Struct(compositeKey).put("id", 1L).put("tenant", "t");
    SinkRecord second = apply(record("srv.public.customers", compositeKey, composite, envelope, pgEvent(envelope, "c", 1L), 1));
    SinkRecord third = apply(record("srv.public.customers", KEY_SCHEMA, key(1L), envelope, pgEvent(envelope, "c", 1L), 2));
    SinkRecord keyless = apply(record("srv.public.customers", null, null, envelope, pgEvent(envelope, "c", 1L), 3));

    assertThat(((Struct) first.value()).getStruct("_cdc").schema().field("key").schema()).isEqualTo(KEY_SCHEMA);
    assertThat(((Struct) second.value()).getStruct("_cdc").schema().field("key").schema()).isEqualTo(compositeKey);
    assertThat(third.valueSchema()).isSameAs(first.valueSchema());
    assertThat(((Struct) keyless.value()).getStruct("_cdc").schema().field("key")).isNull();
  }

  @Test
  public void testSchemasAreReusedPerInputSchemaInstance() {
    Schema envelope = envelope(ROW_V1, PG_SOURCE);

    SinkRecord insert1 = apply(record("srv.public.customers", KEY_SCHEMA, key(1L), envelope, pgEvent(envelope, "c", 1L), 0));
    SinkRecord insert2 = apply(record("srv.public.customers", KEY_SCHEMA, key(2L), envelope, pgEvent(envelope, "c", 2L), 1));
    SinkRecord update = apply(record("srv.public.customers", KEY_SCHEMA, key(2L), envelope, pgEvent(envelope, "u", 2L), 2));
    SinkRecord update2 = apply(record("srv.public.customers", KEY_SCHEMA, key(2L), envelope, pgEvent(envelope, "u", 2L), 3));

    assertThat(insert2.valueSchema()).isSameAs(insert1.valueSchema());
    // an update carries the before image, which makes it another schema
    assertThat(update.valueSchema()).isNotSameAs(insert1.valueSchema());
    assertThat(update.valueSchema().field("_before_image")).isNotNull();
    assertThat(update2.valueSchema()).isSameAs(update.valueSchema());
    // all of them share the one CDC schema of the key schema
    assertThat(update.valueSchema().field("_cdc").schema())
        .isSameAs(insert1.valueSchema().field("_cdc").schema());
  }

  @Test
  public void testEqualButNotIdenticalSchemas() {
    // what a converter without a schema cache hands out: a new instance for every record
    for (int i = 0; i < 5; i++) {
      Schema key = SchemaBuilder.struct().field("id", Schema.INT64_SCHEMA).build();
      Schema envelope = envelope(copyOf(ROW_V1), PG_SOURCE);
      assertSameResult(
          record("srv.public.customers", key, new Struct(key).put("id", 1L), envelope, pgEvent(envelope, "u", 1L), i));
    }
  }

  @Test
  public void testSchemaBuilderIsNotCached() {
    // a SchemaBuilder is no immutable schema, a change to it must show in the next record
    SchemaBuilder row = SchemaBuilder.struct().field("id", Schema.INT64_SCHEMA);
    SchemaBuilder envelope =
        SchemaBuilder.struct()
            .field("op", Schema.STRING_SCHEMA)
            .field("ts_ms", Schema.INT64_SCHEMA)
            .field("source", PG_SOURCE)
            .field("before", row)
            .field("after", row);

    Struct event = pgEvent(envelope, "c", 1L);
    SinkRecord first = apply(record("srv.public.customers", KEY_SCHEMA, key(1L), envelope, event, 0));
    assertThat(first.valueSchema().field("name")).isNull();

    row.field("name", Schema.OPTIONAL_STRING_SCHEMA);
    Struct changed = pgEvent(envelope, "c", 1L);
    SinkRecord second = apply(record("srv.public.customers", KEY_SCHEMA, key(1L), envelope, changed, 1));
    assertThat(second.valueSchema().field("name")).isNotNull();
    assertThat(second.valueSchema()).isNotSameAs(first.valueSchema());
  }

  @Test
  public void testCacheIsBounded() {
    Schema envelope = envelope(ROW_V1, PG_SOURCE);
    for (int i = 0; i < 5000; i++) {
      // a new payload schema instance for every record
      Schema row = copyOf(ROW_V1);
      Schema env = envelope(row, PG_SOURCE);
      SinkRecord record =
          record("srv.public.customers", KEY_SCHEMA, key(i), env, pgEvent(env, "c", i), i);
      SinkRecord result = transform.apply(record);
      assertThat(result.valueSchema().fields()).hasSize(ROW_V1.fields().size() + 3);
    }
    assertThat(transform.cachedSchemaCount()).isLessThanOrEqualTo(2 * 1024);

    // entries pushed out are built again, with the same result
    assertSameResult(record("srv.public.customers", KEY_SCHEMA, key(1L), envelope, pgEvent(envelope, "u", 1L), 0));
  }

  @Test
  public void testConcurrentUse() throws Exception {
    List<Schema> envelopes = new ArrayList<>();
    for (int i = 0; i < 8; i++) {
      envelopes.add(envelope(copyOf(i % 2 == 0 ? ROW_V1 : ROW_V2), i % 3 == 0 ? MYSQL_SOURCE : PG_SOURCE));
    }
    ExecutorService executor = Executors.newFixedThreadPool(4);
    try {
      List<Future<?>> futures = new ArrayList<>();
      for (int t = 0; t < 4; t++) {
        futures.add(
            executor.submit(
                () -> {
                  for (int i = 0; i < 2000; i++) {
                    Schema env = envelopes.get(i % envelopes.size());
                    Struct event =
                        env.field("source").schema() == MYSQL_SOURCE
                            ? mysqlEvent(env, "u", i)
                            : pgEvent(env, "u", i);
                    String topic = env.field("source").schema() == MYSQL_SOURCE ? "srv.shop.t" : "srv.public.t";
                    SinkRecord record = record(topic, KEY_SCHEMA, key(i), env, event, i);
                    assertThat(transform.apply(record).value()).isEqualTo(legacy.apply(record).value());
                  }
                }));
      }
      for (Future<?> future : futures) {
        future.get();
      }
    } finally {
      executor.shutdownNow();
    }
  }

  private void assertSameResult(SinkRecord record) {
    SinkRecord expected = legacy.apply(record);
    SinkRecord actual = transform.apply(record);
    assertThat(actual.topic()).isEqualTo(expected.topic());
    assertThat(actual.kafkaPartition()).isEqualTo(expected.kafkaPartition());
    assertThat(actual.kafkaOffset()).isEqualTo(expected.kafkaOffset());
    assertThat(actual.keySchema()).isEqualTo(expected.keySchema());
    assertThat(actual.key()).isEqualTo(expected.key());
    assertThat(actual.valueSchema()).isEqualTo(expected.valueSchema());
    assertThat(actual.value()).isEqualTo(expected.value());
    assertThat(actual.timestamp()).isEqualTo(expected.timestamp());
  }

  /** The result of the transform, after checking it against the uncached one. */
  private SinkRecord apply(SinkRecord record) {
    assertSameResult(record);
    return transform.apply(record);
  }

  private static Schema copyOf(Schema row) {
    SchemaBuilder copy = SchemaBuilder.struct().name(row.name());
    if (row.isOptional()) {
      copy.optional();
    }
    row.fields().forEach(field -> copy.field(field.name(), field.schema()));
    return copy.build();
  }

  private static Schema envelope(Schema row, Schema source) {
    return SchemaBuilder.struct()
        .name("srv.Envelope")
        .field("op", Schema.STRING_SCHEMA)
        .field("ts_ms", Schema.OPTIONAL_INT64_SCHEMA)
        .field("source", source)
        .field("before", row)
        .field("after", row)
        .build();
  }

  private static SinkRecord record(
      String topic, Schema keySchema, Object key, Schema valueSchema, Object value, long offset) {
    return new SinkRecord(topic, 3, keySchema, key, valueSchema, value, offset, 1_700_000_000_000L, null);
  }

  private static Struct key(long id) {
    return new Struct(KEY_SCHEMA).put("id", id);
  }

  private static Struct row(Schema rowSchema, long id) {
    Struct row = new Struct(rowSchema).put("id", id);
    if (rowSchema.field("name") != null) {
      row.put("name", "name-" + id);
    }
    if (rowSchema.field("balance") != null) {
      row.put("balance", BigDecimal.valueOf(id * 100, 2));
    }
    if (rowSchema.field("address") != null && id % 2 == 0) {
      row.put("address", new Struct(rowSchema.field("address").schema()).put("street", "main").put("zip", 12345));
    }
    if (rowSchema.field("email") != null) {
      row.put("email", id + "@example.com");
    }
    return row;
  }

  private static Struct pgEvent(Schema envelope, String op, long id) {
    Struct source =
        new Struct(PG_SOURCE)
            .put("db", "srv")
            .put("schema", "public")
            .put("table", "customers")
            .put("ts_ms", 1_700_000_000_123L)
            .put("txId", 99L)
            .put("lsn", 123456789L)
            .put("snapshot", "false");
    return event(envelope, op, id, source);
  }

  private static Struct mysqlEvent(Schema envelope, String op, long id) {
    Struct source =
        new Struct(MYSQL_SOURCE)
            .put("db", "shop")
            .put("table", "customers")
            .put("ts_ms", 1_700_000_000_456L)
            .put("gtid", "abc:1")
            .put("file", "binlog.000001")
            .put("pos", 4242L);
    return event(envelope, op, id, source);
  }

  private static Struct event(Schema envelope, String op, long id, Struct source) {
    Struct event = new Struct(envelope).put("op", op).put("ts_ms", 1_700_000_000_999L).put("source", source);
    Schema rowSchema = envelope.field("after").schema();
    switch (op) {
      case "c":
      case "r":
        event.put("after", row(rowSchema, id));
        break;
      case "u":
        event.put("before", row(rowSchema, id - 1));
        event.put("after", row(rowSchema, id));
        break;
      case "d":
        event.put("before", row(rowSchema, id));
        break;
      default:
        break;
    }
    return event;
  }
}
