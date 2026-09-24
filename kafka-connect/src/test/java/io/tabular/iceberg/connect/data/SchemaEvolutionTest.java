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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.tabular.iceberg.connect.IcebergSinkConfig;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class SchemaEvolutionTest {
  @TempDir Path temp;

  private IcebergSinkConfig config(String... settings) {
    Map<String, String> props = Maps.newHashMap();
    props.put("topics", "source");
    props.put("iceberg.catalog.type", "hadoop");
    props.put("iceberg.tables", "db.test");
    props.put("iceberg.tables.evolve-schema-enabled", "true");
    for (int i = 0; i < settings.length; i += 2) {
      props.put(settings[i], settings[i + 1]);
    }
    return new IcebergSinkConfig(props);
  }

  private RecordConverter converter(org.apache.iceberg.Schema schema, IcebergSinkConfig config) {
    Table table = mock(Table.class);
    when(table.schema()).thenReturn(schema);
    return new RecordConverter(table, config);
  }

  private org.apache.iceberg.Schema one(org.apache.iceberg.types.Type type) {
    return new org.apache.iceberg.Schema(Types.NestedField.required(1, "x", type));
  }

  private Schema row(Schema field) {
    return SchemaBuilder.struct().field("x", field).build();
  }

  @Test
  void rejectsLossyAndLogicalTypeChanges() {
    RecordConverter converter = converter(one(Types.IntegerType.get()), config());
    assertThatThrownBy(() -> converter.planSchema(row(Schema.FLOAT64_SCHEMA), false))
        .hasMessageContaining("x")
        .hasMessageContaining("double")
        .hasMessageContaining("int");
    RecordConverter temporal =
        converter(
            one(Types.LongType.get()), config("iceberg.tables.schema-debezium-time-types", "true"));
    assertThatThrownBy(
            () ->
                temporal.planSchema(
                    row(SchemaBuilder.int64().name("io.debezium.time.MicroTimestamp").build()),
                    false))
        .hasMessageContaining("timestamp");
  }

  @Test
  void allowsOlderNarrowerSchema() {
    assertThat(
            converter(one(Types.LongType.get()), config())
                .planSchema(row(Schema.INT32_SCHEMA), false)
                .empty())
        .isTrue();
  }

  @Test
  void plansAllNestedChangesWithoutValues() {
    org.apache.iceberg.Schema target =
        new org.apache.iceberg.Schema(
            Types.NestedField.required(1, "top", Types.IntegerType.get()),
            Types.NestedField.optional(
                2,
                "nested",
                Types.StructType.of(Types.NestedField.required(3, "x", Types.IntegerType.get()))),
            Types.NestedField.required(
                4, "items", Types.ListType.ofRequired(5, Types.IntegerType.get())),
            Types.NestedField.required(
                6,
                "map",
                Types.MapType.ofRequired(7, 8, Types.StringType.get(), Types.FloatType.get())));
    Schema source =
        SchemaBuilder.struct()
            .field("top", Schema.INT64_SCHEMA)
            .field(
                "nested", SchemaBuilder.struct().optional().field("x", Schema.INT64_SCHEMA).build())
            .field("items", SchemaBuilder.array(Schema.OPTIONAL_INT64_SCHEMA).build())
            .field("map", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.FLOAT64_SCHEMA).build())
            .build();
    SchemaUpdate.Consumer plan = converter(target, config()).planSchema(source, false);
    assertThat(plan.updateTypes())
        .extracting(SchemaUpdate.UpdateType::name)
        .containsExactlyInAnyOrder("top", "nested.x", "items.element", "map.value");
    assertThat(plan.makeOptionals())
        .extracting(SchemaUpdate.MakeOptional::name)
        .containsExactly("items.element");
  }

  @Test
  void mapKeysCannotEvolve() {
    RecordConverter converter =
        converter(
            one(Types.MapType.ofRequired(2, 3, Types.IntegerType.get(), Types.StringType.get())),
            config());
    assertThatThrownBy(
            () ->
                converter.planSchema(
                    row(SchemaBuilder.map(Schema.INT64_SCHEMA, Schema.STRING_SCHEMA).build()),
                    false))
        .hasMessageContaining("x.key");
  }

  @Test
  void decimalPrecisionAndScale() {
    RecordConverter converter = converter(one(Types.DecimalType.of(10, 2)), config());
    SchemaUpdate.Consumer plan = converter.planSchema(row(Decimal.schema(2)), false);
    assertThat(plan.updateTypes())
        .singleElement()
        .extracting(SchemaUpdate.UpdateType::type)
        .isEqualTo(Types.DecimalType.of(38, 2));
    assertThatThrownBy(() -> converter.planSchema(row(Decimal.schema(3)), false))
        .hasMessageContaining("decimal(38, 3)");
    assertThatThrownBy(
            () -> converter.convertDecimal(new BigDecimal("1.234"), Types.DecimalType.of(10, 2)))
        .isInstanceOf(ArithmeticException.class);
    assertThatThrownBy(
            () -> converter.convertDecimal(new BigDecimal("123.45"), Types.DecimalType.of(4, 2)))
        .hasMessageContaining("exceeds");
  }

  @Test
  void variableDecimalFlagIsOptInAndExact() {
    Schema variable =
        SchemaBuilder.struct()
            .name("io.debezium.data.VariableScaleDecimal")
            .field("scale", Schema.INT32_SCHEMA)
            .field("value", Schema.BYTES_SCHEMA)
            .build();
    assertThat(SchemaUtils.toIcebergType(variable, config()).isStructType()).isTrue();
    IcebergSinkConfig enabled = config("iceberg.tables.schema-variable-decimal-as-string", "true");
    RecordConverter converter = converter(one(Types.StringType.get()), enabled);
    Struct decimal =
        new Struct(variable)
            .put("scale", 3)
            .put(
                "value",
                ByteBuffer.wrap(
                    new BigInteger("-1234567890123456789012345678901234567890123").toByteArray()));
    assertThat(converter.planSchema(row(variable), false).empty()).isTrue();
    assertThat(converter.convert(new Struct(row(variable)).put("x", decimal)).getField("x"))
        .isEqualTo("-1234567890123456789012345678901234567890.123");
    assertThatThrownBy(
            () ->
                converter(
                        one(
                            Types.StructType.of(
                                Types.NestedField.required(2, "scale", Types.IntegerType.get()),
                                Types.NestedField.required(3, "value", Types.BinaryType.get()))),
                        enabled)
                    .planSchema(row(variable), false))
        .hasMessageContaining("Incompatible schema");
  }

  @Test
  void dropAndReaddRespectRetainedType() {
    RecordConverter converter = converter(one(Types.IntegerType.get()), config());
    assertThat(converter.planSchema(SchemaBuilder.struct().build(), false).makeOptionals())
        .extracting(SchemaUpdate.MakeOptional::name)
        .containsExactly("x");
    assertThatThrownBy(() -> converter.planSchema(row(Schema.STRING_SCHEMA), false))
        .hasMessageContaining("x");
    assertThatThrownBy(() -> converter.planSchema(SchemaBuilder.struct().build(), true))
        .hasMessageContaining("Cannot DROP");
    assertThatThrownBy(
            () ->
                converter.planSchema(
                    SchemaBuilder.struct()
                        .field("x", Schema.INT32_SCHEMA)
                        .field("added", Schema.STRING_SCHEMA)
                        .build(),
                    true))
        .hasMessageContaining("Cannot ADD");
  }

  @Test
  void identifiersMustRemainPresentAndRequired() {
    org.apache.iceberg.Schema schema =
        new org.apache.iceberg.Schema(one(Types.IntegerType.get()).columns(), Set.of(1));
    RecordConverter converter = converter(schema, config());
    assertThatThrownBy(() -> converter.planSchema(SchemaBuilder.struct().build(), false))
        .hasMessageContaining("identifier");
    assertThatThrownBy(() -> converter.planSchema(row(Schema.OPTIONAL_INT32_SCHEMA), false))
        .hasMessageContaining("nullable");
    assertThat(converter.planSchema(row(Schema.INT64_SCHEMA), false).updateTypes()).hasSize(1);
  }

  private Table table(org.apache.iceberg.Schema schema) {
    return new HadoopTables(new Configuration())
        .create(
            schema,
            PartitionSpec.unpartitioned(),
            Map.of("format-version", "2"),
            temp.resolve(UUID.randomUUID().toString()).toString());
  }

  private SinkRecord event(Schema idType, Object id, String op, String data, long offset) {
    Schema key = SchemaBuilder.struct().field("id", idType).build();
    Schema value =
        SchemaBuilder.struct()
            .field("id", idType)
            .field("op", Schema.STRING_SCHEMA)
            .field("data", Schema.STRING_SCHEMA)
            .build();
    return new SinkRecord(
        "source",
        0,
        key,
        new Struct(key).put("id", id),
        value,
        new Struct(value).put("id", id).put("op", op).put("data", data),
        offset);
  }

  private void commit(Table table, IcebergSinkConfig config, SinkRecord... records) {
    IcebergWriter writer = new IcebergWriter(table, "db.test", config, true);
    try {
      for (SinkRecord record : records) {
        writer.write(record);
      }
      WriteComplete result = writer.complete();
      // Match Coordinator: all files from the commit window belong to one RowDelta.
      RowDelta delta = table.newRowDelta();
      for (WriterResult files : result.writerResults()) {
        files.dataFiles().forEach(delta::addRows);
        files.deleteFiles().forEach(delta::addDeletes);
      }
      delta.commit();
    } finally {
      writer.close();
    }
  }

  @Test
  void keyPromotionKeepsOldEqualityDeletesAndUpdatesReadable() throws Exception {
    org.apache.iceberg.Schema schema =
        new org.apache.iceberg.Schema(
            List.of(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.required(2, "op", Types.StringType.get()),
                Types.NestedField.required(3, "data", Types.StringType.get())),
            Set.of(1));
    Table table = table(schema);
    IcebergSinkConfig cfg = config("iceberg.tables.cdc-field", "op");
    commit(
        table,
        cfg,
        event(Schema.INT32_SCHEMA, 1, "I", "old", 0),
        event(Schema.INT32_SCHEMA, 2, "I", "remove", 1));
    commit(table, cfg, event(Schema.INT32_SCHEMA, 2, "D", "remove", 2));
    commit(
        table,
        cfg,
        event(Schema.INT64_SCHEMA, 1L, "U", "new", 3),
        event(Schema.INT64_SCHEMA, 3000000000L, "I", "large", 4));
    assertThat(table.schema().findType("id")).isEqualTo(Types.LongType.get());
    assertThat(table.schema().identifierFieldIds()).containsExactly(1);
    List<String> rows = Lists.newArrayList();
    try (CloseableIterable<Record> scan = IcebergGenerics.read(table).build()) {
      for (Record record : scan) {
        rows.add(record.getField("id") + ":" + record.getField("data"));
      }
    }
    assertThat(rows).containsExactlyInAnyOrder("1:new", "3000000000:large");
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void keyPromotionWithinOneCommitDoesNotLeaveOldRow(boolean partitioned) throws Exception {
    org.apache.iceberg.Schema target =
        new org.apache.iceberg.Schema(
            List.of(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.required(2, "op", Types.StringType.get()),
                Types.NestedField.required(3, "data", Types.StringType.get())),
            Set.of(1));
    Table table =
        new HadoopTables(new Configuration())
            .create(
                target,
                partitioned
                    ? PartitionSpec.builderFor(target).identity("id").build()
                    : PartitionSpec.unpartitioned(),
                Map.of("format-version", "2"),
                temp.resolve("boundary").toString());
    commit(
        table,
        config("iceberg.tables.cdc-field", "op"),
        event(Schema.INT32_SCHEMA, 1, "I", "old", 0),
        event(Schema.INT64_SCHEMA, 3000000000L, "I", "old-large", 1),
        event(Schema.INT64_SCHEMA, Long.MIN_VALUE, "I", "sentinel", 2),
        event(Schema.INT64_SCHEMA, 3000000000L, "U", "large", 3),
        event(Schema.INT64_SCHEMA, 1L, "U", "new", 4));
    List<String> rows = Lists.newArrayList();
    try (CloseableIterable<Record> scan = IcebergGenerics.read(table).build()) {
      for (Record record : scan) {
        rows.add(record.getField("id") + ":" + record.getField("data"));
      }
    }
    assertThat(rows)
        .containsExactlyInAnyOrder("1:new", "3000000000:large", Long.MIN_VALUE + ":sentinel");
  }

  @Test
  void appliesNestedAndDecimalEvolutionBeforeConvertingNullAndEmptyValues() {
    Table table =
        table(
            new org.apache.iceberg.Schema(
                Types.NestedField.optional(
                    1,
                    "nested",
                    Types.StructType.of(
                        Types.NestedField.required(2, "x", Types.IntegerType.get()))),
                Types.NestedField.required(
                    3, "items", Types.ListType.ofRequired(4, Types.IntegerType.get())),
                Types.NestedField.required(5, "amount", Types.DecimalType.of(10, 2))));
    Schema input =
        SchemaBuilder.struct()
            .field(
                "nested", SchemaBuilder.struct().optional().field("x", Schema.INT64_SCHEMA).build())
            .field("items", SchemaBuilder.array(Schema.INT64_SCHEMA).build())
            .field("amount", Decimal.schema(2))
            .build();
    Struct value =
        new Struct(input)
            .put("nested", null)
            .put("items", List.of())
            .put("amount", new BigDecimal("123456789012.34"));
    commit(table, config(), new SinkRecord("source", 0, null, null, input, value, 0));
    assertThat(table.schema().findType("nested.x")).isEqualTo(Types.LongType.get());
    assertThat(table.schema().findType("items.element")).isEqualTo(Types.LongType.get());
    assertThat(table.schema().findType("amount")).isEqualTo(Types.DecimalType.of(38, 2));
  }

  @Test
  void noPkCdcRejectsShapeChangeBeforeUpdatingTableOrTrackingOffset() {
    Table table = table(one(Types.IntegerType.get()));
    IcebergWriter writer =
        new IcebergWriter(
            table, "db.test", config("iceberg.tables.upsert-mode-enabled", "true"), false);
    Schema source =
        SchemaBuilder.struct()
            .field("x", Schema.INT32_SCHEMA)
            .field("new_col", Schema.STRING_SCHEMA)
            .build();
    try {
      assertThatThrownBy(
              () ->
                  writer.write(
                      new SinkRecord(
                          "source",
                          0,
                          null,
                          null,
                          source,
                          new Struct(source).put("x", 1).put("new_col", "new"),
                          42)))
          .hasRootCauseMessage("Cannot ADD new_col in CDC without a primary key");
      assertThat(table.schema().findField("new_col")).isNull();
      assertThat(writer.complete().dataOffsets()).isEmpty();
    } finally {
      writer.close();
    }
  }
}
