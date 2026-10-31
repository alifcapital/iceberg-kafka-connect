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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.tabular.iceberg.connect.IcebergSinkConfig;
import java.util.Set;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.mapping.MappedField;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StructType;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * The converter resolves the table field of every source field once per source schema. The result
 * is compared with a lookup by name for each field of each record.
 */
public class RecordConverterFieldMappingTest {

  private static final Schema TABLE_V1 =
      new Schema(
          Types.NestedField.required(1, "id", Types.LongType.get()),
          Types.NestedField.optional(2, "name", Types.StringType.get()),
          Types.NestedField.optional(
              3,
              "Address",
              Types.StructType.of(
                  Types.NestedField.optional(4, "street", Types.StringType.get()),
                  Types.NestedField.optional(5, "zip", Types.IntegerType.get()))),
          Types.NestedField.optional(
              6,
              "_cdc",
              Types.StructType.of(Types.NestedField.optional(7, "op", Types.StringType.get()))),
          Types.NestedField.optional(
              8,
              "_cdc_before_image",
              Types.StructType.of(
                  Types.NestedField.optional(9, "id", Types.LongType.get()),
                  Types.NestedField.optional(10, "name", Types.StringType.get()))));

  // the table after the source added "email" and evolved
  private static final Schema TABLE_V2 =
      new Schema(
          ImmutableList.<NestedField>builder()
              .addAll(TABLE_V1.columns())
              .add(Types.NestedField.optional(11, "email", Types.StringType.get()))
              .build());

  private static final org.apache.kafka.connect.data.Schema ADDRESS =
      SchemaBuilder.struct()
          .optional()
          .field("Street", org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA)
          .field("zip", org.apache.kafka.connect.data.Schema.OPTIONAL_INT32_SCHEMA)
          .build();

  private static final org.apache.kafka.connect.data.Schema CDC =
      SchemaBuilder.struct()
          .optional()
          .field("op", org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA)
          .build();

  private static final org.apache.kafka.connect.data.Schema IMAGE =
      SchemaBuilder.struct()
          .optional()
          .field("ID", org.apache.kafka.connect.data.Schema.INT64_SCHEMA)
          .field("name", org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA)
          .build();

  private IcebergSinkConfig config;

  @BeforeEach
  public void before() {
    config = mock(IcebergSinkConfig.class);
  }

  private static SchemaBuilder sourceBuilder(boolean withEmail) {
    SchemaBuilder builder =
        SchemaBuilder.struct()
            .field("ID", org.apache.kafka.connect.data.Schema.INT64_SCHEMA)
            .field("Name", org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA)
            .field("address", ADDRESS)
            .field("_cdc", CDC)
            .field("_before_image", IMAGE)
            .field("unknown", org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA);
    if (withEmail) {
      builder.field("email", org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA);
    }
    return builder;
  }

  private static Struct source(org.apache.kafka.connect.data.Schema schema, long id) {
    Struct struct =
        new Struct(schema)
            .put("ID", id)
            .put("Name", "name-" + id)
            .put("address", new Struct(ADDRESS).put("Street", "main " + id).put("zip", 100))
            .put("_cdc", new Struct(CDC).put("op", "u"))
            .put("_before_image", new Struct(IMAGE).put("ID", id - 1).put("name", "old-" + id))
            .put("unknown", "ignored");
    if (schema.field("email") != null) {
      struct.put("email", id + "@example.com");
    }
    return struct;
  }

  private RecordConverter converter(Schema tableSchema, boolean writeBeforeImage) {
    Table table = mock(Table.class);
    when(table.schema()).thenReturn(tableSchema);
    return new RecordConverter(table, config, writeBeforeImage);
  }

  /** A lookup of every field by name, for every record. */
  private static Record expected(
      Struct struct, StructType target, boolean caseInsensitive, boolean writeBeforeImage) {
    GenericRecord record = GenericRecord.create(target);
    for (Field field : struct.schema().fields()) {
      String name = field.name();
      if (name.equals("_before_image")) {
        if (!writeBeforeImage) {
          continue;
        }
        name = "_cdc_before_image";
      }
      NestedField tableField =
          caseInsensitive ? target.caseInsensitiveField(name) : target.field(name);
      if (tableField == null) {
        continue;
      }
      Object value = struct.get(field);
      if (value instanceof Struct) {
        value =
            expected(
                (Struct) value,
                tableField.type().asStructType(),
                caseInsensitive,
                writeBeforeImage);
      }
      record.setField(tableField.name(), value);
    }
    return record;
  }

  @ParameterizedTest
  @CsvSource({"false,false", "false,true", "true,false", "true,true"})
  public void testMatchesLookupByNameForEveryRecord(boolean caseInsensitive, boolean writeBefore) {
    when(config.schemaCaseInsensitive()).thenReturn(caseInsensitive);
    RecordConverter converter = converter(TABLE_V1, writeBefore);
    org.apache.kafka.connect.data.Schema shared = sourceBuilder(false).build();

    for (int i = 0; i < 20; i++) {
      // the shared instance, and equal copies as a converter without a schema cache hands out
      org.apache.kafka.connect.data.Schema schema = i % 3 == 0 ? sourceBuilder(false).build() : shared;
      Struct struct = source(schema, i + 1);
      assertThat(converter.convert(struct))
          .isEqualTo(expected(struct, TABLE_V1.asStruct(), caseInsensitive, writeBefore));
    }

    // spot check the content, not only the equality with the lookup
    Record record = converter.convert(source(shared, 7));
    assertThat(record.getField("id")).isEqualTo(caseInsensitive ? 7L : null);
    assertThat(record.getField("name")).isEqualTo(caseInsensitive ? "name-7" : null);
    assertThat(record.getField("Address") != null).isEqualTo(caseInsensitive);
    assertThat(record.getField("_cdc")).isNotNull();
    assertThat(record.getField("_cdc_before_image") != null).isEqualTo(writeBefore);
  }

  @Test
  public void testSchemaBuilderChangesAreSeen() {
    when(config.schemaCaseInsensitive()).thenReturn(true);
    RecordConverter converter = converter(TABLE_V2, false);
    SchemaBuilder builder = sourceBuilder(false);

    Struct before = source(builder, 1);
    assertThat(converter.convert(before).getField("email")).isNull();

    // a SchemaBuilder is not an immutable schema and may gain a field between two records
    builder.field("email", org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA);
    Struct after = source(builder, 2);
    assertThat(converter.convert(after).getField("email")).isEqualTo("2@example.com");
  }

  @Test
  public void testNewTableSchemaGetsItsOwnMapping() {
    when(config.schemaCaseInsensitive()).thenReturn(true);
    org.apache.kafka.connect.data.Schema schema = sourceBuilder(true).build();
    Struct struct = source(schema, 5);

    RecordConverter v1 = converter(TABLE_V1, false);
    assertThat(v1.convert(struct)).isEqualTo(expected(struct, TABLE_V1.asStruct(), true, false));
    assertThat(v1.convert(struct).struct().field("email")).isNull();

    // the writer replaces its converter when the table schema changes
    RecordConverter v2 = converter(TABLE_V2, false);
    assertThat(v2.convert(struct)).isEqualTo(expected(struct, TABLE_V2.asStruct(), true, false));
    assertThat(v2.convert(struct).getField("email")).isEqualTo("5@example.com");

    // and the first one is not affected by the second
    assertThat(v1.convert(struct)).isEqualTo(expected(struct, TABLE_V1.asStruct(), true, false));
  }

  @Test
  public void testSameSourceStructForDifferentTableStructs() {
    // the one source schema fills two table structs that have different columns
    Schema table =
        new Schema(
            Types.NestedField.optional(
                1,
                "a",
                Types.StructType.of(
                    Types.NestedField.optional(2, "x", Types.LongType.get()),
                    Types.NestedField.optional(3, "y", Types.LongType.get()))),
            Types.NestedField.optional(
                4,
                "b",
                Types.StructType.of(
                    Types.NestedField.optional(5, "y", Types.LongType.get()),
                    Types.NestedField.optional(6, "z", Types.LongType.get()))));
    org.apache.kafka.connect.data.Schema xyz =
        SchemaBuilder.struct()
            .optional()
            .field("x", org.apache.kafka.connect.data.Schema.OPTIONAL_INT64_SCHEMA)
            .field("y", org.apache.kafka.connect.data.Schema.OPTIONAL_INT64_SCHEMA)
            .field("z", org.apache.kafka.connect.data.Schema.OPTIONAL_INT64_SCHEMA)
            .build();
    org.apache.kafka.connect.data.Schema row =
        SchemaBuilder.struct().field("a", xyz).field("b", xyz).build();
    Struct value =
        new Struct(row)
            .put("a", new Struct(xyz).put("x", 1L).put("y", 2L).put("z", 3L))
            .put("b", new Struct(xyz).put("x", 4L).put("y", 5L).put("z", 6L));

    RecordConverter converter = converter(table, false);
    for (int i = 0; i < 3; i++) {
      Record record = converter.convert(value);
      Record a = (Record) record.getField("a");
      Record b = (Record) record.getField("b");
      assertThat(a.getField("x")).isEqualTo(1L);
      assertThat(a.getField("y")).isEqualTo(2L);
      assertThat(b.getField("y")).isEqualTo(5L);
      assertThat(b.getField("z")).isEqualTo(6L);
    }
  }

  @Test
  public void testCaseSensitivityChangeIsNotServedFromTheMapping() {
    RecordConverter converter = converter(TABLE_V1, false);
    org.apache.kafka.connect.data.Schema schema = sourceBuilder(false).build();
    Struct struct = source(schema, 3);

    when(config.schemaCaseInsensitive()).thenReturn(false);
    assertThat(converter.convert(struct).getField("id")).isNull();
    when(config.schemaCaseInsensitive()).thenReturn(true);
    assertThat(converter.convert(struct).getField("id")).isEqualTo(3L);
    when(config.schemaCaseInsensitive()).thenReturn(false);
    assertThat(converter.convert(struct).getField("id")).isNull();
  }

  @Test
  public void testNameMapping() {
    Table table = mock(Table.class);
    when(table.schema()).thenReturn(TABLE_V1);
    NameMapping nameMapping =
        NameMapping.of(
            MappedField.of(1, ImmutableList.of("id", "ID")),
            MappedField.of(2, ImmutableList.of("Name", "name")));
    when(table.properties())
        .thenReturn(
            ImmutableMap.of(
                TableProperties.DEFAULT_NAME_MAPPING, NameMappingParser.toJson(nameMapping)));
    // the case insensitive setting does not apply to a name mapping
    when(config.schemaCaseInsensitive()).thenReturn(true);
    RecordConverter converter = new RecordConverter(table, config, false);
    org.apache.kafka.connect.data.Schema schema = sourceBuilder(false).build();

    for (int i = 0; i < 3; i++) {
      Record record = converter.convert(source(schema, 9));
      assertThat(record.getField("id")).isEqualTo(9L);
      assertThat(record.getField("name")).isEqualTo("name-9");
      // not named in the mapping, which takes the place of the table names
      assertThat(record.getField("Address")).isNull();
    }
  }

  @Test
  public void testMoreSourceSchemasThanTheCacheHolds() {
    when(config.schemaCaseInsensitive()).thenReturn(true);
    RecordConverter converter = converter(TABLE_V2, true);
    for (int i = 0; i < 300; i++) {
      org.apache.kafka.connect.data.Schema schema = sourceBuilder(i % 2 == 0).build();
      Struct struct = source(schema, i + 1);
      assertThat(converter.convert(struct))
          .isEqualTo(expected(struct, TABLE_V2.asStruct(), true, true));
    }
  }

  @Test
  public void testSourceFieldIdsOfCdcWithoutKey() {
    when(config.schemaCaseInsensitive()).thenReturn(true);
    when(config.tablesCdcField()).thenReturn("_cdc.op");
    RecordConverter converter = converter(TABLE_V2, false);
    org.apache.kafka.connect.data.Schema narrow = sourceBuilder(false).build();
    org.apache.kafka.connect.data.Schema wide = sourceBuilder(true).build();

    Set<Integer> narrowIds = RecordWrapper.sourceFieldIds(converter.convert(source(narrow, 1)));
    Set<Integer> wideIds = RecordWrapper.sourceFieldIds(converter.convert(source(wide, 1)));
    assertThat(narrowIds).contains(1, 2, 4, 5, 7).doesNotContain(11);
    assertThat(wideIds).contains(1, 2, 4, 5, 7, 11);

    for (int i = 0; i < 5; i++) {
      // the same instance, an equal one and a SchemaBuilder, again and again
      assertThat(RecordWrapper.sourceFieldIds(converter.convert(source(narrow, 2))))
          .isEqualTo(narrowIds);
      assertThat(RecordWrapper.sourceFieldIds(converter.convert(source(sourceBuilder(false).build(), 2))))
          .isEqualTo(narrowIds);
      assertThat(RecordWrapper.sourceFieldIds(converter.convert(source(sourceBuilder(true), 2))))
          .isEqualTo(wideIds);
      assertThat(RecordWrapper.sourceFieldIds(converter.convert(source(wide, 2)))).isEqualTo(wideIds);
    }
  }
}
