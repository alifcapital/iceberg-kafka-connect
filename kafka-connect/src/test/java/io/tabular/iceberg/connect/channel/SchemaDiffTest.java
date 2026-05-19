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

import java.util.List;
import java.util.Set;
import org.apache.avro.generic.GenericRecord;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

public class SchemaDiffTest {

  @Test
  public void noOpForIdenticalSchemas() {
    Schema s =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get()));
    assertThat(SchemaDiff.diff(s, s)).isEmpty();
  }

  @Test
  public void addColumn() {
    Schema before = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));
    Schema after =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "phone", Types.StringType.get()));

    List<GenericRecord> changes = SchemaDiff.diff(before, after);
    assertThat(changes).hasSize(1);
    GenericRecord c = changes.get(0);
    assertThat(c.get("op")).isEqualTo(DdlEvent.OP_ADD_COLUMN);
    assertThat(c.get("field_id")).isEqualTo(2);
    assertThat(c.get("name").toString()).isEqualTo("phone");
    assertThat(c.get("type").toString()).isEqualTo("string");
    assertThat(c.get("required")).isEqualTo(false);
  }

  @Test
  public void dropColumn() {
    Schema before =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "obsolete", Types.StringType.get()));
    Schema after = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));

    List<GenericRecord> changes = SchemaDiff.diff(before, after);
    assertThat(changes).hasSize(1);
    GenericRecord c = changes.get(0);
    assertThat(c.get("op")).isEqualTo(DdlEvent.OP_DROP_COLUMN);
    assertThat(c.get("field_id")).isEqualTo(2);
    assertThat(c.get("name").toString()).isEqualTo("obsolete");
  }

  @Test
  public void renameColumn() {
    Schema before =
        new Schema(Types.NestedField.required(1, "nickname", Types.StringType.get()));
    Schema after =
        new Schema(Types.NestedField.required(1, "username", Types.StringType.get()));

    List<GenericRecord> changes = SchemaDiff.diff(before, after);
    assertThat(changes).hasSize(1);
    GenericRecord c = changes.get(0);
    assertThat(c.get("op")).isEqualTo(DdlEvent.OP_RENAME_COLUMN);
    assertThat(c.get("field_id")).isEqualTo(1);
    assertThat(c.get("old_name").toString()).isEqualTo("nickname");
    assertThat(c.get("new_name").toString()).isEqualTo("username");
  }

  @Test
  public void renameNestedColumnUsesDottedPath() {
    Schema before =
        new Schema(
            Types.NestedField.required(
                1,
                "address",
                Types.StructType.of(
                    Types.NestedField.optional(2, "city", Types.StringType.get()))));
    Schema after =
        new Schema(
            Types.NestedField.required(
                1,
                "address",
                Types.StructType.of(
                    Types.NestedField.optional(2, "stadt", Types.StringType.get()))));

    List<GenericRecord> changes = SchemaDiff.diff(before, after);
    assertThat(changes).hasSize(1);
    GenericRecord c = changes.get(0);
    assertThat(c.get("op")).isEqualTo(DdlEvent.OP_RENAME_COLUMN);
    assertThat(c.get("field_id")).isEqualTo(2);
    assertThat(c.get("old_name").toString()).isEqualTo("address.city");
    assertThat(c.get("new_name").toString()).isEqualTo("address.stadt");
  }

  @Test
  public void updateType() {
    Schema before =
        new Schema(Types.NestedField.optional(1, "age", Types.IntegerType.get()));
    Schema after = new Schema(Types.NestedField.optional(1, "age", Types.LongType.get()));

    List<GenericRecord> changes = SchemaDiff.diff(before, after);
    assertThat(changes).hasSize(1);
    GenericRecord c = changes.get(0);
    assertThat(c.get("op")).isEqualTo(DdlEvent.OP_UPDATE_TYPE);
    assertThat(c.get("field_id")).isEqualTo(1);
    assertThat(c.get("old_type").toString()).isEqualTo("int");
    assertThat(c.get("new_type").toString()).isEqualTo("long");
  }

  @Test
  public void makeOptional() {
    Schema before =
        new Schema(Types.NestedField.required(1, "email", Types.StringType.get()));
    Schema after =
        new Schema(Types.NestedField.optional(1, "email", Types.StringType.get()));

    List<GenericRecord> changes = SchemaDiff.diff(before, after);
    assertThat(changes).hasSize(1);
    assertThat(changes.get(0).get("op")).isEqualTo(DdlEvent.OP_MAKE_OPTIONAL);
  }

  @Test
  public void requireColumn() {
    Schema before =
        new Schema(Types.NestedField.optional(1, "email", Types.StringType.get()));
    Schema after =
        new Schema(Types.NestedField.required(1, "email", Types.StringType.get()));

    List<GenericRecord> changes = SchemaDiff.diff(before, after);
    assertThat(changes).hasSize(1);
    assertThat(changes.get(0).get("op")).isEqualTo(DdlEvent.OP_REQUIRE_COLUMN);
  }

  @Test
  public void nestedFieldsUseDottedNames() {
    Schema before =
        new Schema(
            Types.NestedField.required(
                1,
                "address",
                Types.StructType.of(
                    Types.NestedField.optional(2, "street", Types.StringType.get()))));
    Schema after =
        new Schema(
            Types.NestedField.required(
                1,
                "address",
                Types.StructType.of(
                    Types.NestedField.optional(2, "street", Types.StringType.get()),
                    Types.NestedField.optional(3, "city", Types.StringType.get()))));

    List<GenericRecord> changes = SchemaDiff.diff(before, after);
    // address itself didn't change shape (struct→struct, parent UPDATE_TYPE filtered),
    // only address.city was added; expect one ADD_COLUMN with the dotted nested name.
    assertThat(changes).hasSize(1);
    GenericRecord c = changes.get(0);
    assertThat(c.get("op")).isEqualTo(DdlEvent.OP_ADD_COLUMN);
    assertThat(c.get("field_id")).isEqualTo(3);
    assertThat(c.get("name").toString()).isEqualTo("address.city");
    assertThat(c.get("type").toString()).isEqualTo("string");
  }

  @Test
  public void addStructEmitsParentAndChildren() {
    Schema before = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));
    Schema after =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(
                2,
                "address",
                Types.StructType.of(
                    Types.NestedField.optional(3, "city", Types.StringType.get()),
                    Types.NestedField.optional(4, "zip", Types.StringType.get()))));

    List<GenericRecord> changes = SchemaDiff.diff(before, after);
    // We expect 3 events: ADD_COLUMN on the parent (type=struct) and ADD_COLUMN
    // for each leaf with full dotted name and primitive type.
    assertThat(changes).hasSize(3);
    GenericRecord parent =
        changes.stream()
            .filter(c -> c.get("field_id").equals(2))
            .findFirst()
            .orElseThrow();
    assertThat(parent.get("op")).isEqualTo(DdlEvent.OP_ADD_COLUMN);
    assertThat(parent.get("name").toString()).isEqualTo("address");
    assertThat(parent.get("type").toString()).isEqualTo("struct");

    GenericRecord city =
        changes.stream()
            .filter(c -> c.get("field_id").equals(3))
            .findFirst()
            .orElseThrow();
    assertThat(city.get("name").toString()).isEqualTo("address.city");
    assertThat(city.get("type").toString()).isEqualTo("string");

    GenericRecord zip =
        changes.stream()
            .filter(c -> c.get("field_id").equals(4))
            .findFirst()
            .orElseThrow();
    assertThat(zip.get("name").toString()).isEqualTo("address.zip");
    assertThat(zip.get("type").toString()).isEqualTo("string");
  }

  @Test
  public void identifierFieldsChanged() {
    Schema before =
        new Schema(
            List.of(
                Types.NestedField.required(1, "id", Types.LongType.get()),
                Types.NestedField.required(2, "tenant_id", Types.LongType.get())),
            Set.of(1));
    Schema after =
        new Schema(
            List.of(
                Types.NestedField.required(1, "id", Types.LongType.get()),
                Types.NestedField.required(2, "tenant_id", Types.LongType.get())),
            Set.of(1, 2));

    List<GenericRecord> changes = SchemaDiff.diff(before, after);
    assertThat(changes).hasSize(1);
    GenericRecord c = changes.get(0);
    assertThat(c.get("op")).isEqualTo(DdlEvent.OP_IDENTIFIER_FIELDS_CHANGED);
    @SuppressWarnings("unchecked")
    List<Integer> idsBefore = (List<Integer>) c.get("identifier_field_ids_before");
    @SuppressWarnings("unchecked")
    List<Integer> idsAfter = (List<Integer>) c.get("identifier_field_ids_after");
    assertThat(idsBefore).containsExactlyInAnyOrder(1);
    assertThat(idsAfter).containsExactlyInAnyOrder(1, 2);
  }

  @Test
  public void multipleChangesInOneDiff() {
    Schema before =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "email", Types.StringType.get()),
            Types.NestedField.required(3, "age", Types.IntegerType.get()),
            Types.NestedField.optional(4, "legacy", Types.StringType.get()));
    Schema after =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "email", Types.StringType.get()),
            Types.NestedField.required(3, "age", Types.LongType.get()),
            Types.NestedField.optional(5, "phone", Types.StringType.get()));

    List<GenericRecord> changes = SchemaDiff.diff(before, after);
    assertThat(changes)
        .extracting(r -> r.get("op").toString())
        .containsExactlyInAnyOrder(
            DdlEvent.OP_MAKE_OPTIONAL,
            DdlEvent.OP_UPDATE_TYPE,
            DdlEvent.OP_DROP_COLUMN,
            DdlEvent.OP_ADD_COLUMN);
  }

  @Test
  public void columnsForReturnsAllFieldsInOrder() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "email", Types.StringType.get()));
    List<GenericRecord> cols = SchemaDiff.columnsFor(schema);
    assertThat(cols).hasSize(2);
    assertThat(cols.get(0).get("name").toString()).isEqualTo("id");
    assertThat(cols.get(0).get("required")).isEqualTo(true);
    assertThat(cols.get(1).get("name").toString()).isEqualTo("email");
    assertThat(cols.get(1).get("required")).isEqualTo(false);
  }
}
