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

import java.util.Arrays;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

final class DdlEvent {

  static final String EVENT_TYPE_TABLE_CREATED = "TABLE_CREATED";
  static final String EVENT_TYPE_SCHEMA_CHANGED = "SCHEMA_CHANGED";

  static final String OP_ADD_COLUMN = "ADD_COLUMN";
  static final String OP_DROP_COLUMN = "DROP_COLUMN";
  static final String OP_RENAME_COLUMN = "RENAME_COLUMN";
  static final String OP_UPDATE_TYPE = "UPDATE_TYPE";
  static final String OP_MAKE_OPTIONAL = "MAKE_OPTIONAL";
  static final String OP_REQUIRE_COLUMN = "REQUIRE_COLUMN";
  static final String OP_IDENTIFIER_FIELDS_CHANGED = "IDENTIFIER_FIELDS_CHANGED";

  static final String REASON_HISTORY_GAP = "history_gap";
  static final String REASON_SCHEMA_ID_REUSED = "schema_id_reused";

  static final Schema COLUMN_SCHEMA =
      SchemaBuilder.record("Column")
          .namespace("io.tabular.iceberg.connect.ddl")
          .fields()
          .name("field_id").type().intType().noDefault()
          .name("name").type().stringType().noDefault()
          .name("type").type().stringType().noDefault()
          .name("required").type().booleanType().noDefault()
          .endRecord();

  static final Schema CHANGE_SCHEMA =
      SchemaBuilder.record("Change")
          .namespace("io.tabular.iceberg.connect.ddl")
          .fields()
          .name("op").type().stringType().noDefault()
          .name("field_id").type().nullable().intType().noDefault()
          .name("name").type().nullable().stringType().noDefault()
          .name("old_name").type().nullable().stringType().noDefault()
          .name("new_name").type().nullable().stringType().noDefault()
          .name("type").type().nullable().stringType().noDefault()
          .name("old_type").type().nullable().stringType().noDefault()
          .name("new_type").type().nullable().stringType().noDefault()
          .name("required").type().nullable().booleanType().noDefault()
          .name("identifier_field_ids_before")
            .type().nullable().array().items().intType().noDefault()
          .name("identifier_field_names_before")
            .type().nullable().array().items().stringType().noDefault()
          .name("identifier_field_ids_after")
            .type().nullable().array().items().intType().noDefault()
          .name("identifier_field_names_after")
            .type().nullable().array().items().stringType().noDefault()
          .endRecord();

  static final Schema INCOMPLETE_HISTORY_SCHEMA =
      SchemaBuilder.record("IncompleteHistory")
          .namespace("io.tabular.iceberg.connect.ddl")
          .fields()
          .name("previous_emitted_schema_id").type().intType().noDefault()
          .name("reason").type().stringType().noDefault()
          .endRecord();

  private static Schema nullable(Schema schema) {
    return Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.NULL), schema));
  }

  static final Schema SCHEMA =
      SchemaBuilder.record("DdlEvent")
          .namespace("io.tabular.iceberg.connect.ddl")
          .fields()
          .name("event_type").type().stringType().noDefault()
          .name("occurred_at").type().longType().noDefault()
          .name("db").type().stringType().noDefault()
          .name("table").type().stringType().noDefault()
          .name("table_uuid").type().stringType().noDefault()
          .name("schema_id").type().intType().noDefault()
          .name("columns").type(nullable(Schema.createArray(COLUMN_SCHEMA))).noDefault()
          .name("identifier_field_ids")
            .type(nullable(Schema.createArray(Schema.create(Schema.Type.INT)))).noDefault()
          .name("identifier_field_names")
            .type(nullable(Schema.createArray(Schema.create(Schema.Type.STRING)))).noDefault()
          .name("incomplete_history").type(nullable(INCOMPLETE_HISTORY_SCHEMA)).noDefault()
          .name("changes").type(nullable(Schema.createArray(CHANGE_SCHEMA))).noDefault()
          .endRecord();

  private DdlEvent() {}

  static GenericRecord tableCreated(
      String db,
      String table,
      String tableUuid,
      long occurredAt,
      int schemaId,
      List<GenericRecord> columns,
      List<Integer> identifierFieldIds,
      List<String> identifierFieldNames,
      GenericRecord incompleteHistoryOrNull) {
    GenericRecord rec = new GenericData.Record(SCHEMA);
    rec.put("event_type", EVENT_TYPE_TABLE_CREATED);
    rec.put("occurred_at", occurredAt);
    rec.put("db", db);
    rec.put("table", table);
    rec.put("table_uuid", tableUuid);
    rec.put("schema_id", schemaId);
    rec.put("columns", columns);
    rec.put("identifier_field_ids", identifierFieldIds);
    rec.put("identifier_field_names", identifierFieldNames);
    rec.put("incomplete_history", incompleteHistoryOrNull);
    rec.put("changes", null);
    return rec;
  }

  static GenericRecord schemaChanged(
      String db,
      String table,
      String tableUuid,
      long occurredAt,
      int schemaId,
      List<GenericRecord> changes) {
    GenericRecord rec = new GenericData.Record(SCHEMA);
    rec.put("event_type", EVENT_TYPE_SCHEMA_CHANGED);
    rec.put("occurred_at", occurredAt);
    rec.put("db", db);
    rec.put("table", table);
    rec.put("table_uuid", tableUuid);
    rec.put("schema_id", schemaId);
    rec.put("columns", null);
    rec.put("identifier_field_ids", null);
    rec.put("identifier_field_names", null);
    rec.put("incomplete_history", null);
    rec.put("changes", changes);
    return rec;
  }

  static GenericRecord column(int fieldId, String name, String type, boolean required) {
    GenericRecord rec = new GenericData.Record(COLUMN_SCHEMA);
    rec.put("field_id", fieldId);
    rec.put("name", name);
    rec.put("type", type);
    rec.put("required", required);
    return rec;
  }

  static GenericRecord incompleteHistory(int previousEmittedSchemaId, String reason) {
    GenericRecord rec = new GenericData.Record(INCOMPLETE_HISTORY_SCHEMA);
    rec.put("previous_emitted_schema_id", previousEmittedSchemaId);
    rec.put("reason", reason);
    return rec;
  }
}
