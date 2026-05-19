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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.iceberg.Schema;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types.NestedField;

final class SchemaDiff {

  private SchemaDiff() {}

  static List<GenericRecord> diff(Schema before, Schema after) {
    List<GenericRecord> changes = new ArrayList<>();

    Set<Integer> allFieldIds = new TreeSet<>(Sets.union(before.idToName().keySet(), after.idToName().keySet()));
    for (Integer fid : allFieldIds) {
      NestedField b = before.findField(fid);
      NestedField a = after.findField(fid);

      if (b == null && a != null) {
        changes.add(change(
            DdlEvent.OP_ADD_COLUMN,
            fid,
            after.findColumnName(fid),
            null,
            null,
            typeLabel(a.type()),
            null,
            null,
            !a.isOptional()));
      } else if (a == null && b != null) {
        changes.add(change(
            DdlEvent.OP_DROP_COLUMN,
            fid,
            before.findColumnName(fid),
            null,
            null,
            null,
            null,
            null,
            null));
      } else if (a != null && b != null) {
        if (!b.name().equals(a.name())) {
          changes.add(change(
              DdlEvent.OP_RENAME_COLUMN,
              fid,
              null,
              before.findColumnName(fid),
              after.findColumnName(fid),
              null,
              null,
              null,
              null));
        }
        // For container types we don't emit a parent UPDATE_TYPE when only the
        // contents changed (nested adds/drops surface separately). We still emit
        // UPDATE_TYPE for primitive-to-primitive and primitive-to-container (and
        // the reverse), using the short type label.
        if (!b.type().equals(a.type())
            && (b.type().isPrimitiveType() || a.type().isPrimitiveType())) {
          changes.add(change(
              DdlEvent.OP_UPDATE_TYPE,
              fid,
              after.findColumnName(fid),
              null,
              null,
              null,
              typeLabel(b.type()),
              typeLabel(a.type()),
              null));
        }
        if (b.isOptional() && !a.isOptional()) {
          changes.add(change(
              DdlEvent.OP_REQUIRE_COLUMN,
              fid,
              after.findColumnName(fid),
              null,
              null,
              null,
              null,
              null,
              null));
        } else if (!b.isOptional() && a.isOptional()) {
          changes.add(change(
              DdlEvent.OP_MAKE_OPTIONAL,
              fid,
              after.findColumnName(fid),
              null,
              null,
              null,
              null,
              null,
              null));
        }
      }
    }

    if (!before.identifierFieldIds().equals(after.identifierFieldIds())) {
      changes.add(identifierFieldsChanged(before, after));
    }

    return changes;
  }

  // Short label for the type field in events. For primitive types use Iceberg's
  // canonical toString() (string, long, decimal(38,9), ...). For containers emit
  // just "struct" / "list" / "map" — consumers don't need the inner field-id
  // breakdown because nested adds/drops surface as their own events with full
  // dotted paths.
  static String typeLabel(Type type) {
    if (type.isPrimitiveType()) {
      return type.toString();
    }
    if (type.isStructType()) {
      return "struct";
    }
    if (type.isListType()) {
      return "list";
    }
    if (type.isMapType()) {
      return "map";
    }
    return type.toString();
  }

  private static GenericRecord change(
      String op,
      Integer fieldId,
      String name,
      String oldName,
      String newName,
      String type,
      String oldType,
      String newType,
      Boolean required) {
    GenericRecord rec = new GenericData.Record(DdlEvent.CHANGE_SCHEMA);
    rec.put("op", op);
    rec.put("field_id", fieldId);
    rec.put("name", name);
    rec.put("old_name", oldName);
    rec.put("new_name", newName);
    rec.put("type", type);
    rec.put("old_type", oldType);
    rec.put("new_type", newType);
    rec.put("required", required);
    rec.put("identifier_field_ids_before", null);
    rec.put("identifier_field_names_before", null);
    rec.put("identifier_field_ids_after", null);
    rec.put("identifier_field_names_after", null);
    return rec;
  }

  private static GenericRecord identifierFieldsChanged(Schema before, Schema after) {
    GenericRecord rec = new GenericData.Record(DdlEvent.CHANGE_SCHEMA);
    rec.put("op", DdlEvent.OP_IDENTIFIER_FIELDS_CHANGED);
    rec.put("field_id", null);
    rec.put("name", null);
    rec.put("old_name", null);
    rec.put("new_name", null);
    rec.put("type", null);
    rec.put("old_type", null);
    rec.put("new_type", null);
    rec.put("required", null);
    rec.put("identifier_field_ids_before", new ArrayList<>(new TreeSet<>(before.identifierFieldIds())));
    rec.put("identifier_field_names_before", identifierFieldNames(before));
    rec.put("identifier_field_ids_after", new ArrayList<>(new TreeSet<>(after.identifierFieldIds())));
    rec.put("identifier_field_names_after", identifierFieldNames(after));
    return rec;
  }

  static List<Integer> identifierFieldIds(Schema schema) {
    return new ArrayList<>(new TreeSet<>(schema.identifierFieldIds()));
  }

  static List<String> identifierFieldNames(Schema schema) {
    List<String> names = new ArrayList<>();
    for (Integer fid : new TreeSet<>(schema.identifierFieldIds())) {
      names.add(schema.findColumnName(fid));
    }
    return names;
  }

  static List<GenericRecord> columnsFor(Schema schema) {
    List<GenericRecord> columns = new ArrayList<>();
    for (Integer fid : new TreeSet<>(schema.idToName().keySet())) {
      NestedField field = schema.findField(fid);
      columns.add(DdlEvent.column(
          fid, schema.findColumnName(fid), field.type().toString(), !field.isOptional()));
    }
    return columns;
  }
}
