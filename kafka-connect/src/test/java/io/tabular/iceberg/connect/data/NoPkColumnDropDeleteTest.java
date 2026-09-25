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
import static org.assertj.core.api.Assertions.tuple;

import io.tabular.iceberg.connect.IcebergSinkConfig;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.assertj.core.groups.Tuple;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class NoPkColumnDropDeleteTest {
  private static final Schema ORIGINAL_SCHEMA =
      SchemaBuilder.struct()
          .field("a", Schema.INT32_SCHEMA)
          .field("b", Schema.OPTIONAL_INT32_SCHEMA)
          .build();
  private static final Schema DROPPED_SCHEMA =
      SchemaBuilder.struct().field("a", Schema.INT32_SCHEMA).build();

  @TempDir Path temp;

  @ParameterizedTest
  @EnumSource(value = Operation.class, names = {"DELETE", "UPDATE"})
  void removesCommittedBeforeImageAfterSourceColumnDrop(Operation operation) throws Exception {
    verifyCommittedBeforeImageRemoved(operation, true);
  }

  @ParameterizedTest
  @EnumSource(value = Operation.class, names = {"DELETE", "UPDATE"})
  void removesCommittedBeforeImageWithUnchangedSourceSchema(Operation operation) throws Exception {
    verifyCommittedBeforeImageRemoved(operation, false);
  }

  private void verifyCommittedBeforeImageRemoved(Operation operation, boolean dropColumn)
      throws Exception {
    IcebergSinkConfig config =
        new IcebergSinkConfig(
            Map.of(
                "topics", "source",
                "iceberg.catalog.type", "hadoop",
                "iceberg.tables", "db.test",
                "iceberg.tables.evolve-schema-enabled", "true",
                "iceberg.tables.cdc-field", "_cdc_op",
                "iceberg.table.db.test.has-real-pk", "false"));
    Table table =
        new HadoopTables(new Configuration())
            .create(
                new org.apache.iceberg.Schema(
                    Types.NestedField.required(1, "a", Types.IntegerType.get()),
                    Types.NestedField.optional(2, "b", Types.IntegerType.get())),
                PartitionSpec.unpartitioned(),
                Map.of("format-version", "2"),
                temp.resolve(UUID.randomUUID().toString()).toString());
    assertThat(table.schema().identifierFieldIds()).isEmpty();
    RecordConverter converter = new RecordConverter(table, config);

    // Commit first: equality deletes must apply to older data, not same-commit inserts.
    try (TaskWriter<Record> writer = Utilities.createTableWriter(table, "db.test", config)) {
      writer.write(
          new RecordWrapper(
              converter.convert(new Struct(ORIGINAL_SCHEMA).put("a", 10).put("b", 20)),
              Operation.INSERT));
      writer.write(
          new RecordWrapper(
              converter.convert(new Struct(ORIGINAL_SCHEMA).put("a", 99).put("b", 30)),
              Operation.INSERT));
      commit(table, writer.complete());
    }
    assertThat(readRows(table)).containsExactlyInAnyOrder(tuple(10, 20), tuple(99, 30));

    // Source DROP retains the old Iceberg column. Deliberately call the delta writer directly:
    // IcebergWriter's no-PK schema preflight currently rejects DROP before reaching this code.
    // Use real source Structs and conversion, rather than constructing a synthetic b=null delete.
    Schema sourceSchema = dropColumn ? DROPPED_SCHEMA : ORIGINAL_SCHEMA;
    Struct before = new Struct(sourceSchema).put("a", 10);
    Struct after = new Struct(sourceSchema).put("a", 11);
    if (!dropColumn) {
      before.put("b", 20);
      after.put("b", 20);
    }
    Record beforeRow = converter.convert(before);
    Record row = operation == Operation.DELETE ? beforeRow : converter.convert(after);
    WriteResult changes;
    try (TaskWriter<Record> writer = Utilities.createTableWriter(table, "db.test", config)) {
      writer.write(new RecordWrapper(row, operation, beforeRow));
      changes = writer.complete();
      commit(table, changes);
    }
    assertThat(changes.deleteFiles()).hasSize(1);

    List<Tuple> expected = Lists.newArrayList();
    expected.add(tuple(99, 30));
    if (operation == Operation.UPDATE) {
      expected.add(tuple(11, dropColumn ? null : 20));
    }
    assertThat(readRows(table))
        .as(
            "%s must remove committed (a=10,b=20); source fields=%s, equality IDs=%s",
            operation,
            sourceSchema.fields(),
            changes.deleteFiles()[0].equalityFieldIds())
        .containsExactlyInAnyOrderElementsOf(expected);
  }

  private void commit(Table table, WriteResult result) {
    RowDelta delta = table.newRowDelta();
    for (DataFile file : result.dataFiles()) {
      delta.addRows(file);
    }
    for (DeleteFile file : result.deleteFiles()) {
      delta.addDeletes(file);
    }
    delta.commit();
  }

  private List<Tuple> readRows(Table table) throws Exception {
    List<Tuple> result = Lists.newArrayList();
    try (CloseableIterable<Record> rows = IcebergGenerics.read(table).build()) {
      for (Record row : rows) {
        result.add(tuple(row.getField("a"), row.getField("b")));
      }
    }
    return result;
  }
}
