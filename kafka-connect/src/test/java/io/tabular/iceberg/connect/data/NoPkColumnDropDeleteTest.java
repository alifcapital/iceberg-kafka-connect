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
import static org.assertj.core.api.Assertions.tuple;

import io.tabular.iceberg.connect.IcebergSinkConfig;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
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
import org.apache.kafka.connect.sink.SinkRecord;
import org.assertj.core.groups.Tuple;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
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
    IcebergSinkConfig config = config();
    Table table = table(false, false);
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
    // The tests below also exercise the complete IcebergWriter schema-evolution path.
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
    assertThat(changes.deleteFiles()[0].equalityFieldIds())
        .containsExactlyInAnyOrderElementsOf(dropColumn ? Set.of(1) : Set.of(1, 2));

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

  @ParameterizedTest
  @CsvSource({
    "DELETE, true, false", "UPDATE, true, false",
    "DELETE, false, false", "UPDATE, false, false",
    "DELETE, true, true", "UPDATE, true, true",
    "DELETE, false, true", "UPDATE, false, true"
  })
  void sinkHandlesDropAcrossCommitsAndWriterRotations(
      Operation operation, boolean commitInitial, boolean partitioned) throws Exception {
    Table table = table(true, partitioned);
    IcebergSinkConfig config = config();
    IcebergWriter writer = new IcebergWriter(table, "db.test", config, false);
    try {
      Schema initial =
          SchemaBuilder.struct()
              .field("a", Schema.INT32_SCHEMA)
              .field("b", Schema.INT32_SCHEMA)
              .build();
      writer.write(event(initial, 10, 20, Operation.INSERT, null, 0));
      writer.write(event(initial, 99, 30, Operation.INSERT, null, 1));
      if (commitInitial) {
        commit(table, writer.complete());
        writer.close();
        writer = new IcebergWriter(table, "db.test", config, false);
      }
      Struct before = new Struct(DROPPED_SCHEMA).put("a", 10);
      writer.write(
          event(
              DROPPED_SCHEMA, operation == Operation.UPDATE ? 11 : 10, null, operation, before, 2));
      commit(table, writer.complete());
    } finally {
      writer.close();
    }
    assertThat(table.schema().findField("b").isOptional()).isTrue();
    if (operation == Operation.UPDATE) {
      assertThat(readRows(table)).containsExactlyInAnyOrder(tuple(99, 30), tuple(11, null));
    } else {
      assertThat(readRows(table)).containsExactly(tuple(99, 30));
    }
  }

  @Test
  void explicitNullAndMissingFieldUseDifferentDeleteFiles() throws Exception {
    Table table = table(false, false);
    IcebergSinkConfig config = config();
    RecordConverter converter = new RecordConverter(table, config);
    try (TaskWriter<Record> writer = Utilities.createTableWriter(table, "db.test", config)) {
      for (Object value :
          List.of(
              new Struct(ORIGINAL_SCHEMA).put("a", 10).put("b", 20),
              new Struct(ORIGINAL_SCHEMA).put("a", 10).put("b", null),
              new Struct(ORIGINAL_SCHEMA).put("a", 99).put("b", 30))) {
        writer.write(converter.convert(value));
      }
      commit(table, writer.complete());
    }
    try (TaskWriter<Record> writer = Utilities.createTableWriter(table, "db.test", config)) {
      writer.write(
          new RecordWrapper(
              converter.convert(new Struct(ORIGINAL_SCHEMA).put("a", 10).put("b", null)),
              Operation.DELETE));
      writer.write(
          new RecordWrapper(
              converter.convert(new Struct(DROPPED_SCHEMA).put("a", 99)), Operation.DELETE));
      WriteResult result = writer.complete();
      assertThat(result.deleteFiles()).hasSize(2);
      assertThat(result.deleteFiles())
          .extracting(file -> Set.copyOf(file.equalityFieldIds()))
          .containsExactlyInAnyOrder(Set.of(1, 2), Set.of(1));
      commit(table, result);
    }
    // Explicit b=null must not match b=20. Missing b matches regardless of the old b value.
    assertThat(readRows(table)).containsExactly(tuple(10, 20));
  }

  @Test
  void partialDeleteRemovesBothCommittedAndAllMatchingPendingRows() throws Exception {
    Table table = table(false, false);
    IcebergSinkConfig config = config();
    RecordConverter converter = new RecordConverter(table, config);
    try (TaskWriter<Record> writer = Utilities.createTableWriter(table, "db.test", config)) {
      writer.write(converter.convert(new Struct(ORIGINAL_SCHEMA).put("a", 10).put("b", 20)));
      commit(table, writer.complete());
    }
    try (TaskWriter<Record> writer = Utilities.createTableWriter(table, "db.test", config)) {
      for (int b : new int[] {20, 20, 30}) {
        writer.write(converter.convert(new Struct(ORIGINAL_SCHEMA).put("a", 10).put("b", b)));
      }
      writer.write(converter.convert(new Struct(ORIGINAL_SCHEMA).put("a", 99).put("b", 40)));
      Record partial = converter.convert(new Struct(DROPPED_SCHEMA).put("a", 10));
      writer.write(new RecordWrapper(partial, Operation.DELETE));
      // Keep the lazily built subset index correct as more rows arrive and are removed.
      writer.write(converter.convert(new Struct(ORIGINAL_SCHEMA).put("a", 10).put("b", 50)));
      writer.write(new RecordWrapper(partial, Operation.DELETE));
      writer.write(partial); // A later INSERT must survive earlier deletes in this commit.
      WriteResult result = writer.complete();
      assertThat(result.deleteFiles())
          .extracting(DeleteFile::content)
          .contains(FileContent.EQUALITY_DELETES, FileContent.POSITION_DELETES);
      commit(table, result);
    }
    assertThat(readRows(table)).containsExactlyInAnyOrder(tuple(99, 40), tuple(10, null));
  }

  @ParameterizedTest
  @EnumSource(
      value = Operation.class,
      names = {"DELETE", "UPDATE"})
  void schemalessSourceRetainsFieldPresence(Operation operation) throws Exception {
    Table table = table(false, false);
    writeAndCommit(
        table,
        new SinkRecord("source", 0, null, null, null, Map.of("a", 10, "b", 20, "_cdc_op", "I"), 0));
    Object value =
        operation == Operation.DELETE
            ? Map.of("a", 10, "_cdc_op", "D")
            : Map.of("a", 11, "_cdc_op", "U", "_before_image", Map.of("a", 10));
    writeAndCommit(table, new SinkRecord("source", 0, null, null, null, value, 1));
    if (operation == Operation.DELETE) {
      assertThat(readRows(table)).isEmpty();
    } else {
      assertThat(readRows(table)).containsExactly(tuple(11, null));
    }
  }

  @Test
  void rejectsDroppingPartitionSourceBeforeSchemaOrOffsetChanges() {
    Table table = table(true, true);
    int schemaId = table.schema().schemaId();
    Schema onlyB =
        SchemaBuilder.struct()
            .field("b", Schema.INT32_SCHEMA)
            .field("_cdc_op", Schema.STRING_SCHEMA)
            .build();
    IcebergWriter writer = new IcebergWriter(table, "db.test", config(), false);
    try {
      assertThatThrownBy(
              () ->
                  writer.write(
                      new SinkRecord(
                          "source",
                          0,
                          null,
                          null,
                          onlyB,
                          new Struct(onlyB).put("b", 20).put("_cdc_op", "D"),
                          0)))
          .hasRootCauseMessage("Cannot DROP partition source field a in CDC without a primary key");
      assertThat(table.schema().schemaId()).isEqualTo(schemaId);
      assertThat(writer.complete().dataOffsets()).isEmpty();
    } finally {
      writer.close();
    }
  }

  private void writeAndCommit(Table table, SinkRecord... events) {
    IcebergWriter writer = new IcebergWriter(table, "db.test", config(), false);
    try {
      for (SinkRecord event : events) {
        writer.write(event);
      }
      commit(table, writer.complete());
    } finally {
      writer.close();
    }
  }

  @ParameterizedTest
  @CsvSource({"true", "false"})
  void nestedDropUsesSchemaEvenWhenStructIsNull(boolean commitInitial) throws Exception {
    org.apache.iceberg.Schema schema =
        new org.apache.iceberg.Schema(
            Types.NestedField.required(1, "a", Types.IntegerType.get()),
            Types.NestedField.optional(
                2,
                "b",
                Types.StructType.of(
                    Types.NestedField.optional(3, "x", Types.IntegerType.get()),
                    Types.NestedField.optional(4, "y", Types.IntegerType.get()))),
            Types.NestedField.optional(5, "_cdc_op", Types.StringType.get()));
    Table table =
        new HadoopTables(new Configuration())
            .create(
                schema,
                PartitionSpec.unpartitioned(),
                Map.of("format-version", "2"),
                temp.resolve(UUID.randomUUID().toString()).toString());
    Schema nestedOld =
        SchemaBuilder.struct()
            .optional()
            .field("x", Schema.OPTIONAL_INT32_SCHEMA)
            .field("y", Schema.OPTIONAL_INT32_SCHEMA)
            .build();
    Schema nestedNew =
        SchemaBuilder.struct().optional().field("x", Schema.OPTIONAL_INT32_SCHEMA).build();
    Schema oldSource =
        SchemaBuilder.struct()
            .field("a", Schema.INT32_SCHEMA)
            .field("b", nestedOld)
            .field("_cdc_op", Schema.STRING_SCHEMA)
            .build();
    Schema newSource =
        SchemaBuilder.struct()
            .field("a", Schema.INT32_SCHEMA)
            .field("b", nestedNew)
            .field("_cdc_op", Schema.STRING_SCHEMA)
            .build();
    SinkRecord insert =
        new SinkRecord(
            "source",
            0,
            null,
            null,
            oldSource,
            new Struct(oldSource)
                .put("a", 10)
                .put("b", new Struct(nestedOld).put("x", null).put("y", 20))
                .put("_cdc_op", "I"),
            0);
    SinkRecord delete =
        new SinkRecord(
            "source",
            0,
            null,
            null,
            newSource,
            new Struct(newSource).put("a", 10).put("b", null).put("_cdc_op", "D"),
            1);
    SinkRecord insertNull =
        new SinkRecord(
            "source",
            0,
            null,
            null,
            oldSource,
            new Struct(oldSource).put("a", 10).put("b", null).put("_cdc_op", "I"),
            1);
    if (commitInitial) {
      writeAndCommit(table, insert, insertNull);
      writeAndCommit(table, delete);
    } else {
      writeAndCommit(table, insert, insertNull, delete);
    }
    try (CloseableIterable<Record> rows = IcebergGenerics.read(table).build()) {
      List<Record> remaining = Lists.newArrayList();
      rows.forEach(row -> remaining.add(row.copy()));
      assertThat(remaining).hasSize(1);
      Record nested = (Record) remaining.get(0).getField("b");
      assertThat(nested.getField("y")).isEqualTo(20);
    }
  }

  private IcebergSinkConfig config() {
    return new IcebergSinkConfig(
        Map.of(
            "topics", "source",
            "iceberg.catalog.type", "hadoop",
            "iceberg.tables", "db.test",
            "iceberg.tables.evolve-schema-enabled", "true",
            "iceberg.tables.cdc-field", "_cdc_op",
            "iceberg.table.db.test.has-real-pk", "false"));
  }

  private Table table(boolean requiredB, boolean partitioned) {
    org.apache.iceberg.Schema schema =
        new org.apache.iceberg.Schema(
            Types.NestedField.required(1, "a", Types.IntegerType.get()),
            requiredB
                ? Types.NestedField.required(2, "b", Types.IntegerType.get())
                : Types.NestedField.optional(2, "b", Types.IntegerType.get()),
            Types.NestedField.optional(3, "_cdc_op", Types.StringType.get()));
    return new HadoopTables(new Configuration())
        .create(
            schema,
            partitioned
                ? PartitionSpec.builderFor(schema).identity("a").build()
                : PartitionSpec.unpartitioned(),
            Map.of("format-version", "2"),
            temp.resolve(UUID.randomUUID().toString()).toString());
  }

  private SinkRecord event(
      Schema source, int valueA, Integer valueB, Operation operation, Struct before, long offset) {
    SchemaBuilder builder = SchemaBuilder.struct();
    source.fields().forEach(field -> builder.field(field.name(), field.schema()));
    builder.field("_cdc_op", Schema.STRING_SCHEMA);
    if (before != null) {
      builder.field("_before_image", before.schema());
    }
    Schema schema = builder.build();
    Struct value = new Struct(schema).put("a", valueA).put("_cdc_op", operation.name());
    if (source.field("b") != null) {
      value.put("b", valueB);
    }
    if (before != null) {
      value.put("_before_image", before);
    }
    return new SinkRecord("source", 0, null, null, schema, value, offset);
  }

  private void commit(Table table, WriteComplete result) {
    RowDelta delta = table.newRowDelta();
    for (WriterResult files : result.writerResults()) {
      files.dataFiles().forEach(delta::addRows);
      files.deleteFiles().forEach(delta::addDeletes);
    }
    delta.commit();
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
