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

import io.tabular.iceberg.connect.IcebergSinkConfig;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The writer remembers the value schemas it checked against the table, by instance and by
 * equality. Both must be forgotten when the table schema changes.
 */
class IcebergWriterSchemaCacheTest {
  @TempDir Path temp;

  private IcebergSinkConfig config(boolean evolve) {
    Map<String, String> props = Maps.newHashMap();
    props.put("topics", "source");
    props.put("iceberg.catalog.type", "hadoop");
    props.put("iceberg.tables", "db.test");
    props.put("iceberg.tables.evolve-schema-enabled", String.valueOf(evolve));
    return new IcebergSinkConfig(props);
  }

  private Table table() {
    org.apache.iceberg.Schema schema =
        new org.apache.iceberg.Schema(
            Types.NestedField.optional(1, "x", Types.LongType.get()));
    return new HadoopTables(new Configuration())
        .create(
            schema,
            PartitionSpec.unpartitioned(),
            Map.of("format-version", "2"),
            temp.resolve(UUID.randomUUID().toString()).toString());
  }

  private static Schema onlyX() {
    return SchemaBuilder.struct().field("x", Schema.OPTIONAL_INT64_SCHEMA).build();
  }

  private static Schema xAndY() {
    return SchemaBuilder.struct()
        .field("x", Schema.OPTIONAL_INT64_SCHEMA)
        .field("y", Schema.OPTIONAL_STRING_SCHEMA)
        .build();
  }

  private static SinkRecord record(Schema schema, long x, long offset) {
    return new SinkRecord("source", 0, null, null, schema, new Struct(schema).put("x", x), offset);
  }

  private static long records(WriteComplete complete) {
    return complete.writerResults().stream()
        .flatMap(result -> result.dataFiles().stream())
        .mapToLong(DataFile::recordCount)
        .sum();
  }

  @Test
  void equalAndIdenticalSchemasAreAcceptedAcrossManyRecords() {
    Table table = table();
    IcebergWriter writer = new IcebergWriter(table, "db.test", config(true), true);
    Schema shared = onlyX();
    try {
      for (int i = 0; i < 100; i++) {
        // alternate the shared instance with equal copies, as converters with and without a cache do
        writer.write(record(i % 2 == 0 ? shared : onlyX(), i, i));
      }
      assertThat(records(writer.complete())).isEqualTo(100);
      assertThat(table.schema().columns()).hasSize(1);
    } finally {
      writer.close();
    }
  }

  @Test
  void schemaIsValidatedAgainAfterTheTableSchemaChanged() {
    Table table = table();
    IcebergWriter writer = new IcebergWriter(table, "db.test", config(true), true);
    Schema shared = onlyX();
    try {
      writer.write(record(shared, 1, 0));
      assertThat(table.schema().findField("x")).isNotNull();

      // another owner of the table renames the column the schema was validated against
      table.updateSchema().renameColumn("x", "renamed").commit();

      // the same instance, and an equal one, are planned against the new table schema
      writer.write(record(shared, 2, 1));
      assertThat(table.schema().findField("x")).isNotNull();
      assertThat(table.schema().findField("renamed")).isNotNull();

      table.updateSchema().deleteColumn("x").commit();
      writer.write(record(onlyX(), 3, 2));
      assertThat(table.schema().findField("x")).isNotNull();
      assertThat(records(writer.complete())).isEqualTo(3);
    } finally {
      writer.close();
    }
  }

  @Test
  void schemaThatEvolvesTheTableDoesNotLeaveStaleEntries() {
    Table table = table();
    IcebergWriter writer = new IcebergWriter(table, "db.test", config(true), true);
    Schema narrow = onlyX();
    Schema wide = xAndY();
    try {
      writer.write(record(narrow, 1, 0));
      assertThat(table.schema().findField("y")).isNull();
      writer.write(
          new SinkRecord("source", 0, null, null, wide, new Struct(wide).put("x", 2L).put("y", "a"), 1));
      assertThat(table.schema().findField("y")).isNotNull();
      // the narrow schema is still fine for the evolved table
      writer.write(record(narrow, 3, 2));
      writer.write(record(wide, 4, 3));
      assertThat(records(writer.complete())).isEqualTo(4);
    } finally {
      writer.close();
    }
  }

  @Test
  void disabledEvolutionStillRejectsAfterTheTableSchemaChanged() {
    Table table = table();
    IcebergWriter writer = new IcebergWriter(table, "db.test", config(false), true);
    Schema shared = onlyX();
    try {
      writer.write(record(shared, 1, 0));
      table.updateSchema().renameColumn("x", "renamed").commit();
      assertThatThrownBy(() -> writer.write(record(shared, 2, 1)))
          .isInstanceOf(RecordConversionException.class)
          .hasRootCauseInstanceOf(DataException.class)
          .hasRootCauseMessage("Schema evolution is disabled for table db.test");
      // the rejected schema is not remembered
      assertThatThrownBy(() -> writer.write(record(shared, 3, 2)))
          .isInstanceOf(RecordConversionException.class);
      assertThat(List.of(writer.complete().dataOffsets().values())).hasSize(1);
    } finally {
      writer.close();
    }
  }

  @Test
  void moreSchemasThanTheCacheHoldsAreStillAccepted() {
    Table table = table();
    IcebergWriter writer = new IcebergWriter(table, "db.test", config(true), true);
    try {
      for (int i = 0; i < 200; i++) {
        Schema schema =
            SchemaBuilder.struct()
                .field("x", Schema.OPTIONAL_INT64_SCHEMA)
                .field("c" + (i % 70), Schema.OPTIONAL_STRING_SCHEMA)
                .build();
        writer.write(record(schema, i, i));
      }
      assertThat(records(writer.complete())).isEqualTo(200);
      assertThat(table.schema().columns()).hasSize(71);
    } finally {
      writer.close();
    }
  }
}
