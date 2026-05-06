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

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

public class TableWatermarkTest {

  @Test
  public void schemaHasAllElevenFields() {
    assertThat(TableWatermark.SCHEMA.getFields())
        .extracting(Schema.Field::name)
        .containsExactly(
            "db",
            "table",
            "commit_id",
            "commit_time",
            "topic",
            "last_consumed_offset",
            "last_consumed_event_time",
            "last_kafka_offset",
            "last_kafka_event_time",
            "iceberg_snapshot_id",
            "iceberg_committed_at");
  }

  @Test
  public void nullableFieldsAreUnion() {
    for (String name :
        new String[] {
          "last_consumed_offset",
          "last_consumed_event_time",
          "last_kafka_offset",
          "last_kafka_event_time",
          "iceberg_snapshot_id",
          "iceberg_committed_at"
        }) {
      Schema fieldSchema = TableWatermark.SCHEMA.getField(name).schema();
      assertThat(fieldSchema.getType()).isEqualTo(Schema.Type.UNION);
      assertThat(fieldSchema.getTypes())
          .extracting(Schema::getType)
          .contains(Schema.Type.NULL, Schema.Type.LONG);
    }
  }

  @Test
  public void buildPopulatesAllFields() {
    GenericRecord rec =
        TableWatermark.build(
            "landing_db",
            "terminals",
            "11111111-1111-1111-1111-111111111111",
            1700000000000L,
            "debezium.public.terminals",
            123L,
            1700000000123L,
            999L,
            1700000000999L,
            8123456789012345678L,
            1700000000500L);

    assertThat(rec.get("db")).isEqualTo("landing_db");
    assertThat(rec.get("table")).isEqualTo("terminals");
    assertThat(rec.get("commit_id")).isEqualTo("11111111-1111-1111-1111-111111111111");
    assertThat(rec.get("commit_time")).isEqualTo(1700000000000L);
    assertThat(rec.get("topic")).isEqualTo("debezium.public.terminals");
    assertThat(rec.get("last_consumed_offset")).isEqualTo(123L);
    assertThat(rec.get("last_consumed_event_time")).isEqualTo(1700000000123L);
    assertThat(rec.get("last_kafka_offset")).isEqualTo(999L);
    assertThat(rec.get("last_kafka_event_time")).isEqualTo(1700000000999L);
    assertThat(rec.get("iceberg_snapshot_id")).isEqualTo(8123456789012345678L);
    assertThat(rec.get("iceberg_committed_at")).isEqualTo(1700000000500L);
  }

  @Test
  public void buildAcceptsNullsForIdleAndEmptyTopic() {
    GenericRecord rec =
        TableWatermark.build(
            "db", "tbl", "cid", 1L, "topic", null, null, null, null, null, null);
    assertThat(rec.get("last_consumed_offset")).isNull();
    assertThat(rec.get("last_consumed_event_time")).isNull();
    assertThat(rec.get("last_kafka_offset")).isNull();
    assertThat(rec.get("last_kafka_event_time")).isNull();
    assertThat(rec.get("iceberg_snapshot_id")).isNull();
    assertThat(rec.get("iceberg_committed_at")).isNull();
  }
}
