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

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

final class TableWatermark {

  static final Schema SCHEMA =
      SchemaBuilder.record("TableWatermark")
          .namespace("io.tabular.iceberg.connect.watermark")
          .fields()
          .name("db")
          .type()
          .stringType()
          .noDefault()
          .name("table")
          .type()
          .stringType()
          .noDefault()
          .name("commit_id")
          .type()
          .stringType()
          .noDefault()
          .name("commit_time")
          .type()
          .longType()
          .noDefault()
          .name("topic")
          .type()
          .stringType()
          .noDefault()
          .name("last_consumed_offset")
          .type()
          .nullable()
          .longType()
          .noDefault()
          .name("last_consumed_event_time")
          .type()
          .nullable()
          .longType()
          .noDefault()
          .name("last_kafka_offset")
          .type()
          .nullable()
          .longType()
          .noDefault()
          .name("last_kafka_event_time")
          .type()
          .nullable()
          .longType()
          .noDefault()
          .endRecord();

  private TableWatermark() {}

  static GenericRecord build(
      String db,
      String table,
      String commitId,
      long commitTime,
      String topic,
      Long lastConsumedOffset,
      Long lastConsumedEventTime,
      Long lastKafkaOffset,
      Long lastKafkaEventTime) {
    GenericRecord rec = new GenericData.Record(SCHEMA);
    rec.put("db", db);
    rec.put("table", table);
    rec.put("commit_id", commitId);
    rec.put("commit_time", commitTime);
    rec.put("topic", topic);
    rec.put("last_consumed_offset", lastConsumedOffset);
    rec.put("last_consumed_event_time", lastConsumedEventTime);
    rec.put("last_kafka_offset", lastKafkaOffset);
    rec.put("last_kafka_event_time", lastKafkaEventTime);
    return rec;
  }
}
