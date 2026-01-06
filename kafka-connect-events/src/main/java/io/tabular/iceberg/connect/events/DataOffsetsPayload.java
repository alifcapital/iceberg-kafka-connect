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
package io.tabular.iceberg.connect.events;

import java.util.List;
import java.util.UUID;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

/**
 * Payload for DATA_OFFSETS event type. Contains data topic partition offsets (start and end)
 * for a specific table, sent by workers to the coordinator for tracking data lineage.
 */
public class DataOffsetsPayload implements Payload {

  private UUID commitId;
  private TableName tableName;
  private List<TopicPartitionOffset> dataOffsets;
  private final Schema avroSchema;

  public static final Schema AVRO_SCHEMA =
      SchemaBuilder.builder()
          .record(DataOffsetsPayload.class.getName())
          .fields()
          .name("commitId")
          .prop(FIELD_ID_PROP, DUMMY_FIELD_ID)
          .type(UUID_SCHEMA)
          .noDefault()
          .name("tableName")
          .prop(FIELD_ID_PROP, DUMMY_FIELD_ID)
          .type(TableName.AVRO_SCHEMA)
          .noDefault()
          .name("dataOffsets")
          .prop(FIELD_ID_PROP, DUMMY_FIELD_ID)
          .type()
          .nullable()
          .array()
          .items(TopicPartitionOffset.AVRO_SCHEMA)
          .noDefault()
          .endRecord();

  // Used by Avro reflection to instantiate this class when reading events
  public DataOffsetsPayload(Schema avroSchema) {
    this.avroSchema = avroSchema;
  }

  public DataOffsetsPayload(
      UUID commitId, TableName tableName, List<TopicPartitionOffset> dataOffsets) {
    this.commitId = commitId;
    this.tableName = tableName;
    this.dataOffsets = dataOffsets;
    this.avroSchema = AVRO_SCHEMA;
  }

  public UUID commitId() {
    return commitId;
  }

  public TableName tableName() {
    return tableName;
  }

  public List<TopicPartitionOffset> dataOffsets() {
    return dataOffsets;
  }

  @Override
  public Schema getSchema() {
    return avroSchema;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void put(int i, Object v) {
    switch (i) {
      case 0:
        this.commitId = (UUID) v;
        return;
      case 1:
        this.tableName = (TableName) v;
        return;
      case 2:
        this.dataOffsets = (List<TopicPartitionOffset>) v;
        return;
      default:
        // ignore the object, it must be from a newer version of the format
    }
  }

  @Override
  public Object get(int i) {
    switch (i) {
      case 0:
        return commitId;
      case 1:
        return tableName;
      case 2:
        return dataOffsets;
      default:
        throw new UnsupportedOperationException("Unknown field ordinal: " + i);
    }
  }
}
