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
package io.tabular.iceberg.connect.transforms;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.Requirements;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DebeziumTransform<R extends ConnectRecord<R>> implements Transformation<R> {

  private static final Logger LOG = LoggerFactory.getLogger(DebeziumTransform.class.getName());

  private static final String CDC_TARGET_PATTERN = "cdc.target.pattern";
  private static final String DB_PLACEHOLDER = "{db}";
  private static final String TABLE_PLACEHOLDER = "{table}";

  public static final ConfigDef CONFIG_DEF =
      new ConfigDef()
          .define(
              CDC_TARGET_PATTERN,
              ConfigDef.Type.STRING,
              null,
              Importance.MEDIUM,
              "Pattern to use for setting the CDC target field value.");

  // The converter hands out the same Schema instance for every record of one Avro schema, so the
  // schemas built from them are cached by instance. Bounded so that schema churn cannot leak.
  private static final int SCHEMA_CACHE_SIZE = 1024;

  private String cdcTargetPattern;
  private final SchemaCache cdcSchemas = new SchemaCache();
  private final SchemaCache updatedSchemas = new SchemaCache();

  @Override
  public void configure(Map<String, ?> props) {
    SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
    cdcTargetPattern = config.getString(CDC_TARGET_PATTERN);
  }

  @Override
  public R apply(R record) {
    if (record.value() == null) {
      return record;
    } else if (record.valueSchema() == null) {
      return applySchemaless(record);
    } else {
      return applyWithSchema(record);
    }
  }

  private R applyWithSchema(R record) {
    Struct value = Requirements.requireStruct(record.value(), "Debezium transform");

    String op = value.get("op").toString();
    if ("m".equals(op)) {
      return null; // Skip message events
    }
    op = mapOperation(op);

    Struct payload;
    Schema payloadSchema;
    Struct beforeImage = null;
    Schema beforeImageSchema = null;

    if (op.equals(CdcConstants.OP_DELETE)) {
      payload = value.getStruct("before");
      payloadSchema = value.schema().field("before").schema();
    } else {
      payload = value.getStruct("after");
      payloadSchema = value.schema().field("after").schema();

      // For UPDATE, capture before image for equality delete
      if (op.equals(CdcConstants.OP_UPDATE)) {
        beforeImage = value.getStruct("before");
        if (beforeImage != null) {
          beforeImageSchema = value.schema().field("before").schema();
        }
      }
    }

    // create the CDC metadata
    Schema cdcSchema = cdcSchema(record.keySchema());
    Struct cdcMetadata = new Struct(cdcSchema);
    cdcMetadata.put(CdcConstants.COL_OP, op);
    cdcMetadata.put(CdcConstants.COL_TS, new java.util.Date(value.getStruct("source").getInt64("ts_ms")));
    if (record instanceof SinkRecord) {
      cdcMetadata.put(CdcConstants.COL_PARTITION, ((SinkRecord) record).kafkaPartition());
      cdcMetadata.put(CdcConstants.COL_OFFSET, ((SinkRecord) record).kafkaOffset());
    }
    setTableSourceFromSourceStruct(value.getStruct("source"), cdcMetadata);
    setTableTargetFromTopic(record.topic(), cdcMetadata);

    if (record.keySchema() != null) {
      cdcMetadata.put(CdcConstants.COL_KEY, record.key());
    }

    // create the new value
    Schema newValueSchema = updatedSchema(payloadSchema, cdcSchema, beforeImageSchema);
    Struct newValue = new Struct(newValueSchema);

    // The payload fields come first in the new schema, in the same order, followed by the CDC
    // fields, so they are set by position instead of by name.
    List<Field> payloadFields = payloadSchema.fields();
    List<Field> newFields = newValueSchema.fields();
    int fieldCount = payloadFields.size();
    for (int i = 0; i < fieldCount; i++) {
      newValue.put(newFields.get(i), payload.get(payloadFields.get(i)));
    }
    newValue.put(newFields.get(fieldCount), cdcMetadata.get(CdcConstants.COL_OP));
    newValue.put(newFields.get(fieldCount + 1), cdcMetadata.get(CdcConstants.COL_TS));
    newValue.put(newFields.get(fieldCount + 2), cdcMetadata);

    // Add before image for UPDATE operations
    if (beforeImage != null) {
      newValue.put(newFields.get(fieldCount + 3), beforeImage);
    }

    return record.newRecord(
        record.topic(),
        record.kafkaPartition(),
        record.keySchema(),
        record.key(),
        newValueSchema,
        newValue,
        record.timestamp());
  }

  @SuppressWarnings("unchecked")
  private R applySchemaless(R record) {
    Map<String, Object> value = Requirements.requireMap(record.value(), "Debezium transform");

    String op = value.get("op").toString();
    if ("m".equals(op)) {
      return null; // Skip message events
    }
    op = mapOperation(op);

    Object payload;
    Object beforeImage = null;
    if (op.equals(CdcConstants.OP_DELETE)) {
      payload = value.get("before");
    } else {
      payload = value.get("after");
      // For UPDATE, capture before image for equality delete
      if (op.equals(CdcConstants.OP_UPDATE)) {
        beforeImage = value.get("before");
      }
    }

    if (!(payload instanceof Map)) {
      LOG.debug("Unable to transform Debezium record, payload is not a map, skipping");
      return null;
    }

    // create the CDC metadata
    Map<String, Object> cdcMetadata = Maps.newHashMap();
    cdcMetadata.put(CdcConstants.COL_OP, op);
    cdcMetadata.put(CdcConstants.COL_TS, value.get("ts_ms"));
    if (record instanceof SinkRecord) {
      cdcMetadata.put(CdcConstants.COL_OFFSET, ((SinkRecord) record).kafkaOffset());
    }
    setTableAndTargetFromSourceMap(value.get("source"), cdcMetadata);

    if (record.key() instanceof Map) {
      cdcMetadata.put(CdcConstants.COL_KEY, record.key());
    }

    // create the new value
    Map<String, Object> newValue = Maps.newHashMap((Map<String, Object>) payload);
    newValue.put(CdcConstants.COL_CDC, cdcMetadata);

    // Add before image for UPDATE operations
    if (beforeImage instanceof Map) {
      newValue.put(CdcConstants.COL_BEFORE_IMAGE, beforeImage);
    }

    return record.newRecord(
        record.topic(),
        record.kafkaPartition(),
        record.keySchema(),
        record.key(),
        null,
        newValue,
        record.timestamp());
  }

  private String mapOperation(String originalOp) {
    switch (originalOp) {
      case "u":
        return CdcConstants.OP_UPDATE;
      case "d":
        return CdcConstants.OP_DELETE;
      case "m":
        return null; // Message events are skipped
      default:
        // Debezium ops "c", "r", and any others
        return CdcConstants.OP_INSERT;
    }
  }

  private void setTableSourceFromSourceStruct(Struct source, Struct cdcMetadata) {
    String db;
    if (source.schema().field("schema") != null) {
      // prefer schema if present, e.g. for Postgres
      db = source.getString("schema");
    } else {
      db = source.getString("db");
    }
    String table = source.getString("table");

    cdcMetadata.put(CdcConstants.COL_SOURCE, db + "." + table);

    // Extract CDC source coordinates for deduplication
    extractSourceCoordinates(source, cdcMetadata);
  }

  private void extractSourceCoordinates(Struct source, Struct cdcMetadata) {
    Schema sourceSchema = source.schema();

    // MySQL GTID (optional, only when GTID mode enabled)
    if (sourceSchema.field("gtid") != null) {
      Object gtid = source.get("gtid");
      if (gtid != null) {
        cdcMetadata.put(CdcConstants.COL_GTID, gtid.toString());
      }
    }

    // MySQL binlog file
    if (sourceSchema.field("file") != null) {
      cdcMetadata.put(CdcConstants.COL_BINLOG_FILE, source.getString("file"));
    }

    // MySQL binlog position
    if (sourceSchema.field("pos") != null) {
      cdcMetadata.put(CdcConstants.COL_BINLOG_POS, source.getInt64("pos"));
    }

    // PostgreSQL transaction ID
    if (sourceSchema.field("txId") != null) {
      Object txId = source.get("txId");
      if (txId != null) {
        cdcMetadata.put(CdcConstants.COL_PG_TXID, ((Number) txId).longValue());
      }
    }

    // PostgreSQL LSN
    if (sourceSchema.field("lsn") != null) {
      Object lsn = source.get("lsn");
      if (lsn != null) {
        cdcMetadata.put(CdcConstants.COL_LSN, ((Number) lsn).longValue());
      }
    }

    // Snapshot indicator
    if (sourceSchema.field("snapshot") != null) {
      Object snapshot = source.get("snapshot");
      if (snapshot != null) {
        cdcMetadata.put(CdcConstants.COL_SNAPSHOT, snapshot.toString());
      }
    }
  }

  private void setTableTargetFromTopic(String topic, Struct cdcMetadata) {
    String db;
    String table;
    String[] parts = topic.split("\\.");

    db = parts[1];
    table = parts[2];
    cdcMetadata.put(CdcConstants.COL_TARGET, target(db, table));
  }

  private void setTableAndTargetFromSourceMap(Object source, Map<String, Object> cdcMetadata) {
    Map<String, Object> map = Requirements.requireMap(source, "Debezium transform");

    String db;
    if (map.containsKey("schema")) {
      // prefer schema if present, e.g. for Postgres
      db = map.get("schema").toString();
    } else {
      db = map.get("db").toString();
    }
    String table = map.get("table").toString();

    cdcMetadata.put(CdcConstants.COL_SOURCE, db + "." + table);
    cdcMetadata.put(CdcConstants.COL_TARGET, target(db, table));

    // Extract CDC source coordinates for deduplication
    extractSourceCoordinatesFromMap(map, cdcMetadata);
  }

  private void extractSourceCoordinatesFromMap(Map<String, Object> source, Map<String, Object> cdcMetadata) {
    // MySQL GTID (optional, only when GTID mode enabled)
    Object gtid = source.get("gtid");
    if (gtid != null) {
      cdcMetadata.put(CdcConstants.COL_GTID, gtid.toString());
    }

    // MySQL binlog file
    Object file = source.get("file");
    if (file != null) {
      cdcMetadata.put(CdcConstants.COL_BINLOG_FILE, file.toString());
    }

    // MySQL binlog position
    Object pos = source.get("pos");
    if (pos != null) {
      cdcMetadata.put(CdcConstants.COL_BINLOG_POS, ((Number) pos).longValue());
    }

    // PostgreSQL transaction ID
    Object txId = source.get("txId");
    if (txId != null) {
      cdcMetadata.put(CdcConstants.COL_PG_TXID, ((Number) txId).longValue());
    }

    // PostgreSQL LSN
    Object lsn = source.get("lsn");
    if (lsn != null) {
      cdcMetadata.put(CdcConstants.COL_LSN, ((Number) lsn).longValue());
    }

    // Snapshot indicator
    Object snapshot = source.get("snapshot");
    if (snapshot != null) {
      cdcMetadata.put(CdcConstants.COL_SNAPSHOT, snapshot.toString());
    }
  }

  private String target(String db, String table) {
    return cdcTargetPattern == null || cdcTargetPattern.isEmpty()
        ? db + "." + table
        : cdcTargetPattern.replace(DB_PLACEHOLDER, db).replace(TABLE_PLACEHOLDER, table);
  }

  private Schema cdcSchema(Schema keySchema) {
    // a SchemaBuilder can still change after it was used, so only built schemas are cached
    if (keySchema != null && !(keySchema instanceof ConnectSchema)) {
      return makeCdcSchema(keySchema);
    }
    SchemaKey key = new SchemaKey(keySchema, null, null);
    Schema cdcSchema = cdcSchemas.get(key);
    if (cdcSchema == null) {
      cdcSchema = makeCdcSchema(keySchema);
      cdcSchemas.put(key, cdcSchema);
    }
    return cdcSchema;
  }

  private Schema updatedSchema(Schema schema, Schema cdcSchema, Schema beforeImageSchema) {
    if (!(schema instanceof ConnectSchema)
        || (beforeImageSchema != null && !(beforeImageSchema instanceof ConnectSchema))) {
      return makeUpdatedSchema(schema, cdcSchema, beforeImageSchema);
    }
    SchemaKey key = new SchemaKey(schema, cdcSchema, beforeImageSchema);
    Schema updatedSchema = updatedSchemas.get(key);
    if (updatedSchema == null) {
      updatedSchema = makeUpdatedSchema(schema, cdcSchema, beforeImageSchema);
      updatedSchemas.put(key, updatedSchema);
    }
    return updatedSchema;
  }

  private Schema makeCdcSchema(Schema keySchema) {
    SchemaBuilder builder =
        SchemaBuilder.struct()
            .field(CdcConstants.COL_OP, Schema.STRING_SCHEMA)
            .field(CdcConstants.COL_TS, Timestamp.SCHEMA)
            .field(CdcConstants.COL_PARTITION, Schema.INT32_SCHEMA)
            .field(CdcConstants.COL_OFFSET, Schema.OPTIONAL_INT64_SCHEMA)
            .field(CdcConstants.COL_SOURCE, Schema.STRING_SCHEMA)
            .field(CdcConstants.COL_TARGET, Schema.STRING_SCHEMA)
            // CDC source coordinates for deduplication
            .field(CdcConstants.COL_GTID, Schema.OPTIONAL_STRING_SCHEMA)
            .field(CdcConstants.COL_BINLOG_FILE, Schema.OPTIONAL_STRING_SCHEMA)
            .field(CdcConstants.COL_BINLOG_POS, Schema.OPTIONAL_INT64_SCHEMA)
            .field(CdcConstants.COL_PG_TXID, Schema.OPTIONAL_INT64_SCHEMA)
            .field(CdcConstants.COL_LSN, Schema.OPTIONAL_INT64_SCHEMA)
            .field(CdcConstants.COL_SNAPSHOT, Schema.OPTIONAL_STRING_SCHEMA);

    if (keySchema != null) {
      builder.field(CdcConstants.COL_KEY, keySchema);
    }

    return builder.build();
  }

  private Schema makeUpdatedSchema(Schema schema, Schema cdcSchema, Schema beforeImageSchema) {
    SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());

    for (Field field : schema.fields()) {
      builder.field(field.name(), field.schema());
    }

    builder.field("_cdc_op", Schema.STRING_SCHEMA);
    builder.field("_cdc_tsms", Timestamp.SCHEMA);
    builder.field(CdcConstants.COL_CDC, cdcSchema);

    // Add optional before image schema for UPDATE operations
    if (beforeImageSchema != null) {
      builder.field(CdcConstants.COL_BEFORE_IMAGE, beforeImageSchema);
    }

    return builder.build();
  }

  @Override
  public ConfigDef config() {
    return CONFIG_DEF;
  }

  @VisibleForTesting
  int cachedSchemaCount() {
    return cdcSchemas.size() + updatedSchemas.size();
  }

  @Override
  public void close() {
    cdcSchemas.clear();
    updatedSchemas.clear();
  }

  /** Up to three schemas compared by identity, a deep Schema.equals would walk every field. */
  private static final class SchemaKey {
    private final Schema first;
    private final Schema second;
    private final Schema third;
    private final int hash;

    SchemaKey(Schema first, Schema second, Schema third) {
      this.first = first;
      this.second = second;
      this.third = third;
      this.hash =
          31 * (31 * System.identityHashCode(first) + System.identityHashCode(second))
              + System.identityHashCode(third);
    }

    @Override
    public int hashCode() {
      return hash;
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      }
      if (!(other instanceof SchemaKey)) {
        return false;
      }
      SchemaKey that = (SchemaKey) other;
      return first == that.first && second == that.second && third == that.third;
    }
  }

  /** Least recently used schemas, the transform may be called from more than one thread. */
  private static final class SchemaCache {
    private final Map<SchemaKey, Schema> cache =
        new LinkedHashMap<SchemaKey, Schema>(16, 0.75f, true) {
          @Override
          protected boolean removeEldestEntry(Map.Entry<SchemaKey, Schema> eldest) {
            return size() > SCHEMA_CACHE_SIZE;
          }
        };

    synchronized Schema get(SchemaKey key) {
      return cache.get(key);
    }

    synchronized void put(SchemaKey key, Schema schema) {
      cache.put(key, schema);
    }

    synchronized int size() {
      return cache.size();
    }

    synchronized void clear() {
      cache.clear();
    }
  }
}
