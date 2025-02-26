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

import static java.util.stream.Collectors.toSet;
import io.tabular.iceberg.connect.IcebergSinkConfig;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.types.Types.StructType;
import org.apache.iceberg.util.Tasks;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IcebergWriterFactory {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergWriterFactory.class);

  private final Catalog catalog;
  private final IcebergSinkConfig config;

  public IcebergWriterFactory(Catalog catalog, IcebergSinkConfig config) {
    this.catalog = catalog;
    this.config = config;
  }

  public RecordWriter createWriter(
          String tableName, SinkRecord sample, boolean ignoreMissingTable) {
    TableIdentifier identifier = TableIdentifier.parse(tableName);
    Table table;
    try {
      table = catalog.loadTable(identifier);
    } catch (NoSuchTableException nst) {
      if (config.autoCreateEnabled()) {
        table = autoCreateTable(tableName, sample);
      } else if (ignoreMissingTable) {
        return new RecordWriter() {};
      } else {
        throw nst;
      }
    }

    return new IcebergWriter(table, tableName, config);
  }

  @VisibleForTesting
  Table autoCreateTable(String tableName, SinkRecord sample) {
    try {
      StructType structType;
      if (sample.valueSchema() == null) {
        structType =
                SchemaUtils.inferIcebergType(sample.value(), config)
                        .orElseThrow(() -> new DataException("Unable to create table from empty object"))
                        .asStructType();
      } else {
        structType = SchemaUtils.toIcebergType(sample.valueSchema(), config).asStructType();
      }

      org.apache.iceberg.Schema schema;
      Map<String, String> tableProperties = new HashMap<>(config.autoCreateProps());

      if (config.tablesCdcField() != null || config.upsertModeEnabled()) {
        Set<Integer> equalityFieldIds = Set.of();
        // Get PK from Kafka topic key
        if (sample.keySchema() != null) {
          equalityFieldIds =
              sample.keySchema().fields().stream()
                .map(col -> structType.field(col.name()).fieldId())
                .collect(toSet());
        }
        // Override PK with table config
        List<String> idCols = config.tableConfig(tableName).idColumns();
        if (!idCols.isEmpty()) {
          equalityFieldIds =
            idCols.stream()
                .map(colName -> structType.field(colName).fieldId())
                .collect(toSet());
        }

        schema = new org.apache.iceberg.Schema(structType.fields(), equalityFieldIds);

        if (!equalityFieldIds.isEmpty()) {
            for (Integer fieldId : equalityFieldIds) {
                String fieldName = schema.findColumnName(fieldId);
                tableProperties.put(
                    "write.parquet.bloom.filter.enabled.column." + fieldName,
                    "true"
                );
            }
        }
      } else {
        schema = new org.apache.iceberg.Schema(structType.fields());
      }
      TableIdentifier identifier = TableIdentifier.parse(tableName);

      List<String> partitionBy = config.tableConfig(tableName).partitionBy();
      PartitionSpec spec;
      try {
        spec = SchemaUtils.createPartitionSpec(schema, partitionBy);
      } catch (Exception e) {
        LOG.error(
                "Unable to create partition spec {}, table {} will be unpartitioned",
                partitionBy,
                identifier,
                e);
        spec = PartitionSpec.unpartitioned();
      }

      PartitionSpec partitionSpec = spec;
      AtomicReference<Table> result = new AtomicReference<>();
      Tasks.range(1)
              .retry(IcebergSinkConfig.CREATE_TABLE_RETRIES)
              .run(
                      notUsed -> {
                        try {
                          result.set(catalog.loadTable(identifier));
                        } catch (NoSuchTableException e) {
                          result.set(
                                  catalog.createTable(
                                          identifier, schema, partitionSpec, tableProperties));
                          LOG.info("Created new table {} from record at topic: {}, partition: {}, offset: {}", identifier, sample.topic(), sample.kafkaPartition(), sample.kafkaOffset());
                        }
                      });
      return result.get();
    } catch (Exception e) {
      LOG.error("Error creating new table {} from record at topic: {}, partition: {}, offset: {}", tableName, sample.topic(), sample.kafkaPartition(), sample.kafkaOffset());
      throw e;
    }
  }
}
