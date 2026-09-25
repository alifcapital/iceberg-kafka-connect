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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.relocated.com.google.common.primitives.Ints;
import org.apache.iceberg.types.TypeUtil;

/**
 * Abstract task writer that uses CompactEqualityDeltaWriter for memory-efficient CDC processing.
 *
 * <p>This is a drop-in replacement for BaseDeltaTaskWriter that uses ~6x less memory for tracking
 * inserted rows.
 */
public abstract class CompactDeltaTaskWriter implements TaskWriter<Record> {

  private final Schema schema;
  private final Set<Integer> identifierFieldIds;
  private final PartitionSpec spec;
  private final FileFormat format;
  private final FileAppenderFactory<Record> appenderFactory;
  private final OutputFileFactory fileFactory;
  private final FileIO io;
  private final long targetFileSize;
  private final boolean upsertMode;
  private final boolean hasRealPk;
  private final RecordProjection keyProjection;

  private WriteResult result = null;
  private Map<String, String> writerProperties = Map.of();
  private final Map<Set<Integer>, FileAppenderFactory<Record>> deleteFactories = Maps.newHashMap();

  void setWriterProperties(Map<String, String> properties) {
    writerProperties = Map.copyOf(properties);
  }

  protected CompactDeltaTaskWriter(
      PartitionSpec spec,
      FileFormat format,
      FileAppenderFactory<Record> appenderFactory,
      OutputFileFactory fileFactory,
      FileIO io,
      long targetFileSize,
      Schema schema,
      Set<Integer> identifierFieldIds,
      boolean upsertMode,
      boolean hasRealPk) {
    this.spec = spec;
    this.format = format;
    this.appenderFactory = appenderFactory;
    this.fileFactory = fileFactory;
    this.io = io;
    this.targetFileSize = targetFileSize;
    this.schema = schema;
    this.identifierFieldIds = identifierFieldIds;
    this.upsertMode = upsertMode;
    this.hasRealPk = hasRealPk;

    Schema deleteSchema =
        org.apache.iceberg.types.TypeUtil.select(
            schema, org.apache.iceberg.relocated.com.google.common.collect.Sets.newHashSet(identifierFieldIds));
    this.keyProjection = RecordProjection.create(schema, deleteSchema);
  }

  protected Schema schema() {
    return schema;
  }

  protected Set<Integer> identifierFieldIds() {
    return identifierFieldIds;
  }

  protected PartitionSpec spec() {
    return spec;
  }

  protected FileFormat format() {
    return format;
  }

  protected FileAppenderFactory<Record> appenderFactory() {
    return appenderFactory;
  }

  protected OutputFileFactory fileFactory() {
    return fileFactory;
  }

  protected FileIO io() {
    return io;
  }

  protected long targetFileSize() {
    return targetFileSize;
  }

  protected boolean hasRealPk() {
    return hasRealPk;
  }

  /** Route record to appropriate partition writer. */
  private Map<List<Object>, CompactKeyMap> pendingKeys;

  void usePendingKeys(Map<List<Object>, CompactKeyMap> keys) {
    this.pendingKeys = keys;
  }

  protected abstract CompactEqualityDeltaWriter route(Record row);

  /**
   * Close all writers and collect results.
   */
  protected abstract WriteResult closeWriters() throws IOException;

  @Override
  public void write(Record row) throws IOException {
    Operation op;
    Record before = null;

    if (row instanceof RecordWrapper) {
      RecordWrapper wrapper = (RecordWrapper) row;
      op = wrapper.op();
      before = wrapper.before();
      if (upsertMode && op == Operation.INSERT) {
        op = Operation.UPDATE;
      }
    } else {
      op = upsertMode ? Operation.UPDATE : Operation.INSERT;
    }

    switch (op) {
      case DELETE:
        // For DELETE, DebeziumTransform puts before image into row
        if (hasRealPk) {
          writerFor(row).deleteKey(keyProjection.wrap(row));
        } else {
          deleteWithoutPk(row);
        }
        break;

      case UPDATE:
        // For UPDATE without real PK, we must use before image for equality delete
        // because all columns are used as identifier fields
        if (!hasRealPk) {
          if (before == null) {
            throw new IllegalStateException(
                "UPDATE operation requires before image for tables without real PK");
          }
          deleteWithoutPk(before);
        } else {
          // For tables with real PK, keyProjection extracts only PK fields which are same in
          // before/after
          writerFor(row).deleteKey(keyProjection.wrap(row));
        }
        writerFor(row).write(row);
        break;

      case INSERT:
        writerFor(row).write(row);
        break;
    }
  }

  private CompactEqualityDeltaWriter writerFor(Record row) {
    CompactEqualityDeltaWriter writer = route(row);
    if (pendingKeys != null) {
      writer.usePendingKeys(pendingKeys);
    }
    return writer;
  }

  private void deleteWithoutPk(Record row) throws IOException {
    Set<Integer> sourceIds = RecordWrapper.sourceFieldIds(row);
    Set<Integer> ids =
        sourceIds == null
            ? identifierFieldIds
            : Set.copyOf(Sets.intersection(sourceIds, identifierFieldIds));
    if (ids.isEmpty()) {
      throw new IllegalArgumentException("Cannot delete without any source equality fields");
    }
    if (sourceIds != null) {
      spec.fields()
          .forEach(
              field -> {
                if (!field.transform().isVoid() && !sourceIds.contains(field.sourceId())) {
                  throw new IllegalArgumentException(
                      "Missing partition source field for no-PK delete: "
                          + schema.findColumnName(field.sourceId()));
                }
              });
    }
    CompactEqualityDeltaWriter writer = writerFor(row);
    Record key = keyProjection.wrap(row);
    if (ids.equals(identifierFieldIds)) {
      writer.deleteKey(key);
    } else {
      FileAppenderFactory<Record> factory =
          deleteFactories.computeIfAbsent(
              ids,
              fields ->
                  new GenericAppenderFactory(
                          schema, spec, Ints.toArray(fields), TypeUtil.select(schema, fields), null)
                      .setAll(writerProperties));
      writer.deleteFields(key, ids, factory);
    }
  }

  @Override
  public void abort() throws IOException {
    close();
    // Files will be cleaned up by orphan file cleanup
  }

  @Override
  public WriteResult complete() throws IOException {
    close();
    return result;
  }

  @Override
  public void close() throws IOException {
    if (result == null) {
      result = closeWriters();
    }
  }
}
