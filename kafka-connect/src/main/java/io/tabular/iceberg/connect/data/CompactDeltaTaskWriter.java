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
import java.util.Set;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.io.WriteResult;

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

  /**
   * Route record to appropriate partition writer.
   */
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

    CompactEqualityDeltaWriter writer = route(row);

    switch (op) {
      case DELETE:
        // For DELETE, DebeziumTransform puts before image into row
        writer.deleteKey(keyProjection.wrap(row));
        break;

      case UPDATE:
        // For UPDATE without real PK, we must use before image for equality delete
        // because all columns are used as identifier fields
        if (!hasRealPk) {
          if (before == null) {
            throw new IllegalStateException(
                "UPDATE operation requires before image for tables without real PK");
          }
          writer.deleteKey(keyProjection.wrap(before));
        } else {
          // For tables with real PK, keyProjection extracts only PK fields which are same in before/after
          writer.deleteKey(keyProjection.wrap(row));
        }
        writer.write(row);
        break;

      case INSERT:
        writer.write(row);
        break;
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
