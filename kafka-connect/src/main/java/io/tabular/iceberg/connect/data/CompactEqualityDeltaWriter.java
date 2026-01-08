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

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.DeleteGranularity;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.SortingPositionOnlyDeleteWriter;
import org.apache.iceberg.io.DataWriteResult;
import org.apache.iceberg.io.DeleteWriteResult;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.FileWriter;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.util.CharSequenceSet;

/**
 * Compact equality delta writer that uses CompactKeyMap for memory-efficient tracking of inserted
 * rows.
 *
 * <p>This writer replaces BaseEqualityDeltaWriter with a more memory-efficient implementation:
 * <ul>
 *   <li>Uses primitive arrays for single-column INT/LONG keys (~20 bytes vs ~120 bytes per entry)
 *   <li>Uses optimized hash maps for STRING/DECIMAL keys
 *   <li>Interns file paths to avoid storing same string millions of times
 * </ul>
 *
 * <p>Memory savings: ~6x reduction for typical CDC workloads.
 */
public class CompactEqualityDeltaWriter implements Closeable {

  private final Schema deleteSchema;
  private final PartitionSpec spec;
  private final StructLike partition;
  private final FileFormat format;
  private final FileAppenderFactory<Record> appenderFactory;
  private final OutputFileFactory fileFactory;
  private final FileIO io;
  private final long targetFileSize;
  private final RecordProjection keyProjection;

  // Compact key-to-position map instead of StructLikeMap
  private final CompactKeyMap insertedRowMap;

  // If true, deduplicate inserts in buffer. If false, track all positions for tables without real PK.
  private final boolean deduplicateInserts;

  // Writers
  private RollingDataWriterWrapper dataWriter;
  private RollingEqDeleteWriterWrapper eqDeleteWriter;
  private FileWriter<PositionDelete<Record>, DeleteWriteResult> posDeleteWriter;

  // Results
  private final List<DataFile> completedDataFiles = Lists.newArrayList();
  private final List<DeleteFile> completedDeleteFiles = Lists.newArrayList();
  private final CharSequenceSet referencedDataFiles = CharSequenceSet.empty();
  private boolean closed = false;

  public CompactEqualityDeltaWriter(
      Schema schema,
      Set<Integer> identifierFieldIds,
      PartitionSpec spec,
      StructLike partition,
      FileFormat format,
      FileAppenderFactory<Record> appenderFactory,
      OutputFileFactory fileFactory,
      FileIO io,
      long targetFileSize,
      boolean deduplicateInserts) {
    this.deleteSchema = TypeUtil.select(schema, Sets.newHashSet(identifierFieldIds));
    this.spec = spec;
    this.partition = partition;
    this.format = format;
    this.appenderFactory = appenderFactory;
    this.fileFactory = fileFactory;
    this.io = io;
    this.targetFileSize = targetFileSize;
    this.deduplicateInserts = deduplicateInserts;

    this.keyProjection = RecordProjection.create(schema, deleteSchema);

    // Create compact key map based on delete schema
    // For tables without real PK, use multi-value map to track duplicate positions
    this.insertedRowMap = CompactKeyMap.create(deleteSchema, deduplicateInserts);

    // Initialize writers
    this.dataWriter = new RollingDataWriterWrapper();
    this.eqDeleteWriter = new RollingEqDeleteWriterWrapper();
    this.posDeleteWriter =
        new SortingPositionOnlyDeleteWriter<>(
            () -> appenderFactory.newPosDeleteWriter(newOutputFile(), format, partition),
            DeleteGranularity.FILE);
  }

  private org.apache.iceberg.encryption.EncryptedOutputFile newOutputFile() {
    if (spec.isUnpartitioned() || partition == null) {
      return fileFactory.newOutputFile();
    } else {
      return fileFactory.newOutputFile(spec, partition);
    }
  }

  /**
   * Write a data record (INSERT or UPDATE_AFTER).
   */
  public void write(Record row) throws IOException {
    String currentPath = dataWriter.currentPath();
    int currentRows = (int) dataWriter.currentRows();

    // Project key from row
    Record keyRecord = keyProjection.wrap(row);

    // Track position for deleteKey() lookups
    // For deduplicateInserts=true: put() replaces previous and returns it for position delete
    // For deduplicateInserts=false: put() tracks all positions, returns null (handled by MultiValueWrapper)
    CompactKeyMap.PathOffset previous = insertedRowMap.put(keyRecord, currentPath, currentRows);

    if (deduplicateInserts && previous != null) {
      // Deduplicate: delete previous row with same key
      writePosDelete(insertedRowMap.getPath(previous.pathIndex), previous.position);
    }

    dataWriter.write(row);
  }

  /**
   * Delete by key. If key exists in current session, writes pos delete. Otherwise writes eq
   * delete.
   */
  public void deleteKey(Record keyRecord) throws IOException {
    CompactKeyMap.PathOffset previous = insertedRowMap.remove(keyRecord);
    if (previous != null) {
      writePosDelete(insertedRowMap.getPath(previous.pathIndex), previous.position);
    } else {
      eqDeleteWriter.write(keyRecord);
    }
  }

  /**
   * Delete by full row.
   */
  public void delete(Record row) throws IOException {
    Record keyRecord = keyProjection.wrap(row);
    deleteKey(keyRecord);
  }

  private void writePosDelete(String path, long position) {
    PositionDelete<Record> positionDelete = PositionDelete.create();
    positionDelete.set(path, position, null);
    posDeleteWriter.write(positionDelete);
  }

  public WriteResult complete() throws IOException {
    close();

    return WriteResult.builder()
        .addDataFiles(completedDataFiles)
        .addDeleteFiles(completedDeleteFiles)
        .addReferencedDataFiles(referencedDataFiles)
        .build();
  }

  /**
   * Abort and cleanup any written files.
   */
  public void abort() throws IOException {
    // Close writers first to finalize any in-progress files
    close();

    // Delete all written files
    for (DataFile file : completedDataFiles) {
      try {
        io.deleteFile(file.location());
      } catch (Exception e) {
        // Ignore cleanup failures
      }
    }

    for (DeleteFile file : completedDeleteFiles) {
      try {
        io.deleteFile(file.location());
      } catch (Exception e) {
        // Ignore cleanup failures
      }
    }

    completedDataFiles.clear();
    completedDeleteFiles.clear();
  }

  @Override
  public void close() throws IOException {
    if (closed) {
      return;
    }

    try {
      // Close data writer
      if (dataWriter != null) {
        dataWriter.close();
        DataWriteResult dataResult = dataWriter.result();
        if (dataResult != null) {
          completedDataFiles.addAll(dataResult.dataFiles());
        }
        dataWriter = null;
      }

      // Close eq delete writer
      if (eqDeleteWriter != null) {
        eqDeleteWriter.close();
        DeleteWriteResult eqResult = eqDeleteWriter.result();
        if (eqResult != null) {
          completedDeleteFiles.addAll(eqResult.deleteFiles());
        }
        eqDeleteWriter = null;
      }

      // Close pos delete writer
      if (posDeleteWriter != null) {
        posDeleteWriter.close();
        DeleteWriteResult posResult = posDeleteWriter.result();
        if (posResult != null) {
          completedDeleteFiles.addAll(posResult.deleteFiles());
          referencedDataFiles.addAll(posResult.referencedDataFiles());
        }
        posDeleteWriter = null;
      }

      // Clear the map
      insertedRowMap.clear();

    } finally {
      closed = true;
    }
  }

  // ==================== Rolling Data Writer Wrapper ====================

  /**
   * Simple rolling data writer that wraps FileAppenderFactory.
   */
  private class RollingDataWriterWrapper implements Closeable {
    private static final int ROWS_DIVISOR = 1000;

    private org.apache.iceberg.encryption.EncryptedOutputFile currentFile;
    private org.apache.iceberg.io.DataWriter<Record> currentWriter;
    private long currentRows;
    private final List<DataFile> dataFiles = Lists.newArrayList();
    private boolean closed = false;

    RollingDataWriterWrapper() {
      openCurrent();
    }

    String currentPath() {
      return currentFile.encryptingOutputFile().location();
    }

    long currentRows() {
      return currentRows;
    }

    void write(Record row) throws IOException {
      currentWriter.write(row);
      currentRows++;

      if (shouldRoll()) {
        closeCurrent();
        openCurrent();
      }
    }

    private boolean shouldRoll() {
      return currentRows % ROWS_DIVISOR == 0 && currentWriter.length() >= targetFileSize;
    }

    private void openCurrent() {
      this.currentFile = newOutputFile();
      this.currentWriter = appenderFactory.newDataWriter(currentFile, format, partition);
      this.currentRows = 0;
    }

    private void closeCurrent() throws IOException {
      if (currentWriter != null) {
        currentWriter.close();
        if (currentRows > 0) {
          dataFiles.add(currentWriter.toDataFile());
        } else {
          // Delete empty file
          try {
            io.deleteFile(currentFile.encryptingOutputFile());
          } catch (Exception e) {
            // Ignore cleanup failures
          }
        }
        currentWriter = null;
        currentFile = null;
        currentRows = 0;
      }
    }

    @Override
    public void close() throws IOException {
      if (!closed) {
        closeCurrent();
        closed = true;
      }
    }

    DataWriteResult result() {
      return new DataWriteResult(dataFiles);
    }
  }

  // ==================== Rolling Eq Delete Writer Wrapper ====================

  /**
   * Simple rolling eq delete writer that wraps FileAppenderFactory.
   */
  private class RollingEqDeleteWriterWrapper implements Closeable {
    private static final int ROWS_DIVISOR = 1000;

    private org.apache.iceberg.encryption.EncryptedOutputFile currentFile;
    private org.apache.iceberg.deletes.EqualityDeleteWriter<Record> currentWriter;
    private long currentRows;
    private final List<DeleteFile> deleteFiles = Lists.newArrayList();
    private boolean closed = false;

    RollingEqDeleteWriterWrapper() {
      openCurrent();
    }

    void write(Record row) throws IOException {
      currentWriter.write(row);
      currentRows++;

      if (shouldRoll()) {
        closeCurrent();
        openCurrent();
      }
    }

    private boolean shouldRoll() {
      return currentRows % ROWS_DIVISOR == 0 && currentWriter.length() >= targetFileSize;
    }

    private void openCurrent() {
      this.currentFile = newOutputFile();
      this.currentWriter = appenderFactory.newEqDeleteWriter(currentFile, format, partition);
      this.currentRows = 0;
    }

    private void closeCurrent() throws IOException {
      if (currentWriter != null) {
        currentWriter.close();
        if (currentRows > 0) {
          deleteFiles.add(currentWriter.toDeleteFile());
        } else {
          // Delete empty file
          try {
            io.deleteFile(currentFile.encryptingOutputFile());
          } catch (Exception e) {
            // Ignore cleanup failures
          }
        }
        currentWriter = null;
        currentFile = null;
        currentRows = 0;
      }
    }

    @Override
    public void close() throws IOException {
      if (!closed) {
        closeCurrent();
        closed = true;
      }
    }

    DeleteWriteResult result() {
      return new DeleteWriteResult(deleteFiles);
    }
  }
}
