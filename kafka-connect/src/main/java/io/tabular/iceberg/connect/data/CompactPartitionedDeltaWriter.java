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
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.InternalRecordWrapper;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.Tasks;

/**
 * Compact delta writer for partitioned tables.
 */
public class CompactPartitionedDeltaWriter extends CompactDeltaTaskWriter {

  private final PartitionKey partitionKey;
  private final InternalRecordWrapper wrapper;
  private final Map<PartitionKey, CompactEqualityDeltaWriter> writers = Maps.newHashMap();

  public CompactPartitionedDeltaWriter(
      PartitionSpec spec,
      FileFormat format,
      FileAppenderFactory<Record> appenderFactory,
      OutputFileFactory fileFactory,
      FileIO io,
      long targetFileSize,
      Schema schema,
      Set<Integer> identifierFieldIds,
      boolean upsertMode,
      boolean deduplicateInserts) {
    super(
        spec,
        format,
        appenderFactory,
        fileFactory,
        io,
        targetFileSize,
        schema,
        identifierFieldIds,
        upsertMode,
        deduplicateInserts);

    this.partitionKey = new PartitionKey(spec, schema);
    this.wrapper = new InternalRecordWrapper(schema.asStruct());
  }

  @Override
  protected CompactEqualityDeltaWriter route(Record row) {
    partitionKey.partition(wrapper.wrap(row));

    CompactEqualityDeltaWriter writer = writers.get(partitionKey);
    if (writer == null) {
      // Need to copy partition key as it's mutable
      PartitionKey copiedKey = partitionKey.copy();
      writer =
          new CompactEqualityDeltaWriter(
              schema(),
              identifierFieldIds(),
              spec(),
              copiedKey,
              format(),
              appenderFactory(),
              fileFactory(),
              io(),
              targetFileSize(),
              hasRealPk());
      writers.put(copiedKey, writer);
    }

    return writer;
  }

  @Override
  protected WriteResult closeWriters() throws IOException {
    WriteResult.Builder resultBuilder = WriteResult.builder();

    try {
      Tasks.foreach(writers.values())
          .throwFailureWhenFinished()
          .noRetry()
          .run(
              writer -> {
                WriteResult writerResult = writer.complete();
                synchronized (resultBuilder) {
                  resultBuilder.addDataFiles(writerResult.dataFiles());
                  resultBuilder.addDeleteFiles(writerResult.deleteFiles());
                  resultBuilder.addReferencedDataFiles(writerResult.referencedDataFiles());
                }
              },
              IOException.class);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to close writers", e);
    }

    writers.clear();
    return resultBuilder.build();
  }
}
