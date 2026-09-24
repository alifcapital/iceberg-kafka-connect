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

import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.types.Type;

/** Normalize uncommitted file partition metadata after an identity-column type promotion. */
public final class PendingFileNormalizer {
  private PendingFileNormalizer() {}

  private static PartitionData partition(Table table, ContentFile<?> file) {
    PartitionSpec spec = table.specs().get(file.specId());
    PartitionData result = new PartitionData(spec.partitionType());
    boolean changed = false;
    for (int i = 0; i < result.size(); i++) {
      Object value = file.partition().get(i, Object.class);
      Type.TypeID type = spec.partitionType().fields().get(i).type().typeId();
      if (value instanceof Integer && type == Type.TypeID.LONG) {
        value = ((Integer) value).longValue();
        changed = true;
      } else if (value instanceof Float && type == Type.TypeID.DOUBLE) {
        value = ((Float) value).doubleValue();
        changed = true;
      }
      result.set(i, value);
    }
    return changed ? result : null;
  }

  private static Metrics metrics(ContentFile<?> file) {
    return new Metrics(
        file.recordCount(),
        file.columnSizes(),
        file.valueCounts(),
        file.nullValueCounts(),
        file.nanValueCounts(),
        file.lowerBounds(),
        file.upperBounds());
  }

  public static DataFile normalize(Table table, DataFile file) {
    PartitionData partition = partition(table, file);
    if (partition == null) {
      return file;
    }
    DataFiles.Builder builder =
        DataFiles.builder(table.specs().get(file.specId()))
            .withPath(file.location())
            .withFormat(file.format())
            .withPartition(partition)
            .withFileSizeInBytes(file.fileSizeInBytes())
            .withMetrics(metrics(file))
            .withSplitOffsets(file.splitOffsets())
            .withEncryptionKeyMetadata(file.keyMetadata());
    if (file.sortOrderId() != null) {
      builder.withSortOrder(table.sortOrders().get(file.sortOrderId()));
    }
    return builder.build();
  }

  public static DeleteFile normalize(Table table, DeleteFile file) {
    PartitionData partition = partition(table, file);
    if (partition == null) {
      return file;
    }
    FileMetadata.Builder builder = FileMetadata.deleteFileBuilder(table.specs().get(file.specId()));
    if (file.content() == FileContent.EQUALITY_DELETES) {
      builder.ofEqualityDeletes(
          file.equalityFieldIds().stream().mapToInt(Integer::intValue).toArray());
    } else {
      builder.ofPositionDeletes();
    }
    builder
        .withPath(file.location())
        .withFormat(file.format())
        .withPartition(partition)
        .withFileSizeInBytes(file.fileSizeInBytes())
        .withMetrics(metrics(file))
        .withSplitOffsets(file.splitOffsets())
        .withEncryptionKeyMetadata(file.keyMetadata());
    if (file.sortOrderId() != null) {
      builder.withSortOrder(table.sortOrders().get(file.sortOrderId()));
    }
    if (file.referencedDataFile() != null) {
      builder.withReferencedDataFile(file.referencedDataFile());
    }
    return builder.build();
  }
}
