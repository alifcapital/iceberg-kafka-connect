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

import static org.apache.iceberg.types.Types.NestedField.required;

import java.nio.ByteBuffer;
import java.util.List;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.common.DynConstructors;
import org.apache.iceberg.common.DynConstructors.Ctor;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types.StringType;
import org.apache.iceberg.types.Types.StructType;

public class EventTestUtil {
  public static DataFile createDataFile() {
    Ctor<DataFile> ctor =
        DynConstructors.builder(DataFile.class)
            .hiddenImpl(
                "org.apache.iceberg.GenericDataFile",
                int.class,
                String.class,
                FileFormat.class,
                PartitionData.class,
                long.class,
                Metrics.class,
                ByteBuffer.class,
                List.class,
                Integer.class)
            .build();

    PartitionData partitionData =
        new PartitionData(StructType.of(required(999, "type", StringType.get())));
    Metrics metrics =
        new Metrics(1L, ImmutableMap.of(), ImmutableMap.of(), ImmutableMap.of(), ImmutableMap.of());

    return ctor.newInstance(
        1,
        "path",
        FileFormat.PARQUET,
        partitionData,
        1L,
        metrics,
        ByteBuffer.wrap(new byte[] {0}),
        null,
        1);
  }

  public static DeleteFile createDeleteFile() {
    Ctor<DeleteFile> ctor =
        DynConstructors.builder(DeleteFile.class)
            .hiddenImpl(
                "org.apache.iceberg.GenericDeleteFile",
                int.class,
                FileContent.class,
                String.class,
                FileFormat.class,
                PartitionData.class,
                long.class,
                Metrics.class,
                int[].class,
                Integer.class,
                List.class,
                ByteBuffer.class)
            .build();

    PartitionData partitionData =
        new PartitionData(StructType.of(required(999, "type", StringType.get())));
    Metrics metrics =
        new Metrics(1L, ImmutableMap.of(), ImmutableMap.of(), ImmutableMap.of(), ImmutableMap.of());

    return ctor.newInstance(
        1,
        FileContent.EQUALITY_DELETES,
        "path",
        FileFormat.PARQUET,
        partitionData,
        1L,
        metrics,
        new int[] {1},
        1,
        ImmutableList.of(1L),
        ByteBuffer.wrap(new byte[] {0}));
  }
}