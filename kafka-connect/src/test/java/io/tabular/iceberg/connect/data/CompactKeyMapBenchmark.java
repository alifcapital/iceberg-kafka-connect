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

import java.math.BigDecimal;
import java.util.Map;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.StructLikeMap;
import org.apache.iceberg.util.StructLikeUtil;
import org.apache.iceberg.util.StructProjection;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

/**
 * Benchmark to compare memory usage between StructLikeMap and CompactKeyMap.
 *
 * <p>Run with: ./gradlew :iceberg-kafka-connect:test --tests "CompactKeyMapBenchmark"
 * -Pbenchmark=true
 */
@EnabledIfSystemProperty(named = "benchmark", matches = "true")
public class CompactKeyMapBenchmark {

  private static final int NUM_ENTRIES = 10_000_000;

  // Schema for LONG key
  private static final Schema LONG_KEY_SCHEMA =
      new Schema(ImmutableList.of(Types.NestedField.required(1, "id", Types.LongType.get())));

  // Schema for STRING key
  private static final Schema STRING_KEY_SCHEMA =
      new Schema(ImmutableList.of(Types.NestedField.required(1, "id", Types.StringType.get())));

  // Schema for DECIMAL key
  private static final Schema DECIMAL_KEY_SCHEMA =
      new Schema(
          ImmutableList.of(Types.NestedField.required(1, "id", Types.DecimalType.of(18, 2))));

  // Schema for multi-column key
  private static final Schema MULTI_KEY_SCHEMA =
      new Schema(
          ImmutableList.of(
              Types.NestedField.required(1, "id1", Types.LongType.get()),
              Types.NestedField.required(2, "id2", Types.StringType.get())));

  @Test
  public void benchmarkLongKey() {
    System.out.println("\n=== LONG KEY BENCHMARK (" + NUM_ENTRIES + " entries) ===\n");

    // Warmup GC
    System.gc();
    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    // Benchmark StructLikeMap
    long beforeStructLike = getUsedMemory();
    Map<StructLike, PathOffset> structLikeMap = StructLikeMap.create(LONG_KEY_SCHEMA.asStruct());
    StructProjection projection = StructProjection.create(LONG_KEY_SCHEMA, LONG_KEY_SCHEMA);

    for (long i = 0; i < NUM_ENTRIES; i++) {
      Record record = GenericRecord.create(LONG_KEY_SCHEMA);
      record.setField("id", i);
      StructLike key = StructLikeUtil.copy(projection.wrap(record));
      structLikeMap.put(key, new PathOffset("/path/to/file.parquet", i));
    }

    System.gc();
    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    long afterStructLike = getUsedMemory();
    long structLikeMemory = afterStructLike - beforeStructLike;

    System.out.printf("StructLikeMap: %,d bytes (%,d MB)%n", structLikeMemory, structLikeMemory / 1024 / 1024);
    System.out.printf("  Per entry: %.1f bytes%n", (double) structLikeMemory / NUM_ENTRIES);

    // Clear and GC
    structLikeMap.clear();
    structLikeMap = null;
    System.gc();
    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    // Benchmark CompactKeyMap
    long beforeCompact = getUsedMemory();
    CompactKeyMap compactMap = CompactKeyMap.create(LONG_KEY_SCHEMA);

    for (int i = 0; i < NUM_ENTRIES; i++) {
      Record record = GenericRecord.create(LONG_KEY_SCHEMA);
      record.setField("id", (long) i);
      compactMap.put(record, "/path/to/file.parquet", i);
    }

    System.gc();
    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    long afterCompact = getUsedMemory();
    long compactMemory = afterCompact - beforeCompact;

    System.out.printf("CompactKeyMap: %,d bytes (%,d MB)%n", compactMemory, compactMemory / 1024 / 1024);
    System.out.printf("  Per entry: %.1f bytes%n", (double) compactMemory / NUM_ENTRIES);
    System.out.printf("%nSavings: %.1fx less memory%n", (double) structLikeMemory / compactMemory);
  }

  @Test
  public void benchmarkStringKey() {
    System.out.println("\n=== STRING KEY BENCHMARK (" + NUM_ENTRIES + " entries) ===\n");

    System.gc();
    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    // Benchmark StructLikeMap
    long beforeStructLike = getUsedMemory();
    Map<StructLike, PathOffset> structLikeMap = StructLikeMap.create(STRING_KEY_SCHEMA.asStruct());
    StructProjection projection = StructProjection.create(STRING_KEY_SCHEMA, STRING_KEY_SCHEMA);

    for (long i = 0; i < NUM_ENTRIES; i++) {
      Record record = GenericRecord.create(STRING_KEY_SCHEMA);
      record.setField("id", "key-" + i);
      StructLike key = StructLikeUtil.copy(projection.wrap(record));
      structLikeMap.put(key, new PathOffset("/path/to/file.parquet", i));
    }

    System.gc();
    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    long afterStructLike = getUsedMemory();
    long structLikeMemory = afterStructLike - beforeStructLike;

    System.out.printf("StructLikeMap: %,d bytes (%,d MB)%n", structLikeMemory, structLikeMemory / 1024 / 1024);
    System.out.printf("  Per entry: %.1f bytes%n", (double) structLikeMemory / NUM_ENTRIES);

    structLikeMap.clear();
    structLikeMap = null;
    System.gc();
    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    // Benchmark CompactKeyMap
    long beforeCompact = getUsedMemory();
    CompactKeyMap compactMap = CompactKeyMap.create(STRING_KEY_SCHEMA);

    for (int i = 0; i < NUM_ENTRIES; i++) {
      Record record = GenericRecord.create(STRING_KEY_SCHEMA);
      record.setField("id", "key-" + i);
      compactMap.put(record, "/path/to/file.parquet", i);
    }

    System.gc();
    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    long afterCompact = getUsedMemory();
    long compactMemory = afterCompact - beforeCompact;

    System.out.printf("CompactKeyMap: %,d bytes (%,d MB)%n", compactMemory, compactMemory / 1024 / 1024);
    System.out.printf("  Per entry: %.1f bytes%n", (double) compactMemory / NUM_ENTRIES);
    System.out.printf("%nSavings: %.1fx less memory%n", (double) structLikeMemory / compactMemory);
  }

  @Test
  public void benchmarkUuidKey() {
    System.out.println("\n=== UUID KEY BENCHMARK (" + NUM_ENTRIES + " entries) ===\n");

    System.gc();
    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    // Benchmark StructLikeMap
    long beforeStructLike = getUsedMemory();
    Map<StructLike, PathOffset> structLikeMap = StructLikeMap.create(STRING_KEY_SCHEMA.asStruct());
    StructProjection projection = StructProjection.create(STRING_KEY_SCHEMA, STRING_KEY_SCHEMA);

    for (int i = 0; i < NUM_ENTRIES; i++) {
      Record record = GenericRecord.create(STRING_KEY_SCHEMA);
      // Generate UUID-like string
      record.setField("id", String.format("%08x-%04x-%04x-%04x-%012x", i, i % 0xFFFF, i % 0xFFFF, i % 0xFFFF, (long) i));
      StructLike key = StructLikeUtil.copy(projection.wrap(record));
      structLikeMap.put(key, new PathOffset("/path/to/file.parquet", i));
    }

    System.gc();
    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    long afterStructLike = getUsedMemory();
    long structLikeMemory = afterStructLike - beforeStructLike;

    System.out.printf("StructLikeMap: %,d bytes (%,d MB)%n", structLikeMemory, structLikeMemory / 1024 / 1024);
    System.out.printf("  Per entry: %.1f bytes%n", (double) structLikeMemory / NUM_ENTRIES);

    structLikeMap.clear();
    structLikeMap = null;
    System.gc();
    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    // Benchmark CompactKeyMap (should auto-detect UUID)
    long beforeCompact = getUsedMemory();
    CompactKeyMap compactMap = CompactKeyMap.create(STRING_KEY_SCHEMA);

    for (int i = 0; i < NUM_ENTRIES; i++) {
      Record record = GenericRecord.create(STRING_KEY_SCHEMA);
      record.setField("id", String.format("%08x-%04x-%04x-%04x-%012x", i, i % 0xFFFF, i % 0xFFFF, i % 0xFFFF, (long) i));
      compactMap.put(record, "/path/to/file.parquet", i);
    }

    System.gc();
    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    long afterCompact = getUsedMemory();
    long compactMemory = afterCompact - beforeCompact;

    System.out.printf("CompactKeyMap: %,d bytes (%,d MB)%n", compactMemory, compactMemory / 1024 / 1024);
    System.out.printf("  Per entry: %.1f bytes%n", (double) compactMemory / NUM_ENTRIES);
    System.out.printf("%nSavings: %.1fx less memory%n", (double) structLikeMemory / compactMemory);
  }

  @Test
  public void benchmarkDecimalKey() {
    System.out.println("\n=== DECIMAL KEY BENCHMARK (" + NUM_ENTRIES + " entries) ===\n");

    System.gc();
    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    // Benchmark StructLikeMap
    long beforeStructLike = getUsedMemory();
    Map<StructLike, PathOffset> structLikeMap = StructLikeMap.create(DECIMAL_KEY_SCHEMA.asStruct());
    StructProjection projection = StructProjection.create(DECIMAL_KEY_SCHEMA, DECIMAL_KEY_SCHEMA);

    for (long i = 0; i < NUM_ENTRIES; i++) {
      Record record = GenericRecord.create(DECIMAL_KEY_SCHEMA);
      record.setField("id", BigDecimal.valueOf(i, 2));
      StructLike key = StructLikeUtil.copy(projection.wrap(record));
      structLikeMap.put(key, new PathOffset("/path/to/file.parquet", i));
    }

    System.gc();
    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    long afterStructLike = getUsedMemory();
    long structLikeMemory = afterStructLike - beforeStructLike;

    System.out.printf("StructLikeMap: %,d bytes (%,d MB)%n", structLikeMemory, structLikeMemory / 1024 / 1024);
    System.out.printf("  Per entry: %.1f bytes%n", (double) structLikeMemory / NUM_ENTRIES);

    structLikeMap.clear();
    structLikeMap = null;
    System.gc();
    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    // Benchmark CompactKeyMap
    long beforeCompact = getUsedMemory();
    CompactKeyMap compactMap = CompactKeyMap.create(DECIMAL_KEY_SCHEMA);

    for (int i = 0; i < NUM_ENTRIES; i++) {
      Record record = GenericRecord.create(DECIMAL_KEY_SCHEMA);
      record.setField("id", BigDecimal.valueOf(i, 2));
      compactMap.put(record, "/path/to/file.parquet", i);
    }

    System.gc();
    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    long afterCompact = getUsedMemory();
    long compactMemory = afterCompact - beforeCompact;

    System.out.printf("CompactKeyMap: %,d bytes (%,d MB)%n", compactMemory, compactMemory / 1024 / 1024);
    System.out.printf("  Per entry: %.1f bytes%n", (double) compactMemory / NUM_ENTRIES);
    System.out.printf("%nSavings: %.1fx less memory%n", (double) structLikeMemory / compactMemory);
  }

  @Test
  public void benchmarkMultiColumnKey() {
    System.out.println("\n=== MULTI-COLUMN KEY BENCHMARK (" + NUM_ENTRIES + " entries) ===\n");

    System.gc();
    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    // Benchmark StructLikeMap
    long beforeStructLike = getUsedMemory();
    Map<StructLike, PathOffset> structLikeMap = StructLikeMap.create(MULTI_KEY_SCHEMA.asStruct());
    StructProjection projection = StructProjection.create(MULTI_KEY_SCHEMA, MULTI_KEY_SCHEMA);

    for (long i = 0; i < NUM_ENTRIES; i++) {
      Record record = GenericRecord.create(MULTI_KEY_SCHEMA);
      record.setField("id1", i);
      record.setField("id2", "val-" + (i % 1000));
      StructLike key = StructLikeUtil.copy(projection.wrap(record));
      structLikeMap.put(key, new PathOffset("/path/to/file.parquet", i));
    }

    System.gc();
    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    long afterStructLike = getUsedMemory();
    long structLikeMemory = afterStructLike - beforeStructLike;

    System.out.printf("StructLikeMap: %,d bytes (%,d MB)%n", structLikeMemory, structLikeMemory / 1024 / 1024);
    System.out.printf("  Per entry: %.1f bytes%n", (double) structLikeMemory / NUM_ENTRIES);

    structLikeMap.clear();
    structLikeMap = null;
    System.gc();
    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    // Benchmark CompactKeyMap
    long beforeCompact = getUsedMemory();
    CompactKeyMap compactMap = CompactKeyMap.create(MULTI_KEY_SCHEMA);

    for (int i = 0; i < NUM_ENTRIES; i++) {
      Record record = GenericRecord.create(MULTI_KEY_SCHEMA);
      record.setField("id1", (long) i);
      record.setField("id2", "val-" + (i % 1000));
      compactMap.put(record, "/path/to/file.parquet", i);
    }

    System.gc();
    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    long afterCompact = getUsedMemory();
    long compactMemory = afterCompact - beforeCompact;

    System.out.printf("CompactKeyMap: %,d bytes (%,d MB)%n", compactMemory, compactMemory / 1024 / 1024);
    System.out.printf("  Per entry: %.1f bytes%n", (double) compactMemory / NUM_ENTRIES);
    System.out.printf("%nSavings: %.1fx less memory%n", (double) structLikeMemory / compactMemory);
  }

  private long getUsedMemory() {
    Runtime runtime = Runtime.getRuntime();
    return runtime.totalMemory() - runtime.freeMemory();
  }

  // Simple PathOffset for StructLikeMap benchmark
  private static class PathOffset {
    final String path;
    final long offset;

    PathOffset(String path, long offset) {
      this.path = path;
      this.offset = offset;
    }
  }
}
