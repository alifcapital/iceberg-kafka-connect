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

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

/** Correctness tests for CompactKeyMap implementations. */
public class CompactKeyMapTest {

  private static final Schema LONG_KEY_SCHEMA =
      new Schema(ImmutableList.of(Types.NestedField.required(1, "id", Types.LongType.get())));

  private static final Schema STRING_KEY_SCHEMA =
      new Schema(ImmutableList.of(Types.NestedField.required(1, "id", Types.StringType.get())));

  private static final Schema DECIMAL_KEY_SCHEMA =
      new Schema(
          ImmutableList.of(Types.NestedField.required(1, "id", Types.DecimalType.of(18, 2))));

  private static final Schema MULTI_KEY_SCHEMA =
      new Schema(
          ImmutableList.of(
              Types.NestedField.required(1, "id1", Types.LongType.get()),
              Types.NestedField.required(2, "id2", Types.StringType.get())));

  @Test
  public void testLongKeyPutGetRemove() {
    CompactKeyMap map = CompactKeyMap.create(LONG_KEY_SCHEMA);

    Record key1 = GenericRecord.create(LONG_KEY_SCHEMA);
    key1.setField("id", 100L);

    Record key2 = GenericRecord.create(LONG_KEY_SCHEMA);
    key2.setField("id", 200L);

    // Put first key
    CompactKeyMap.PathOffset result = map.put(key1, "/path/file1.parquet", 10);
    assertThat(result).isNull();
    assertThat(map.size()).isEqualTo(1);

    // Put second key
    result = map.put(key2, "/path/file2.parquet", 20);
    assertThat(result).isNull();
    assertThat(map.size()).isEqualTo(2);

    // Update first key - should return old value
    result = map.put(key1, "/path/file3.parquet", 30);
    assertThat(result).isNotNull();
    assertThat(result.position).isEqualTo(10);
    assertThat(map.getPath(result.pathIndex)).isEqualTo("/path/file1.parquet");
    assertThat(map.size()).isEqualTo(2);

    // Remove first key
    result = map.remove(key1);
    assertThat(result).isNotNull();
    assertThat(result.position).isEqualTo(30);
    assertThat(map.size()).isEqualTo(1);

    // Remove non-existent key
    result = map.remove(key1);
    assertThat(result).isNull();

    // Clear
    map.clear();
    assertThat(map.size()).isEqualTo(0);
  }

  @Test
  public void testStringKeyPutGetRemove() {
    CompactKeyMap map = CompactKeyMap.create(STRING_KEY_SCHEMA);

    Record key1 = GenericRecord.create(STRING_KEY_SCHEMA);
    key1.setField("id", "abc-123");

    Record key2 = GenericRecord.create(STRING_KEY_SCHEMA);
    key2.setField("id", "xyz-456");

    // Put and verify
    assertThat(map.put(key1, "/path/file.parquet", 5)).isNull();
    assertThat(map.put(key2, "/path/file.parquet", 15)).isNull();
    assertThat(map.size()).isEqualTo(2);

    // Update
    CompactKeyMap.PathOffset old = map.put(key1, "/path/file.parquet", 25);
    assertThat(old).isNotNull();
    assertThat(old.position).isEqualTo(5);

    // Remove
    old = map.remove(key2);
    assertThat(old).isNotNull();
    assertThat(old.position).isEqualTo(15);
    assertThat(map.size()).isEqualTo(1);
  }

  @Test
  public void testUuidKeyDetection() {
    CompactKeyMap map = CompactKeyMap.create(STRING_KEY_SCHEMA);

    // UUID format should trigger UuidKeyMap internally
    Record key1 = GenericRecord.create(STRING_KEY_SCHEMA);
    key1.setField("id", "550e8400-e29b-41d4-a716-446655440000");

    Record key2 = GenericRecord.create(STRING_KEY_SCHEMA);
    key2.setField("id", "550e8400-e29b-41d4-a716-446655440001");

    assertThat(map.put(key1, "/path/file.parquet", 100)).isNull();
    assertThat(map.put(key2, "/path/file.parquet", 200)).isNull();
    assertThat(map.size()).isEqualTo(2);

    // Update
    CompactKeyMap.PathOffset old = map.put(key1, "/path/file.parquet", 300);
    assertThat(old).isNotNull();
    assertThat(old.position).isEqualTo(100);

    // Remove
    old = map.remove(key1);
    assertThat(old).isNotNull();
    assertThat(old.position).isEqualTo(300);
    assertThat(map.size()).isEqualTo(1);
  }

  @Test
  public void testUuidWithoutDashes() {
    CompactKeyMap map = CompactKeyMap.create(STRING_KEY_SCHEMA);

    Record key1 = GenericRecord.create(STRING_KEY_SCHEMA);
    key1.setField("id", "550e8400e29b41d4a716446655440000");

    assertThat(map.put(key1, "/path/file.parquet", 50)).isNull();
    assertThat(map.size()).isEqualTo(1);

    CompactKeyMap.PathOffset old = map.put(key1, "/path/file.parquet", 60);
    assertThat(old).isNotNull();
    assertThat(old.position).isEqualTo(50);
  }

  @Test
  public void testDecimalKeyPutGetRemove() {
    CompactKeyMap map = CompactKeyMap.create(DECIMAL_KEY_SCHEMA);

    Record key1 = GenericRecord.create(DECIMAL_KEY_SCHEMA);
    key1.setField("id", new BigDecimal("12345.67"));

    Record key2 = GenericRecord.create(DECIMAL_KEY_SCHEMA);
    key2.setField("id", new BigDecimal("98765.43"));

    assertThat(map.put(key1, "/path/file.parquet", 1)).isNull();
    assertThat(map.put(key2, "/path/file.parquet", 2)).isNull();
    assertThat(map.size()).isEqualTo(2);

    CompactKeyMap.PathOffset old = map.put(key1, "/path/file.parquet", 3);
    assertThat(old).isNotNull();
    assertThat(old.position).isEqualTo(1);

    old = map.remove(key1);
    assertThat(old).isNotNull();
    assertThat(old.position).isEqualTo(3);
    assertThat(map.size()).isEqualTo(1);
  }

  @Test
  public void testMultiColumnKeyPutGetRemove() {
    CompactKeyMap map = CompactKeyMap.create(MULTI_KEY_SCHEMA);

    Record key1 = GenericRecord.create(MULTI_KEY_SCHEMA);
    key1.setField("id1", 100L);
    key1.setField("id2", "alpha");

    Record key2 = GenericRecord.create(MULTI_KEY_SCHEMA);
    key2.setField("id1", 100L);
    key2.setField("id2", "beta");

    Record key3 = GenericRecord.create(MULTI_KEY_SCHEMA);
    key3.setField("id1", 200L);
    key3.setField("id2", "alpha");

    assertThat(map.put(key1, "/path/file.parquet", 10)).isNull();
    assertThat(map.put(key2, "/path/file.parquet", 20)).isNull();
    assertThat(map.put(key3, "/path/file.parquet", 30)).isNull();
    assertThat(map.size()).isEqualTo(3);

    // Update key1
    CompactKeyMap.PathOffset old = map.put(key1, "/path/file.parquet", 40);
    assertThat(old).isNotNull();
    assertThat(old.position).isEqualTo(10);

    // Remove key2
    old = map.remove(key2);
    assertThat(old).isNotNull();
    assertThat(old.position).isEqualTo(20);
    assertThat(map.size()).isEqualTo(2);
  }

  @Test
  public void testPathInterning() {
    CompactKeyMap map = CompactKeyMap.create(LONG_KEY_SCHEMA);

    String path = "/path/to/data/file.parquet";

    for (int i = 0; i < 100; i++) {
      Record key = GenericRecord.create(LONG_KEY_SCHEMA);
      key.setField("id", (long) i);
      map.put(key, path, i);
    }

    // All 100 entries should reference the same path index
    Record key0 = GenericRecord.create(LONG_KEY_SCHEMA);
    key0.setField("id", 0L);
    CompactKeyMap.PathOffset result = map.put(key0, path, 999);
    assertThat(result).isNotNull();
    assertThat(map.getPath(result.pathIndex)).isEqualTo(path);
  }

  @Test
  public void testLargeNumberOfEntries() {
    CompactKeyMap map = CompactKeyMap.create(LONG_KEY_SCHEMA);

    int numEntries = 10_000;
    for (int i = 0; i < numEntries; i++) {
      Record key = GenericRecord.create(LONG_KEY_SCHEMA);
      key.setField("id", (long) i);
      map.put(key, "/path/file.parquet", i);
    }

    assertThat(map.size()).isEqualTo(numEntries);

    // Verify some random lookups by updating
    for (int i = 0; i < 100; i++) {
      int idx = i * 100;
      Record key = GenericRecord.create(LONG_KEY_SCHEMA);
      key.setField("id", (long) idx);
      CompactKeyMap.PathOffset old = map.put(key, "/path/file.parquet", idx + 1000000);
      assertThat(old).isNotNull();
      assertThat(old.position).isEqualTo(idx);
    }

    // Remove half
    for (int i = 0; i < numEntries / 2; i++) {
      Record key = GenericRecord.create(LONG_KEY_SCHEMA);
      key.setField("id", (long) i);
      map.remove(key);
    }

    assertThat(map.size()).isEqualTo(numEntries / 2);
  }

  @Test
  public void testIntegerKeyCoercion() {
    CompactKeyMap map = CompactKeyMap.create(LONG_KEY_SCHEMA);

    // Test that Integer values are properly coerced to Long
    Record key = GenericRecord.create(LONG_KEY_SCHEMA);
    key.setField("id", 42); // Integer, not Long

    assertThat(map.put(key, "/path/file.parquet", 1)).isNull();

    // Lookup with same value as Long
    Record keyLong = GenericRecord.create(LONG_KEY_SCHEMA);
    keyLong.setField("id", 42L);

    CompactKeyMap.PathOffset old = map.put(keyLong, "/path/file.parquet", 2);
    assertThat(old).isNotNull();
    assertThat(old.position).isEqualTo(1);
  }
}
