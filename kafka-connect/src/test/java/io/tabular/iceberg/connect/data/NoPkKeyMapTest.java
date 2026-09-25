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

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

class NoPkKeyMapTest {
  private static final Schema ORIGINAL =
      new Schema(
          Types.NestedField.required(1, "a", Types.IntegerType.get()),
          Types.NestedField.optional(2, "b", Types.FloatType.get()),
          Types.NestedField.optional(3, "c", Types.BinaryType.get()));
  private static final Schema WIDENED =
      new Schema(
          Types.NestedField.required(1, "a", Types.LongType.get()),
          Types.NestedField.optional(2, "b", Types.DoubleType.get()),
          Types.NestedField.optional(3, "c", Types.BinaryType.get()));

  @Test
  void keepsPartialIndexesConsistentAcrossPromotionAndFurtherInserts() {
    CompactKeyMap map = CompactKeyMap.create(ORIGINAL, false);
    Record first = row(ORIGINAL, 10, 1.5F);
    map.put(first, "file", 0);
    map.put(first, "file", 1);
    map.put(row(ORIGINAL, 20, 2.5F), "file", 2);
    List<CompactKeyMap.PathOffset> removed = Lists.newArrayList();
    // Build the first index without removing anything.
    map.removeMatching(row(ORIGINAL, 99, 0F), Set.of(1), removed::add);
    assertThat(removed).isEmpty();
    map.updateSchema(WIDENED);
    // A second projection must match all duplicates after compatible numeric widening.
    map.removeMatching(row(WIDENED, 10L, 1.5D), Set.of(1, 2), removed::add);
    assertThat(removed).extracting(offset -> offset.position).containsExactlyInAnyOrder(0, 1);
    map.put(row(WIDENED, 10L, 4D), "file", 3);
    removed.clear();
    map.removeMatching(row(WIDENED, 10L, 9D), Set.of(1), removed::add);
    assertThat(removed).extracting(offset -> offset.position).containsExactly(3);
    CompactKeyMap.PathOffset remaining = map.remove(row(WIDENED, 20L, 2.5D));
    assertThat(remaining.position).isEqualTo(2);
    assertThat(map.getPath(remaining.pathIndex)).isEqualTo("file");
    assertThat(map.size()).isZero();
  }

  @Test
  void snapshotsMutableValuesBeforeIndexing() {
    CompactKeyMap map = CompactKeyMap.create(ORIGINAL, false);
    Record original = row(ORIGINAL, 10, 1.5F);
    ByteBuffer bytes = ByteBuffer.wrap(new byte[] {1, 2});
    original.setField("c", bytes);
    map.put(original, "file", 7);
    bytes.put(0, (byte) 9);
    original.setField("a", 99);
    Record expected = row(ORIGINAL, 10, 1.5F);
    expected.setField("c", ByteBuffer.wrap(new byte[] {1, 2}));
    List<CompactKeyMap.PathOffset> removed = Lists.newArrayList();
    map.removeMatching(expected, Set.of(1, 3), removed::add);
    assertThat(removed).extracting(offset -> offset.position).containsExactly(7);
    assertThat(map.size()).isZero();
  }

  @Test
  void nullableSingleColumnStillTracksDuplicateRowsIndividually() {
    Schema schema = new Schema(Types.NestedField.optional(1, "value", Types.IntegerType.get()));
    CompactKeyMap map = CompactKeyMap.create(schema, false);
    Record key = GenericRecord.create(schema);
    map.put(key, "first", 0);
    map.put(key, "second", 1);
    CompactKeyMap.PathOffset last = map.remove(key);
    assertThat(map.getPath(last.pathIndex)).isEqualTo("second");
    CompactKeyMap.PathOffset first = map.remove(key);
    assertThat(map.getPath(first.pathIndex)).isEqualTo("first");
    assertThat(map.remove(key)).isNull();
    assertThat(map.size()).isZero();
  }

  private Record row(Schema schema, Object valueA, Object valueB) {
    return GenericRecord.create(schema).copy(Map.of("a", valueA, "b", valueB));
  }
}
