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

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;

/** Pending no-PK rows, with lazily built indexes for deletes after a source column DROP. */
class NoPkKeyMap extends CompactKeyMap {
  private final List<Integer> fieldIds;
  private Schema schema;
  private final Map<CompactKey, List<Long>> positions = Maps.newHashMap();
  private final Map<Set<Integer>, PartialIndex> partialIndexes = Maps.newHashMap();
  private int size;

  NoPkKeyMap(Schema schema) {
    this.fieldIds = primitiveIds(schema);
    updateSchema(schema);
  }

  private static List<Integer> primitiveIds(Schema schema) {
    return TypeUtil.getProjectedIds(schema).stream()
        .filter(id -> schema.findType(id).isPrimitiveType())
        .sorted()
        .collect(Collectors.toList());
  }

  @Override
  public void updateSchema(Schema newSchema) {
    if (!fieldIds.equals(primitiveIds(newSchema))
        || (this.schema != null && !sameFieldOrder(this.schema.asStruct(), newSchema.asStruct()))) {
      throw new IllegalArgumentException("Cannot change pending no-PK equality fields");
    }
    this.schema = newSchema;
  }

  private static boolean sameFieldOrder(Types.StructType left, Types.StructType right) {
    if (left.fields().size() != right.fields().size()) {
      return false;
    }
    for (int i = 0; i < left.fields().size(); i++) {
      Types.NestedField oldField = left.fields().get(i);
      Types.NestedField newField = right.fields().get(i);
      if (oldField.fieldId() != newField.fieldId()
          || (oldField.type().isStructType()
              && (!newField.type().isStructType()
                  || !sameFieldOrder(
                      oldField.type().asStructType(), newField.type().asStructType())))) {
        return false;
      }
    }
    return true;
  }

  @Override
  public PathOffset put(Record key, String path, int position) {
    CompactKey fullKey = key(key);
    List<Long> offsets = positions.get(fullKey);
    if (offsets == null) {
      offsets = Lists.newArrayList();
      positions.put(fullKey, offsets);
      partialIndexes.values().forEach(index -> index.add(fullKey));
    }
    offsets.add(pack(internPath(path), position));
    size++;
    return null;
  }

  @Override
  public PathOffset remove(Record key) {
    CompactKey fullKey = key(key);
    List<Long> offsets = positions.get(fullKey);
    if (offsets == null) {
      return null;
    }
    long position = offsets.remove(offsets.size() - 1);
    size--;
    if (offsets.isEmpty()) {
      positions.remove(fullKey);
      partialIndexes.values().forEach(index -> index.remove(fullKey));
    }
    return unpack(position);
  }

  @Override
  void removeMatching(Record key, Set<Integer> ids, Consumer<PathOffset> removed) {
    PartialIndex index = partialIndexes.get(ids);
    if (index == null) {
      if (partialIndexes.size() >= 64) {
        partialIndexes.clear();
      }
      index = new PartialIndex(ids);
      positions.keySet().forEach(index::add);
      partialIndexes.put(Set.copyOf(ids), index);
    }
    Set<CompactKey> matches = index.keys.remove(index.project(key(key)));
    if (matches == null) {
      return;
    }
    for (CompactKey match : matches) {
      List<Long> offsets = positions.remove(match);
      partialIndexes.values().forEach(other -> other.remove(match));
      size -= offsets.size();
      offsets.forEach(offset -> removed.accept(unpack(offset)));
    }
  }

  private static CompactKey key(StructLike record) {
    Object[] values = new Object[record.size()];
    for (int i = 0; i < values.length; i++) {
      values[i] = normalize(record.get(i, Object.class));
    }
    return new CompactKey(values);
  }

  private static Object normalize(Object value) {
    if (value instanceof StructLike) {
      return key((StructLike) value);
    }
    if (value instanceof Integer) {
      return ((Integer) value).longValue();
    }
    if (value instanceof Float) {
      return ((Float) value).doubleValue();
    }
    if (value instanceof CharSequence) {
      return value.toString();
    }
    if (value instanceof byte[]) {
      return ByteBuffer.wrap(((byte[]) value).clone());
    }
    if (value instanceof ByteBuffer) {
      ByteBuffer buffer = ((ByteBuffer) value).duplicate();
      byte[] copy = new byte[buffer.remaining()];
      buffer.get(copy);
      return ByteBuffer.wrap(copy);
    }
    return value;
  }

  @Override
  public int size() {
    return size;
  }

  @Override
  public void clear() {
    positions.clear();
    partialIndexes.clear();
    paths.clear();
    pathToIndex.clear();
    size = 0;
  }

  // Preserve struct presence as well as leaf values, matching Iceberg's equality projection.
  private static class KeyProjection {
    private final int[] positions;
    private final KeyProjection[] nested;

    KeyProjection(Types.StructType source, Types.StructType target) {
      positions = new int[target.fields().size()];
      nested = new KeyProjection[positions.length];
      for (int i = 0; i < positions.length; i++) {
        Types.NestedField field = target.fields().get(i);
        int position = source.fields().indexOf(source.field(field.fieldId()));
        positions[i] = position;
        if (field.type().isStructType()) {
          nested[i] =
              new KeyProjection(
                  source.fields().get(position).type().asStructType(), field.type().asStructType());
        }
      }
    }

    CompactKey project(CompactKey fullKey) {
      Object[] values = new Object[positions.length];
      for (int i = 0; i < positions.length; i++) {
        Object value = fullKey.values[positions[i]];
        values[i] =
            nested[i] != null && value != null ? nested[i].project((CompactKey) value) : value;
      }
      return new CompactKey(values);
    }
  }

  private class PartialIndex {
    private final KeyProjection projection;
    private final Map<CompactKey, Set<CompactKey>> keys = Maps.newHashMap();

    PartialIndex(Set<Integer> ids) {
      if (!fieldIds.containsAll(ids)) {
        throw new IllegalArgumentException("Unknown equality field in " + ids);
      }
      projection = new KeyProjection(schema.asStruct(), TypeUtil.select(schema, ids).asStruct());
    }

    CompactKey project(CompactKey fullKey) {
      return projection.project(fullKey);
    }

    void add(CompactKey fullKey) {
      keys.computeIfAbsent(project(fullKey), ignored -> Sets.newHashSet()).add(fullKey);
    }

    void remove(CompactKey fullKey) {
      CompactKey partial = project(fullKey);
      Set<CompactKey> matches = keys.get(partial);
      if (matches != null) {
        matches.remove(fullKey);
        if (matches.isEmpty()) {
          keys.remove(partial);
        }
      }
    }
  }
}
