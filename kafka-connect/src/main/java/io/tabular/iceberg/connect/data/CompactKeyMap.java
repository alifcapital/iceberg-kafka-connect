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
import java.util.Arrays;
import java.util.List;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

/**
 * Ultra-compact key-to-position map optimized for CDC workloads.
 *
 * <p>Storage format:
 * <ul>
 *   <li>Path → int index via dictionary (interning)
 *   <li>Position → int (supports up to 2B rows per file)
 *   <li>Value = (pathIndex << 32) | position packed into single long
 * </ul>
 *
 * <p>Memory per entry:
 * <ul>
 *   <li>LONG key: ~20 bytes (open addressing: long key + long value)
 *   <li>STRING key: ~28 bytes (open addressing: String ref + long value + overhead)
 *   <li>DECIMAL key: ~36 bytes (open addressing: BigDecimal ref + long value + overhead)
 * </ul>
 */
public abstract class CompactKeyMap {

  /** Returned position info. Created on-demand, not stored. */
  public static class PathOffset {
    public final int pathIndex;
    public final int position;

    PathOffset(int pathIndex, int position) {
      this.pathIndex = pathIndex;
      this.position = position;
    }
  }

  // Interned file paths - each unique path stored once
  protected final List<String> paths = Lists.newArrayList();
  protected final java.util.Map<String, Integer> pathToIndex = Maps.newHashMap();

  protected int internPath(String path) {
    Integer idx = pathToIndex.get(path);
    if (idx != null) {
      return idx;
    }
    int newIdx = paths.size();
    paths.add(path);
    pathToIndex.put(path, newIdx);
    return newIdx;
  }

  public String getPath(int index) {
    return paths.get(index);
  }

  /** Pack pathIndex and position into single long. */
  protected static long pack(int pathIndex, int position) {
    return ((long) pathIndex << 32) | (position & 0xFFFFFFFFL);
  }

  /** Unpack to PathOffset. */
  protected static PathOffset unpack(long packed) {
    int pathIndex = (int) (packed >>> 32);
    int position = (int) packed;
    return new PathOffset(pathIndex, position);
  }

  /** Creates a CompactKeyMap optimized for the given equality delete schema. */
  public static CompactKeyMap create(Schema deleteSchema) {
    List<Types.NestedField> columns = deleteSchema.columns();

    if (columns.size() == 1) {
      Type.TypeID typeId = columns.get(0).type().typeId();
      switch (typeId) {
        case INTEGER:
        case LONG:
          return new LongKeyMap();
        case STRING:
          return new LazyStringKeyMap();
        case DECIMAL:
          return new DecimalKeyMap();
        default:
          return new MultiColumnKeyMap(columns.size());
      }
    }

    return new MultiColumnKeyMap(columns.size());
  }

  public abstract PathOffset put(Record key, String path, int position);

  public abstract PathOffset remove(Record key);

  public abstract int size();

  public abstract void clear();

  // ==================== LONG key: primitive open addressing ====================

  static class LongKeyMap extends CompactKeyMap {
    private static final long EMPTY_KEY = Long.MIN_VALUE;
    private static final long TOMBSTONE = Long.MIN_VALUE + 1;
    private static final int INITIAL_CAPACITY = 1024;
    private static final float LOAD_FACTOR = 0.5f;

    private long[] keys;
    private long[] values; // packed (pathIndex << 32) | position
    private int size;
    private int threshold;

    LongKeyMap() {
      keys = new long[INITIAL_CAPACITY];
      values = new long[INITIAL_CAPACITY];
      Arrays.fill(keys, EMPTY_KEY);
      threshold = (int) (INITIAL_CAPACITY * LOAD_FACTOR);
    }

    @Override
    public PathOffset put(Record key, String path, int position) {
      Object val = key.get(0, Object.class);
      long k = val instanceof Integer ? ((Integer) val).longValue() : (Long) val;
      return putInternal(k, internPath(path), position);
    }

    private PathOffset putInternal(long k, int pathIndex, int position) {
      if (size >= threshold) {
        resize();
      }

      int mask = keys.length - 1;
      int idx = hash(k) & mask;

      while (true) {
        long existing = keys[idx];
        if (existing == EMPTY_KEY || existing == TOMBSTONE) {
          keys[idx] = k;
          values[idx] = pack(pathIndex, position);
          size++;
          return null;
        }
        if (existing == k) {
          PathOffset old = unpack(values[idx]);
          values[idx] = pack(pathIndex, position);
          return old;
        }
        idx = (idx + 1) & mask;
      }
    }

    @Override
    public PathOffset remove(Record key) {
      Object val = key.get(0, Object.class);
      long k = val instanceof Integer ? ((Integer) val).longValue() : (Long) val;

      int mask = keys.length - 1;
      int idx = hash(k) & mask;

      while (true) {
        long existing = keys[idx];
        if (existing == EMPTY_KEY) {
          return null;
        }
        if (existing == k) {
          PathOffset old = unpack(values[idx]);
          keys[idx] = TOMBSTONE;
          size--;
          return old;
        }
        idx = (idx + 1) & mask;
      }
    }

    private int hash(long k) {
      // FNV-1a inspired mixing
      long h = k * 0x9E3779B97F4A7C15L;
      return (int) (h ^ (h >>> 32));
    }

    private void resize() {
      int newCapacity = keys.length * 2;
      long[] oldKeys = keys;
      long[] oldValues = values;

      keys = new long[newCapacity];
      values = new long[newCapacity];
      Arrays.fill(keys, EMPTY_KEY);
      threshold = (int) (newCapacity * LOAD_FACTOR);
      size = 0;

      for (int i = 0; i < oldKeys.length; i++) {
        long k = oldKeys[i];
        if (k != EMPTY_KEY && k != TOMBSTONE) {
          long v = oldValues[i];
          putInternal(k, (int) (v >>> 32), (int) v);
        }
      }
    }

    @Override
    public int size() {
      return size;
    }

    @Override
    public void clear() {
      Arrays.fill(keys, EMPTY_KEY);
      size = 0;
      paths.clear();
      pathToIndex.clear();
    }
  }

  // ==================== STRING key: lazy detection (UUID vs generic) ====================

  /**
   * Lazy string key map that detects UUID format on first put and delegates to optimal impl.
   */
  static class LazyStringKeyMap extends CompactKeyMap {
    private CompactKeyMap delegate;

    @Override
    public PathOffset put(Record key, String path, int position) {
      String k = key.get(0, Object.class).toString();
      if (delegate == null) {
        delegate = isUuid(k) ? new UuidKeyMap() : new StringKeyMap();
        // Copy path interning to delegate
        delegate.paths.addAll(this.paths);
        delegate.pathToIndex.putAll(this.pathToIndex);
      }
      return delegate.put(key, path, position);
    }

    @Override
    public PathOffset remove(Record key) {
      if (delegate == null) {
        return null;
      }
      return delegate.remove(key);
    }

    @Override
    public int size() {
      return delegate == null ? 0 : delegate.size();
    }

    @Override
    public void clear() {
      if (delegate != null) {
        delegate.clear();
        delegate = null;
      }
      paths.clear();
      pathToIndex.clear();
    }

    @Override
    public String getPath(int index) {
      return delegate != null ? delegate.getPath(index) : super.getPath(index);
    }

    private static boolean isUuid(String s) {
      // UUID format: 8-4-4-4-12 = 36 chars with dashes, or 32 without
      if (s == null) return false;
      int len = s.length();
      if (len == 36) {
        // With dashes: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
        return s.charAt(8) == '-' && s.charAt(13) == '-'
            && s.charAt(18) == '-' && s.charAt(23) == '-'
            && isHexString(s, 0, 8) && isHexString(s, 9, 13)
            && isHexString(s, 14, 18) && isHexString(s, 19, 23)
            && isHexString(s, 24, 36);
      } else if (len == 32) {
        // Without dashes
        return isHexString(s, 0, 32);
      }
      return false;
    }

    private static boolean isHexString(String s, int start, int end) {
      for (int i = start; i < end; i++) {
        char c = s.charAt(i);
        if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F'))) {
          return false;
        }
      }
      return true;
    }
  }

  // ==================== UUID key: two longs ====================

  /**
   * UUID stored as two longs. ~24 bytes per entry vs ~83 bytes for String.
   */
  static class UuidKeyMap extends CompactKeyMap {
    private static final int INITIAL_CAPACITY = 1024;
    private static final float LOAD_FACTOR = 0.5f;
    private static final long EMPTY = Long.MIN_VALUE;
    private static final long TOMBSTONE_MARKER = Long.MIN_VALUE + 1;

    private long[] highBits;
    private long[] lowBits;
    private long[] values;
    private int size;
    private int threshold;

    UuidKeyMap() {
      highBits = new long[INITIAL_CAPACITY];
      lowBits = new long[INITIAL_CAPACITY];
      values = new long[INITIAL_CAPACITY];
      Arrays.fill(highBits, EMPTY);
      threshold = (int) (INITIAL_CAPACITY * LOAD_FACTOR);
    }

    @Override
    public PathOffset put(Record key, String path, int position) {
      String k = key.get(0, Object.class).toString();
      long[] uuid = parseUuid(k);
      return putInternal(uuid[0], uuid[1], internPath(path), position);
    }

    private PathOffset putInternal(long high, long low, int pathIndex, int position) {
      if (size >= threshold) {
        resize();
      }

      int mask = highBits.length - 1;
      int idx = hash(high, low) & mask;

      while (true) {
        long existingHigh = highBits[idx];
        if (existingHigh == EMPTY || existingHigh == TOMBSTONE_MARKER) {
          highBits[idx] = high;
          lowBits[idx] = low;
          values[idx] = pack(pathIndex, position);
          size++;
          return null;
        }
        if (existingHigh == high && lowBits[idx] == low) {
          PathOffset old = unpack(values[idx]);
          values[idx] = pack(pathIndex, position);
          return old;
        }
        idx = (idx + 1) & mask;
      }
    }

    @Override
    public PathOffset remove(Record key) {
      String k = key.get(0, Object.class).toString();
      long[] uuid = parseUuid(k);
      long high = uuid[0], low = uuid[1];

      int mask = highBits.length - 1;
      int idx = hash(high, low) & mask;

      while (true) {
        long existingHigh = highBits[idx];
        if (existingHigh == EMPTY) {
          return null;
        }
        if (existingHigh == high && lowBits[idx] == low) {
          PathOffset old = unpack(values[idx]);
          highBits[idx] = TOMBSTONE_MARKER;
          size--;
          return old;
        }
        idx = (idx + 1) & mask;
      }
    }

    private int hash(long high, long low) {
      long h = high * 31 + low;
      h = h * 0x9E3779B97F4A7C15L;
      return (int) (h ^ (h >>> 32));
    }

    private void resize() {
      int newCapacity = highBits.length * 2;
      long[] oldHigh = highBits;
      long[] oldLow = lowBits;
      long[] oldValues = values;

      highBits = new long[newCapacity];
      lowBits = new long[newCapacity];
      values = new long[newCapacity];
      Arrays.fill(highBits, EMPTY);
      threshold = (int) (newCapacity * LOAD_FACTOR);
      size = 0;

      for (int i = 0; i < oldHigh.length; i++) {
        long high = oldHigh[i];
        if (high != EMPTY && high != TOMBSTONE_MARKER) {
          long v = oldValues[i];
          putInternal(high, oldLow[i], (int) (v >>> 32), (int) v);
        }
      }
    }

    /** Parse UUID string to two longs (high, low). */
    private static long[] parseUuid(String s) {
      long high, low;
      if (s.length() == 36) {
        // With dashes: 8-4-4-4-12
        high = (parseHex(s, 0, 8) << 32) | (parseHex(s, 9, 13) << 16) | parseHex(s, 14, 18);
        low = (parseHex(s, 19, 23) << 48) | parseHex(s, 24, 36);
      } else {
        // Without dashes: 32 chars
        high = parseHex(s, 0, 16);
        low = parseHex(s, 16, 32);
      }
      return new long[] {high, low};
    }

    private static long parseHex(String s, int start, int end) {
      long result = 0;
      for (int i = start; i < end; i++) {
        char c = s.charAt(i);
        int digit;
        if (c >= '0' && c <= '9') {
          digit = c - '0';
        } else if (c >= 'a' && c <= 'f') {
          digit = c - 'a' + 10;
        } else if (c >= 'A' && c <= 'F') {
          digit = c - 'A' + 10;
        } else {
          continue; // skip dashes
        }
        result = (result << 4) | digit;
      }
      return result;
    }

    @Override
    public int size() {
      return size;
    }

    @Override
    public void clear() {
      Arrays.fill(highBits, EMPTY);
      size = 0;
      paths.clear();
      pathToIndex.clear();
    }
  }

  // ==================== Generic STRING key: open addressing ====================

  static class StringKeyMap extends CompactKeyMap {
    private static final int INITIAL_CAPACITY = 1024;
    private static final float LOAD_FACTOR = 0.5f;
    private static final String TOMBSTONE = new String(""); // unique instance

    private String[] keys;
    private long[] values;
    private int size;
    private int threshold;

    StringKeyMap() {
      keys = new String[INITIAL_CAPACITY];
      values = new long[INITIAL_CAPACITY];
      threshold = (int) (INITIAL_CAPACITY * LOAD_FACTOR);
    }

    @Override
    public PathOffset put(Record key, String path, int position) {
      String k = key.get(0, Object.class).toString();
      return putInternal(k, internPath(path), position);
    }

    private PathOffset putInternal(String k, int pathIndex, int position) {
      if (size >= threshold) {
        resize();
      }

      int mask = keys.length - 1;
      int idx = hash(k) & mask;

      while (true) {
        String existing = keys[idx];
        if (existing == null || existing == TOMBSTONE) {
          keys[idx] = k;
          values[idx] = pack(pathIndex, position);
          size++;
          return null;
        }
        if (existing.equals(k)) {
          PathOffset old = unpack(values[idx]);
          values[idx] = pack(pathIndex, position);
          return old;
        }
        idx = (idx + 1) & mask;
      }
    }

    @Override
    public PathOffset remove(Record key) {
      String k = key.get(0, Object.class).toString();

      int mask = keys.length - 1;
      int idx = hash(k) & mask;

      while (true) {
        String existing = keys[idx];
        if (existing == null) {
          return null;
        }
        if (existing != TOMBSTONE && existing.equals(k)) {
          PathOffset old = unpack(values[idx]);
          keys[idx] = TOMBSTONE;
          size--;
          return old;
        }
        idx = (idx + 1) & mask;
      }
    }

    private int hash(String k) {
      return k.hashCode();
    }

    private void resize() {
      int newCapacity = keys.length * 2;
      String[] oldKeys = keys;
      long[] oldValues = values;

      keys = new String[newCapacity];
      values = new long[newCapacity];
      threshold = (int) (newCapacity * LOAD_FACTOR);
      size = 0;

      for (int i = 0; i < oldKeys.length; i++) {
        String k = oldKeys[i];
        if (k != null && k != TOMBSTONE) {
          long v = oldValues[i];
          putInternal(k, (int) (v >>> 32), (int) v);
        }
      }
    }

    @Override
    public int size() {
      return size;
    }

    @Override
    public void clear() {
      Arrays.fill(keys, null);
      size = 0;
      paths.clear();
      pathToIndex.clear();
    }
  }

  // ==================== DECIMAL key: open addressing ====================

  static class DecimalKeyMap extends CompactKeyMap {
    private static final int INITIAL_CAPACITY = 1024;
    private static final float LOAD_FACTOR = 0.5f;
    private static final BigDecimal TOMBSTONE = new BigDecimal(Long.MIN_VALUE);

    private BigDecimal[] keys;
    private long[] values;
    private int size;
    private int threshold;

    DecimalKeyMap() {
      keys = new BigDecimal[INITIAL_CAPACITY];
      values = new long[INITIAL_CAPACITY];
      threshold = (int) (INITIAL_CAPACITY * LOAD_FACTOR);
    }

    @Override
    public PathOffset put(Record key, String path, int position) {
      BigDecimal k = (BigDecimal) key.get(0, Object.class);
      return putInternal(k, internPath(path), position);
    }

    private PathOffset putInternal(BigDecimal k, int pathIndex, int position) {
      if (size >= threshold) {
        resize();
      }

      int mask = keys.length - 1;
      int idx = hash(k) & mask;

      while (true) {
        BigDecimal existing = keys[idx];
        if (existing == null || existing == TOMBSTONE) {
          keys[idx] = k;
          values[idx] = pack(pathIndex, position);
          size++;
          return null;
        }
        if (existing.equals(k)) {
          PathOffset old = unpack(values[idx]);
          values[idx] = pack(pathIndex, position);
          return old;
        }
        idx = (idx + 1) & mask;
      }
    }

    @Override
    public PathOffset remove(Record key) {
      BigDecimal k = (BigDecimal) key.get(0, Object.class);

      int mask = keys.length - 1;
      int idx = hash(k) & mask;

      while (true) {
        BigDecimal existing = keys[idx];
        if (existing == null) {
          return null;
        }
        if (existing != TOMBSTONE && existing.equals(k)) {
          PathOffset old = unpack(values[idx]);
          keys[idx] = TOMBSTONE;
          size--;
          return old;
        }
        idx = (idx + 1) & mask;
      }
    }

    private int hash(BigDecimal k) {
      return k.hashCode();
    }

    private void resize() {
      int newCapacity = keys.length * 2;
      BigDecimal[] oldKeys = keys;
      long[] oldValues = values;

      keys = new BigDecimal[newCapacity];
      values = new long[newCapacity];
      threshold = (int) (newCapacity * LOAD_FACTOR);
      size = 0;

      for (int i = 0; i < oldKeys.length; i++) {
        BigDecimal k = oldKeys[i];
        if (k != null && k != TOMBSTONE) {
          long v = oldValues[i];
          putInternal(k, (int) (v >>> 32), (int) v);
        }
      }
    }

    @Override
    public int size() {
      return size;
    }

    @Override
    public void clear() {
      Arrays.fill(keys, null);
      size = 0;
      paths.clear();
      pathToIndex.clear();
    }
  }

  // ==================== Multi-column key: open addressing ====================

  static class CompactKey {
    final Object[] values;
    final int hash;

    CompactKey(Object[] values) {
      this.values = values;
      this.hash = Arrays.hashCode(values);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof CompactKey)) return false;
      return Arrays.equals(values, ((CompactKey) o).values);
    }

    @Override
    public int hashCode() {
      return hash;
    }
  }

  static class MultiColumnKeyMap extends CompactKeyMap {
    private static final int INITIAL_CAPACITY = 1024;
    private static final float LOAD_FACTOR = 0.5f;
    private static final CompactKey TOMBSTONE = new CompactKey(new Object[0]);

    private CompactKey[] keys;
    private long[] values;
    private int size;
    private int threshold;
    private final int columnCount;

    MultiColumnKeyMap(int columnCount) {
      this.columnCount = columnCount;
      keys = new CompactKey[INITIAL_CAPACITY];
      values = new long[INITIAL_CAPACITY];
      threshold = (int) (INITIAL_CAPACITY * LOAD_FACTOR);
    }

    @Override
    public PathOffset put(Record key, String path, int position) {
      CompactKey k = extractKey(key);
      return putInternal(k, internPath(path), position);
    }

    private PathOffset putInternal(CompactKey k, int pathIndex, int position) {
      if (size >= threshold) {
        resize();
      }

      int mask = keys.length - 1;
      int idx = k.hash & mask;

      while (true) {
        CompactKey existing = keys[idx];
        if (existing == null || existing == TOMBSTONE) {
          keys[idx] = k;
          values[idx] = pack(pathIndex, position);
          size++;
          return null;
        }
        if (existing.equals(k)) {
          PathOffset old = unpack(values[idx]);
          values[idx] = pack(pathIndex, position);
          return old;
        }
        idx = (idx + 1) & mask;
      }
    }

    @Override
    public PathOffset remove(Record key) {
      CompactKey k = extractKey(key);

      int mask = keys.length - 1;
      int idx = k.hash & mask;

      while (true) {
        CompactKey existing = keys[idx];
        if (existing == null) {
          return null;
        }
        if (existing != TOMBSTONE && existing.equals(k)) {
          PathOffset old = unpack(values[idx]);
          keys[idx] = TOMBSTONE;
          size--;
          return old;
        }
        idx = (idx + 1) & mask;
      }
    }

    private CompactKey extractKey(Record key) {
      Object[] vals = new Object[columnCount];
      for (int i = 0; i < columnCount; i++) {
        Object val = key.get(i, Object.class);
        if (val == null) {
          vals[i] = null;
        } else if (val instanceof Integer) {
          vals[i] = ((Integer) val).longValue();
        } else if (val instanceof Long || val instanceof BigDecimal || val instanceof Boolean
            || val instanceof Double || val instanceof Float) {
          vals[i] = val; // immutable
        } else if (val instanceof CharSequence) {
          vals[i] = val.toString();
        } else if (val instanceof byte[]) {
          vals[i] = ((byte[]) val).clone();
        } else if (val instanceof java.nio.ByteBuffer) {
          java.nio.ByteBuffer buf = (java.nio.ByteBuffer) val;
          byte[] bytes = new byte[buf.remaining()];
          buf.duplicate().get(bytes);
          vals[i] = java.nio.ByteBuffer.wrap(bytes);
        } else {
          vals[i] = val;
        }
      }
      return new CompactKey(vals);
    }

    private void resize() {
      int newCapacity = keys.length * 2;
      CompactKey[] oldKeys = keys;
      long[] oldValues = values;

      keys = new CompactKey[newCapacity];
      values = new long[newCapacity];
      threshold = (int) (newCapacity * LOAD_FACTOR);
      size = 0;

      for (int i = 0; i < oldKeys.length; i++) {
        CompactKey k = oldKeys[i];
        if (k != null && k != TOMBSTONE) {
          long v = oldValues[i];
          putInternal(k, (int) (v >>> 32), (int) v);
        }
      }
    }

    @Override
    public int size() {
      return size;
    }

    @Override
    public void clear() {
      Arrays.fill(keys, null);
      size = 0;
      paths.clear();
      pathToIndex.clear();
    }
  }
}
