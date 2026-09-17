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
import org.apache.iceberg.data.GenericRecord;
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

  /**
   * Try to parse a string as UUID. Returns two longs [high, low] if valid UUID, null otherwise.
   * Supports both formats: with dashes (36 chars) and without (32 chars).
   */
  protected static long[] tryParseUuid(String s) {
    if (s == null) {
      return null;
    }
    int len = s.length();
    if (len == 36) {
      // With dashes: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
      if (s.charAt(8) != '-' || s.charAt(13) != '-'
          || s.charAt(18) != '-' || s.charAt(23) != '-') {
        return null;
      }
      long p0 = parseHexSegment(s, 0, 8);
      long p1 = parseHexSegment(s, 9, 13);
      long p2 = parseHexSegment(s, 14, 18);
      long p3 = parseHexSegment(s, 19, 23);
      long p4 = parseHexSegment(s, 24, 36);
      if (p0 < 0 || p1 < 0 || p2 < 0 || p3 < 0 || p4 < 0) {
        return null;
      }
      long high = (p0 << 32) | (p1 << 16) | p2;
      long low = (p3 << 48) | p4;
      return new long[] {high, low};
    } else if (len == 32) {
      // Without dashes: split into smaller segments to avoid overflow issues
      long p0 = parseHexSegment(s, 0, 8);
      long p1 = parseHexSegment(s, 8, 12);
      long p2 = parseHexSegment(s, 12, 16);
      long p3 = parseHexSegment(s, 16, 20);
      long p4 = parseHexSegment(s, 20, 32);
      if (p0 < 0 || p1 < 0 || p2 < 0 || p3 < 0 || p4 < 0) {
        return null;
      }
      long high = (p0 << 32) | (p1 << 16) | p2;
      long low = (p3 << 48) | p4;
      return new long[] {high, low};
    }
    return null;
  }

  /**
   * Parse hex segment. Returns -1 if any character is not a valid hex digit.
   */
  private static long parseHexSegment(String s, int start, int end) {
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
        return -1;
      }
      result = (result << 4) | digit;
    }
    return result;
  }

  /** Creates a CompactKeyMap optimized for the given equality delete schema. */
  public static CompactKeyMap create(Schema deleteSchema) {
    return create(deleteSchema, true);
  }

  /**
   * Creates a CompactKeyMap optimized for the given equality delete schema.
   *
   * @param deleteSchema schema for equality delete columns
   * @param hasRealPk if true, uses single-value map (duplicates replace previous).
   *                  if false, uses multi-value map (duplicates are tracked separately).
   */
  public static CompactKeyMap create(Schema deleteSchema, boolean hasRealPk) {
    CompactKeyMap base = createBase(deleteSchema);
    return hasRealPk ? base : new MultiValueWrapper(base, deleteSchema.columns().size());
  }

  private static CompactKeyMap createBase(Schema deleteSchema) {
    List<Types.NestedField> columns = deleteSchema.columns();

    if (columns.size() == 1) {
      Type.TypeID typeId = columns.get(0).type().typeId();
      switch (typeId) {
        case INTEGER:
        case LONG:
          return new LongKeyMap(deleteSchema);
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

    private final boolean keyIsInteger;
    private final String keyFieldName;
    private final Record reusableKeyRecord;
    private CompactKeyMap delegate;

    private long[] keys;
    private long[] values; // packed (pathIndex << 32) | position
    private int size; // live entries
    private int occupied; // live entries + tombstones == all non-empty slots
    private int threshold;

    LongKeyMap(Schema deleteSchema) {
      this.keyIsInteger = deleteSchema.columns().get(0).type().typeId() == Type.TypeID.INTEGER;
      this.keyFieldName = deleteSchema.columns().get(0).name();
      this.reusableKeyRecord = GenericRecord.create(deleteSchema);
      keys = new long[INITIAL_CAPACITY];
      values = new long[INITIAL_CAPACITY];
      Arrays.fill(keys, EMPTY_KEY);
      threshold = (int) (INITIAL_CAPACITY * LOAD_FACTOR);
    }

    @Override
    public PathOffset put(Record key, String path, int position) {
      if (delegate != null) {
        return delegate.put(key, path, position);
      }
      Object val = key.get(0, Object.class);
      long k = val instanceof Integer ? ((Integer) val).longValue() : (Long) val;
      if (k == EMPTY_KEY || k == TOMBSTONE) {
        migrateToDelegate();
        return delegate.put(keyRecord(k), path, position);
      }
      return putInternal(k, internPath(path), position);
    }

    private PathOffset putInternal(long k, int pathIndex, int position) {
      // Resize on occupied slots, not on live entries: tombstones take up slots too, and
      // resize() is the only place that reclaims them. Gating on size alone means an
      // insert/delete workload (queue tables) never resizes and never reclaims, until no
      // empty slot is left and lookups for absent keys spin forever.
      if (occupied >= threshold) {
        resize();
      }

      int mask = keys.length - 1;
      int idx = hash(k) & mask;
      int firstTombstone = -1;

      while (true) {
        long existing = keys[idx];
        if (existing == EMPTY_KEY) {
          // End of the probe chain: the key is not present. Prefer a tombstone seen earlier.
          if (firstTombstone >= 0) {
            keys[firstTombstone] = k;
            values[firstTombstone] = pack(pathIndex, position);
          } else {
            keys[idx] = k;
            values[idx] = pack(pathIndex, position);
            occupied++;
          }
          size++;
          return null;
        }
        if (existing == TOMBSTONE) {
          // Remember it, but keep probing: the key may live further down the chain.
          if (firstTombstone < 0) {
            firstTombstone = idx;
          }
        } else if (existing == k) {
          PathOffset old = unpack(values[idx]);
          values[idx] = pack(pathIndex, position);
          return old;
        }
        idx = (idx + 1) & mask;
      }
    }

    @Override
    public PathOffset remove(Record key) {
      if (delegate != null) {
        return delegate.remove(key);
      }
      Object val = key.get(0, Object.class);
      long k = val instanceof Integer ? ((Integer) val).longValue() : (Long) val;
      if (k == EMPTY_KEY || k == TOMBSTONE) {
        migrateToDelegate();
        return delegate.remove(keyRecord(k));
      }

      int mask = keys.length - 1;
      int idx = hash(k) & mask;

      // Bounded probing: a full pass means the key is absent. Belt and braces on top of the
      // occupied-based resize, so a future regression degrades to a slow lookup instead of
      // an uninterruptible spin that no task restart can clear.
      for (int probes = 0; probes < keys.length; probes++) {
        long existing = keys[idx];
        if (existing == EMPTY_KEY) {
          return null;
        }
        if (existing == k) {
          PathOffset old = unpack(values[idx]);
          keys[idx] = TOMBSTONE; // slot stays occupied, so occupied is left untouched
          size--;
          return old;
        }
        idx = (idx + 1) & mask;
      }
      return null;
    }

    private int hash(long k) {
      // FNV-1a inspired mixing
      long h = k * 0x9E3779B97F4A7C15L;
      return (int) (h ^ (h >>> 32));
    }

    private void resize() {
      // Tombstones alone must not grow the map: when the live entries still fit comfortably,
      // rehash at the same capacity and simply drop the tombstones.
      int newCapacity = size >= threshold / 2 ? keys.length * 2 : keys.length;
      long[] oldKeys = keys;
      long[] oldValues = values;

      keys = new long[newCapacity];
      values = new long[newCapacity];
      Arrays.fill(keys, EMPTY_KEY);
      threshold = (int) (newCapacity * LOAD_FACTOR);
      size = 0;
      occupied = 0; // tombstones are dropped here; putInternal rebuilds both counters

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
      return delegate != null ? delegate.size() : size;
    }

    @Override
    public void clear() {
      if (delegate != null) {
        delegate.clear();
        delegate = null;
      }
      if (keys != null) {
        Arrays.fill(keys, EMPTY_KEY);
        size = 0;
        occupied = 0;
      }
      paths.clear();
      pathToIndex.clear();
    }

    @Override
    public String getPath(int index) {
      return delegate != null ? delegate.getPath(index) : super.getPath(index);
    }

    private Record keyRecord(long k) {
      reusableKeyRecord.setField(keyFieldName, keyIsInteger ? (int) k : k);
      return reusableKeyRecord;
    }

    private void migrateToDelegate() {
      if (delegate != null) {
        return;
      }
      delegate = new MultiColumnKeyMap(1);
      delegate.paths.addAll(this.paths);
      delegate.pathToIndex.putAll(this.pathToIndex);

      for (int i = 0; i < keys.length; i++) {
        long k = keys[i];
        if (k != EMPTY_KEY && k != TOMBSTONE) {
          long packed = values[i];
          int pathIndex = (int) (packed >>> 32);
          int position = (int) packed;
          delegate.put(keyRecord(k), getPath(pathIndex), position);
        }
      }

      keys = null;
      values = null;
      size = 0;
      occupied = 0;
      threshold = 0;
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
        delegate = (tryParseUuid(k) != null) ? new UuidKeyMap() : new StringKeyMap();
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
  }

  // ==================== UUID key: two longs ====================

  /**
   * UUID stored as two longs. ~24 bytes per entry vs ~83 bytes for String.
   * Falls back to StringKeyMap if a non-UUID value is encountered.
   */
  static class UuidKeyMap extends CompactKeyMap {
    private static final int INITIAL_CAPACITY = 1024;
    private static final float LOAD_FACTOR = 0.5f;
    private static final long EMPTY = Long.MIN_VALUE;
    private static final long TOMBSTONE_MARKER = Long.MIN_VALUE + 1;

    private long[] highBits;
    private long[] lowBits;
    private long[] values;
    private int size; // live entries
    private int occupied; // live entries + tombstones == all non-empty slots
    private int threshold;
    private StringKeyMap fallback;

    UuidKeyMap() {
      highBits = new long[INITIAL_CAPACITY];
      lowBits = new long[INITIAL_CAPACITY];
      values = new long[INITIAL_CAPACITY];
      Arrays.fill(highBits, EMPTY);
      threshold = (int) (INITIAL_CAPACITY * LOAD_FACTOR);
    }

    @Override
    public PathOffset put(Record key, String path, int position) {
      if (fallback != null) {
        return fallback.put(key, path, position);
      }
      String k = key.get(0, Object.class).toString();
      long[] uuid = tryParseUuid(k);
      if (uuid == null) {
        migrateToFallback();
        return fallback.put(key, path, position);
      }
      return putInternal(uuid[0], uuid[1], internPath(path), position);
    }

    private PathOffset putInternal(long high, long low, int pathIndex, int position) {
      // See LongKeyMap.putInternal: the threshold must count occupied slots, not live entries.
      if (occupied >= threshold) {
        resize();
      }

      int mask = highBits.length - 1;
      int idx = hash(high, low) & mask;
      int firstTombstone = -1;

      while (true) {
        long existingHigh = highBits[idx];
        if (existingHigh == EMPTY) {
          if (firstTombstone >= 0) {
            highBits[firstTombstone] = high;
            lowBits[firstTombstone] = low;
            values[firstTombstone] = pack(pathIndex, position);
          } else {
            highBits[idx] = high;
            lowBits[idx] = low;
            values[idx] = pack(pathIndex, position);
            occupied++;
          }
          size++;
          return null;
        }
        if (existingHigh == TOMBSTONE_MARKER) {
          if (firstTombstone < 0) {
            firstTombstone = idx;
          }
        } else if (existingHigh == high && lowBits[idx] == low) {
          PathOffset old = unpack(values[idx]);
          values[idx] = pack(pathIndex, position);
          return old;
        }
        idx = (idx + 1) & mask;
      }
    }

    @Override
    public PathOffset remove(Record key) {
      if (fallback != null) {
        return fallback.remove(key);
      }
      String k = key.get(0, Object.class).toString();
      long[] uuid = tryParseUuid(k);
      if (uuid == null) {
        return null; // non-UUID key can't exist in UUID map
      }
      long high = uuid[0], low = uuid[1];

      int mask = highBits.length - 1;
      int idx = hash(high, low) & mask;

      for (int probes = 0; probes < highBits.length; probes++) {
        long existingHigh = highBits[idx];
        if (existingHigh == EMPTY) {
          return null;
        }
        if (existingHigh == high && lowBits[idx] == low) {
          PathOffset old = unpack(values[idx]);
          highBits[idx] = TOMBSTONE_MARKER; // slot stays occupied
          size--;
          return old;
        }
        idx = (idx + 1) & mask;
      }
      return null;
    }

    private int hash(long high, long low) {
      long h = high * 31 + low;
      h = h * 0x9E3779B97F4A7C15L;
      return (int) (h ^ (h >>> 32));
    }

    private void resize() {
      // Tombstones alone must not grow the map: see LongKeyMap.resize.
      int newCapacity = size >= threshold / 2 ? highBits.length * 2 : highBits.length;
      long[] oldHigh = highBits;
      long[] oldLow = lowBits;
      long[] oldValues = values;

      highBits = new long[newCapacity];
      lowBits = new long[newCapacity];
      values = new long[newCapacity];
      Arrays.fill(highBits, EMPTY);
      threshold = (int) (newCapacity * LOAD_FACTOR);
      size = 0;
      occupied = 0;

      for (int i = 0; i < oldHigh.length; i++) {
        long high = oldHigh[i];
        if (high != EMPTY && high != TOMBSTONE_MARKER) {
          long v = oldValues[i];
          putInternal(high, oldLow[i], (int) (v >>> 32), (int) v);
        }
      }
    }

    private void migrateToFallback() {
      fallback = new StringKeyMap();
      fallback.paths.addAll(this.paths);
      fallback.pathToIndex.putAll(this.pathToIndex);

      // Re-insert all existing entries as strings
      for (int i = 0; i < highBits.length; i++) {
        long high = highBits[i];
        if (high != EMPTY && high != TOMBSTONE_MARKER) {
          String uuidStr = formatUuid(high, lowBits[i]);
          long packed = values[i];
          int pathIndex = (int) (packed >>> 32);
          int position = (int) packed;
          fallback.putInternal(uuidStr, pathIndex, position);
        }
      }

      // Release UUID storage
      highBits = null;
      lowBits = null;
      values = null;
      size = 0;
      occupied = 0;
    }

    private static String formatUuid(long high, long low) {
      return String.format(
          "%08x-%04x-%04x-%04x-%012x",
          (high >>> 32) & 0xFFFFFFFFL,
          (high >>> 16) & 0xFFFFL,
          high & 0xFFFFL,
          (low >>> 48) & 0xFFFFL,
          low & 0xFFFFFFFFFFFFL);
    }

    @Override
    public int size() {
      return fallback != null ? fallback.size() : size;
    }

    @Override
    public void clear() {
      if (fallback != null) {
        fallback.clear();
        fallback = null;
      }
      if (highBits != null) {
        Arrays.fill(highBits, EMPTY);
      }
      size = 0;
      occupied = 0;
      paths.clear();
      pathToIndex.clear();
    }

    @Override
    public String getPath(int index) {
      return fallback != null ? fallback.getPath(index) : super.getPath(index);
    }
  }

  // ==================== Generic STRING key: open addressing ====================

  static class StringKeyMap extends CompactKeyMap {
    private static final int INITIAL_CAPACITY = 1024;
    private static final float LOAD_FACTOR = 0.5f;
    private static final String TOMBSTONE = new String(""); // unique instance

    private String[] keys;
    private long[] values;
    private int size; // live entries
    private int occupied; // live entries + tombstones == all non-empty slots
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
      // See LongKeyMap.putInternal: the threshold must count occupied slots, not live entries.
      if (occupied >= threshold) {
        resize();
      }

      int mask = keys.length - 1;
      int idx = hash(k) & mask;
      int firstTombstone = -1;

      while (true) {
        String existing = keys[idx];
        if (existing == null) {
          if (firstTombstone >= 0) {
            keys[firstTombstone] = k;
            values[firstTombstone] = pack(pathIndex, position);
          } else {
            keys[idx] = k;
            values[idx] = pack(pathIndex, position);
            occupied++;
          }
          size++;
          return null;
        }
        if (existing == TOMBSTONE) {
          if (firstTombstone < 0) {
            firstTombstone = idx;
          }
        } else if (existing.equals(k)) {
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

      for (int probes = 0; probes < keys.length; probes++) {
        String existing = keys[idx];
        if (existing == null) {
          return null;
        }
        if (existing != TOMBSTONE && existing.equals(k)) {
          PathOffset old = unpack(values[idx]);
          keys[idx] = TOMBSTONE; // slot stays occupied
          size--;
          return old;
        }
        idx = (idx + 1) & mask;
      }
      return null;
    }

    private int hash(String k) {
      return k.hashCode();
    }

    private void resize() {
      // Tombstones alone must not grow the map: see LongKeyMap.resize.
      int newCapacity = size >= threshold / 2 ? keys.length * 2 : keys.length;
      String[] oldKeys = keys;
      long[] oldValues = values;

      keys = new String[newCapacity];
      values = new long[newCapacity];
      threshold = (int) (newCapacity * LOAD_FACTOR);
      size = 0;
      occupied = 0;

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
      occupied = 0;
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
    private int size; // live entries
    private int occupied; // live entries + tombstones == all non-empty slots
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
      // See LongKeyMap.putInternal: the threshold must count occupied slots, not live entries.
      if (occupied >= threshold) {
        resize();
      }

      int mask = keys.length - 1;
      int idx = hash(k) & mask;
      int firstTombstone = -1;

      while (true) {
        BigDecimal existing = keys[idx];
        if (existing == null) {
          if (firstTombstone >= 0) {
            keys[firstTombstone] = k;
            values[firstTombstone] = pack(pathIndex, position);
          } else {
            keys[idx] = k;
            values[idx] = pack(pathIndex, position);
            occupied++;
          }
          size++;
          return null;
        }
        if (existing == TOMBSTONE) {
          if (firstTombstone < 0) {
            firstTombstone = idx;
          }
        } else if (existing.equals(k)) {
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

      for (int probes = 0; probes < keys.length; probes++) {
        BigDecimal existing = keys[idx];
        if (existing == null) {
          return null;
        }
        if (existing != TOMBSTONE && existing.equals(k)) {
          PathOffset old = unpack(values[idx]);
          keys[idx] = TOMBSTONE; // slot stays occupied
          size--;
          return old;
        }
        idx = (idx + 1) & mask;
      }
      return null;
    }

    private int hash(BigDecimal k) {
      return k.hashCode();
    }

    private void resize() {
      // Tombstones alone must not grow the map: see LongKeyMap.resize.
      int newCapacity = size >= threshold / 2 ? keys.length * 2 : keys.length;
      BigDecimal[] oldKeys = keys;
      long[] oldValues = values;

      keys = new BigDecimal[newCapacity];
      values = new long[newCapacity];
      threshold = (int) (newCapacity * LOAD_FACTOR);
      size = 0;
      occupied = 0;

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
      occupied = 0;
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
    private int size; // live entries
    private int occupied; // live entries + tombstones == all non-empty slots
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
      // See LongKeyMap.putInternal: the threshold must count occupied slots, not live entries.
      if (occupied >= threshold) {
        resize();
      }

      int mask = keys.length - 1;
      int idx = k.hash & mask;
      int firstTombstone = -1;

      while (true) {
        CompactKey existing = keys[idx];
        if (existing == null) {
          if (firstTombstone >= 0) {
            keys[firstTombstone] = k;
            values[firstTombstone] = pack(pathIndex, position);
          } else {
            keys[idx] = k;
            values[idx] = pack(pathIndex, position);
            occupied++;
          }
          size++;
          return null;
        }
        if (existing == TOMBSTONE) {
          if (firstTombstone < 0) {
            firstTombstone = idx;
          }
        } else if (existing.equals(k)) {
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

      for (int probes = 0; probes < keys.length; probes++) {
        CompactKey existing = keys[idx];
        if (existing == null) {
          return null;
        }
        if (existing != TOMBSTONE && existing.equals(k)) {
          PathOffset old = unpack(values[idx]);
          keys[idx] = TOMBSTONE; // slot stays occupied
          size--;
          return old;
        }
        idx = (idx + 1) & mask;
      }
      return null;
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
      // Tombstones alone must not grow the map: see LongKeyMap.resize.
      int newCapacity = size >= threshold / 2 ? keys.length * 2 : keys.length;
      CompactKey[] oldKeys = keys;
      long[] oldValues = values;

      keys = new CompactKey[newCapacity];
      values = new long[newCapacity];
      threshold = (int) (newCapacity * LOAD_FACTOR);
      size = 0;
      occupied = 0;

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
      occupied = 0;
      paths.clear();
      pathToIndex.clear();
    }
  }

  // ==================== Multi-value wrapper for tables without real PK ====================

  /**
   * Wrapper that allows multiple positions per key.
   * Used for tables without real PK where duplicate rows are legitimate.
   */
  static class MultiValueWrapper extends CompactKeyMap {
    private final CompactKeyMap delegate;
    private final int columnCount;

    // Overflow storage for duplicate keys: maps CompactKey -> list of additional positions
    private final java.util.Map<CompactKey, List<Long>> duplicates = Maps.newHashMap();

    MultiValueWrapper(CompactKeyMap delegate, int columnCount) {
      this.delegate = delegate;
      this.columnCount = columnCount;
    }

    @Override
    public PathOffset put(Record key, String path, int position) {
      int pathIndex = internPath(path);
      PathOffset previous = delegate.put(key, path, position);

      if (previous != null) {
        // Key already exists - store previous in duplicates
        CompactKey ck = extractKey(key);
        duplicates.computeIfAbsent(ck, k -> Lists.newArrayList()).add(pack(previous.pathIndex, previous.position));
      }

      return null; // Never return previous - we're keeping all values
    }

    @Override
    public PathOffset remove(Record key) {
      CompactKey ck = extractKey(key);

      // First check duplicates
      List<Long> dups = duplicates.get(ck);
      if (dups != null && !dups.isEmpty()) {
        long packed = dups.remove(dups.size() - 1);
        if (dups.isEmpty()) {
          duplicates.remove(ck);
        }
        return unpack(packed);
      }

      // Then check delegate
      return delegate.remove(key);
    }

    @Override
    public int size() {
      int dupCount = 0;
      for (List<Long> list : duplicates.values()) {
        dupCount += list.size();
      }
      return delegate.size() + dupCount;
    }

    @Override
    public void clear() {
      delegate.clear();
      duplicates.clear();
      paths.clear();
      pathToIndex.clear();
    }

    @Override
    public String getPath(int index) {
      return delegate.getPath(index);
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
          vals[i] = val;
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
  }
}
