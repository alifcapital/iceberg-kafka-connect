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

import java.util.Collection;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Type.PrimitiveType;

public class SchemaUpdate {

  public static class Consumer {
    private final Map<String, AddColumn> addColumns = Maps.newHashMap();
    private final Map<String, UpdateType> updateTypes = Maps.newHashMap();
    private final Map<String, MakeOptional> makeOptionals = Maps.newHashMap();
    private final Map<String, SourceDrop> sourceDrops = Maps.newHashMap();

    public Collection<AddColumn> addColumns() {
      return addColumns.values();
    }

    public Collection<UpdateType> updateTypes() {
      return updateTypes.values();
    }

    public Collection<MakeOptional> makeOptionals() {
      return makeOptionals.values();
    }

    public Collection<SourceDrop> sourceDrops() {
      return sourceDrops.values();
    }

    public boolean empty() {
      return addColumns.isEmpty()
          && updateTypes.isEmpty()
          && makeOptionals.isEmpty()
          && sourceDrops.isEmpty();
    }

    public void addColumn(String parentName, String name, Type type) {
      AddColumn addCol = new AddColumn(parentName, name, type);
      addColumns.put(addCol.key(), addCol);
    }

    public void updateType(String name, PrimitiveType type) {
      updateTypes.put(name, new UpdateType(name, type));
    }

    public void makeOptional(String name) {
      makeOptionals.put(name, new MakeOptional(name));
    }

    /** Column is in the table but missing from source records: it was dropped in the source. */
    public void markDropped(String name) {
      sourceDrops.put(name, new SourceDrop(name, true));
    }

    /** Column marked as dropped is present in source records again. */
    public void markRestored(String name) {
      sourceDrops.put(name, new SourceDrop(name, false));
    }
  }

  public static class AddColumn extends SchemaUpdate {
    private final String parentName;
    private final String name;
    private final Type type;

    public AddColumn(String parentName, String name, Type type) {
      this.parentName = parentName;
      this.name = name;
      this.type = type;
    }

    public String parentName() {
      return parentName;
    }

    public String name() {
      return name;
    }

    public String key() {
      return parentName == null ? name : parentName + "." + name;
    }

    public Type type() {
      return type;
    }
  }

  public static class UpdateType extends SchemaUpdate {
    private final String name;
    private final PrimitiveType type;

    public UpdateType(String name, PrimitiveType type) {
      this.name = name;
      this.type = type;
    }

    public String name() {
      return name;
    }

    public PrimitiveType type() {
      return type;
    }
  }

  public static class MakeOptional extends SchemaUpdate {
    private final String name;

    public MakeOptional(String name) {
      this.name = name;
    }

    public String name() {
      return name;
    }
  }

  /** Sets (dropped=true) or clears (dropped=false) the dropped-in-source marker of a column. */
  public static class SourceDrop extends SchemaUpdate {
    private final String name;
    private final boolean dropped;

    public SourceDrop(String name, boolean dropped) {
      this.name = name;
      this.dropped = dropped;
    }

    public String name() {
      return name;
    }

    public boolean dropped() {
      return dropped;
    }
  }
}
