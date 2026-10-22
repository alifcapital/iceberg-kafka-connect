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

import java.time.Instant;

/**
 * Marks columns that disappeared from source records. Iceberg keeps the column (old rows keep their
 * values, new rows get null), so the schema itself does not change and no DDL event would be sent.
 * The marker is stored in the column doc: changing it creates a new schema version, which the DDL
 * event publisher reports as DROP_COLUMN, and as ADD_COLUMN once the column is back in the source.
 *
 * <p>The doc carries a timestamp so every mark/unmark produces a schema that differs from all
 * previous ones: Iceberg reuses the id of an identical earlier schema, which would look like a
 * schema id going backwards.
 */
public final class SourceDropMarker {

  private static final String DROPPED = "[dropped in source ";
  private static final String RESTORED = "[restored in source ";

  private SourceDropMarker() {}

  public static boolean isDropped(String doc) {
    return doc != null && doc.startsWith(DROPPED);
  }

  /** Marks the column as dropped, keeping the column comment after the marker. */
  static String droppedDoc(Instant at, String currentDoc) {
    return withComment(DROPPED + at + "]", currentDoc);
  }

  /** Marks the column as back in the source, keeping the column comment after the marker. */
  static String restoredDoc(Instant at, String currentDoc) {
    return withComment(RESTORED + at + "]", currentDoc);
  }

  /** Column comment without our marker; null if there is none. */
  static String comment(String doc) {
    if (doc == null) {
      return null;
    }
    String rest = doc;
    if (doc.startsWith(DROPPED) || doc.startsWith(RESTORED)) {
      int end = doc.indexOf(']');
      rest = end < 0 ? "" : doc.substring(end + 1);
    }
    rest = rest.trim();
    return rest.isEmpty() ? null : rest;
  }

  private static String withComment(String marker, String currentDoc) {
    String comment = comment(currentDoc);
    return comment == null ? marker : marker + " " + comment;
  }
}
