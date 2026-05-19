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

import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;

/**
 * Carries the result of one catalog.loadTable() done by the coordinator after a commit cycle,
 * shared by both the watermark publisher (needs the snapshot) and the DDL event publisher (needs
 * the table for schemaId/schemas/properties). snapshotOrNull may be null if this commit cycle did
 * not produce a snapshot for the table (e.g. all files deduplicated, or our snapshot was expired
 * by a concurrent writer).
 */
final class TableInspection {

  private final Table table;
  private final Snapshot snapshotOrNull;

  TableInspection(Table table, Snapshot snapshotOrNull) {
    this.table = table;
    this.snapshotOrNull = snapshotOrNull;
  }

  Table table() {
    return table;
  }

  Snapshot snapshotOrNull() {
    return snapshotOrNull;
  }
}
