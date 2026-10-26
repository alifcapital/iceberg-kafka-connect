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

import io.tabular.iceberg.connect.IcebergSinkConfig;
import io.tabular.iceberg.connect.data.Utilities;
import java.util.Collection;
import java.util.UUID;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TaskImpl implements Task, AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(TaskImpl.class);
  private final SinkTaskContext context;
  private final Catalog catalog;
  private final Writer writer;
  private final Committer committer;
  private final long deferredFailureTimeoutMs;
  private RuntimeException deferredFailure;
  private UUID commitBeforeFailure;
  private long deferredFailureDeadline;

  public TaskImpl(SinkTaskContext context, IcebergSinkConfig config) {
    this(context, config, Utilities.loadCatalog(config));
  }

  private TaskImpl(SinkTaskContext context, IcebergSinkConfig config, Catalog catalog) {
    this(
        context,
        config,
        catalog,
        new Worker(config, catalog),
        new CommitterImpl(context, config, catalog));
  }

  @VisibleForTesting
  TaskImpl(
      SinkTaskContext context,
      IcebergSinkConfig config,
      Catalog catalog,
      Writer writer,
      Committer committer) {
    this.context = context;
    this.catalog = catalog;
    this.writer = writer;
    this.committer = committer;
    // wait for the next commit to start, then for it to finish
    this.deferredFailureTimeoutMs = config.commitIntervalMs() + 2L * config.commitTimeoutMs();
  }

  @Override
  public void put(Collection<SinkRecord> sinkRecords) {
    if (deferredFailure == null) {
      writer.write(sinkRecords);
      deferFailure();
    }
    committer.commit(writer);
    if (deferredFailure != null) {
      throwDeferredFailureIfCommitted();
    }
  }

  // Stops consuming at the failed record, so the records written before it go into the next
  // commit instead of being discarded with the task.
  private void deferFailure() {
    RuntimeException failure = writer.deferredFailure();
    if (failure == null) {
      return;
    }
    if (!writer.madeProgress()) {
      // nothing to commit before the failed record
      throw failure;
    }
    LOG.warn(
        "Record conversion failed, pausing consumption and failing after the next commit",
        failure);
    context.pause(context.assignment().toArray(new TopicPartition[0]));
    deferredFailure = failure;
    commitBeforeFailure = committer.lastResponseCommitId();
    deferredFailureDeadline = System.currentTimeMillis() + deferredFailureTimeoutMs;
  }

  private void throwDeferredFailureIfCommitted() {
    UUID commitId = committer.lastResponseCommitId();
    if (commitId != null
        && !commitId.equals(commitBeforeFailure)
        && committer.isCommitComplete(commitId)) {
      LOG.info("Commit {} completed, failing on the deferred record conversion error", commitId);
      throw deferredFailure;
    }
    if (System.currentTimeMillis() >= deferredFailureDeadline) {
      LOG.warn(
          "No commit completed within {} ms, failing without committing the written records",
          deferredFailureTimeoutMs);
      throw deferredFailure;
    }
  }

  @Override
  public void close() throws Exception {
    Utilities.close(writer);
    Utilities.close(committer);
    Utilities.close(catalog);
  }
}
