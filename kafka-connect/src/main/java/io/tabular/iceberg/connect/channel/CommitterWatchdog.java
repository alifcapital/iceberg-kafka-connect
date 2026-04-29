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

import java.util.Collections;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsOptions;
import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Detects a stuck SinkTask thread by polling the state of the connect-&lt;name&gt; consumer group
 * via the admin client. If the group stays unhealthy (EMPTY/DEAD/UNKNOWN/missing) longer than the
 * grace window, the watchdog interrupts the SinkTask main thread and sets the dead flag so the
 * next commit() call throws ConnectException, transitioning the task to FAILED.
 *
 * <p>Limitation: mainThread.interrupt() is best-effort. If the SinkTask thread is blocked in
 * non-interruptible IO (e.g. native socket read without timeout), the interrupt is silently
 * dropped and the task only fails when control eventually returns to commit() and the dead flag
 * is observed. Configure timeouts on Iceberg / S3 / HTTP clients so blocked IO eventually returns.
 */
class CommitterWatchdog {

  private static final Logger LOG = LoggerFactory.getLogger(CommitterWatchdog.class);

  private static final long CHECK_INTERVAL_MS = 30_000L;
  private static final long DEAD_GRACE_MS = 600_000L;
  private static final long DEAD_GRACE_NANOS = TimeUnit.MILLISECONDS.toNanos(DEAD_GRACE_MS);
  private static final long ADMIN_TIMEOUT_MS = 15_000L;

  private final Admin admin;
  private final String groupId;
  private final Thread mainThread;
  private final Thread thread;
  private volatile boolean stopped;
  private volatile boolean dead;
  private volatile String deadReason;

  CommitterWatchdog(Admin admin, String groupId, Thread mainThread) {
    this.admin = admin;
    this.groupId = groupId;
    this.mainThread = mainThread;
    this.thread = new Thread(this::run, "iceberg-committer-watchdog-" + groupId);
    this.thread.setDaemon(true);
  }

  void start() {
    thread.start();
  }

  void stop() {
    stopped = true;
    thread.interrupt();
    try {
      thread.join(5_000L);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  boolean isDead() {
    return dead;
  }

  String deadReason() {
    return deadReason;
  }

  private void run() {
    LOG.info(
        "Watchdog started for group {} (check={}ms, grace={}ms)",
        groupId, CHECK_INTERVAL_MS, DEAD_GRACE_MS);

    long unhealthySinceNanos = 0L;
    ConsumerGroupState lastState = null;
    boolean adminFailureLogged = false;

    while (!stopped) {
      try {
        Thread.sleep(CHECK_INTERVAL_MS);
      } catch (InterruptedException e) {
        return;
      }
      if (stopped) {
        return;
      }

      ConsumerGroupState state;
      try {
        ConsumerGroupDescription desc =
            admin
                .describeConsumerGroups(
                    Collections.singletonList(groupId),
                    new DescribeConsumerGroupsOptions().timeoutMs((int) ADMIN_TIMEOUT_MS))
                .describedGroups()
                .get(groupId)
                .get(ADMIN_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        state = desc.state();
        adminFailureLogged = false;
      } catch (InterruptedException e) {
        return;
      } catch (ExecutionException e) {
        if (e.getCause() instanceof GroupIdNotFoundException) {
          // Group simply does not exist on the broker — treat as terminal unhealthy.
          state = ConsumerGroupState.DEAD;
          adminFailureLogged = false;
        } else {
          logAdminFailure(e, adminFailureLogged);
          adminFailureLogged = true;
          continue;
        }
      } catch (Exception e) {
        logAdminFailure(e, adminFailureLogged);
        adminFailureLogged = true;
        continue;
      }

      long nowNanos = System.nanoTime();
      boolean healthy =
          state == ConsumerGroupState.STABLE
              || state == ConsumerGroupState.PREPARING_REBALANCE
              || state == ConsumerGroupState.COMPLETING_REBALANCE;

      if (healthy) {
        if (unhealthySinceNanos != 0L) {
          LOG.info(
              "Watchdog: group {} recovered to {} after {}ms",
              groupId, state,
              TimeUnit.NANOSECONDS.toMillis(nowNanos - unhealthySinceNanos));
        }
        unhealthySinceNanos = 0L;
        lastState = state;
        continue;
      }

      if (unhealthySinceNanos == 0L) {
        unhealthySinceNanos = nowNanos;
        LOG.warn(
            "Watchdog: group {} entered {}, starting {}ms grace window",
            groupId, state, DEAD_GRACE_MS);
      } else {
        long unhealthyForNanos = nowNanos - unhealthySinceNanos;
        if (unhealthyForNanos >= DEAD_GRACE_NANOS) {
          long unhealthyForMs = TimeUnit.NANOSECONDS.toMillis(unhealthyForNanos);
          deadReason =
              String.format(
                  "consumer group %s in state %s for %dms (>= %dms grace)",
                  groupId, state, unhealthyForMs, DEAD_GRACE_MS);
          LOG.error(
              "Watchdog: {}; interrupting SinkTask thread {} (id={}) to fail task",
              deadReason, mainThread.getName(), mainThread.getId());
          dead = true;
          mainThread.interrupt();
          return;
        } else if (lastState != state) {
          LOG.warn(
              "Watchdog: group {} state {} (unhealthy for {}ms / {}ms grace)",
              groupId, state,
              TimeUnit.NANOSECONDS.toMillis(unhealthyForNanos),
              DEAD_GRACE_MS);
        }
      }
      lastState = state;
    }
  }

  private void logAdminFailure(Exception e, boolean alreadyLogged) {
    if (alreadyLogged) {
      LOG.debug("Watchdog admin describe still failing for {}", groupId, e);
    } else {
      LOG.warn("Watchdog admin describe failed for {}, will retry", groupId, e);
    }
  }
}
