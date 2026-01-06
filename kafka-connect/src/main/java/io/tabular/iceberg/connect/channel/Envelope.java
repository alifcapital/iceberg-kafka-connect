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

import io.tabular.iceberg.connect.events.EventType;
import org.apache.iceberg.connect.events.Event;

/**
 * Wrapper for control topic messages. Contains either an Iceberg Event or a local Event.
 */
public class Envelope {
  private final Event event;
  private final io.tabular.iceberg.connect.events.Event localEvent;
  private final int partition;
  private final long offset;

  /** Constructor for Iceberg Events (DataWritten, DataComplete, etc.) */
  public Envelope(Event event, int partition, long offset) {
    this(event, null, partition, offset);
  }

  /** Constructor for local Events (DATA_OFFSETS, legacy events before conversion) */
  public Envelope(io.tabular.iceberg.connect.events.Event localEvent, int partition, long offset) {
    this(null, localEvent, partition, offset);
  }

  private Envelope(
      Event event,
      io.tabular.iceberg.connect.events.Event localEvent,
      int partition,
      long offset) {
    this.event = event;
    this.localEvent = localEvent;
    this.partition = partition;
    this.offset = offset;
  }

  /** Returns the Iceberg Event, or null if this is a local event. */
  public Event event() {
    return event;
  }

  /** Returns the local Event, or null if this is an Iceberg event. */
  public io.tabular.iceberg.connect.events.Event localEvent() {
    return localEvent;
  }

  /** Returns true if this envelope contains a local event (not an Iceberg event). */
  public boolean isLocalEvent() {
    return localEvent != null;
  }

  /** Returns the local event type, or null if this is an Iceberg event. */
  public EventType localEventType() {
    return localEvent != null ? localEvent.type() : null;
  }

  /** Returns the groupId from either the Iceberg event or local event. */
  public String groupId() {
    if (localEvent != null) {
      return localEvent.groupId();
    }
    return event != null ? event.groupId() : null;
  }

  public int partition() {
    return partition;
  }

  public long offset() {
    return offset;
  }
}
