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

import io.tabular.iceberg.connect.events.CommitCompletePayload;
import io.tabular.iceberg.connect.events.CommitReadyPayload;
import io.tabular.iceberg.connect.events.CommitRequestPayload;
import io.tabular.iceberg.connect.events.CommitResponsePayload;
import io.tabular.iceberg.connect.events.CommitTablePayload;
import io.tabular.iceberg.connect.events.EventType;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.connect.events.AvroUtil;
import org.apache.iceberg.connect.events.CommitComplete;
import org.apache.iceberg.connect.events.CommitToTable;
import org.apache.iceberg.connect.events.DataComplete;
import org.apache.iceberg.connect.events.DataWritten;
import org.apache.iceberg.connect.events.Event;
import org.apache.iceberg.connect.events.Payload;
import org.apache.iceberg.connect.events.StartCommit;
import org.apache.iceberg.connect.events.TableReference;
import org.apache.iceberg.connect.events.TopicPartitionOffset;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

/**
 * Decodes events from the control topic. This class handles two types of events:
 *
 * <p>1. Standard Iceberg events (DataWritten, DataComplete, etc.) - decoded using AvroUtil.decode()
 * with a fallback to legacy 1.4.x format decoding (deprecated).
 *
 * <p>2. DATA_OFFSETS events - local events used to track data topic partition offsets per
 * table. These are actively used and decoded using the local Event format.
 *
 * <p>The legacy event decoding (for types other than DATA_OFFSETS) is deprecated and exists only
 * to handle messages left on the control topic during upgrades from Iceberg 1.4.x to 1.5.x.
 */
public class EventDecoder {

  private final String catalogName;

  public EventDecoder(String catalogName) {
    this.catalogName = catalogName;
  }

  /**
   * Decodes an event from bytes and wraps it in an Envelope.
   *
   * @param value the raw event bytes
   * @param partition the control topic partition
   * @param offset the control topic offset
   * @return Envelope containing either an Iceberg Event or a local Event, or null if decoding fails
   */
  public Envelope decode(byte[] value, int partition, long offset) {
    try {
      Event event = AvroUtil.decode(value);
      return new Envelope(event, partition, offset);
    } catch (SchemaParseException exception) {
      // Try to decode as local event format (DATA_OFFSETS or legacy)
      io.tabular.iceberg.connect.events.Event localEvent =
          io.tabular.iceberg.connect.events.Event.decode(value);

      if (localEvent.type() == EventType.DATA_OFFSETS) {
        // DATA_OFFSETS is actively used - return as local event
        return new Envelope(localEvent, partition, offset);
      }

      // Legacy event - convert to Iceberg Event (deprecated path)
      Event convertedEvent = convertLegacy(localEvent);
      if (convertedEvent == null) {
        return null;
      }
      return new Envelope(convertedEvent, partition, offset);
    }
  }

  /**
   * @deprecated Use {@link #decode(byte[], int, long)} instead. This method is kept for backward
   *     compatibility and test purposes.
   */
  @Deprecated
  public Event decodeEvent(byte[] value) {
    try {
      return AvroUtil.decode(value);
    } catch (SchemaParseException exception) {
      io.tabular.iceberg.connect.events.Event event =
          io.tabular.iceberg.connect.events.Event.decode(value);
      return convertLegacy(event);
    }
  }

  private Event convertLegacy(io.tabular.iceberg.connect.events.Event event) {
    Payload payload = convertPayload(event.payload());
    if (payload == null) {
      return null;
    }
    return new Event(event.groupId(), payload);
  }

  private Payload convertPayload(io.tabular.iceberg.connect.events.Payload payload) {
    if (payload instanceof CommitRequestPayload) {
      CommitRequestPayload pay = (CommitRequestPayload) payload;
      return new StartCommit(pay.commitId());
    } else if (payload instanceof CommitResponsePayload) {
      CommitResponsePayload pay = (CommitResponsePayload) payload;
      return convertCommitResponse(pay);
    } else if (payload instanceof CommitReadyPayload) {
      CommitReadyPayload pay = (CommitReadyPayload) payload;
      List<io.tabular.iceberg.connect.events.TopicPartitionOffset> legacyTPO = pay.assignments();
      List<TopicPartitionOffset> converted =
          legacyTPO.stream()
              .map(
                  t ->
                      new TopicPartitionOffset(
                          t.topic(),
                          t.partition(),
                          t.offset(),
                          t.timestamp() == null
                              ? null
                              : OffsetDateTime.ofInstant(
                                  Instant.ofEpochMilli(t.timestamp()), ZoneOffset.UTC)))
              .collect(Collectors.toList());
      return new DataComplete(pay.commitId(), converted);
    } else if (payload instanceof CommitTablePayload) {
      CommitTablePayload pay = (CommitTablePayload) payload;
      return new CommitToTable(
          pay.commitId(),
          TableReference.of(catalogName, pay.tableName().toIdentifier()),
          pay.snapshotId(),
          pay.vtts() == null ? null : OffsetDateTime.ofInstant(Instant.ofEpochMilli(pay.vtts()), ZoneOffset.UTC));
    } else if (payload instanceof CommitCompletePayload) {
      CommitCompletePayload pay = (CommitCompletePayload) payload;
      return new CommitComplete(
          pay.commitId(),
          pay.vtts() == null ? null : OffsetDateTime.ofInstant(Instant.ofEpochMilli(pay.vtts()), ZoneOffset.UTC));
    } else {
      throw new IllegalStateException(
          String.format("Unknown event payload: %s", payload.getSchema()));
    }
  }

  private Payload convertCommitResponse(CommitResponsePayload payload) {
    List<DataFile> dataFiles = payload.dataFiles();
    List<DeleteFile> deleteFiles = payload.deleteFiles();
    if (dataFiles.isEmpty() && deleteFiles.isEmpty()) {
      return null;
    }

    String target = (dataFiles.isEmpty()) ? "deleteFiles" : "dataFiles";
    List<Schema.Field> fields =
        payload.getSchema().getField(target).schema().getTypes().stream()
            .filter(s -> s.getType() != Schema.Type.NULL)
            .findFirst()
            .get()
            .getElementType()
            .getField("partition")
            .schema()
            .getFields();

    List<Types.NestedField> convertedFields = Lists.newArrayListWithExpectedSize(fields.size());

    for (Schema.Field f : fields) {
      Schema fieldSchema =
          f.schema().getTypes().stream()
              .filter(s -> s.getType() != Schema.Type.NULL)
              .findFirst()
              .get();
      Type fieldType = AvroSchemaUtil.convert(fieldSchema);
      int fieldId = (int) f.getObjectProp("field-id");
      convertedFields.add(Types.NestedField.of(fieldId, f.schema().isNullable(), f.name(), fieldType));
    }

    Types.StructType convertedStructType = Types.StructType.of(convertedFields);
    return new DataWritten(
        convertedStructType,
        payload.commitId(),
        TableReference.of(catalogName, payload.tableName().toIdentifier()),
        payload.dataFiles(),
        payload.deleteFiles());
  }
}
