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

import static java.util.stream.Collectors.toList;

import io.tabular.iceberg.connect.IcebergSinkConfig;
import io.tabular.iceberg.connect.data.Offset;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;

import org.apache.iceberg.connect.events.AvroUtil;
import org.apache.iceberg.connect.events.Event;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.Pair;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class Channel {

  private static final Logger LOG = LoggerFactory.getLogger(Channel.class);

  private final String controlTopic;
  private final String groupId;
  private final Producer<String, byte[]> producer;
  private final Consumer<String, byte[]> consumer;
  private final Admin admin;
  private final Map<Integer, Long> controlTopicOffsets = Maps.newHashMap();
  private final String producerId;

  private final EventDecoder eventDecoder;

  public Channel(
      String name,
      String consumerGroupId,
      IcebergSinkConfig config,
      KafkaClientFactory clientFactory) {
    this.controlTopic = config.controlTopic();
    this.groupId = config.controlGroupId();

    String transactionalId = name + config.transactionalSuffix();
    Pair<UUID, Producer<String, byte[]>> pair = clientFactory.createProducer(transactionalId);
    this.producer = pair.second();
    this.consumer = clientFactory.createConsumer(consumerGroupId);
    consumer.subscribe(ImmutableList.of(controlTopic));
    this.admin = clientFactory.createAdmin();
    this.producerId = pair.first().toString();
    this.eventDecoder = new EventDecoder(config.catalogName());
  }

  protected void send(Event event) {
    send(ImmutableList.of(event), ImmutableList.of(), ImmutableMap.of(), null);
  }

  protected void send(
      List<Event> events,
      List<io.tabular.iceberg.connect.events.Event> localEvents,
      Map<TopicPartition, Offset> dataOffsets,
      ConsumerGroupMetadata consumerGroupMetadata) {
    Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = Maps.newHashMap();
    dataOffsets.forEach((k, v) -> offsetsToCommit.put(k, new OffsetAndMetadata(v.offset())));

    List<ProducerRecord<String, byte[]>> recordList =
        events.stream()
            .map(
                event -> {
                  LOG.debug("Sending event of type: {}", event.type().name());
                  byte[] data = AvroUtil.encode(event);
                  // key by producer ID to keep event order
                  return new ProducerRecord<>(controlTopic, producerId, data);
                })
            .collect(toList());

    // Add local events (e.g., DATA_OFFSETS) to the same transaction
    localEvents.forEach(
        event -> {
          LOG.debug("Sending local event of type: {}", event.type().name());
          byte[] data = io.tabular.iceberg.connect.events.Event.encode(event);
          recordList.add(new ProducerRecord<>(controlTopic, producerId, data));
        });

    synchronized (producer) {
      producer.beginTransaction();
      try {
        recordList.forEach(producer::send);
        producer.flush();
        if (!dataOffsets.isEmpty()) {
          producer.sendOffsetsToTransaction(offsetsToCommit, consumerGroupMetadata);
        }
        producer.commitTransaction();
      } catch (Exception e) {
        try {
          producer.abortTransaction();
        } catch (Exception ex) {
          LOG.warn("Error aborting producer transaction", ex);
        }
        throw e;
      }
    }
  }

  protected void consumeAvailable(Duration pollDuration, Function<Envelope, Boolean> receiveFn) {
    ConsumerRecords<String, byte[]> records = consumer.poll(pollDuration);
    while (!records.isEmpty()) {
      records.forEach(
          record -> {
            // the consumer stores the offsets that corresponds to the next record to consume,
            // so increment the record offset by one
            controlTopicOffsets.put(record.partition(), record.offset() + 1);

            Envelope envelope =
                eventDecoder.decode(record.value(), record.partition(), record.offset());
            if (envelope != null && groupId.equals(envelope.groupId())) {
              String eventType =
                  envelope.isLocalEvent()
                      ? envelope.localEventType().name()
                      : envelope.event().type().name();
              LOG.debug("Received event of type: {}", eventType);
              if (receiveFn.apply(envelope)) {
                LOG.debug("Handled event of type: {}", eventType);
              }
            }
          });
      records = consumer.poll(pollDuration);
    }
  }

  protected Map<Integer, Long> controlTopicOffsets() {
    return controlTopicOffsets;
  }

  protected void commitConsumerOffsets() {
    Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = Maps.newHashMap();
    controlTopicOffsets()
        .forEach(
            (k, v) ->
                offsetsToCommit.put(new TopicPartition(controlTopic, k), new OffsetAndMetadata(v)));
    consumer.commitSync(offsetsToCommit);
  }

  protected Admin admin() {
    return admin;
  }

  public void stop() {
    LOG.info("Channel stopping");
    producer.close();
    consumer.close();
    admin.close();
  }
}
