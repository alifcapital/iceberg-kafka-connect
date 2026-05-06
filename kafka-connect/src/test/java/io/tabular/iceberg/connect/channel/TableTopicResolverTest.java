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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.tabular.iceberg.connect.IcebergSinkConfig;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.junit.jupiter.api.Test;

public class TableTopicResolverTest {

  @Test
  public void resolveReturnsEmptyWhenPatternMissing() {
    Admin admin = mock(Admin.class);
    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.debeziumTransformPattern()).thenReturn(Optional.empty());
    when(config.topicsRegex()).thenReturn(Optional.empty());
    when(config.topicsList()).thenReturn(Collections.emptyList());

    TableTopicResolver resolver = new TableTopicResolver(admin, config);
    assertThat(resolver.resolve()).isEmpty();
  }

  @Test
  public void resolveAppliesPatternToLastTopicSegment() {
    Admin admin = mock(Admin.class);
    mockListTopics(admin, ImmutableSet.of("debezium_x.public.terminals", "debezium_x.public.orders"));
    mockDescribeTopics(
        admin,
        ImmutableMap.of(
            "debezium_x.public.terminals", 0,
            "debezium_x.public.orders", 0));

    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.debeziumTransformPattern()).thenReturn(Optional.of("landing_x.{table}"));
    when(config.topicsRegex()).thenReturn(Optional.of(Pattern.compile("debezium_x\\..*")));
    when(config.topicsList()).thenReturn(Collections.emptyList());

    Map<TableIdentifier, TopicPartition> result = new TableTopicResolver(admin, config).resolve();

    assertThat(result)
        .containsEntry(
            TableIdentifier.parse("landing_x.terminals"),
            new TopicPartition("debezium_x.public.terminals", 0))
        .containsEntry(
            TableIdentifier.parse("landing_x.orders"),
            new TopicPartition("debezium_x.public.orders", 0));
  }

  @Test
  public void resolveFiltersListTopicsByRegex() {
    Admin admin = mock(Admin.class);
    mockListTopics(
        admin,
        ImmutableSet.of("debezium_x.public.terminals", "other.public.x", "debezium_x.public.orders"));
    mockDescribeTopics(
        admin,
        ImmutableMap.of(
            "debezium_x.public.terminals", 0,
            "debezium_x.public.orders", 0));

    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.debeziumTransformPattern()).thenReturn(Optional.of("landing_x.{table}"));
    when(config.topicsRegex()).thenReturn(Optional.of(Pattern.compile("debezium_x\\..*")));
    when(config.topicsList()).thenReturn(Collections.emptyList());

    Map<TableIdentifier, TopicPartition> result = new TableTopicResolver(admin, config).resolve();

    assertThat(result.keySet())
        .containsExactlyInAnyOrder(
            TableIdentifier.parse("landing_x.terminals"),
            TableIdentifier.parse("landing_x.orders"));
  }

  @Test
  public void resolvePrefersTopicsListOverRegex() {
    Admin admin = mock(Admin.class);
    mockDescribeTopics(admin, ImmutableMap.of("explicit.t", 0));

    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.debeziumTransformPattern()).thenReturn(Optional.of("landing_x.{table}"));
    when(config.topicsList()).thenReturn(ImmutableList.of("explicit.t"));
    when(config.topicsRegex()).thenReturn(Optional.empty());

    Map<TableIdentifier, TopicPartition> result = new TableTopicResolver(admin, config).resolve();
    assertThat(result)
        .containsExactlyEntriesOf(
            ImmutableMap.of(TableIdentifier.parse("landing_x.t"), new TopicPartition("explicit.t", 0)));
  }

  @Test
  public void resolveHandlesTopicWithoutDots() {
    Admin admin = mock(Admin.class);
    mockListTopics(admin, ImmutableSet.of("flat_topic"));
    mockDescribeTopics(admin, ImmutableMap.of("flat_topic", 0));

    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.debeziumTransformPattern()).thenReturn(Optional.of("landing.{table}"));
    when(config.topicsRegex()).thenReturn(Optional.of(Pattern.compile(".*")));
    when(config.topicsList()).thenReturn(Collections.emptyList());

    Map<TableIdentifier, TopicPartition> result = new TableTopicResolver(admin, config).resolve();
    assertThat(result)
        .containsEntry(TableIdentifier.parse("landing.flat_topic"), new TopicPartition("flat_topic", 0));
  }

  @Test
  public void resolveSwallowsAdminException() {
    Admin admin = mock(Admin.class);
    ListTopicsResult listResult = mock(ListTopicsResult.class);
    KafkaFuture<Set<String>> failed = KafkaFuture.allOf().thenApply(v -> ImmutableSet.<String>of());
    // simulate admin failure: throw via stub returning a future that fails
    KafkaFuture<Set<String>> bad = mock(KafkaFuture.class);
    try {
      when(bad.get()).thenThrow(new RuntimeException("boom"));
    } catch (Exception e) {
      throw new AssertionError(e);
    }
    when(listResult.names()).thenReturn(bad);
    when(admin.listTopics()).thenReturn(listResult);

    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.debeziumTransformPattern()).thenReturn(Optional.of("landing.{table}"));
    when(config.topicsRegex()).thenReturn(Optional.of(Pattern.compile(".*")));
    when(config.topicsList()).thenReturn(Collections.emptyList());

    assertThat(new TableTopicResolver(admin, config).resolve()).isEmpty();
  }

  private static void mockListTopics(Admin admin, Set<String> names) {
    ListTopicsResult listResult = mock(ListTopicsResult.class);
    when(listResult.names()).thenReturn(KafkaFuture.completedFuture(names));
    when(admin.listTopics()).thenReturn(listResult);
  }

  private static void mockDescribeTopics(Admin admin, Map<String, Integer> topicToPartitionId) {
    Map<String, KafkaFuture<TopicDescription>> futures = new java.util.HashMap<>();
    topicToPartitionId.forEach(
        (topic, partition) -> {
          TopicPartitionInfo info = mock(TopicPartitionInfo.class);
          when(info.partition()).thenReturn(partition);
          TopicDescription desc =
              new TopicDescription(topic, false, ImmutableList.<TopicPartitionInfo>of(info));
          futures.put(topic, KafkaFuture.completedFuture(desc));
        });
    DescribeTopicsResult describeResult = mock(DescribeTopicsResult.class);
    when(describeResult.values()).thenReturn(futures);
    KafkaFuture<Map<String, TopicDescription>> all =
        KafkaFuture.completedFuture(
            topicToPartitionId.keySet().stream()
                .collect(
                    java.util.stream.Collectors.toMap(
                        t -> t, t -> {
                          try {
                            return futures.get(t).get();
                          } catch (Exception e) {
                            throw new RuntimeException(e);
                          }
                        })));
    when(describeResult.allTopicNames()).thenReturn(all);
    when(admin.describeTopics(anyCollection())).thenReturn(describeResult);
    when(admin.describeTopics(any(java.util.Collection.class))).thenReturn(describeResult);
  }
}
