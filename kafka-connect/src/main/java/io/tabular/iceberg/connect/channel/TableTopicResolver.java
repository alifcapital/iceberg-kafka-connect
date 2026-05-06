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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Derives target Iceberg-table identity from each subscribed Kafka topic by reverse-applying the
 * DebeziumTransform pattern. Knowingly coupled to the DebeziumTransform's
 * {@code cdc.target.pattern} config — used so that every commit cycle can publish a watermark per
 * table even when the table received no records this cycle (single-partition CDC topology).
 */
class TableTopicResolver {

  private static final Logger LOG = LoggerFactory.getLogger(TableTopicResolver.class);
  private static final String TABLE_PLACEHOLDER = "{table}";

  private final Admin adminClient;
  private final Optional<Pattern> topicsRegex;
  private final List<String> topicsList;
  private final Optional<String> pattern;

  TableTopicResolver(Admin adminClient, IcebergSinkConfig config) {
    this.adminClient = adminClient;
    this.topicsRegex = config.topicsRegex();
    this.topicsList = config.topicsList();
    this.pattern = config.debeziumTransformPattern();
  }

  /**
   * Resolve the current set of (table → primary topic-partition) mappings from Kafka. Empty map if
   * pattern or subscription not configured. Errors are swallowed and logged — caller treats empty
   * result as "no known tables this cycle".
   */
  Map<TableIdentifier, TopicPartition> resolve() {
    if (!pattern.isPresent()) {
      return ImmutableMap.of();
    }
    try {
      Set<String> matchedTopics = listMatchedTopics();
      if (matchedTopics.isEmpty()) {
        return ImmutableMap.of();
      }
      Map<String, TopicDescription> descriptions =
          adminClient.describeTopics(matchedTopics).allTopicNames().get();

      Map<TableIdentifier, TopicPartition> result = Maps.newHashMap();
      for (Map.Entry<String, TopicDescription> entry : descriptions.entrySet()) {
        String topic = entry.getKey();
        try {
          if (entry.getValue().partitions().isEmpty()) {
            continue;
          }
          int partition = entry.getValue().partitions().get(0).partition();
          TableIdentifier tableId = deriveTableId(topic);
          result.put(tableId, new TopicPartition(topic, partition));
        } catch (Exception e) {
          LOG.warn(
              "Failed to derive table identifier for topic '{}', skipping for watermark", topic, e);
        }
      }
      return result;
    } catch (Exception e) {
      LOG.error("Failed to resolve table→topic mapping for watermark publishing", e);
      return ImmutableMap.of();
    }
  }

  private Set<String> listMatchedTopics() throws Exception {
    if (!topicsList.isEmpty()) {
      return Sets.newHashSet(topicsList);
    }
    if (!topicsRegex.isPresent()) {
      return Collections.emptySet();
    }
    Pattern regex = topicsRegex.get();
    Set<String> all = adminClient.listTopics().names().get();
    return all.stream().filter(t -> regex.matcher(t).matches()).collect(Collectors.toSet());
  }

  private TableIdentifier deriveTableId(String topic) {
    int dotIdx = topic.lastIndexOf('.');
    String tableName = dotIdx < 0 ? topic : topic.substring(dotIdx + 1);
    String full = pattern.get().replace(TABLE_PLACEHOLDER, tableName);
    return TableIdentifier.parse(full);
  }
}
