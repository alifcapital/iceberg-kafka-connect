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

import java.util.HashMap;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

public class CoordinatorWatermarkPropsTest {

  @Test
  public void inheritsSchemaRegistryUrlFromValueConverter() {
    Map<String, Object> props = new HashMap<>();
    Map<String, String> source =
        ImmutableMap.of(
            "bootstrap.servers", "host:9092",
            "value.converter.schema.registry.url", "http://sr:8081",
            "value.converter.basic.auth.credentials.source", "USER_INFO",
            "value.converter.basic.auth.user.info", "key:secret");

    Coordinator.inheritFromValueConverter(props, source);

    assertThat(props)
        .containsEntry("schema.registry.url", "http://sr:8081")
        .containsEntry("basic.auth.credentials.source", "USER_INFO")
        .containsEntry("basic.auth.user.info", "key:secret");
  }

  @Test
  public void doesNotOverrideExplicitIcebergKafkaValue() {
    Map<String, Object> props = new HashMap<>();
    props.put("schema.registry.url", "http://override:8081");

    Map<String, String> source =
        ImmutableMap.of("value.converter.schema.registry.url", "http://worker-fallback:8081");

    Coordinator.inheritFromValueConverter(props, source);

    assertThat(props).containsEntry("schema.registry.url", "http://override:8081");
  }

  @Test
  public void ignoresValueConverterClassItself() {
    Map<String, Object> props = new HashMap<>();
    Map<String, String> source =
        ImmutableMap.of("value.converter", "io.confluent.connect.avro.AvroConverter");

    Coordinator.inheritFromValueConverter(props, source);

    // No empty-key insertion; the bare "value.converter" without trailing dot is skipped.
    assertThat(props).isEmpty();
  }

  @Test
  public void ignoresKeysWithoutValueConverterPrefix() {
    Map<String, Object> props = new HashMap<>();
    Map<String, String> source =
        ImmutableMap.of(
            "key.converter.schema.registry.url", "http://key-sr:8081",
            "offset.storage.topic", "connect-offsets");

    Coordinator.inheritFromValueConverter(props, source);

    assertThat(props).isEmpty();
  }
}
