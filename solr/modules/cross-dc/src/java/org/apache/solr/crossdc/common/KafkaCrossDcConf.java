/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.crossdc.common;

import static org.apache.solr.crossdc.common.SensitivePropRedactionUtils.redactPropertyIfNecessary;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs;
import org.apache.solr.common.util.CollectionUtil;

public class KafkaCrossDcConf extends CrossDcConf {

  public static final String DEFAULT_BATCH_SIZE_BYTES = "2097152";
  public static final String DEFAULT_BUFFER_MEMORY_BYTES = "536870912";
  public static final String DEFAULT_LINGER_MS = "30";
  public static final String DEFAULT_REQUEST_TIMEOUT = "60000";
  public static final String DEFAULT_MAX_REQUEST_SIZE = "5242880";
  public static final String DEFAULT_ENABLE_DATA_COMPRESSION = "none";
  private static final String DEFAULT_INDEX_UNMIRRORABLE_DOCS = "false";
  public static final String DEFAULT_SLOW_SEND_THRESHOLD = "1000";
  // by default, we control retries with DELIVERY_TIMEOUT_MS_DOC
  public static final String DEFAULT_NUM_RETRIES = null;
  private static final String DEFAULT_RETRY_BACKOFF_MS = "500";

  private static final String DEFAULT_CONSUMER_PROCESSING_THREADS = "5";

  private static final String DEFAULT_DELIVERY_TIMEOUT_MS = "120000";

  public static final String DEFAULT_MAX_POLL_RECORDS = "500"; // same default as Kafka

  private static final String DEFAULT_FETCH_MIN_BYTES = "1";
  private static final String DEFAULT_FETCH_MAX_WAIT_MS = "500"; // Kafka default is 500

  public static final String DEFAULT_FETCH_MAX_BYTES = "100663296";

  public static final String DEFAULT_MAX_PARTITION_FETCH_BYTES = "1048576";

  public static final String DEFAULT_MAX_POLL_INTERVAL_MS = "90000";

  public static final String DEFAULT_SESSION_TIMEOUT_MS = "10000";

  public static final String DEFAULT_PORT = "8090";

  private static final String DEFAULT_GROUP_ID = "SolrCrossDCManager";

  private static final String DEFAULT_MIRROR_COLLECTIONS = "";

  private static final String DEFAULT_MIRROR_COMMITS = "false";

  private static final String DEFAULT_EXPAND_DBQ = ExpandDbq.EXPAND.name();

  private static final String DEFAULT_COLLAPSE_UPDATES = CollapseUpdates.PARTIAL.name();

  private static final String DEFAULT_MAX_COLLAPSE_RECORDS = "500";

  public static final String TOPIC_NAME = "solr.crossdc.topicName";

  public static final String DLQ_TOPIC_NAME = "solr.crossdc.dlqTopicName";

  public static final String MAX_ATTEMPTS = "solr.crossdc.maxAttempts";

  public static final String BOOTSTRAP_SERVERS = "solr.crossdc.bootstrapServers";

  public static final String BATCH_SIZE_BYTES = "solr.crossdc.batchSizeBytes";

  public static final String BUFFER_MEMORY_BYTES = "solr.crossdc.bufferMemoryBytes";

  public static final String LINGER_MS = "solr.crossdc.lingerMs";

  public static final String REQUEST_TIMEOUT_MS = "solr.crossdc.requestTimeoutMS";

  public static final String MAX_REQUEST_SIZE_BYTES = "solr.crossdc.maxRequestSizeBytes";

  public static final String ENABLE_DATA_COMPRESSION = "solr.crossdc.enableDataCompression";

  public static final String INDEX_UNMIRRORABLE_DOCS = "solr.crossdc.indexUnmirrorableDocs";

  public static final String SLOW_SUBMIT_THRESHOLD_MS = "solr.crossdc.slowSubmitThresholdMs";

  public static final String NUM_RETRIES = "solr.crossdc.numRetries";

  public static final String RETRY_BACKOFF_MS = "solr.crossdc.retryBackoffMs";

  public static final String CONSUMER_PROCESSING_THREADS = "solr.crossdc.consumerProcessingThreads";

  public static final String DELIVERY_TIMEOUT_MS = "solr.crossdc.deliveryTimeoutMS";

  public static final String FETCH_MIN_BYTES = "solr.crossdc.fetchMinBytes";

  public static final String FETCH_MAX_WAIT_MS = "solr.crossdc.fetchMaxWaitMS";

  public static final String MAX_POLL_RECORDS = "solr.crossdc.maxPollRecords";

  public static final String FETCH_MAX_BYTES = "solr.crossdc.fetchMaxBytes";

  /**
   * The maximum delay between invocations of {@code poll()} when using consumer group management.
   * This places an upper bound on the amount of time that the consumer can be idle before fetching
   * more records. If {@code poll()} is not called before expiration of this timeout, then the
   * consumer is considered failed and the group will rebalance in order to reassign the partitions
   * to another member. For consumers using a non-null {@code group.instance.id} which reach this
   * timeout, partitions will not be immediately reassigned. Instead, the consumer will stop sending
   * heartbeats and partitions will be reassigned after expiration of {@code session.timeout.ms}.
   * This mirrors the behavior of a static consumer which has shutdown.
   */
  public static final String MAX_POLL_INTERVAL_MS = "solr.crossdc.maxPollIntervalMs";

  public static final String SESSION_TIMEOUT_MS = "solr.crossdc.sessionTimeoutMs";

  public static final String MAX_PARTITION_FETCH_BYTES = "solr.crossdc.maxPartitionFetchBytes";

  public static final String ZK_CONNECT_STRING = "solr.crossdc.zkConnectString";

  public static final String MIRROR_COLLECTIONS = "solr.crossdc.mirrorCollections";

  public static final String MIRROR_COMMITS = "solr.crossdc.mirrorCommits";

  public static final List<ConfigProperty> CONFIG_PROPERTIES;
  private static final Map<String, ConfigProperty> CONFIG_PROPERTIES_MAP;

  public static final List<ConfigProperty> SECURITY_CONFIG_PROPERTIES;

  public static final String PORT = "solr.crossdc.manager.port";

  public static final String GROUP_ID = "solr.crossdc.groupId";

  static {
    List<ConfigProperty> configProperties =
        new ArrayList<>(
            List.of(
                new ConfigProperty(TOPIC_NAME),
                new ConfigProperty(DLQ_TOPIC_NAME),
                new ConfigProperty(MAX_ATTEMPTS, "3"),
                new ConfigProperty(BOOTSTRAP_SERVERS),
                new ConfigProperty(BATCH_SIZE_BYTES, DEFAULT_BATCH_SIZE_BYTES),
                new ConfigProperty(BUFFER_MEMORY_BYTES, DEFAULT_BUFFER_MEMORY_BYTES),
                new ConfigProperty(LINGER_MS, DEFAULT_LINGER_MS),
                new ConfigProperty(REQUEST_TIMEOUT_MS, DEFAULT_REQUEST_TIMEOUT),
                new ConfigProperty(MAX_REQUEST_SIZE_BYTES, DEFAULT_MAX_REQUEST_SIZE),
                new ConfigProperty(ENABLE_DATA_COMPRESSION, DEFAULT_ENABLE_DATA_COMPRESSION),
                new ConfigProperty(INDEX_UNMIRRORABLE_DOCS, DEFAULT_INDEX_UNMIRRORABLE_DOCS),
                new ConfigProperty(SLOW_SUBMIT_THRESHOLD_MS, DEFAULT_SLOW_SEND_THRESHOLD),
                new ConfigProperty(NUM_RETRIES, DEFAULT_NUM_RETRIES),
                new ConfigProperty(RETRY_BACKOFF_MS, DEFAULT_RETRY_BACKOFF_MS),
                new ConfigProperty(DELIVERY_TIMEOUT_MS, DEFAULT_DELIVERY_TIMEOUT_MS),

                // Consumer only zkConnectString
                new ConfigProperty(ZK_CONNECT_STRING, null),
                new ConfigProperty(FETCH_MIN_BYTES, DEFAULT_FETCH_MIN_BYTES),
                new ConfigProperty(FETCH_MAX_BYTES, DEFAULT_FETCH_MAX_BYTES),
                new ConfigProperty(FETCH_MAX_WAIT_MS, DEFAULT_FETCH_MAX_WAIT_MS),
                new ConfigProperty(
                    CONSUMER_PROCESSING_THREADS, DEFAULT_CONSUMER_PROCESSING_THREADS),
                new ConfigProperty(MAX_POLL_INTERVAL_MS, DEFAULT_MAX_POLL_INTERVAL_MS),
                new ConfigProperty(SESSION_TIMEOUT_MS, DEFAULT_SESSION_TIMEOUT_MS),
                new ConfigProperty(MIRROR_COLLECTIONS, DEFAULT_MIRROR_COLLECTIONS),
                new ConfigProperty(MIRROR_COMMITS, DEFAULT_MIRROR_COMMITS),
                new ConfigProperty(EXPAND_DBQ, DEFAULT_EXPAND_DBQ),
                new ConfigProperty(COLLAPSE_UPDATES, DEFAULT_COLLAPSE_UPDATES),
                new ConfigProperty(MAX_COLLAPSE_RECORDS, DEFAULT_MAX_COLLAPSE_RECORDS),
                new ConfigProperty(MAX_PARTITION_FETCH_BYTES, DEFAULT_MAX_PARTITION_FETCH_BYTES),
                new ConfigProperty(MAX_POLL_RECORDS, DEFAULT_MAX_POLL_RECORDS),
                new ConfigProperty(PORT, DEFAULT_PORT),
                new ConfigProperty(GROUP_ID, DEFAULT_GROUP_ID)));

    SECURITY_CONFIG_PROPERTIES =
        List.of(
            new ConfigProperty(SslConfigs.SSL_PROTOCOL_CONFIG),
            new ConfigProperty(SslConfigs.SSL_PROVIDER_CONFIG),
            new ConfigProperty(SslConfigs.SSL_CIPHER_SUITES_CONFIG),
            new ConfigProperty(SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG),
            new ConfigProperty(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG),
            new ConfigProperty(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG),
            new ConfigProperty(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG),
            new ConfigProperty(SslConfigs.SSL_KEY_PASSWORD_CONFIG),
            new ConfigProperty(SslConfigs.SSL_KEYSTORE_KEY_CONFIG),
            new ConfigProperty(SslConfigs.SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG),
            new ConfigProperty(SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_CONFIG),
            new ConfigProperty(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG),
            new ConfigProperty(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG),
            new ConfigProperty(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG),
            new ConfigProperty(SslConfigs.SSL_KEYMANAGER_ALGORITHM_CONFIG),
            new ConfigProperty(SslConfigs.SSL_TRUSTMANAGER_ALGORITHM_CONFIG),
            new ConfigProperty(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG),
            new ConfigProperty(SslConfigs.SSL_SECURE_RANDOM_IMPLEMENTATION_CONFIG),
            new ConfigProperty(BrokerSecurityConfigs.SSL_CLIENT_AUTH_CONFIG),

            // From Common and Admin Client Security
            new ConfigProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG),
            new ConfigProperty(AdminClientConfig.SECURITY_PROVIDERS_CONFIG));

    configProperties.addAll(SECURITY_CONFIG_PROPERTIES);
    CONFIG_PROPERTIES = Collections.unmodifiableList(configProperties);

    Map<String, ConfigProperty> configPropertiesMap =
        CollectionUtil.newHashMap(CONFIG_PROPERTIES.size());
    for (ConfigProperty prop : CONFIG_PROPERTIES) {
      configPropertiesMap.put(prop.getKey(), prop);
    }
    CONFIG_PROPERTIES_MAP = configPropertiesMap;
  }

  private final Map<String, Object> properties;

  public KafkaCrossDcConf(Map<String, Object> properties) {
    List<String> nullValueKeys = new ArrayList<String>();
    properties.forEach(
        (k, v) -> {
          if (v == null) {
            nullValueKeys.add(k);
          }
        });
    nullValueKeys.forEach(properties::remove);
    this.properties = properties;
  }

  public static void addSecurityProps(KafkaCrossDcConf conf, Properties kafkaConsumerProps) {
    for (ConfigProperty property : SECURITY_CONFIG_PROPERTIES) {
      String val = conf.get(property.getKey());
      if (val != null) {
        kafkaConsumerProps.put(property.getKey(), val);
      }
    }
  }

  public String get(String property) {
    return CONFIG_PROPERTIES_MAP.get(property).getValue(properties);
  }

  public Integer getInt(String property) {
    ConfigProperty prop = CONFIG_PROPERTIES_MAP.get(property);
    if (prop == null) {
      throw new IllegalArgumentException("Property not found key=" + property);
    }
    return prop.getValueAsInt(properties);
  }

  public Boolean getBool(String property) {
    ConfigProperty prop = CONFIG_PROPERTIES_MAP.get(property);
    if (prop == null) {
      throw new IllegalArgumentException("Property not found key=" + property);
    }
    return prop.getValueAsBoolean(properties);
  }

  public Map<String, Object> getAdditionalProperties() {
    Map<String, Object> additional = new HashMap<>(properties);
    for (ConfigProperty configProperty : CONFIG_PROPERTIES) {
      additional.remove(configProperty.getKey());
    }
    Map<String, Object> integerProperties = new HashMap<>();
    additional.forEach(
        (key, v) -> {
          try {
            int intVal = Integer.parseInt((String) v);
            integerProperties.put(key.toString(), intVal);
          } catch (NumberFormatException ignored) {

          }
        });
    additional.putAll(integerProperties);
    return additional;
  }

  public static void readZkProps(Map<String, Object> properties, Properties zkProps) {
    Map<Object, Object> zkPropsUnprocessed = new HashMap<>(zkProps);
    for (ConfigProperty configKey : KafkaCrossDcConf.CONFIG_PROPERTIES) {
      if (properties.get(configKey.getKey()) == null
          || ((String) properties.get(configKey.getKey())).isBlank()) {
        properties.put(configKey.getKey(), zkProps.getProperty(configKey.getKey()));
        zkPropsUnprocessed.remove(configKey.getKey());
      }
    }
    zkPropsUnprocessed.forEach(
        (key, val) -> {
          if (properties.get(key) == null) {
            properties.put((String) key, val);
          }
        });
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder(128);
    for (ConfigProperty configProperty : CONFIG_PROPERTIES) {
      if (properties.get(configProperty.getKey()) != null) {
        final String printablePropertyValue =
            redactPropertyIfNecessary(
                configProperty.getKey(), String.valueOf(properties.get(configProperty.getKey())));
        sb.append(configProperty.getKey()).append("=").append(printablePropertyValue).append(",");
      }
    }
    if (sb.length() > 0) {
      sb.setLength(sb.length() - 1);
    }

    return "KafkaCrossDcConf{" + sb + "}";
  }
}
