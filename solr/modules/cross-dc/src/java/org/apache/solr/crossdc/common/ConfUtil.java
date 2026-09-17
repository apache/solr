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

import static org.apache.solr.crossdc.common.KafkaCrossDcConf.BOOTSTRAP_SERVERS;
import static org.apache.solr.crossdc.common.KafkaCrossDcConf.TOPIC_NAME;

import java.io.ByteArrayInputStream;
import java.lang.invoke.MethodHandles;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.util.SuppressForbidden;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressForbidden(reason = "load properties from byte array")
public class ConfUtil {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final String KAFKA_ENV_PREFIX = "SOLR_CROSSDC_KAFKA_";
  public static final String KAFKA_PROP_PREFIX = "solr.crossdc.kafka.";

  public static void fillProperties(SolrZkClient solrClient, Map<String, Object> properties) {
    // fill in from environment
    Map<String, String> env = System.getenv();
    for (ConfigProperty configKey : KafkaCrossDcConf.CONFIG_PROPERTIES) {
      String val = env.get(configKey.getKey());
      if (val == null) {
        // try upper-case
        val = env.get(configKey.getKey().toUpperCase(Locale.ROOT));
      }
      if (val != null) {
        properties.put(configKey.getKey(), val);
      }
    }
    // fill in aux Kafka env with prefix
    addAdditionalKafkaProperties(properties, env, System.getProperties());

    // fill in from system properties
    for (ConfigProperty configKey : KafkaCrossDcConf.CONFIG_PROPERTIES) {
      String val = System.getProperty(configKey.getKey());
      if (val != null) {
        properties.put(configKey.getKey(), val);
      }
    }

    Properties zkProps = new Properties();
    if (solrClient != null) {
      try {
        if (solrClient.exists(
            System.getProperty(
                CrossDcConf.ZK_CROSSDC_PROPS_PATH, CrossDcConf.CROSSDC_PROPERTIES))) {
          byte[] data =
              solrClient.getData(
                  System.getProperty(
                      CrossDcConf.ZK_CROSSDC_PROPS_PATH, CrossDcConf.CROSSDC_PROPERTIES),
                  null,
                  null);

          if (data == null) {
            log.error("{} file in Zookeeper has no data", CrossDcConf.CROSSDC_PROPERTIES);
            throw new SolrException(
                SolrException.ErrorCode.SERVER_ERROR,
                CrossDcConf.CROSSDC_PROPERTIES + " file in Zookeeper has no data");
          }

          zkProps.load(new ByteArrayInputStream(data));

          KafkaCrossDcConf.readZkProps(properties, zkProps);
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        log.error("Interrupted looking for CrossDC configuration in Zookeeper", e);
        throw new SolrException(SolrException.ErrorCode.SERVICE_UNAVAILABLE, e);
      } catch (Exception e) {
        log.error("Exception looking for CrossDC configuration in Zookeeper", e);
        throw new SolrException(
            SolrException.ErrorCode.SERVER_ERROR,
            "Exception looking for CrossDC configuration in Zookeeper",
            e);
      }
    }
    // normalize any left aux properties by stripping prefixes
    if (!properties.isEmpty()) {
      Set<String> keys = new HashSet<>(properties.keySet());
      keys.forEach(
          key -> {
            Object value = properties.get(key);
            if (key.startsWith(KAFKA_ENV_PREFIX)) {
              properties.remove(key);
              putIfNonBlankAndMissing(properties, normalizeKafkaEnvKey(key), value);
            } else if (key.startsWith(KAFKA_PROP_PREFIX)) {
              properties.remove(key);
              putIfNonBlankAndMissing(properties, normalizeKafkaSysPropKey(key), value);
            }
          });
    }
  }

  // System properties override environment variables for pass-through Kafka properties.
  // Existing explicit keys in properties are preserved.
  static void addAdditionalKafkaProperties(
      Map<String, Object> properties, Map<String, String> env, Properties sysProps) {
    Set<String> envDerivedKeys = new LinkedHashSet<>();
    env.forEach(
        (key, val) -> {
          if (!key.startsWith(KAFKA_ENV_PREFIX)) {
            return;
          }
          String normalized = normalizeKafkaEnvKey(key);
          if (!isValidAdditionalProperty(normalized, val)) {
            return;
          }
          Object existingValue = properties.get(normalized);
          if (isBlankValue(existingValue)) {
            properties.put(normalized, val);
            envDerivedKeys.add(normalized);
          }
        });

    sysProps.forEach(
        (key, val) -> {
          String propKey = key.toString();
          if (!propKey.startsWith(KAFKA_PROP_PREFIX)) {
            return;
          }
          String normalized = normalizeKafkaSysPropKey(propKey);
          if (!isValidAdditionalProperty(normalized, val)) {
            return;
          }
          Object existingValue = properties.get(normalized);
          if (isBlankValue(existingValue) || envDerivedKeys.contains(normalized)) {
            properties.put(normalized, val.toString());
          }
        });
  }

  public static String normalizeKafkaEnvKey(String key) {
    if (key.startsWith(KAFKA_ENV_PREFIX)) {
      return key.substring(KAFKA_ENV_PREFIX.length()).toLowerCase(Locale.ROOT).replace('_', '.');
    } else {
      return key;
    }
  }

  public static String normalizeKafkaSysPropKey(String key) {
    if (key.startsWith(KAFKA_PROP_PREFIX)) {
      return key.substring(KAFKA_PROP_PREFIX.length()).toLowerCase(Locale.ROOT);
    } else {
      return key;
    }
  }

  private static boolean isValidAdditionalProperty(String key, Object value) {
    return key != null && !key.isBlank() && !isBlankValue(value);
  }

  private static boolean isBlankValue(Object value) {
    return value == null || (value instanceof String && ((String) value).isBlank());
  }

  private static void putIfNonBlankAndMissing(
      Map<String, Object> properties, String key, Object value) {
    if (isValidAdditionalProperty(key, value) && properties.get(key) == null) {
      properties.put(key, value);
    }
  }

  public static void verifyProperties(Map<String, Object> properties) {
    if (properties.get(BOOTSTRAP_SERVERS) == null) {
      log.error(
          "solr.crossdc.bootstrapServers not specified for producer in CrossDC configuration props={}",
          properties);
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR,
          "solr.crossdc.bootstrapServers not specified in configuration");
    }

    if (properties.get(TOPIC_NAME) == null) {
      log.error(
          "solr.crossdc.topicName not specified for producer in CrossDC configuration props={}",
          properties);
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR,
          "solr.crossdc.topicName not specified in configuration");
    }
  }
}
