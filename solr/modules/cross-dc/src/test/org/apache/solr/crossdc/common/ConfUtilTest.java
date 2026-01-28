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

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.SolrZkClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/**
 * Comprehensive unit tests for ConfUtil utility class. Tests configuration property resolution from
 * multiple sources: - Environment variables - System properties - ZooKeeper And validates the
 * correct priority and handling of custom Kafka properties.
 */
public class ConfUtilTest extends SolrTestCaseJ4 {

  @Mock private SolrZkClient mockZkClient;

  private AutoCloseable mocks;
  private Properties originalSysProps;

  @Before
  public void setUp() throws Exception {
    super.setUp();
    assumeWorkingMockito();
    mocks = MockitoAnnotations.openMocks(this);

    // Save original system properties
    originalSysProps = new Properties();
    originalSysProps.putAll(System.getProperties());
  }

  @After
  public void tearDown() throws Exception {
    super.tearDown();
    // Restore original system properties
    System.setProperties(originalSysProps);

    if (mocks != null) {
      mocks.close();
    }
  }

  @Test
  public void testFillProperties_WithStandardConfigKeys() {
    Map<String, Object> properties = new HashMap<>();

    // Set system properties with standard config keys
    System.setProperty(KafkaCrossDcConf.BOOTSTRAP_SERVERS, "localhost:9092");
    System.setProperty(KafkaCrossDcConf.TOPIC_NAME, "test-topic");
    System.setProperty(KafkaCrossDcConf.GROUP_ID, "test-group");

    ConfUtil.fillProperties(null, properties);

    assertEquals("localhost:9092", properties.get(KafkaCrossDcConf.BOOTSTRAP_SERVERS));
    assertEquals("test-topic", properties.get(KafkaCrossDcConf.TOPIC_NAME));
    assertEquals("test-group", properties.get(KafkaCrossDcConf.GROUP_ID));
  }

  @Test
  public void testFillProperties_WithKafkaPrefixSystemProperties() {
    Map<String, Object> properties = new HashMap<>();

    // Set custom Kafka properties with kafka. prefix
    System.setProperty("kafka.max.request.size", "2097152");
    System.setProperty("kafka.compression.type", "gzip");
    System.setProperty("kafka.acks", "all");

    ConfUtil.fillProperties(null, properties);

    // Verify custom Kafka properties are added with correct keys (lowercase, dots)
    assertEquals("2097152", properties.get("max.request.size"));
    assertEquals("gzip", properties.get("compression.type"));
    assertEquals("all", properties.get("acks"));
  }

  @Test
  public void testFillProperties_WithMixedCaseKafkaPrefix() {
    Map<String, Object> properties = new HashMap<>();

    // Test kafka. prefix handles case conversion correctly
    System.setProperty("kafka.SSL.Protocol", "TLSv1.2");
    System.setProperty("kafka.security.protocol", "SSL");

    ConfUtil.fillProperties(null, properties);

    // Properties should be converted to lowercase
    assertEquals("TLSv1.2", properties.get("ssl.protocol"));
    assertEquals("SSL", properties.get("security.protocol"));
  }

  @Test
  public void testFillProperties_FromZooKeeper() throws Exception {
    Map<String, Object> properties = new HashMap<>();

    // Mock ZooKeeper data
    Properties zkProps = new Properties();
    zkProps.setProperty(KafkaCrossDcConf.BOOTSTRAP_SERVERS, "zk-kafka:9092");
    zkProps.setProperty(KafkaCrossDcConf.TOPIC_NAME, "zk-topic");
    zkProps.setProperty("custom.zk.property", "zk-value");

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    zkProps.store(baos, null);
    byte[] zkData = baos.toByteArray();

    when(mockZkClient.exists(anyString())).thenReturn(true);
    when(mockZkClient.getData(anyString(), isNull(), isNull())).thenReturn(zkData);

    ConfUtil.fillProperties(mockZkClient, properties);

    // Verify ZooKeeper properties are loaded
    assertEquals("zk-kafka:9092", properties.get(KafkaCrossDcConf.BOOTSTRAP_SERVERS));
    assertEquals("zk-topic", properties.get(KafkaCrossDcConf.TOPIC_NAME));
    assertEquals("zk-value", properties.get("custom.zk.property"));
  }

  @Test
  public void testFillProperties_PriorityOrder() throws Exception {
    Map<String, Object> properties = new HashMap<>();

    // Set up ZooKeeper with lowest priority
    Properties zkProps = new Properties();
    zkProps.setProperty(KafkaCrossDcConf.BOOTSTRAP_SERVERS, "zk-kafka:9092");
    zkProps.setProperty(KafkaCrossDcConf.TOPIC_NAME, "zk-topic");

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    zkProps.store(baos, null);
    byte[] zkData = baos.toByteArray();

    when(mockZkClient.exists(anyString())).thenReturn(true);
    when(mockZkClient.getData(anyString(), isNull(), isNull())).thenReturn(zkData);

    // Set system property with higher priority
    System.setProperty(KafkaCrossDcConf.BOOTSTRAP_SERVERS, "sys-kafka:9092");

    ConfUtil.fillProperties(mockZkClient, properties);

    // System property should override ZooKeeper value
    assertEquals("sys-kafka:9092", properties.get(KafkaCrossDcConf.BOOTSTRAP_SERVERS));
    // ZooKeeper value should be used when no system property is set
    assertEquals("zk-topic", properties.get(KafkaCrossDcConf.TOPIC_NAME));
  }

  @Test
  public void testFillProperties_CustomKafkaPropertiesFromSystemProps() {
    Map<String, Object> properties = new HashMap<>();

    // Set various custom Kafka properties
    System.setProperty("kafka.batch.size", "16384");
    System.setProperty("kafka.linger.ms", "10");
    System.setProperty("kafka.buffer.memory", "33554432");
    System.setProperty("kafka.retries", "3");

    ConfUtil.fillProperties(null, properties);

    // Verify all custom properties are present with correct transformation
    assertEquals("16384", properties.get("batch.size"));
    assertEquals("10", properties.get("linger.ms"));
    assertEquals("33554432", properties.get("buffer.memory"));
    assertEquals("3", properties.get("retries"));
  }

  @Test
  public void testFillProperties_NullZkClient() {
    Map<String, Object> properties = new HashMap<>();

    System.setProperty(KafkaCrossDcConf.BOOTSTRAP_SERVERS, "localhost:9092");
    System.setProperty("kafka.compression.type", "snappy");

    // Should not throw exception with null ZK client
    ConfUtil.fillProperties(null, properties);

    assertEquals("localhost:9092", properties.get(KafkaCrossDcConf.BOOTSTRAP_SERVERS));
    assertEquals("snappy", properties.get("compression.type"));
  }

  @Test(expected = SolrException.class)
  public void testFillProperties_ZkDataNull() throws Exception {
    Map<String, Object> properties = new HashMap<>();

    when(mockZkClient.exists(anyString())).thenReturn(true);
    when(mockZkClient.getData(anyString(), isNull(), isNull())).thenReturn(null);

    // Should throw SolrException when ZK data is null
    ConfUtil.fillProperties(mockZkClient, properties);
  }

  @Test(expected = SolrException.class)
  public void testFillProperties_ZkInterrupted() throws Exception {
    Map<String, Object> properties = new HashMap<>();

    when(mockZkClient.exists(anyString())).thenReturn(true);
    when(mockZkClient.getData(anyString(), isNull(), isNull()))
        .thenThrow(new InterruptedException("Test interrupt"));

    // Should throw SolrException and set interrupt flag
    ConfUtil.fillProperties(mockZkClient, properties);
  }

  @Test(expected = SolrException.class)
  public void testFillProperties_ZkException() throws Exception {
    Map<String, Object> properties = new HashMap<>();

    when(mockZkClient.exists(anyString())).thenReturn(true);
    when(mockZkClient.getData(anyString(), isNull(), isNull()))
        .thenThrow(new RuntimeException("Test exception"));

    // Should throw SolrException wrapping the original exception
    ConfUtil.fillProperties(mockZkClient, properties);
  }

  @Test
  public void testVerifyProperties_ValidConfiguration() {
    Map<String, Object> properties = new HashMap<>();
    properties.put(KafkaCrossDcConf.BOOTSTRAP_SERVERS, "localhost:9092");
    properties.put(KafkaCrossDcConf.TOPIC_NAME, "test-topic");

    // Should not throw exception
    ConfUtil.verifyProperties(properties);
  }

  @Test(expected = SolrException.class)
  public void testVerifyProperties_MissingBootstrapServers() {
    Map<String, Object> properties = new HashMap<>();
    properties.put(KafkaCrossDcConf.TOPIC_NAME, "test-topic");

    // Should throw SolrException due to missing bootstrapServers
    ConfUtil.verifyProperties(properties);
  }

  @Test(expected = SolrException.class)
  public void testVerifyProperties_MissingTopicName() {
    Map<String, Object> properties = new HashMap<>();
    properties.put(KafkaCrossDcConf.BOOTSTRAP_SERVERS, "localhost:9092");

    // Should throw SolrException due to missing topicName
    ConfUtil.verifyProperties(properties);
  }

  @Test(expected = SolrException.class)
  public void testVerifyProperties_BothMissing() {
    Map<String, Object> properties = new HashMap<>();

    // Should throw SolrException due to missing both required properties
    ConfUtil.verifyProperties(properties);
  }

  @Test
  public void testFillProperties_EmptyProperties() {
    Map<String, Object> properties = new HashMap<>();

    // No system properties or environment variables set
    ConfUtil.fillProperties(null, properties);

    // Should complete without error, properties may be empty or contain defaults
    assertNotNull(properties);
  }

  @Test
  public void testFillProperties_SecurityProperties() throws Exception {
    Map<String, Object> properties = new HashMap<>();

    // Set security-related properties
    System.setProperty("kafka.ssl.truststore.location", "/path/to/truststore");
    System.setProperty("kafka.ssl.keystore.location", "/path/to/keystore");
    System.setProperty("kafka.security.protocol", "SSL");

    ConfUtil.fillProperties(null, properties);

    // Verify security properties are correctly transformed
    assertEquals("/path/to/truststore", properties.get("ssl.truststore.location"));
    assertEquals("/path/to/keystore", properties.get("ssl.keystore.location"));
    assertEquals("SSL", properties.get("security.protocol"));
  }

  @Test
  public void testFillProperties_ComplexScenario() throws Exception {
    Map<String, Object> properties = new HashMap<>();

    // Create a complex scenario with multiple sources
    Properties zkProps = new Properties();
    zkProps.setProperty(KafkaCrossDcConf.BOOTSTRAP_SERVERS, "zk-kafka:9092");
    zkProps.setProperty(KafkaCrossDcConf.TOPIC_NAME, "zk-topic");
    zkProps.setProperty(KafkaCrossDcConf.GROUP_ID, "zk-group");
    zkProps.setProperty("zk.only.property", "zk-value");

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    zkProps.store(baos, null);
    byte[] zkData = baos.toByteArray();

    when(mockZkClient.exists(anyString())).thenReturn(true);
    when(mockZkClient.getData(anyString(), isNull(), isNull())).thenReturn(zkData);

    // Set system properties - should override ZK
    System.setProperty(KafkaCrossDcConf.BOOTSTRAP_SERVERS, "sys-kafka:9092");
    System.setProperty("kafka.max.poll.records", "1000");
    System.setProperty("kafka.enable.auto.commit", "false");

    ConfUtil.fillProperties(mockZkClient, properties);

    // Verify priority: system props > ZK
    assertEquals("sys-kafka:9092", properties.get(KafkaCrossDcConf.BOOTSTRAP_SERVERS));
    assertEquals("zk-topic", properties.get(KafkaCrossDcConf.TOPIC_NAME));
    assertEquals("zk-group", properties.get(KafkaCrossDcConf.GROUP_ID));

    // Verify custom Kafka properties from system props
    assertEquals("1000", properties.get("max.poll.records"));
    assertEquals("false", properties.get("enable.auto.commit"));

    // Verify ZK-only property
    assertEquals("zk-value", properties.get("zk.only.property"));
  }

  // we can't easily modify envvars, test just the key conversion in properties
  @Test
  public void testUnderscoreToDotsConversion() {
    Map<String, Object> properties = new HashMap<>();

    // Test underscore to dot conversion in kafka. prefix env
    properties.put("KAFKA_MAX_POLL_RECORDS", "500");
    properties.put("KAFKA_FETCH_MAX_WAIT_MS", "1000");

    ConfUtil.fillProperties(null, properties);

    // Verify underscores are converted to dots
    assertEquals("500", properties.get("max.poll.records"));
    assertEquals("1000", properties.get("fetch.max.wait.ms"));
  }
}
