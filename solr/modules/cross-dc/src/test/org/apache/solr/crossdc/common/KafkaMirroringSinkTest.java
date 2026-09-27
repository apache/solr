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

import static org.apache.solr.SolrTestCaseJ4.assumeWorkingMockito;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.config.SslConfigs;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;

public class KafkaMirroringSinkTest {

  @BeforeClass
  public static void ensureWorkingMockito() {
    assumeWorkingMockito();
  }

  @Test
  @SuppressWarnings({"rawtypes", "try"})
  public void testSecurityPropsApplied() throws Exception {
    Map<String, Object> properties = new HashMap<>();
    properties.put(KafkaCrossDcConf.TOPIC_NAME, "test-topic");
    properties.put(KafkaCrossDcConf.BOOTSTRAP_SERVERS, "localhost:9092");
    properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
    properties.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, "/tmp/truststore.jks");
    properties.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, "changeit");

    KafkaCrossDcConf conf = new KafkaCrossDcConf(properties);

    // KafkaProducer/KafkaConsumer construction is mocked out so no real client or network is used
    // need to suppress "try" warning to ignore args that are unused on the surface
    try (MockedConstruction<KafkaProducer> ignoredProducer = mockConstruction(KafkaProducer.class);
        MockedConstruction<KafkaConsumer> mockedConsumer =
            mockConstruction(
                KafkaConsumer.class,
                // need this topic, too
                (mock, context) ->
                    when(mock.listTopics()).thenReturn(Map.of("test-topic", List.of())));
        MockedStatic<KafkaCrossDcConf> mockedStatic =
            mockStatic(KafkaCrossDcConf.class, CALLS_REAL_METHODS)) {

      // collect all newly created properties
      ArgumentCaptor<Properties> propsCaptor = ArgumentCaptor.forClass(Properties.class);

      // create the sink, which will create the producer and consumer
      // suppress "try" warning because of unused sink
      try (KafkaMirroringSink sink = new KafkaMirroringSink(conf)) {
        mockedStatic.verify(
            () -> KafkaCrossDcConf.addSecurityProps(same(conf), propsCaptor.capture()), times(2));
      }

      List<Properties> allProps = propsCaptor.getAllValues();
      Properties producerProps = allProps.get(0);
      Properties consumerProps = allProps.get(1);

      // check that the captured producer props and the consumer props are filled in
      assertTrue(producerProps.containsKey("key.serializer"));
      assertTrue(consumerProps.containsKey(ConsumerConfig.GROUP_ID_CONFIG));

      for (Properties props : allProps) {
        assertEquals("SSL", props.getProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG));
        assertEquals(
            "/tmp/truststore.jks", props.getProperty(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG));
        assertEquals("changeit", props.getProperty(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG));
      }
    }
  }
}
