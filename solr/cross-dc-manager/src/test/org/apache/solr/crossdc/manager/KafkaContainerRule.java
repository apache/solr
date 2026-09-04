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

package org.apache.solr.crossdc.manager;

import java.lang.invoke.MethodHandles;
import org.junit.Assume;
import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * JUnit rule that manages a single Kafka broker Testcontainer. Declare as a {@code @ClassRule} so
 * the (expensive to start) broker is shared across all {@code @Test} methods in a class instead of
 * being restarted for each one; JUnit starts it before, and stops it after, the whole class runs.
 *
 * <p>Skips the calling test (via {@link Assume}) instead of failing outright if
 * Docker/Testcontainers isn't available in this environment.
 */
public class KafkaContainerRule extends ExternalResource {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final String KAFKA_DOCKER_IMAGE = "apache/kafka:4.3.1";

  private KafkaContainer kafkaContainer;

  @Override
  protected void before() {
    Assume.assumeTrue(
        "Docker/Testcontainers not available; skipping test",
        DockerClientFactory.instance().isDockerAvailable());
    kafkaContainer = new KafkaContainer(DockerImageName.parse(KAFKA_DOCKER_IMAGE));
    kafkaContainer.start();
  }

  @Override
  protected void after() {
    if (kafkaContainer != null) {
      try {
        kafkaContainer.stop();
      } catch (Exception e) {
        log.error("Exception stopping Kafka container", e);
      } finally {
        kafkaContainer = null;
      }
    }
  }

  public String getBootstrapServers() {
    return kafkaContainer.getBootstrapServers();
  }
}
