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
package org.apache.solr.handler.extraction;

import java.lang.invoke.MethodHandles;
import org.junit.Assume;
import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

/**
 * JUnit rule that manages a single Apache Tika Server Testcontainer. Declare as a
 * {@code @ClassRule} so the (expensive to start) server is shared across all {@code @Test} methods
 * in a class instead of being restarted for each one; JUnit starts it before, and stops it after,
 * the whole class runs.
 *
 * <p>Skips the calling test (via {@link Assume}) instead of failing outright if
 * Docker/Testcontainers isn't available in this environment.
 */
public class TikaServerContainerRule extends ExternalResource {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final String TIKA_DOCKER_IMAGE = "apache/tika:3.2.3.0-full";

  private GenericContainer<?> tika;
  private String baseUrl;

  @Override
  @SuppressWarnings("resource")
  protected void before() {
    Assume.assumeFalse(
        "Skipping on s390x", "s390x".equalsIgnoreCase(System.getProperty("os.arch")));
    Assume.assumeTrue(
        "Docker/Testcontainers not available; skipping test",
        DockerClientFactory.instance().isDockerAvailable());

    tika =
        new GenericContainer<>(TIKA_DOCKER_IMAGE)
            .withExposedPorts(9998)
            .waitingFor(Wait.forListeningPort());
    tika.start();
    baseUrl = "http://" + tika.getHost() + ":" + tika.getMappedPort(9998);
  }

  @Override
  protected void after() {
    if (tika != null) {
      try {
        tika.stop();
      } catch (Exception e) {
        log.error("Exception stopping Tika container", e);
      } finally {
        tika = null;
      }
    }
  }

  public String getBaseUrl() {
    return baseUrl;
  }
}
