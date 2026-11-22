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

package org.apache.solr.shell;

import java.lang.invoke.MethodHandles;
import org.junit.Assume;
import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.ContainerFetchException;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

/**
 * JUnit rule that manages a single Apache Tika Server Testcontainer for a shell test class, a copy
 * of the extraction module's own {@code TikaServerContainerRule} kept local to these suites.
 *
 * <p>Declare as a {@code @ClassRule}: starting the server is expensive, so it is shared across the
 * class rather than restarted per {@code @Test}.
 *
 * <p>Skips the calling class (via {@link Assume}) rather than failing where Docker is unavailable,
 * or where the daemon is running but the image cannot be fetched — offline, behind a firewall, or
 * rate-limited by the registry. That is the {@code DOCKER_UNAVAILABLE} bypass of {@code
 * test_extraction.bats}. Tika failing to come up is a real failure and is left to fail.
 *
 * <p>The host port is whatever Docker mapped, not a port derived from {@code SOLR_PORT}: a shell
 * test class owns four ports and no more, and a derived one would sit inside another class' block.
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
    try {
      tika.start();
    } catch (ContainerFetchException e) {
      // Thrown only while obtaining the image, so it separates "this machine cannot get the
      // image" from "Tika did not start", which arrives as a ContainerLaunchException and is a
      // real failure. isDockerAvailable() above proves the daemon exists, nothing more.
      tika = null;
      Assume.assumeNoException(
          "could not obtain "
              + TIKA_DOCKER_IMAGE
              + "; Docker is running, the image is not available",
          e);
    }
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
