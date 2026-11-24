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

package org.apache.solr.docker;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.carrotsearch.randomizedtesting.ThreadFilter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.testcontainers.containers.Container.ExecResult;
import org.testcontainers.containers.GenericContainer;

/** Constants and helpers shared by the Testcontainers-based Solr Docker tests. */
public final class DockerHelpers {

  /** Solr's heap inside each container. */
  static final String HEAP = "200m";

  /** The container memory limit, twice {@link #HEAP}. */
  static final long MEMORY_LIMIT_BYTES = 400L * 1024 * 1024;

  /** How long Testcontainers waits for a container's port to listen before giving up. */
  static final Duration STARTUP_TIMEOUT = Duration.ofMinutes(2);

  private DockerHelpers() {}

  /** Non-daemon threads Testcontainers and its Docker client leave behind. */
  @SuppressWarnings("NewClassNamingConvention")
  public static class TestcontainersThreadsFilter implements ThreadFilter {
    @Override
    public boolean reject(Thread t) {
      if (t == null || t.getName() == null) {
        return false;
      }
      String name = t.getName();
      return name.startsWith("testcontainers-")
          || name.startsWith("ducttape-")
          || name.startsWith("docker-java-")
          || name.startsWith("HttpClient-")
          || name.startsWith("OkHttp")
          || name.startsWith("Okio");
    }
  }

  /**
   * A property the Gradle task is responsible for; absent means the test was run some other way.
   */
  static String requireProperty(String name, String gradleTask) {
    String value = System.getProperty(name);
    if (value == null || value.isBlank()) {
      throw new IllegalStateException(
          name + " is not set. Run this through './gradlew -p solr/docker " + gradleTask + "'.");
    }
    return value;
  }

  static String imageOverride(String variable, String fallback) {
    String override = System.getenv(variable);
    return (override == null || override.isBlank()) ? fallback : override;
  }

  static ExecResult exec(GenericContainer<?> container, String... command) {
    try {
      return container.execInContainer(command);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new AssertionError(e);
    }
  }

  static void assertExec(ExecResult result) {
    assertEquals(
        "exec failed:\nstdout: " + result.getStdout() + "\nstderr: " + result.getStderr(),
        0,
        result.getExitCode());
  }

  static String firstGroup(String text, String regex) {
    Matcher matcher = Pattern.compile(regex).matcher(text);
    assertTrue("Output did not match " + regex + ": " + text, matcher.find());
    return matcher.group(1);
  }

  /** Port of {@code jq -r '.cluster.live_nodes | length'}; the array holds no nested brackets. */
  static int liveNodeCount(String clusterStatusJson) {
    String entries = firstGroup(clusterStatusJson, "\"live_nodes\"\\s*:\\s*\\[([^]]*)]").strip();
    return entries.isEmpty() ? 0 : entries.split(",").length;
  }
}
