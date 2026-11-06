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

package org.apache.solr.util;

import com.github.dockerjava.api.command.PullImageResultCallback;
import java.lang.invoke.MethodHandles;
import java.util.concurrent.TimeUnit;
import org.junit.Assume;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.DockerClientFactory;

/** Utility methods for Testcontainers-based tests. */
public final class TestcontainersUtil {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final String S390X = "s390x";
  private static final long S390X_PULL_TIMEOUT_MINUTES = 5;

  private TestcontainersUtil() {}

  /**
   * Asserts that Docker/Testcontainers is available in the current environment and that the
   * specified image has a platform variant compatible with the current architecture (specifically
   * verifying on s390x). If either check fails, the calling test is skipped via {@link Assume}.
   *
   * @param dockerImage the Docker image to check
   */
  public static void assumeContainerAvailable(String dockerImage) {
    Assume.assumeTrue(
        "Docker/Testcontainers not available; skipping test",
        DockerClientFactory.instance().isDockerAvailable());
    if (S390X.equalsIgnoreCase(System.getProperty("os.arch"))) {
      Assume.assumeTrue(
          dockerImage + " has no s390x variant; skipping test", hasS390xVariant(dockerImage));
    }
  }

  /**
   * Whether the image can be pulled for s390x, i.e. its registry manifest lists that platform.
   *
   * @param dockerImage the Docker image name
   * @return true if the image can be pulled for s390x, false otherwise
   */
  public static boolean hasS390xVariant(String dockerImage) {
    try {
      return DockerClientFactory.instance()
          .client()
          .pullImageCmd(dockerImage)
          .withPlatform("linux/" + S390X)
          .exec(new PullImageResultCallback())
          .awaitCompletion(S390X_PULL_TIMEOUT_MINUTES, TimeUnit.MINUTES);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return false;
    } catch (RuntimeException e) {
      log.info("Could not pull {} for {}: {}", dockerImage, S390X, e.toString());
      return false;
    }
  }
}
