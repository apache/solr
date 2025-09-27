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

import com.carrotsearch.randomizedtesting.ThreadFilter;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import java.lang.invoke.MethodHandles;
import org.apache.lucene.tests.util.QuickPatchThreadsFilter;
import org.apache.solr.SolrIgnoredThreadsFilter;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

/**
 * Generic tests, randomized between local and tikaserver backends TODO: This test still has thread
 * leaks.
 */
@ThreadLeakFilters(
    defaultFilters = true,
    filters = {
      SolrIgnoredThreadsFilter.class,
      QuickPatchThreadsFilter.class,
      ExtractingRequestHandlerTikaServerTest.TestcontainersThreadsFilter.class
    })
public class ExtractingRequestHandlerTikaServerTest extends ExtractingRequestHandlerTestAbstract {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static GenericContainer<?> tika;

  // Ignore known non-daemon threads spawned by Testcontainers and Java HttpClient in this test
  @SuppressWarnings("NewClassNamingConvention")
  public static class TestcontainersThreadsFilter implements ThreadFilter {
    @Override
    public boolean reject(Thread t) {
      if (t == null || t.getName() == null) return false;
      String n = t.getName();
      return n.startsWith("testcontainers-ryuk")
          || n.startsWith("testcontainers-wait-")
          || n.startsWith("HttpClient-")
          || n.startsWith("HttpClient-TestContainers");
    }
  }

  @BeforeClass
  public static void beforeClassTika() throws Exception {
    String baseUrl = null;
    tika =
        new GenericContainer<>("apache/tika:3.2.3.0-full")
            .withExposedPorts(9998)
            .waitingFor(Wait.forListeningPort());
    try {
      tika.start();
      baseUrl = "http://" + tika.getHost() + ":" + tika.getMappedPort(9998);
      System.setProperty("solr.test.tikaserver.url", baseUrl);
      System.setProperty("solr.test.extraction.backend", "tikaserver");
      log.info("Using extraction backend 'tikaserver'. Tika server running on {}", baseUrl);
      ExtractingRequestHandlerTestAbstract.beforeClass();
    } catch (Throwable t) {
      // Best-effort cleanup to avoid leaking resources if class initialization fails
      System.clearProperty("solr.test.tikaserver.url");
      System.clearProperty("solr.test.extraction.backend");
      // Skip tests if Docker/Testcontainers are not available in the environment
      Assume.assumeNoException("Docker/Testcontainers not available; skipping test", t);
    }
  }

  @AfterClass
  public static void afterClassTika() throws Exception {
    // TODO: There are still thread leaks after these tests, probably due to failing tests
    deleteCore();
    // Stop and dispose of the Tika container if it was started
    if (tika != null) {
      try {
        tika.stop();
      } finally {
        try {
          tika.close();
        } catch (Throwable ignore2) {
        }
        tika = null;
      }
    }
    System.clearProperty("solr.test.tikaserver.url");
    System.clearProperty("solr.test.extraction.backend");
  }
}
