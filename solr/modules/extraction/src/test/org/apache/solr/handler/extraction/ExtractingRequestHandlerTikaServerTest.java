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

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import java.lang.invoke.MethodHandles;
import org.apache.lucene.tests.util.QuickPatchThreadsFilter;
import org.apache.solr.SolrIgnoredThreadsFilter;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

/** Generic tests, randomized between local and tikaserver backends */
@ThreadLeakFilters(
    filters = {
      SolrIgnoredThreadsFilter.class,
      QuickPatchThreadsFilter.class
    })
public class ExtractingRequestHandlerTikaServerTest extends ExtractingRequestHandlerTestAbstract {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @SuppressWarnings("resource")
  @ClassRule
  public static GenericContainer<?> tika =
      new GenericContainer<>("apache/tika:3.2.3.0-full")
          .withExposedPorts(9998)
          .waitingFor(Wait.forListeningPort());

  @BeforeClass
  public static void beforeClassTika() {
    String baseUrl;
    try {
      tika.start();
      baseUrl = "http://" + tika.getHost() + ":" + tika.getMappedPort(9998);
      System.setProperty("solr.test.tikaserver.url", baseUrl);
      System.setProperty("solr.test.extraction.backend", "tikaserver");
      log.info("Using extraction backend 'tikaserver'. Tika server running on {}", baseUrl);
    } catch (Throwable t) {
      // Skip tests if Docker/Testcontainers are not available in the environment
      Assume.assumeNoException("Docker/Testcontainers not available; skipping test", t);
    }
  }

  @AfterClass
  public static void afterClassTika() {
    System.clearProperty("solr.test.tikaserver.url");
    System.clearProperty("solr.test.extraction.backend");
  }
}
