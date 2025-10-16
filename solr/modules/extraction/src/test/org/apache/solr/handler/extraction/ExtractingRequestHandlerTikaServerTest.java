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
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

@ThreadLeakFilters(filters = {SolrIgnoredThreadsFilter.class, QuickPatchThreadsFilter.class})
public class ExtractingRequestHandlerTikaServerTest extends ExtractingRequestHandlerTestAbstract {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static GenericContainer<?> tika;

  @BeforeClass
  @SuppressWarnings("resource")
  public static void beforeClassTika() {
    String baseUrl;
    try {
      tika =
          new GenericContainer<>("apache/tika:3.2.3.0-full")
              .withExposedPorts(9998)
              .waitingFor(Wait.forListeningPort());
      tika.start();
      baseUrl = "http://" + tika.getHost() + ":" + tika.getMappedPort(9998);
      System.setProperty("solr.test.tikaserver.url", baseUrl);
      System.setProperty("solr.test.extraction.backend", "tikaserver");
      System.setProperty("solr.test.tikaserver.metadata.compatibility", "true");
      log.info("Using extraction backend 'tikaserver'. Tika server running on {}", baseUrl);
      initCore("solrconfig.xml", "schema.xml", getFile("extraction/solr/collection1").getParent());
    } catch (Throwable t) {
      // Skip tests if Docker/Testcontainers are not available in the environment
      Assume.assumeNoException("Docker/Testcontainers not available; skipping test", t);
    }
  }

  @AfterClass
  public static void afterClassTika() {
    if (tika != null) {
      try {
        tika.stop();
      } catch (Throwable t) {
        // ignore
      } finally {
        tika = null;
      }
    }
    System.clearProperty("solr.test.tikaserver.url");
    System.clearProperty("solr.test.extraction.backend");
    System.clearProperty("solr.test.tikaserver.metadata.compatibility");
  }

  @Test
  public void testXPathWithTikaServer() throws Exception {
    // Verify extractOnly with XPath expression returns expected content
    var rsp =
        loadLocal(
            "extraction/example.html",
            ExtractingParams.XPATH_EXPRESSION,
            "/xhtml:html/xhtml:body/xhtml:a/descendant::node()",
            ExtractingParams.EXTRACT_ONLY,
            "true");
    assertNotNull("rsp is null and it shouldn't be", rsp);
    var list = rsp.getValues();
    String val = (String) list.get("example.html");
    assertEquals("News", val.trim());

    // Verify capture + xpath mapping into a field works and can be queried
    loadLocal(
        "extraction/example.html",
        "literal.id",
        "tikaxpath1",
        "captureAttr",
        "true",
        "defaultField",
        "text",
        "capture",
        "h1",
        "fmap.h1",
        "foo_t",
        "boost.foo_t",
        "3",
        "xpath",
        "/xhtml:html/xhtml:body/xhtml:cite//node()",
        "commit",
        "true");
    assertQ(req("+id:tikaxpath1 +foo_t:\"a h1 tag\""), "//*[@numFound='1']");
  }
}
