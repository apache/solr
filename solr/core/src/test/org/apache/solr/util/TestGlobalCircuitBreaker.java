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

import org.apache.commons.exec.OS;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.util.circuitbreaker.CircuitBreakerRegistry;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

/** Tests the pluggable circuit breaker implementation. The actual tests are in base class. */
public class TestGlobalCircuitBreaker extends SolrTestCaseJ4 {
  @BeforeClass
  public static void setUpClass() throws Exception {
    System.setProperty("filterCache.enabled", "false");
    System.setProperty("queryResultCache.enabled", "false");
    System.setProperty("documentCache.enabled", "true");

    // Set a global update breaker for a low CPU, which will trip during indexing
    System.setProperty(CircuitBreakerRegistry.SYSPROP_UPDATE_LOADAVG, "0.1");

    initCore("solrconfig-basic.xml", "schema.xml");
  }

  @AfterClass
  public static void afterClass() throws Exception {
    System.clearProperty(CircuitBreakerRegistry.SYSPROP_UPDATE_LOADAVG);
    // Deregister the global breaker to not interfere with other tests
    CircuitBreakerRegistry.deregisterGlobal();
  }

  @Test
  public void testGlobalCbRegistered() {
    assertEquals(1, CircuitBreakerRegistry.listGlobal().size());
  }

  /**
   * Index some docs and see that load avg is tripped. This test will not run on Windows, as it does
   * not support load average. See <a
   * href="https://issues.apache.org/jira/browse/SOLR-17082">SOLR-17082</a>.
   */
  @Test
  public void testIndexingTripsLoadavgCb() {
    Assume.assumeFalse(OS.isFamilyWindows());
    try {
      for (int i = 0; i < 100; i++) {
        assertU(adoc("name", "john smith", "id", "1"));
        assertU(adoc("name", "johathon smith", "id", "2"));
        assertU(adoc("name", "john percival smith", "id", "3"));
        assertU(adoc("id", "1", "title", "this is a title.", "inStock_b1", "true"));
        assertU(adoc("id", "2", "title", "this is another title.", "inStock_b1", "true"));
        assertU(adoc("id", "3", "title", "Mary had a little lamb.", "inStock_b1", "false"));

        // commit inside the loop to get multiple segments to make search as realistic as possible
        assertU(commit());
      }
      fail("Should have tripped");
    } catch (SolrException e) {
      // We get a load average above 0.1, which trips the breaker
      assertEquals(SolrException.ErrorCode.TOO_MANY_REQUESTS.code, e.code());
    }
  }
}
