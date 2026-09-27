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
package org.apache.solr.handler.admin.api;

import org.apache.solr.SolrTestCase;
import org.apache.solr.client.solrj.request.SystemApi;
import org.apache.solr.util.SolrJettyTestRule;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

/** HTTP coverage for the node thread dump through the generated SolrJ client. */
public class NodeThreadsAPITest extends SolrTestCase {

  @ClassRule public static final SolrJettyTestRule solrTestRule = new SolrJettyTestRule();

  @BeforeClass
  public static void setupSolr() throws Exception {
    solrTestRule.startSolr(createTempDir());
  }

  @Test
  public void testThreadDump() throws Exception {
    final var response = new SystemApi.GetThreadDump().process(solrTestRule.getAdminClient());

    assertNotNull(response.responseHeader);
    assertEquals(0, response.responseHeader.status);
    assertNull(response.error);
    assertTrue(response.system.threadCount.current > 0);
    assertFalse(response.system.threadDump.isEmpty());
    final var currentThread =
        response.system.threadDump.stream()
            .map(entry -> entry.thread)
            .filter(thread -> thread.id == Thread.currentThread().threadId())
            .findFirst()
            .orElseThrow();
    assertEquals(Thread.currentThread().getName(), currentThread.name);
    assertFalse(currentThread.stackTrace.isEmpty());
  }
}
