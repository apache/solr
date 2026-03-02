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

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.request.V2Request;
import org.apache.solr.client.solrj.response.V2Response;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.util.SolrJettyTestRule;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

/**
 * Integration tests for the {@link NodeHealthAPI} V2 endpoint using {@link SolrJettyTestRule} in
 * standalone (legacy, non-ZooKeeper) mode.
 */
public class NodeHealthAPITest2 extends SolrTestCaseJ4 {

  @ClassRule public static SolrJettyTestRule solrTestRule = new SolrJettyTestRule();

  @BeforeClass
  public static void beforeClass() throws Exception {
    solrTestRule.startSolr(createTempDir());
  }

  /**
   * Verifies that the V2 node health API returns OK in legacy (standalone) mode when {@code
   * maxGenerationLag} is not specified in the request.
   */
  @Test
  public void testLegacyMode_WithoutMaxGenerationLagReturnsOk() throws Exception {
    final V2Response response =
        new V2Request.Builder("/node/health").build().process(solrTestRule.getSolrClient(null));
    assertEquals(CommonParams.OK, response.getResponse().get(CommonParams.STATUS));
  }
}
