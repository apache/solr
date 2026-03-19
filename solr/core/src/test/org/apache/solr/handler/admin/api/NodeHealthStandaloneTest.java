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

import static org.apache.solr.client.api.model.NodeHealthResponse.NodeStatus.FAILURE;
import static org.apache.solr.client.api.model.NodeHealthResponse.NodeStatus.OK;
import static org.hamcrest.Matchers.containsString;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.request.NodeApi;
import org.apache.solr.util.SolrJettyTestRule;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

public class NodeHealthStandaloneTest extends SolrTestCaseJ4 {

  @ClassRule public static SolrJettyTestRule solrTestRule = new SolrJettyTestRule();

  @BeforeClass
  public static void setupCluster() throws Exception {
    solrTestRule.startSolr(createTempDir());
  }

  @Test
  public void testStandaloneMode_WithoutMaxGenerationLagReturnsOk() throws Exception {

    final var request = new NodeApi.Healthcheck();
    final var response = request.process(solrTestRule.getAdminClient());

    assertNotNull(response);
    assertEquals(OK, response.status);
    assertThat(
        "Expected message about maxGenerationLag not being specified",
        response.message,
        containsString("maxGenerationLag isn't specified"));
  }

  @Test
  public void testStandaloneMode_WithNegativeMaxGenerationLagReturnsFailure() {
    // maxGenerationLag is a v1-only parameter: NodeHealth.healthcheck() (v2) hardcodes it to
    // null and never forwards it from request params. NodeApi.Healthcheck therefore cannot be used
    // to exercise this code path, so we call the JAX-RS implementation directly.
    // FIXME: IInteresting!  Do we have a gap?
    final var response = new NodeHealth(solrTestRule.getCoreContainer()).checkNodeHealth(null, -1);

    assertNotNull(response);
    assertEquals(FAILURE, response.status);
    assertThat(
        "Expected message about invalid maxGenerationLag",
        response.message,
        containsString("Invalid value of maxGenerationLag"));
  }
}
