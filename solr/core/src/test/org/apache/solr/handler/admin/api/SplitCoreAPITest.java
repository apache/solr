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

import static org.apache.solr.core.CoreContainer.ALLOW_PATHS_SYSPROP;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.api.model.SolrJerseyResponse;
import org.apache.solr.client.solrj.request.CoresApi;
import org.apache.solr.common.util.EnvUtils;
import org.apache.solr.util.ExternalPaths;
import org.apache.solr.util.SolrJettyTestRule;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

/** Tests for the split core V2 endpoint via the generated SolrJ client. */
public class SplitCoreAPITest extends SolrTestCaseJ4 {

  @ClassRule public static SolrJettyTestRule solrTestRule = new SolrJettyTestRule();

  @BeforeClass
  public static void beforeClass() throws Exception {
    EnvUtils.setProperty(
        ALLOW_PATHS_SYSPROP, ExternalPaths.SERVER_HOME.toAbsolutePath().toString());
    solrTestRule.startSolr(createTempDir());
    solrTestRule
        .newCollection(DEFAULT_TEST_CORENAME)
        .withConfigSet(ExternalPaths.DEFAULT_CONFIGSET)
        .create();
  }

  @Test
  public void testSplitWithNoTargetCoreOrPathReturnsError() throws Exception {
    var request = new CoresApi.SplitCore(DEFAULT_TEST_CORENAME);

    SolrJerseyResponse response = request.process(solrTestRule.getSolrClient());
    assertNotNull("Expected error in response", response.error);
    assertEquals(400, (int) response.error.code);
    assertTrue(
        "Expected error about missing targetCore or path",
        response.error.msg.contains("path") || response.error.msg.contains("targetCore"));
  }
}
