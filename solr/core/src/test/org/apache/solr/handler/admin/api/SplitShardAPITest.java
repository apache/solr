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
import org.apache.solr.client.solrj.request.ShardsApi;
import org.apache.solr.common.util.EnvUtils;
import org.apache.solr.util.ExternalPaths;
import org.apache.solr.util.SolrJettyTestRule;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

/** Tests for the split shard V2 endpoint via the generated SolrJ client. */
public class SplitShardAPITest extends SolrTestCaseJ4 {

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
  public void testSplitShardOnNonexistentCollectionReturnsError() throws Exception {
    var request = new ShardsApi.SplitShard("nonexistent_collection");
    request.setShard("shard1");

    SolrJerseyResponse response = request.process(solrTestRule.getSolrClient());
    assertNotNull("Expected error in response", response.error);
    assertNotNull("Expected error code in response", response.error.code);
  }

  @Test
  public void testSplitShardNotInCloudModeReturnsError() throws Exception {
    var request = new ShardsApi.SplitShard(DEFAULT_TEST_CORENAME);
    request.setShard("shard1");

    SolrJerseyResponse response = request.process(solrTestRule.getSolrClient());
    assertNotNull("Expected error in response when not in SolrCloud mode", response.error);
    assertNotNull("Expected error code in response", response.error.code);
  }
}
