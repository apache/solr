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

import java.util.Locale;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.api.model.SplitShardRequestBody;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.EnvUtils;
import org.apache.solr.request.SolrQueryRequestBase;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.util.ExternalPaths;
import org.apache.solr.util.SolrJettyTestRule;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

/** Tests for the JAX-RS {@link SplitShardAPI} V2 endpoint. */
public class SplitShardAPITest extends SolrTestCaseJ4 {

  @ClassRule public static SolrJettyTestRule solrTestRule = new SolrJettyTestRule();

  private SplitShardAPI api;

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

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    api =
        new SplitShardAPI(
            solrTestRule.getCoreContainer(),
            new SolrQueryRequestBase(null, new ModifiableSolrParams()),
            new SolrQueryResponse());
  }

  @Test
  public void testSplitShardWithMissingCollectionReturnsError() throws Exception {
    SplitShardRequestBody requestBody = new SplitShardRequestBody();
    requestBody.shard = "shard1";

    SolrException ex = assertThrows(SolrException.class, () -> api.splitShard(null, requestBody));
    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, ex.code());
    assertTrue("Expected error about missing collection", ex.getMessage().contains("collection"));
  }

  @Test
  public void testSplitShardNotInCloudModeReturnsError() throws Exception {
    SplitShardRequestBody requestBody = new SplitShardRequestBody();
    requestBody.shard = "shard1";

    SolrException ex =
        assertThrows(SolrException.class, () -> api.splitShard(DEFAULT_TEST_CORENAME, requestBody));
    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, ex.code());
    assertTrue(
        "Expected error about not being in SolrCloud mode",
        ex.getMessage().toLowerCase(Locale.ROOT).contains("solrcloud")
            || ex.getMessage().toLowerCase(Locale.ROOT).contains("zookeeper")
            || ex.getMessage().toLowerCase(Locale.ROOT).contains("cloud"));
  }
}
