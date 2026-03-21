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
import static org.apache.solr.security.AllowListUrlChecker.ENABLE_URL_ALLOW_LIST;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.api.model.FlexibleSolrJerseyResponse;
import org.apache.solr.client.api.model.IndexType;
import org.apache.solr.client.solrj.request.TasksApi;
import org.apache.solr.common.util.EnvUtils;
import org.apache.solr.util.ExternalPaths;
import org.apache.solr.util.SolrJettyTestRule;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

/** Integration tests for the {@link CancelTaskAPI} V2 endpoint. */
public class CancelTaskAPITest extends SolrTestCaseJ4 {

  @ClassRule public static SolrJettyTestRule solrTestRule = new SolrJettyTestRule();

  @BeforeClass
  public static void beforeTest() throws Exception {
    EnvUtils.setProperty(
        ALLOW_PATHS_SYSPROP, ExternalPaths.SERVER_HOME.toAbsolutePath().toString());
    // Disable URL allow-list checks to allow standalone shard dispatch in non-cloud mode
    EnvUtils.setProperty(ENABLE_URL_ALLOW_LIST, "false");
    solrTestRule.startSolr(createTempDir());
    solrTestRule
        .newCollection(DEFAULT_TEST_COLLECTION_NAME)
        .withConfigSet(ExternalPaths.DEFAULT_CONFIGSET)
        .create();
  }

  @Test
  public void testCancelNonExistentTask() throws Exception {
    final TasksApi.CancelTask request =
        new TasksApi.CancelTask(IndexType.CORE, DEFAULT_TEST_COLLECTION_NAME);
    request.setQueryUUID("nonexistent-uuid");

    final FlexibleSolrJerseyResponse response =
        request.process(solrTestRule.getSolrClient(DEFAULT_TEST_COLLECTION_NAME));
    assertNotNull(response);
    assertEquals("not found", response.unknownProperties().get("cancellationResult"));
  }
}
