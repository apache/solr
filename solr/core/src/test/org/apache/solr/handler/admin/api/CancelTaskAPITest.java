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

import org.apache.lucene.search.TotalHitCountCollector;
import org.apache.solr.SolrTestCase;
import org.apache.solr.client.api.model.CancelTaskResponse;
import org.apache.solr.client.api.model.IndexType;
import org.apache.solr.client.solrj.RemoteSolrException;
import org.apache.solr.client.solrj.request.TasksApi;
import org.apache.solr.core.SolrCore;
import org.apache.solr.search.CancellableCollector;
import org.apache.solr.util.ExternalPaths;
import org.apache.solr.util.SolrJettyTestRule;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

/**
 * HTTP-level test for the {@link CancelTask} JAX-RS endpoint -- exercises real Jersey route
 * registration, DELETE dispatch, response serialization, and HTTP 404 mapping, none of which {@link
 * CancelTaskTest} (a direct in-process method call) can catch.
 */
public class CancelTaskAPITest extends SolrTestCase {

  private static final String CORE_NAME = "cancelTaskApiTestCore";

  @ClassRule public static SolrJettyTestRule solrTestRule = new SolrJettyTestRule();

  @BeforeClass
  public static void beforeClass() throws Exception {
    System.setProperty(ALLOW_PATHS_SYSPROP, ExternalPaths.SERVER_HOME.toAbsolutePath().toString());
    solrTestRule.startSolr(createTempDir());
    solrTestRule.newCollection(CORE_NAME).withConfigSet(ExternalPaths.DEFAULT_CONFIGSET).create();
  }

  @Test
  public void testCancelRunningTaskHttp() throws Exception {
    final String taskId = "cancel-task-api-test";
    try (SolrCore core = solrTestRule.getJetty().getCoreContainer().getCore(CORE_NAME)) {
      core.getCancellableQueryTracker()
          .addShardLevelActiveQuery(taskId, new CancellableCollector(new TotalHitCountCollector()));

      var req = new TasksApi.CancelRunningTask(IndexType.CORE, CORE_NAME, taskId);
      CancelTaskResponse response = req.process(solrTestRule.getSolrClient(null));

      assertEquals(0, response.responseHeader.status);
      assertEquals(CancelTaskResponse.CancellationStatus.SUCCESS, response.status);
    }
  }

  @Test
  public void testCancelNonExistentTaskHttpReturns404() {
    var req = new TasksApi.CancelRunningTask(IndexType.CORE, CORE_NAME, "does-not-exist");

    RemoteSolrException ex =
        expectThrows(
            RemoteSolrException.class, () -> req.process(solrTestRule.getSolrClient(null)));
    assertEquals("Expected 404 for non-existent task", 404, ex.code());
  }
}
