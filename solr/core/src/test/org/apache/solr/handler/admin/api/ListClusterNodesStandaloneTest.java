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
import org.apache.solr.client.solrj.RemoteSolrException;
import org.apache.solr.client.solrj.request.ClusterApi;
import org.apache.solr.util.SolrJettyTestRule;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

/** Standalone coverage for {@code GET /api/cluster/nodes}. */
public class ListClusterNodesStandaloneTest extends SolrTestCase {

  @ClassRule public static final SolrJettyTestRule solrTestRule = new SolrJettyTestRule();

  @BeforeClass
  public static void setupSolr() throws Exception {
    solrTestRule.startSolr(createTempDir());
  }

  @Test
  public void testRequiresSolrCloud() {
    final RemoteSolrException ex =
        expectThrows(
            RemoteSolrException.class,
            () -> new ClusterApi.ListClusterNodes().process(solrTestRule.getAdminClient()));
    assertEquals(400, ex.code());
  }
}
