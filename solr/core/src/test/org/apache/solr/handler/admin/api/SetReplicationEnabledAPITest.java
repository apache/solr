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

import org.apache.solr.SolrTestCase;
import org.apache.solr.client.solrj.request.ReplicationApi;
import org.apache.solr.common.SolrException;
import org.apache.solr.util.ExternalPaths;
import org.apache.solr.util.SolrJettyTestRule;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

/** HTTP tests for {@code PUT /api/cores/coreName/replication} via the generated SolrJ request. */
public class SetReplicationEnabledAPITest extends SolrTestCase {

  private static final String CORE_NAME = "setReplicationEnabledApiTestCore";

  @ClassRule
  public static final SolrJettyTestRule solrTestRule = new SolrJettyTestRule();

  @BeforeClass
  public static void setupSolr() throws Exception {
    System.setProperty(ALLOW_PATHS_SYSPROP, ExternalPaths.SERVER_HOME.toAbsolutePath().toString());
    solrTestRule.startSolr(createTempDir());
    solrTestRule.newCollection(CORE_NAME).withConfigSet(ExternalPaths.DEFAULT_CONFIGSET).create();
  }

  @Test
  public void testDisableThenEnableReplication() throws Exception {
    assertEquals(Boolean.TRUE, fetchReplicationEnabledStatus());

    setReplicationEnabled(false);
    assertEquals(Boolean.FALSE, fetchReplicationEnabledStatus());

    setReplicationEnabled(true);
    assertEquals(Boolean.TRUE, fetchReplicationEnabledStatus());
  }

  @Test
  public void testMissingEnabledIsRejectedAndStateUnchanged() throws Exception {
    assertEquals(Boolean.TRUE, fetchReplicationEnabledStatus());

    var request = new ReplicationApi.SetReplicationEnabled(CORE_NAME);
    SolrException ex =
        expectThrows(
            SolrException.class, () -> request.process(solrTestRule.getSolrClient(CORE_NAME)));
    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, ex.code());

    assertEquals(Boolean.TRUE, fetchReplicationEnabledStatus());
  }

  private static void setReplicationEnabled(boolean enabled) throws Exception {
    var request = new ReplicationApi.SetReplicationEnabled(CORE_NAME);
    request.setEnabled(enabled);
    var response = request.process(solrTestRule.getSolrClient(CORE_NAME));
    assertNull(response.error);
  }

  private static Boolean fetchReplicationEnabledStatus() throws Exception {
    var request = new ReplicationApi.FetchReplicationDetails(CORE_NAME);
    return request.process(solrTestRule.getSolrClient(CORE_NAME)).details.leader.replicationEnabled;
  }
}
