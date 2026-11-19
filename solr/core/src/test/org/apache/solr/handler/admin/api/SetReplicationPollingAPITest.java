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

import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.commons.io.file.PathUtils;
import org.apache.solr.SolrTestCase;
import org.apache.solr.client.solrj.RemoteSolrException;
import org.apache.solr.client.solrj.request.ReplicationApi;
import org.apache.solr.common.SolrException;
import org.apache.solr.util.ExternalPaths;
import org.apache.solr.util.SolrJettyTestRule;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

/**
 * HTTP tests for {@code PUT /api/cores/coreName/replication/polling} via the generated SolrJ
 * request.
 */
public class SetReplicationPollingAPITest extends SolrTestCase {

  private static final String CORE_NAME = "setReplicationPollingApiTestCore";
  private static final String FOLLOWER_CORE_NAME = "setReplicationPollingApiTestFollowerCore";

  @ClassRule public static final SolrJettyTestRule solrTestRule = new SolrJettyTestRule();

  @BeforeClass
  public static void setupSolr() throws Exception {
    System.setProperty(ALLOW_PATHS_SYSPROP, ExternalPaths.SERVER_HOME.toAbsolutePath().toString());
    System.setProperty("solr.security.allow.urls.enabled", "false");
    final Path solrHome = createTempDir();
    solrTestRule.startSolr(solrHome);
    solrTestRule.newCollection(CORE_NAME).withConfigSet(ExternalPaths.DEFAULT_CONFIGSET).create();

    final Path followerConfigSet = solrHome.resolve("followerConfigSet");
    PathUtils.copyDirectory(ExternalPaths.DEFAULT_CONFIGSET, followerConfigSet);
    final Path solrConfig = followerConfigSet.resolve("solrconfig.xml");
    final String followerHandler =
        "<requestHandler name=\"/replication\" class=\"solr.ReplicationHandler\">"
            + "<lst name=\"follower\"><str name=\"leaderUrl\">"
            + solrTestRule.getBaseUrl()
            + "/"
            + CORE_NAME
            + "</str><str name=\"pollInterval\">01:00:00</str></lst></requestHandler></config>";
    Files.writeString(
        solrConfig, Files.readString(solrConfig).replace("</config>", followerHandler));
    solrTestRule.newCollection(FOLLOWER_CORE_NAME).withConfigSet(followerConfigSet).create();
  }

  @Before
  public void beforeEach() throws Exception {
    setPolling(true);
  }

  @Test
  public void testDisableThenEnablePolling() throws Exception {
    assertEquals(Boolean.FALSE, fetchPollingDisabled());

    setPolling(false);
    assertEquals(Boolean.TRUE, fetchPollingDisabled());

    setPolling(true);
    assertEquals(Boolean.FALSE, fetchPollingDisabled());
  }

  @Test
  public void testMissingEnabledIsRejectedAndStateUnchanged() throws Exception {
    var request = new ReplicationApi.SetReplicationPolling(FOLLOWER_CORE_NAME);
    RemoteSolrException ex =
        expectThrows(
            RemoteSolrException.class,
            () -> request.process(solrTestRule.getSolrClient(FOLLOWER_CORE_NAME)));
    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, ex.code());

    assertEquals(Boolean.FALSE, fetchPollingDisabled());
  }

  @Test
  public void testNoFollowerIsRejected() {
    var request = new ReplicationApi.SetReplicationPolling(CORE_NAME);
    request.setEnabled(true);
    RemoteSolrException ex =
        expectThrows(
            RemoteSolrException.class,
            () -> request.process(solrTestRule.getSolrClient(CORE_NAME)));
    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, ex.code());
  }

  @Test
  public void testNullEnabledIsRejected() {
    var request = new ReplicationApi.SetReplicationPolling(FOLLOWER_CORE_NAME);
    request.setEnabled(null);
    RemoteSolrException ex =
        expectThrows(
            RemoteSolrException.class,
            () -> request.process(solrTestRule.getSolrClient(FOLLOWER_CORE_NAME)));
    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, ex.code());
  }

  private static void setPolling(boolean enabled) throws Exception {
    var request = new ReplicationApi.SetReplicationPolling(FOLLOWER_CORE_NAME);
    request.setEnabled(enabled);
    var response = request.process(solrTestRule.getSolrClient(FOLLOWER_CORE_NAME));
    assertNull(response.error);
  }

  private static Boolean fetchPollingDisabled() throws Exception {
    var request = new ReplicationApi.FetchReplicationDetails(FOLLOWER_CORE_NAME);
    return request.process(solrTestRule.getSolrClient(FOLLOWER_CORE_NAME))
        .details
        .follower
        .isPollingDisabled;
  }
}
