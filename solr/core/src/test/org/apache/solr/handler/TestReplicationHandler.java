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
package org.apache.solr.handler;

import static org.apache.solr.handler.ReplicationTestHelper.CONF_DIR;
import static org.apache.solr.handler.ReplicationTestHelper.SolrInstance;
import static org.apache.solr.handler.ReplicationTestHelper.assertVersions;
import static org.apache.solr.handler.ReplicationTestHelper.createNewSolrClient;
import static org.apache.solr.handler.ReplicationTestHelper.invokeReplicationCommand;
import static org.hamcrest.CoreMatchers.containsString;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.net.URL;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.Constants;
import org.apache.solr.BaseDistributedSearchTestCase;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.SolrTestCaseJ4.SuppressSSL;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.SimpleSolrResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.core.CachingDirectoryFactory;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.StandardDirectoryFactory;
import org.apache.solr.core.snapshots.SolrSnapshotMetaDataManager;
import org.apache.solr.embedded.JettySolrRunner;
import org.apache.solr.security.AllowListUrlChecker;
import org.apache.solr.util.TestInjection;
import org.apache.solr.util.TimeOut;
import org.hamcrest.MatcherAssert;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test for ReplicationHandler
 *
 * @since 1.4
 */
@LuceneTestCase.Nightly
@SuppressSSL // Currently, unknown why SSL does not work with this test
public class TestReplicationHandler extends SolrTestCaseJ4 {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final long TIMEOUT = 30000;

  JettySolrRunner leaderJetty, followerJetty, repeaterJetty;
  SolrClient leaderClient, followerClient, repeaterClient;
  SolrInstance leader = null, follower = null, repeater = null;

  static String context = "/solr";

  // number of docs to index... decremented for each test case to tell if we accidentally reuse
  // index from previous test method
  static int nDocs = 500;

  /* For testing backward compatibility, remove for 10.x */
  private static boolean useLegacyParams = false;

  @BeforeClass
  public static void beforeClass() {
    useLegacyParams = rarely();
  }

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    systemSetPropertySolrDisableUrlAllowList("true");
    //    System.setProperty("solr.directoryFactory", "solr.StandardDirectoryFactory");
    // For manual testing only
    // useFactory(null); // force an FS factory.
    leader = new SolrInstance(createTempDir("solr-instance").toFile(), "leader", null);
    leader.setUp();
    leaderJetty = createAndStartJetty(leader);
    leaderClient =
        ReplicationTestHelper.createNewSolrClient(
            buildUrl(leaderJetty.getLocalPort(), context) + "/" + DEFAULT_TEST_CORENAME);

    follower =
        new SolrInstance(
            createTempDir("solr-instance").toFile(), "follower", leaderJetty.getLocalPort());
    follower.setUp();
    followerJetty = createAndStartJetty(follower);
    followerClient =
        ReplicationTestHelper.createNewSolrClient(
            buildUrl(followerJetty.getLocalPort(), context) + "/" + DEFAULT_TEST_CORENAME);

    System.setProperty("solr.indexfetcher.sotimeout2", "45000");
  }

  public void clearIndexWithReplication() throws Exception {
    if (numFound(query("*:*", leaderClient)) != 0) {
      leaderClient.deleteByQuery("*:*");
      leaderClient.commit();
      // wait for replication to sync & verify
      assertEquals(0, numFound(rQuery(0, "*:*", followerClient)));
    }
  }

  @Override
  @After
  public void tearDown() throws Exception {
    super.tearDown();
    systemClearPropertySolrDisableUrlAllowList();
    if (null != leaderJetty) {
      leaderJetty.stop();
      leaderJetty = null;
    }
    if (null != followerJetty) {
      followerJetty.stop();
      followerJetty = null;
    }
    if (null != leaderClient) {
      leaderClient.close();
      leaderClient = null;
    }
    if (null != followerClient) {
      followerClient.close();
      followerClient = null;
    }
    System.clearProperty("solr.indexfetcher.sotimeout");
  }

  static JettySolrRunner createAndStartJetty(SolrInstance instance) throws Exception {
    return ReplicationTestHelper.createAndStartJetty(instance);
  }

  static int index(SolrClient client, Object... fields) throws Exception {
    return ReplicationTestHelper.index(client, fields);
  }

  NamedList<Object> query(String query, SolrClient s) throws SolrServerException, IOException {
    ModifiableSolrParams params = new ModifiableSolrParams();

    params.add("q", query);
    params.add("sort", "id desc");

    QueryResponse qres = s.query(params);
    return qres.getResponse();
  }

  /** will sleep up to 30 seconds, looking for expectedDocCount */
  private NamedList<Object> rQuery(int expectedDocCount, String query, SolrClient client)
      throws Exception {
    int timeSlept = 0;
    NamedList<Object> res = query(query, client);
    while (expectedDocCount != numFound(res) && timeSlept < 30000) {
      log.info("Waiting for {} docs", expectedDocCount);
      timeSlept += 100;
      Thread.sleep(100);
      res = query(query, client);
    }
    if (log.isInfoEnabled()) {
      log.info("Waited for {}ms and found {} docs", timeSlept, numFound(res));
    }
    return res;
  }

  private long numFound(NamedList<Object> res) {
    return ((SolrDocumentList) res.get("response")).getNumFound();
  }

  private NamedList<Object> getDetails(SolrClient s) throws Exception {

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("command", "details");
    params.set("_trace", "getDetails");
    params.set("qt", ReplicationHandler.PATH);
    QueryRequest req = new QueryRequest(params);

    NamedList<Object> res = s.request(req);
    assertReplicationResponseSucceeded(res);

    @SuppressWarnings("unchecked")
    NamedList<Object> details = (NamedList<Object>) res.get("details");

    assertNotNull("null details", details);

    return details;
  }

  private NamedList<Object> getIndexVersion(SolrClient s) throws Exception {

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("command", "indexversion");
    params.set("_trace", "getIndexVersion");
    params.set("qt", ReplicationHandler.PATH);
    QueryRequest req = new QueryRequest(params);

    NamedList<Object> res = s.request(req);
    assertReplicationResponseSucceeded(res);

    return res;
  }

  private void reloadCore(JettySolrRunner jettySolrRunner, String core) throws Exception {

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("action", "reload");
    params.set("core", core);
    params.set("qt", "/admin/cores");
    QueryRequest req = new QueryRequest(params);

    try (SolrClient adminClient = adminClient(jettySolrRunner)) {
      NamedList<Object> res = adminClient.request(req);
      assertNotNull("null response from server", res);
    }
  }

  private SolrClient adminClient(JettySolrRunner client) {
    String adminUrl =
        client.getBaseUrl().toString().replace("/" + DEFAULT_TEST_COLLECTION_NAME, "");
    return getHttpSolrClient(adminUrl);
  }

  @Test
  public void doTestHandlerPathUnchanged() {
    assertEquals("/replication", ReplicationHandler.PATH);
  }

  @Test
  public void testUrlAllowList() throws Exception {
    // Run another test with URL allow-list enabled and allow-list is empty.
    // Expect an exception because the leader URL is not allowed.
    systemClearPropertySolrDisableUrlAllowList();
    SolrException e = expectThrows(SolrException.class, this::doTestDetails);
    assertTrue(
        e.getMessage()
            .contains("nor in the configured '" + AllowListUrlChecker.URL_ALLOW_LIST + "'"));

    // Set the allow-list to allow the leader URL.
    // Expect the same test to pass now.
    System.setProperty(
        TEST_URL_ALLOW_LIST, leaderJetty.getBaseUrl() + "," + followerJetty.getBaseUrl());
    try {
      doTestDetails();
    } finally {
      System.clearProperty(TEST_URL_ALLOW_LIST);
    }
  }

  @Test
  public void doTestDetails() throws Exception {
    followerJetty.stop();

    follower.setTestPort(leaderJetty.getLocalPort());
    follower.copyConfigFile(CONF_DIR + "solrconfig-follower.xml", "solrconfig.xml");
    followerJetty = createAndStartJetty(follower);

    followerClient.close();
    leaderClient.close();
    leaderClient =
        ReplicationTestHelper.createNewSolrClient(
            buildUrl(leaderJetty.getLocalPort(), context) + "/" + DEFAULT_TEST_CORENAME);
    followerClient =
        ReplicationTestHelper.createNewSolrClient(
            buildUrl(followerJetty.getLocalPort(), context) + "/" + DEFAULT_TEST_CORENAME);

    clearIndexWithReplication();
    {
      NamedList<Object> details = getDetails(leaderClient);

      assertEquals("leader isLeader?", "true", details.get("isLeader"));
      assertEquals("leader isFollower?", "false", details.get("isFollower"));
      assertNotNull("leader has leader section", details.get("leader"));
    }

    // check details on the follower a couple of times before & after fetching
    for (int i = 0; i < 3; i++) {
      NamedList<Object> details = getDetails(followerClient);
      assertNotNull(i + ": " + details);
      assertNotNull(i + ": " + details.toString(), details.get("follower"));

      if (i > 0) {
        rQuery(i, "*:*", followerClient);
        List<?> replicatedAtCount =
            (List<?>) ((NamedList<?>) details.get("follower")).get("indexReplicatedAtList");
        int tries = 0;
        while ((replicatedAtCount == null || replicatedAtCount.size() < i) && tries++ < 5) {
          Thread.sleep(1000);
          details = getDetails(followerClient);
          replicatedAtCount =
              (List<?>) ((NamedList<?>) details.get("follower")).get("indexReplicatedAtList");
        }

        assertNotNull(
            "Expected to see that the follower has replicated" + i + ": " + details,
            replicatedAtCount);

        // we can have more replications than we added docs because a replication can legally fail
        // and try again (sometimes we cannot merge into a live index and have to try again)
        assertTrue(
            "i:" + i + " replicationCount:" + replicatedAtCount.size(),
            replicatedAtCount.size() >= i);
      }

      assertEquals(i + ": " + "follower isLeader?", "false", details.get("isLeader"));
      assertEquals(i + ": " + "follower isFollower?", "true", details.get("isFollower"));
      assertNotNull(i + ": " + "follower has follower section", details.get("follower"));
      // SOLR-2677: assert not false negatives
      Object timesFailed = ((NamedList) details.get("follower")).get(IndexFetcher.TIMES_FAILED);
      // SOLR-7134: we can have a fail because some mock index files have no checksum, will
      // always be downloaded, and may not be able to be moved into the existing index
      assertTrue(
          i + ": " + "follower has fetch error count: " + timesFailed,
          timesFailed == null || ((Number) timesFailed).intValue() == 1);

      if (3 != i) {
        // index & fetch
        index(leaderClient, "id", i, "name", "name = " + i);
        leaderClient.commit();
        pullFromTo(leaderJetty, followerJetty);
      }
    }

    SolrInstance repeater = null;
    JettySolrRunner repeaterJetty = null;
    SolrClient repeaterClient = null;
    try {
      repeater =
          new SolrInstance(
              createTempDir("solr-instance").toFile(), "repeater", leaderJetty.getLocalPort());
      repeater.setUp();
      repeaterJetty = createAndStartJetty(repeater);
      repeaterClient =
          ReplicationTestHelper.createNewSolrClient(
              buildUrl(repeaterJetty.getLocalPort(), context) + "/" + DEFAULT_TEST_CORENAME);

      NamedList<Object> details = getDetails(repeaterClient);

      assertEquals("repeater isLeader?", "true", details.get("isLeader"));
      assertEquals("repeater isFollower?", "true", details.get("isFollower"));
      assertNotNull("repeater has leader section", details.get("leader"));
      assertNotNull("repeater has follower section", details.get("follower"));

    } finally {
      try {
        if (repeaterJetty != null) repeaterJetty.stop();
      } catch (Exception e) {
        /* :NOOP: */
      }
      if (repeaterClient != null) repeaterClient.close();
    }
  }

  @Test
  public void testLegacyConfiguration() throws Exception {
    SolrInstance solrInstance = null;
    JettySolrRunner instanceJetty = null;
    SolrClient client = null;
    try {
      solrInstance =
          new SolrInstance(
              createTempDir("solr-instance").toFile(),
              "replication-legacy",
              leaderJetty.getLocalPort());
      solrInstance.setUp();
      instanceJetty = createAndStartJetty(solrInstance);
      client =
          ReplicationTestHelper.createNewSolrClient(
              buildUrl(instanceJetty.getLocalPort(), context) + "/" + DEFAULT_TEST_CORENAME);

      NamedList<Object> details = getDetails(client);

      assertEquals("repeater isLeader?", "true", details.get("isLeader"));
      assertEquals("repeater isFollower?", "true", details.get("isFollower"));
      assertNotNull("repeater has leader section", details.get("leader"));
      assertNotNull("repeater has follower section", details.get("follower"));

    } finally {
      if (instanceJetty != null) {
        instanceJetty.stop();
      }
      if (client != null) client.close();
    }
  }

  /**
   * Verify that empty commits and/or commits with openSearcher=false on the leader do not cause
   * subsequent replication problems on the follower
   */
  public void testEmptyCommits() throws Exception {
    clearIndexWithReplication();

    // add a doc to leader and commit
    index(leaderClient, "id", "1", "name", "empty1");
    emptyUpdate(leaderClient, "commit", "true");
    // force replication
    pullFromTo(leaderJetty, followerJetty);
    // verify doc is on follower
    rQuery(1, "name:empty1", followerClient);
    assertVersions(leaderClient, followerClient);

    // do a completely empty commit on leader and force replication
    emptyUpdate(leaderClient, "commit", "true");
    pullFromTo(leaderJetty, followerJetty);

    // add another doc and verify follower gets it
    index(leaderClient, "id", "2", "name", "empty2");
    emptyUpdate(leaderClient, "commit", "true");
    // force replication
    pullFromTo(leaderJetty, followerJetty);

    rQuery(1, "name:empty2", followerClient);
    assertVersions(leaderClient, followerClient);

    // add a third doc but don't open a new searcher on leader
    index(leaderClient, "id", "3", "name", "empty3");
    emptyUpdate(leaderClient, "commit", "true", "openSearcher", "false");
    pullFromTo(leaderJetty, followerJetty);

    // verify follower can search the doc, but leader doesn't
    rQuery(0, "name:empty3", leaderClient);
    rQuery(1, "name:empty3", followerClient);

    // final doc with hard commit, follower and leader both showing all docs
    index(leaderClient, "id", "4", "name", "empty4");
    emptyUpdate(leaderClient, "commit", "true");
    pullFromTo(leaderJetty, followerJetty);

    String q = "name:(empty1 empty2 empty3 empty4)";
    rQuery(4, q, leaderClient);
    rQuery(4, q, followerClient);
    assertVersions(leaderClient, followerClient);
  }

  @Test
  public void doTestReplicateAfterWrite2Follower() throws Exception {
    clearIndexWithReplication();
    nDocs--;
    for (int i = 0; i < nDocs; i++) {
      index(leaderClient, "id", i, "name", "name = " + i);
    }

    invokeReplicationCommand(
        buildUrl(leaderJetty.getLocalPort()) + "/" + DEFAULT_TEST_CORENAME, "disableReplication");
    invokeReplicationCommand(
        buildUrl(followerJetty.getLocalPort()) + "/" + DEFAULT_TEST_CORENAME, "disablepoll");

    leaderClient.commit();

    assertEquals(nDocs, numFound(rQuery(nDocs, "*:*", leaderClient)));

    // Make sure that both the index version and index generation on the follower is
    // higher than that of the leader, just to make the test harder.

    index(followerClient, "id", 551, "name", "name = " + 551);
    followerClient.commit(true, true);
    index(followerClient, "id", 552, "name", "name = " + 552);
    followerClient.commit(true, true);
    index(followerClient, "id", 553, "name", "name = " + 553);
    followerClient.commit(true, true);
    index(followerClient, "id", 554, "name", "name = " + 554);
    followerClient.commit(true, true);
    index(followerClient, "id", 555, "name", "name = " + 555);
    followerClient.commit(true, true);

    // this doc is added to follower so it should show an item w/ that result
    assertEquals(1, numFound(rQuery(1, "id:555", followerClient)));

    // Let's fetch the index rather than rely on the polling.
    invokeReplicationCommand(
        buildUrl(leaderJetty.getLocalPort()) + "/" + DEFAULT_TEST_CORENAME, "enablereplication");
    invokeReplicationCommand(
        buildUrl(followerJetty.getLocalPort()) + "/" + DEFAULT_TEST_CORENAME, "fetchindex");

    /*
    //the follower should have done a full copy of the index so the doc with id:555 should not be there in the follower now
    followerQueryRsp = rQuery(0, "id:555", followerClient);
    followerQueryResult = (SolrDocumentList) followerQueryRsp.get("response");
    assertEquals(0, followerQueryResult.getNumFound());

    // make sure we replicated the correct index from the leader
    followerQueryRsp = rQuery(nDocs, "*:*", followerClient);
    followerQueryResult = (SolrDocumentList) followerQueryRsp.get("response");
    assertEquals(nDocs, followerQueryResult.getNumFound());

    */
  }

  @Test
  public void doTestIndexAndConfigReplication() throws Exception {

    TestInjection.delayBeforeFollowerCommitRefresh = random().nextInt(10);

    clearIndexWithReplication();

    nDocs--;
    for (int i = 0; i < nDocs; i++) index(leaderClient, "id", i, "name", "name = " + i);

    leaderClient.commit();

    NamedList<Object> leaderQueryRsp = rQuery(nDocs, "*:*", leaderClient);
    SolrDocumentList leaderQueryResult = (SolrDocumentList) leaderQueryRsp.get("response");
    assertEquals(nDocs, numFound(leaderQueryRsp));

    // get docs from follower and check if number is equal to leader
    NamedList<Object> followerQueryRsp = rQuery(nDocs, "*:*", followerClient);
    SolrDocumentList followerQueryResult = (SolrDocumentList) followerQueryRsp.get("response");
    assertEquals(nDocs, numFound(followerQueryRsp));

    // compare results
    String cmp =
        BaseDistributedSearchTestCase.compare(leaderQueryResult, followerQueryResult, 0, null);
    assertNull(cmp);

    assertVersions(leaderClient, followerClient);

    // start config files replication test
    leaderClient.deleteByQuery("*:*");
    leaderClient.commit();

    // change the schema on leader
    leader.copyConfigFile(CONF_DIR + "schema-replication2.xml", "schema.xml");

    leaderJetty.stop();

    leaderJetty = createAndStartJetty(leader);
    leaderClient.close();
    leaderClient =
        createNewSolrClient(buildUrl(leaderJetty.getLocalPort()) + "/" + DEFAULT_TEST_CORENAME);

    follower.setTestPort(leaderJetty.getLocalPort());
    follower.copyConfigFile(follower.getSolrConfigFile(), "solrconfig.xml");

    followerJetty.stop();

    // setup an sub directory /foo/ in order to force subdir file replication
    File leaderFooDir = new File(leader.getConfDir() + File.separator + "foo");
    File leaderBarFile = new File(leaderFooDir, "bar.txt");
    assertTrue("could not make dir " + leaderFooDir, leaderFooDir.mkdirs());
    assertTrue(leaderBarFile.createNewFile());

    File followerFooDir = new File(follower.getConfDir() + File.separator + "foo");
    File followerBarFile = new File(followerFooDir, "bar.txt");
    assertFalse(followerFooDir.exists());

    followerJetty = createAndStartJetty(follower);
    followerClient.close();
    followerClient =
        createNewSolrClient(buildUrl(followerJetty.getLocalPort()) + "/" + DEFAULT_TEST_CORENAME);
    // add a doc with new field and commit on leader to trigger index fetch from follower.
    index(leaderClient, "id", "2000", "name", "name = " + 2000, "newname", "newname = " + 2000);
    leaderClient.commit();

    assertEquals(1, numFound(rQuery(1, "*:*", leaderClient)));

    followerQueryRsp = rQuery(1, "*:*", followerClient);
    assertVersions(leaderClient, followerClient);
    SolrDocument d = ((SolrDocumentList) followerQueryRsp.get("response")).get(0);
    assertEquals("newname = 2000", (String) d.getFieldValue("newname"));

    assertTrue(followerFooDir.isDirectory());
    assertTrue(followerBarFile.exists());

    checkForSingleIndex(leaderJetty);
    checkForSingleIndex(followerJetty, true);
  }

  @Test
  public void doTestStopPoll() throws Exception {
    clearIndexWithReplication();

    // Test:
    // setup leader/follower.
    // stop polling on follower, add a doc to leader and verify follower hasn't picked it.
    nDocs--;
    for (int i = 0; i < nDocs; i++) index(leaderClient, "id", i, "name", "name = " + i);

    leaderClient.commit();

    NamedList<Object> leaderQueryRsp = rQuery(nDocs, "*:*", leaderClient);
    SolrDocumentList leaderQueryResult = (SolrDocumentList) leaderQueryRsp.get("response");
    assertEquals(nDocs, numFound(leaderQueryRsp));

    // get docs from follower and check if number is equal to leader
    NamedList<Object> followerQueryRsp = rQuery(nDocs, "*:*", followerClient);
    SolrDocumentList followerQueryResult = (SolrDocumentList) followerQueryRsp.get("response");
    assertEquals(nDocs, numFound(followerQueryRsp));

    // compare results
    String cmp =
        BaseDistributedSearchTestCase.compare(leaderQueryResult, followerQueryResult, 0, null);
    assertNull(cmp);

    // start stop polling test
    invokeReplicationCommand(
        buildUrl(followerJetty.getLocalPort()) + "/" + DEFAULT_TEST_CORENAME, "disablepoll");

    index(leaderClient, "id", 501, "name", "name = " + 501);
    leaderClient.commit();

    // get docs from leader and check if number is equal to leader
    assertEquals(nDocs + 1, numFound(rQuery(nDocs + 1, "*:*", leaderClient)));

    // NOTE: this test is wierd, we want to verify it DOESNT replicate...
    // for now, add a sleep for this.., but the logic is wierd.
    Thread.sleep(3000);

    // get docs from follower and check if number is not equal to leader; polling is disabled
    assertEquals(nDocs, numFound(rQuery(nDocs, "*:*", followerClient)));

    // re-enable replication
    invokeReplicationCommand(
        buildUrl(followerJetty.getLocalPort()) + "/" + DEFAULT_TEST_CORENAME, "enablepoll");

    assertEquals(nDocs + 1, numFound(rQuery(nDocs + 1, "*:*", followerClient)));
  }

  /**
   * We assert that if leader is down for more than poll interval, the follower doesn't re-fetch the
   * whole index from leader again if the index hasn't changed. See SOLR-9036
   */
  @Test
  public void doTestIndexFetchOnLeaderRestart() throws Exception {
    useFactory(null);
    try {
      clearIndexWithReplication();
      // change solrconfig having 'replicateAfter startup' option on leader
      leader.copyConfigFile(CONF_DIR + "solrconfig-leader2.xml", "solrconfig.xml");

      leaderJetty.stop();
      leaderJetty.start();

      // close and re-create leader client because its connection pool has stale connections
      leaderClient.close();
      leaderClient =
          createNewSolrClient(buildUrl(leaderJetty.getLocalPort()) + "/" + DEFAULT_TEST_CORENAME);

      nDocs--;
      for (int i = 0; i < nDocs; i++) index(leaderClient, "id", i, "name", "name = " + i);

      leaderClient.commit();

      NamedList<Object> leaderQueryRsp = rQuery(nDocs, "*:*", leaderClient);
      SolrDocumentList leaderQueryResult = (SolrDocumentList) leaderQueryRsp.get("response");
      assertEquals(nDocs, numFound(leaderQueryRsp));

      // get docs from follower and check if number is equal to leader
      NamedList<Object> followerQueryRsp = rQuery(nDocs, "*:*", followerClient);
      SolrDocumentList followerQueryResult = (SolrDocumentList) followerQueryRsp.get("response");
      assertEquals(nDocs, numFound(followerQueryRsp));

      // compare results
      String cmp =
          BaseDistributedSearchTestCase.compare(leaderQueryResult, followerQueryResult, 0, null);
      assertNull(cmp);

      String timesReplicatedString = getFollowerDetails("timesIndexReplicated");
      String timesFailed;
      Integer previousTimesFailed = null;
      if (timesReplicatedString == null) {
        timesFailed = "0";
      } else {
        int timesReplicated = Integer.parseInt(timesReplicatedString);
        timesFailed = getFollowerDetails("timesFailed");
        if (null == timesFailed) {
          timesFailed = "0";
        }

        previousTimesFailed = Integer.parseInt(timesFailed);
        // Sometimes replication will fail because leader's core is still loading; make sure there
        // was one success
        assertEquals(1, timesReplicated - previousTimesFailed);
      }

      leaderJetty.stop();

      final TimeOut waitForLeaderToShutdown =
          new TimeOut(300, TimeUnit.SECONDS, TimeSource.NANO_TIME);
      waitForLeaderToShutdown.waitFor(
          "Gave up after waiting an obscene amount of time for leader to shut down",
          () -> leaderJetty.isStopped());

      for (int retries = 0; ; retries++) {

        Thread.yield(); // might not be necessary at all
        // poll interval on follower is 1 second, so we just sleep for a few seconds
        Thread.sleep(2000);

        NamedList<Object> followerDetails = null;
        try {
          followerDetails = getFollowerDetails();
          int failed = Integer.parseInt(getStringOrNull(followerDetails, "timesFailed"));
          if (previousTimesFailed != null) {
            assertTrue(failed > previousTimesFailed);
          }
          assertEquals(
              1,
              Integer.parseInt(getStringOrNull(followerDetails, "timesIndexReplicated")) - failed);
          break;
        } catch (NumberFormatException | AssertionError notYet) {
          if (log.isInfoEnabled()) {
            log.info(
                "{}th attempt failure on {} details are {}",
                retries + 1,
                notYet,
                followerDetails); // nowarn
          }
          if (retries > 9) {
            log.error("giving up: ", notYet);
            throw notYet;
          }
        }
      }

      leaderJetty.start();

      final TimeOut waitForLeaderToStart = new TimeOut(30, TimeUnit.SECONDS, TimeSource.NANO_TIME);
      waitForLeaderToStart.waitFor(
          "Gave up after waiting an obscene amount of time for leader to start",
          () -> leaderJetty.isRunning());

      // poll interval on follower is 1 second, so we just sleep for a few seconds
      Thread.sleep(2000);
      // get docs from follower and assert that they are still the same as before
      followerQueryRsp = rQuery(nDocs, "*:*", followerClient);
      assertEquals(nDocs, numFound(followerQueryRsp));
    } finally {
      resetFactory();
    }
  }

  private String getFollowerDetails(String keyName) throws SolrServerException, IOException {
    NamedList<Object> details = getFollowerDetails();
    return getStringOrNull(details, keyName);
  }

  private String getStringOrNull(NamedList<Object> details, String keyName) {
    Object o = details.get(keyName);
    return o != null ? o.toString() : null;
  }

  private NamedList<Object> getFollowerDetails() throws SolrServerException, IOException {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set(CommonParams.QT, "/replication");
    params.set("command", "details");
    if (useLegacyParams) {
      params.set("slave", "true");
    } else {
      params.set("follower", "true");
    }
    QueryResponse response = followerClient.query(params);

    // details/follower/timesIndexReplicated
    @SuppressWarnings({"unchecked"})
    NamedList<Object> details = (NamedList<Object>) response.getResponse().get("details");
    @SuppressWarnings({"unchecked"})
    NamedList<Object> follower = (NamedList<Object>) details.get("follower");
    return follower;
  }

  @Test
  public void doTestIndexFetchWithLeaderUrl() throws Exception {
    // change solrconfig on follower
    // this has no entry for pollinginterval
    follower.setTestPort(leaderJetty.getLocalPort());
    follower.copyConfigFile(CONF_DIR + "solrconfig-follower1.xml", "solrconfig.xml");
    followerJetty.stop();
    followerJetty = createAndStartJetty(follower);
    followerClient.close();
    followerClient =
        ReplicationTestHelper.createNewSolrClient(
            buildUrl(followerJetty.getLocalPort(), context) + "/" + DEFAULT_TEST_CORENAME);

    leaderClient.deleteByQuery("*:*");
    followerClient.deleteByQuery("*:*");
    followerClient.commit();
    nDocs--;
    for (int i = 0; i < nDocs; i++) index(leaderClient, "id", i, "name", "name = " + i);

    // make sure prepareCommit doesn't mess up commit  (SOLR-3938)

    // todo: make SolrJ easier to pass arbitrary params to
    // TODO: precommit WILL screw with the rest of this test

    leaderClient.commit();

    NamedList<Object> leaderQueryRsp = rQuery(nDocs, "*:*", leaderClient);
    SolrDocumentList leaderQueryResult = (SolrDocumentList) leaderQueryRsp.get("response");
    assertEquals(nDocs, leaderQueryResult.getNumFound());

    String urlKey = "leaderUrl";
    if (useLegacyParams) {
      urlKey = "masterUrl";
    }

    // index fetch
    String leaderUrl =
        buildUrl(followerJetty.getLocalPort())
            + "/"
            + DEFAULT_TEST_CORENAME
            + ReplicationHandler.PATH
            + "?command=fetchindex&"
            + urlKey
            + "=";
    leaderUrl +=
        buildUrl(leaderJetty.getLocalPort())
            + "/"
            + DEFAULT_TEST_CORENAME
            + ReplicationHandler.PATH;
    URL url = new URL(leaderUrl);
    InputStream stream = url.openStream();
    stream.close();

    // get docs from follower and check if number is equal to leader
    NamedList<Object> followerQueryRsp = rQuery(nDocs, "*:*", followerClient);
    SolrDocumentList followerQueryResult = (SolrDocumentList) followerQueryRsp.get("response");
    assertEquals(nDocs, followerQueryResult.getNumFound());
    // compare results
    String cmp =
        BaseDistributedSearchTestCase.compare(leaderQueryResult, followerQueryResult, 0, null);
    assertNull(cmp);

    // index fetch from the follower to the leader

    for (int i = nDocs; i < nDocs + 3; i++) index(followerClient, "id", i, "name", "name = " + i);

    followerClient.commit();

    pullFromTo(followerJetty, leaderJetty);
    rQuery(nDocs + 3, "*:*", leaderClient);

    // get docs from follower and check if number is equal to leader
    followerQueryRsp = rQuery(nDocs + 3, "*:*", followerClient);
    followerQueryResult = (SolrDocumentList) followerQueryRsp.get("response");
    assertEquals(nDocs + 3, followerQueryResult.getNumFound());
    // compare results
    leaderQueryRsp = rQuery(nDocs + 3, "*:*", leaderClient);
    leaderQueryResult = (SolrDocumentList) leaderQueryRsp.get("response");
    cmp = BaseDistributedSearchTestCase.compare(leaderQueryResult, followerQueryResult, 0, null);
    assertNull(cmp);

    assertVersions(leaderClient, followerClient);

    pullFromTo(followerJetty, leaderJetty);

    // get docs from follower and check if number is equal to leader
    followerQueryRsp = rQuery(nDocs + 3, "*:*", followerClient);
    followerQueryResult = (SolrDocumentList) followerQueryRsp.get("response");
    assertEquals(nDocs + 3, followerQueryResult.getNumFound());
    // compare results
    leaderQueryRsp = rQuery(nDocs + 3, "*:*", leaderClient);
    leaderQueryResult = (SolrDocumentList) leaderQueryRsp.get("response");
    cmp = BaseDistributedSearchTestCase.compare(leaderQueryResult, followerQueryResult, 0, null);
    assertNull(cmp);

    assertVersions(leaderClient, followerClient);

    // now force a new index directory
    for (int i = nDocs + 3; i < nDocs + 7; i++) {
      index(leaderClient, "id", i, "name", "name = " + i);
    }

    leaderClient.commit();

    pullFromTo(followerJetty, leaderJetty);
    rQuery((int) followerQueryResult.getNumFound(), "*:*", leaderClient);

    // get docs from follower and check if number is equal to leader
    followerQueryRsp = rQuery(nDocs + 3, "*:*", followerClient);
    followerQueryResult = (SolrDocumentList) followerQueryRsp.get("response");
    assertEquals(nDocs + 3, followerQueryResult.getNumFound());
    // compare results
    leaderQueryRsp = rQuery(nDocs + 3, "*:*", leaderClient);
    leaderQueryResult = (SolrDocumentList) leaderQueryRsp.get("response");
    cmp = BaseDistributedSearchTestCase.compare(leaderQueryResult, followerQueryResult, 0, null);
    assertNull(cmp);

    assertVersions(leaderClient, followerClient);
    pullFromTo(followerJetty, leaderJetty);

    // get docs from follower and check if number is equal to leader
    followerQueryRsp = rQuery(nDocs + 3, "*:*", followerClient);
    followerQueryResult = (SolrDocumentList) followerQueryRsp.get("response");
    assertEquals(nDocs + 3, followerQueryResult.getNumFound());
    // compare results
    leaderQueryRsp = rQuery(nDocs + 3, "*:*", leaderClient);
    leaderQueryResult = (SolrDocumentList) leaderQueryRsp.get("response");
    cmp = BaseDistributedSearchTestCase.compare(leaderQueryResult, followerQueryResult, 0, null);
    assertNull(cmp);

    assertVersions(leaderClient, followerClient);

    checkForSingleIndex(leaderJetty);
    checkForSingleIndex(followerJetty);
  }

  @Test
  public void doTestStressReplication() throws Exception {
    // change solrconfig on follower
    // this has no entry for pollinginterval

    // get us a straight standard fs dir rather than mock*dir
    boolean useStraightStandardDirectory = random().nextBoolean();

    if (useStraightStandardDirectory) {
      useFactory(null);
    }
    final String FOLLOWER_SCHEMA_1 = "schema-replication1.xml";
    final String FOLLOWER_SCHEMA_2 = "schema-replication2.xml";
    String followerSchema = FOLLOWER_SCHEMA_1;

    try {

      follower.setTestPort(leaderJetty.getLocalPort());
      follower.copyConfigFile(CONF_DIR + "solrconfig-follower1.xml", "solrconfig.xml");
      follower.copyConfigFile(CONF_DIR + followerSchema, "schema.xml");
      followerJetty.stop();
      followerJetty = createAndStartJetty(follower);
      followerClient.close();
      followerClient =
          createNewSolrClient(buildUrl(followerJetty.getLocalPort()) + "/" + DEFAULT_TEST_CORENAME);

      leader.copyConfigFile(CONF_DIR + "solrconfig-leader3.xml", "solrconfig.xml");
      leaderJetty.stop();
      leaderJetty = createAndStartJetty(leader);
      leaderClient.close();
      leaderClient =
          createNewSolrClient(buildUrl(leaderJetty.getLocalPort()) + "/" + DEFAULT_TEST_CORENAME);

      leaderClient.deleteByQuery("*:*");
      followerClient.deleteByQuery("*:*");
      followerClient.commit();

      int maxDocs = TEST_NIGHTLY ? 1000 : 75;
      int rounds = TEST_NIGHTLY ? 45 : 3;
      int totalDocs = 0;
      int id = 0;
      for (int x = 0; x < rounds; x++) {

        final boolean confCoreReload = random().nextBoolean();
        if (confCoreReload) {
          // toggle the schema file used

          followerSchema =
              followerSchema.equals(FOLLOWER_SCHEMA_1) ? FOLLOWER_SCHEMA_2 : FOLLOWER_SCHEMA_1;
          leader.copyConfigFile(CONF_DIR + followerSchema, "schema.xml");
        }

        int docs = random().nextInt(maxDocs) + 1;
        for (int i = 0; i < docs; i++) {
          index(leaderClient, "id", id++, "name", "name = " + i);
        }

        totalDocs += docs;
        leaderClient.commit();

        NamedList<Object> leaderQueryRsp = rQuery(totalDocs, "*:*", leaderClient);
        SolrDocumentList leaderQueryResult = (SolrDocumentList) leaderQueryRsp.get("response");
        assertEquals(totalDocs, leaderQueryResult.getNumFound());

        // index fetch
        Date followerCoreStart = watchCoreStartAt(followerJetty, null);
        pullFromTo(leaderJetty, followerJetty);
        if (confCoreReload) {
          watchCoreStartAt(followerJetty, followerCoreStart);
        }

        // get docs from follower and check if number is equal to leader
        NamedList<Object> followerQueryRsp = rQuery(totalDocs, "*:*", followerClient);
        SolrDocumentList followerQueryResult = (SolrDocumentList) followerQueryRsp.get("response");
        assertEquals(totalDocs, followerQueryResult.getNumFound());
        // compare results
        String cmp =
            BaseDistributedSearchTestCase.compare(leaderQueryResult, followerQueryResult, 0, null);
        assertNull(cmp);

        assertVersions(leaderClient, followerClient);

        checkForSingleIndex(leaderJetty);

        if (!Constants.WINDOWS) {
          checkForSingleIndex(followerJetty);
        }

        if (random().nextBoolean()) {
          // move the follower ahead
          for (int i = 0; i < 3; i++) {
            index(followerClient, "id", id++, "name", "name = " + i);
          }
          followerClient.commit();
        }
      }

    } finally {
      if (useStraightStandardDirectory) {
        resetFactory();
      }
    }
  }

  private CachingDirectoryFactory getCachingDirectoryFactory(SolrCore core) {
    return (CachingDirectoryFactory) core.getDirectoryFactory();
  }

  private void checkForSingleIndex(JettySolrRunner jetty) {
    checkForSingleIndex(jetty, false);
  }

  private void checkForSingleIndex(JettySolrRunner jetty, boolean afterReload) {
    CoreContainer cores = jetty.getCoreContainer();
    Collection<SolrCore> theCores = cores.getCores();
    for (SolrCore core : theCores) {
      String ddir = core.getDataDir();
      CachingDirectoryFactory dirFactory = getCachingDirectoryFactory(core);
      synchronized (dirFactory) {
        Set<String> livePaths = dirFactory.getLivePaths();
        // one for data, one for the index under data and one for the snapshot metadata.
        // we also allow one extra index dir - it may not be removed until the core is closed
        if (afterReload) {
          assertTrue(
              livePaths.toString() + ":" + livePaths.size(),
              3 == livePaths.size() || 4 == livePaths.size());
        } else {
          assertEquals(livePaths.toString() + ":" + livePaths.size(), 3, livePaths.size());
        }

        // :TODO: assert that one of the paths is a subpath of hte other
      }
      if (dirFactory instanceof StandardDirectoryFactory) {
        System.out.println(Arrays.asList(new File(ddir).list()));
        // we also allow one extra index dir - it may not be removed until the core is closed
        int cnt = indexDirCount(ddir);
        // if after reload, there may be 2 index dirs while the reloaded SolrCore closes.
        if (afterReload) {
          assertTrue("found:" + cnt + Arrays.asList(new File(ddir).list()), 1 == cnt || 2 == cnt);
        } else {
          assertEquals("found:" + cnt + Arrays.asList(new File(ddir).list()), 1, cnt);
        }
      }
    }
  }

  private int indexDirCount(String ddir) {
    String[] list =
        new File(ddir)
            .list(
                new FilenameFilter() {
                  @Override
                  public boolean accept(File dir, String name) {
                    File f = new File(dir, name);
                    return f.isDirectory()
                        && !SolrSnapshotMetaDataManager.SNAPSHOT_METADATA_DIR.equals(name);
                  }
                });
    return list.length;
  }

  public static void pullFromTo(JettySolrRunner srcSolr, JettySolrRunner destSolr)
      throws IOException {
    String srcUrl = buildUrl(srcSolr.getLocalPort()) + "/" + DEFAULT_TEST_CORENAME;
    String destUrl = buildUrl(destSolr.getLocalPort()) + "/" + DEFAULT_TEST_CORENAME;
    ReplicationTestHelper.pullFromTo(srcUrl, destUrl);
  }

  @Test
  public void doTestRepeater() throws Exception {
    // no polling
    follower.setTestPort(leaderJetty.getLocalPort());
    follower.copyConfigFile(CONF_DIR + "solrconfig-follower1.xml", "solrconfig.xml");
    followerJetty.stop();
    followerJetty = createAndStartJetty(follower);
    followerClient.close();
    followerClient =
        createNewSolrClient(buildUrl(followerJetty.getLocalPort()) + "/" + DEFAULT_TEST_CORENAME);

    try {
      repeater =
          new SolrInstance(
              createTempDir("solr-instance").toFile(), "repeater", leaderJetty.getLocalPort());
      repeater.setUp();
      repeater.copyConfigFile(CONF_DIR + "solrconfig-repeater.xml", "solrconfig.xml");
      repeaterJetty = createAndStartJetty(repeater);
      if (repeaterClient != null) {
        repeaterClient.close();
      }
      repeaterClient =
          createNewSolrClient(buildUrl(repeaterJetty.getLocalPort()) + "/" + DEFAULT_TEST_CORENAME);

      for (int i = 0; i < 3; i++) index(leaderClient, "id", i, "name", "name = " + i);

      leaderClient.commit();

      pullFromTo(leaderJetty, repeaterJetty);

      rQuery(3, "*:*", repeaterClient);

      pullFromTo(repeaterJetty, followerJetty);

      rQuery(3, "*:*", followerClient);

      assertVersions(leaderClient, repeaterClient);
      assertVersions(repeaterClient, followerClient);

      for (int i = 0; i < 4; i++) index(repeaterClient, "id", i, "name", "name = " + i);
      repeaterClient.commit();

      pullFromTo(leaderJetty, repeaterJetty);

      rQuery(3, "*:*", repeaterClient);

      pullFromTo(repeaterJetty, followerJetty);

      rQuery(3, "*:*", followerClient);

      for (int i = 3; i < 6; i++) index(leaderClient, "id", i, "name", "name = " + i);

      leaderClient.commit();

      pullFromTo(leaderJetty, repeaterJetty);

      rQuery(6, "*:*", repeaterClient);

      pullFromTo(repeaterJetty, followerJetty);

      rQuery(6, "*:*", followerClient);

    } finally {
      if (repeater != null) {
        repeaterJetty.stop();
        repeaterJetty = null;
      }
      if (repeaterClient != null) {
        repeaterClient.close();
      }
    }
  }

  @Test
  public void doTestReplicateAfterStartup() throws Exception {
    // stop follower
    followerJetty.stop();

    nDocs--;
    leaderClient.deleteByQuery("*:*");

    leaderClient.commit();

    // change solrconfig having 'replicateAfter startup' option on leader
    leader.copyConfigFile(CONF_DIR + "solrconfig-leader2.xml", "solrconfig.xml");

    leaderJetty.stop();

    leaderJetty = createAndStartJetty(leader);
    leaderClient.close();
    leaderClient =
        createNewSolrClient(buildUrl(leaderJetty.getLocalPort()) + "/" + DEFAULT_TEST_CORENAME);

    for (int i = 0; i < nDocs; i++) index(leaderClient, "id", i, "name", "name = " + i);

    leaderClient.commit();

    NamedList<Object> leaderQueryRsp = rQuery(nDocs, "*:*", leaderClient);
    SolrDocumentList leaderQueryResult = (SolrDocumentList) leaderQueryRsp.get("response");
    assertEquals(nDocs, leaderQueryResult.getNumFound());

    follower.setTestPort(leaderJetty.getLocalPort());
    follower.copyConfigFile(follower.getSolrConfigFile(), "solrconfig.xml");

    // start follower
    followerJetty = createAndStartJetty(follower);
    followerClient.close();
    followerClient =
        createNewSolrClient(buildUrl(followerJetty.getLocalPort()) + "/" + DEFAULT_TEST_CORENAME);

    // get docs from follower and check if number is equal to leader
    NamedList<Object> followerQueryRsp = rQuery(nDocs, "*:*", followerClient);
    SolrDocumentList followerQueryResult = (SolrDocumentList) followerQueryRsp.get("response");
    assertEquals(nDocs, followerQueryResult.getNumFound());

    // compare results
    String cmp =
        BaseDistributedSearchTestCase.compare(leaderQueryResult, followerQueryResult, 0, null);
    assertNull(cmp);
  }

  @Test
  public void doTestReplicateAfterStartupWithNoActivity() throws Exception {
    useFactory(null);
    try {

      // stop follower
      followerJetty.stop();

      nDocs--;
      leaderClient.deleteByQuery("*:*");

      leaderClient.commit();

      // change solrconfig having 'replicateAfter startup' option on leader
      leader.copyConfigFile(CONF_DIR + "solrconfig-leader2.xml", "solrconfig.xml");

      leaderJetty.stop();

      leaderJetty = createAndStartJetty(leader);
      leaderClient.close();
      leaderClient =
          createNewSolrClient(buildUrl(leaderJetty.getLocalPort()) + "/" + DEFAULT_TEST_CORENAME);

      for (int i = 0; i < nDocs; i++) index(leaderClient, "id", i, "name", "name = " + i);

      leaderClient.commit();

      // now we restart to test what happens with no activity before the follower
      // tries to
      // replicate
      leaderJetty.stop();
      leaderJetty.start();

      // leaderClient = createNewSolrClient(leaderJetty.getLocalPort());

      NamedList<Object> leaderQueryRsp = rQuery(nDocs, "*:*", leaderClient);
      SolrDocumentList leaderQueryResult = (SolrDocumentList) leaderQueryRsp.get("response");
      assertEquals(nDocs, leaderQueryResult.getNumFound());

      follower.setTestPort(leaderJetty.getLocalPort());
      follower.copyConfigFile(follower.getSolrConfigFile(), "solrconfig.xml");

      // start follower
      followerJetty = createAndStartJetty(follower);
      followerClient.close();
      followerClient =
          createNewSolrClient(buildUrl(followerJetty.getLocalPort()) + "/" + DEFAULT_TEST_CORENAME);

      // get docs from follower and check if number is equal to leader
      NamedList<Object> followerQueryRsp = rQuery(nDocs, "*:*", followerClient);
      SolrDocumentList followerQueryResult = (SolrDocumentList) followerQueryRsp.get("response");
      assertEquals(nDocs, followerQueryResult.getNumFound());

      // compare results
      String cmp =
          BaseDistributedSearchTestCase.compare(leaderQueryResult, followerQueryResult, 0, null);
      assertNull(cmp);

    } finally {
      resetFactory();
    }
  }

  @Test
  public void doTestReplicateAfterCoreReload() throws Exception {
    int docs = TEST_NIGHTLY ? 200000 : 10;

    // stop follower
    followerJetty.stop();

    // change solrconfig having 'replicateAfter startup' option on leader
    leader.copyConfigFile(CONF_DIR + "solrconfig-leader3.xml", "solrconfig.xml");

    leaderJetty.stop();

    leaderJetty = createAndStartJetty(leader);
    leaderClient.close();
    leaderClient =
        createNewSolrClient(buildUrl(leaderJetty.getLocalPort()) + "/" + DEFAULT_TEST_CORENAME);

    leaderClient.deleteByQuery("*:*");
    for (int i = 0; i < docs; i++) index(leaderClient, "id", i, "name", "name = " + i);

    leaderClient.commit();

    NamedList<Object> leaderQueryRsp = rQuery(docs, "*:*", leaderClient);
    SolrDocumentList leaderQueryResult = (SolrDocumentList) leaderQueryRsp.get("response");
    assertEquals(docs, leaderQueryResult.getNumFound());

    follower.setTestPort(leaderJetty.getLocalPort());
    follower.copyConfigFile(follower.getSolrConfigFile(), "solrconfig.xml");

    // start follower
    followerJetty = createAndStartJetty(follower);
    followerClient.close();
    followerClient =
        createNewSolrClient(buildUrl(followerJetty.getLocalPort()) + "/" + DEFAULT_TEST_CORENAME);

    // get docs from follower and check if number is equal to leader
    NamedList<Object> followerQueryRsp = rQuery(docs, "*:*", followerClient);
    SolrDocumentList followerQueryResult = (SolrDocumentList) followerQueryRsp.get("response");
    assertEquals(docs, followerQueryResult.getNumFound());

    // compare results
    String cmp =
        BaseDistributedSearchTestCase.compare(leaderQueryResult, followerQueryResult, 0, null);
    assertNull(cmp);

    Object version = getIndexVersion(leaderClient).get("indexversion");

    reloadCore(leaderJetty, DEFAULT_TEST_COLLECTION_NAME);

    assertEquals(version, getIndexVersion(leaderClient).get("indexversion"));

    index(leaderClient, "id", docs + 10, "name", "name = 1");
    index(leaderClient, "id", docs + 20, "name", "name = 2");

    leaderClient.commit();

    NamedList<Object> resp = rQuery(docs + 2, "*:*", leaderClient);
    leaderQueryResult = (SolrDocumentList) resp.get("response");
    assertEquals(docs + 2, leaderQueryResult.getNumFound());

    // get docs from follower and check if number is equal to leader
    followerQueryRsp = rQuery(docs + 2, "*:*", followerClient);
    followerQueryResult = (SolrDocumentList) followerQueryRsp.get("response");
    assertEquals(docs + 2, followerQueryResult.getNumFound());
  }

  @Test
  // 12-Jun-2018 @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // 04-May-2018
  public void doTestIndexAndConfigAliasReplication() throws Exception {
    clearIndexWithReplication();

    nDocs--;
    for (int i = 0; i < nDocs; i++) index(leaderClient, "id", i, "name", "name = " + i);

    leaderClient.commit();

    NamedList<Object> leaderQueryRsp = rQuery(nDocs, "*:*", leaderClient);
    SolrDocumentList leaderQueryResult = (SolrDocumentList) leaderQueryRsp.get("response");
    assertEquals(nDocs, leaderQueryResult.getNumFound());

    // get docs from follower and check if number is equal to leader
    NamedList<Object> followerQueryRsp = rQuery(nDocs, "*:*", followerClient);
    SolrDocumentList followerQueryResult = (SolrDocumentList) followerQueryRsp.get("response");

    assertEquals(nDocs, followerQueryResult.getNumFound());

    // compare results
    String cmp =
        BaseDistributedSearchTestCase.compare(leaderQueryResult, followerQueryResult, 0, null);
    assertNull(cmp);

    // start config files replication test
    // clear leader index
    leaderClient.deleteByQuery("*:*");
    leaderClient.commit();
    rQuery(0, "*:*", leaderClient); // sanity check w/retry

    // change solrconfig on leader
    leader.copyConfigFile(CONF_DIR + "solrconfig-leader1.xml", "solrconfig.xml");

    // change schema on leader
    leader.copyConfigFile(CONF_DIR + "schema-replication2.xml", "schema.xml");

    // keep a copy of the new schema
    leader.copyConfigFile(CONF_DIR + "schema-replication2.xml", "schema-replication2.xml");

    leaderJetty.stop();

    leaderJetty = createAndStartJetty(leader);
    leaderClient.close();
    leaderClient =
        createNewSolrClient(buildUrl(leaderJetty.getLocalPort()) + "/" + DEFAULT_TEST_CORENAME);

    follower.setTestPort(leaderJetty.getLocalPort());
    follower.copyConfigFile(follower.getSolrConfigFile(), "solrconfig.xml");

    followerJetty.stop();
    followerJetty = createAndStartJetty(follower);
    followerClient.close();
    followerClient =
        createNewSolrClient(buildUrl(followerJetty.getLocalPort()) + "/" + DEFAULT_TEST_CORENAME);

    followerClient.deleteByQuery("*:*");
    followerClient.commit();
    rQuery(0, "*:*", followerClient); // sanity check w/retry

    // record collection1's start time on follower
    final Date followerStartTime = watchCoreStartAt(followerJetty, null);

    // add a doc with new field and commit on leader to trigger index fetch from follower.
    index(leaderClient, "id", "2000", "name", "name = " + 2000, "newname", "n2000");
    leaderClient.commit();
    rQuery(1, "newname:n2000", leaderClient); // sanity check

    // wait for follower to reload core by watching updated startTime
    watchCoreStartAt(followerJetty, followerStartTime);

    NamedList<Object> leaderQueryRsp2 = rQuery(1, "id:2000", leaderClient);
    SolrDocumentList leaderQueryResult2 = (SolrDocumentList) leaderQueryRsp2.get("response");
    assertEquals(1, leaderQueryResult2.getNumFound());

    NamedList<Object> followerQueryRsp2 = rQuery(1, "id:2000", followerClient);
    SolrDocumentList followerQueryResult2 = (SolrDocumentList) followerQueryRsp2.get("response");
    assertEquals(1, followerQueryResult2.getNumFound());

    checkForSingleIndex(leaderJetty);
    checkForSingleIndex(followerJetty, true);
  }

  @Test
  public void testRateLimitedReplication() throws Exception {

    // clean index
    leaderClient.deleteByQuery("*:*");
    followerClient.deleteByQuery("*:*");
    leaderClient.commit();
    followerClient.commit();

    leaderJetty.stop();
    followerJetty.stop();

    // Start leader with the new solrconfig
    leader.copyConfigFile(CONF_DIR + "solrconfig-leader-throttled.xml", "solrconfig.xml");
    useFactory(null);
    leaderJetty = createAndStartJetty(leader);
    leaderClient.close();
    leaderClient =
        createNewSolrClient(buildUrl(leaderJetty.getLocalPort()) + "/" + DEFAULT_TEST_CORENAME);

    // index docs
    final int totalDocs = TestUtil.nextInt(random(), 17, 53);
    for (int i = 0; i < totalDocs; i++)
      index(leaderClient, "id", i, "name", TestUtil.randomSimpleString(random(), 1000, 5000));

    leaderClient.commit();

    // Check Index Size
    String dataDir = leader.getDataDir();
    leaderClient.close();
    leaderJetty.stop();

    Directory dir = FSDirectory.open(Paths.get(dataDir).resolve("index"));
    String[] files = dir.listAll();
    long totalBytes = 0;
    for (String file : files) {
      totalBytes += dir.fileLength(file);
    }

    float approximateTimeInSeconds =
        Math.round((float) totalBytes / 1024 / 1024 / 0.1); // maxWriteMBPerSec=0.1 in solrconfig

    // Start again and replicate the data
    useFactory(null);
    leaderJetty = createAndStartJetty(leader);
    leaderClient =
        createNewSolrClient(buildUrl(leaderJetty.getLocalPort()) + "/" + DEFAULT_TEST_CORENAME);

    // start follower
    follower.setTestPort(leaderJetty.getLocalPort());
    follower.copyConfigFile(CONF_DIR + "solrconfig-follower1.xml", "solrconfig.xml");
    followerJetty = createAndStartJetty(follower);
    followerClient.close();
    followerClient =
        createNewSolrClient(buildUrl(followerJetty.getLocalPort()) + "/" + DEFAULT_TEST_CORENAME);

    long startTime = System.nanoTime();

    pullFromTo(leaderJetty, followerJetty);

    // Add a few more docs in the leader. Just to make sure that we are replicating the correct
    // index point. These extra docs should not get replicated
    new Thread(new AddExtraDocs(leaderClient, totalDocs)).start();

    // Wait and make sure that it actually replicated correctly.
    NamedList<Object> followerQueryRsp = rQuery(totalDocs, "*:*", followerClient);
    SolrDocumentList followerQueryResult = (SolrDocumentList) followerQueryRsp.get("response");
    assertEquals(totalDocs, followerQueryResult.getNumFound());

    long timeTaken = System.nanoTime() - startTime;

    long timeTakenInSeconds = TimeUnit.SECONDS.convert(timeTaken, TimeUnit.NANOSECONDS);

    // Let's make sure it took more than approximateTimeInSeconds to make sure that it was throttled
    log.info(
        "approximateTimeInSeconds = {} timeTakenInSeconds = {}",
        approximateTimeInSeconds,
        timeTakenInSeconds);
    assertTrue(timeTakenInSeconds - approximateTimeInSeconds > 0);
  }

  @Test
  public void doTestIllegalFilePaths() {
    // Loop through the file=, cf=, tlogFile= params and prove that it throws exception for path
    // traversal attempts
    String absFile = Paths.get("foo").toAbsolutePath().toString();
    List<String> illegalFilenames =
        Arrays.asList(absFile, "../dir/traversal", "illegal\rfile\nname\t");
    List<String> params =
        Arrays.asList(
            ReplicationHandler.FILE,
            ReplicationHandler.CONF_FILE_SHORT,
            ReplicationHandler.TLOG_FILE);
    for (String param : params) {
      for (String filename : illegalFilenames) {
        expectThrows(
            Exception.class,
            () ->
                invokeReplicationCommand(
                    buildUrl(leaderJetty.getLocalPort()) + "/" + DEFAULT_TEST_CORENAME,
                    "filecontent&" + param + "=" + filename));
      }
    }
  }

  @Test
  public void testFileListShouldReportErrorsWhenTheyOccur() throws Exception {
    SolrQuery q = new SolrQuery();
    q.add("qt", "/replication")
        .add("wt", "json")
        .add("command", "filelist")
        .add(
            "generation",
            "-2"); // A 'generation' value not matching any commit point should cause error.
    QueryResponse response = followerClient.query(q);
    NamedList<Object> resp = response.getResponse();
    assertNotNull(resp);
    assertEquals("ERROR", resp.get("status"));
    assertEquals("invalid index generation", resp.get("message"));
  }

  @Test
  public void testFetchIndexShouldReportErrorsWhenTheyOccur() throws Exception {
    int leaderPort = leaderJetty.getLocalPort();
    leaderJetty.stop();
    SolrQuery q = new SolrQuery();
    q.add("qt", "/replication")
        .add("wt", "json")
        .add("wait", "true")
        .add("command", "fetchindex")
        .add("leaderUrl", buildUrl(leaderPort));
    QueryResponse response = followerClient.query(q);
    NamedList<Object> resp = response.getResponse();
    assertNotNull(resp);
    assertEquals(
        "Fetch index with wait=true should have returned an error response",
        "ERROR",
        resp.get("status"));
  }

  @Test
  public void testShouldReportErrorWhenRequiredCommandArgMissing() {
    SolrQuery q = new SolrQuery();
    q.add("qt", "/replication").add("wt", "json");
    SolrException thrown =
        expectThrows(
            SolrException.class,
            () -> {
              followerClient.query(q);
            });
    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, thrown.code());
    MatcherAssert.assertThat(
        thrown.getMessage(), containsString("Missing required parameter: command"));
  }

  @Test
  public void testShouldReportErrorWhenDeletingBackupButNameMissing() {
    SolrQuery q = new SolrQuery();
    q.add("qt", "/replication").add("wt", "json").add("command", "deletebackup");
    SolrException thrown =
        expectThrows(
            SolrException.class,
            () -> {
              followerClient.query(q);
            });
    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, thrown.code());
    MatcherAssert.assertThat(
        thrown.getMessage(), containsString("Missing required parameter: name"));
  }

  @Test
  public void testEmptyBackups() throws Exception {
    final File backupDir = createTempDir().toFile();
    final BackupStatusChecker backupStatus = new BackupStatusChecker(leaderClient);

    leaderJetty.getCoreContainer().getAllowPaths().add(backupDir.toPath());

    { // initial request w/o any committed docs
      final String backupName = "empty_backup1";
      final GenericSolrRequest req =
          new GenericSolrRequest(
              SolrRequest.METHOD.GET,
              "/replication",
              params(
                  "command",
                  "backup",
                  "location",
                  backupDir.getAbsolutePath(),
                  "name",
                  backupName));
      final TimeOut timeout = new TimeOut(30, TimeUnit.SECONDS, TimeSource.NANO_TIME);
      final SimpleSolrResponse rsp = req.process(leaderClient);

      final String dirName = backupStatus.waitForBackupSuccess(backupName, timeout);
      assertEquals(
          "Did not get expected dir name for backup, did API change?",
          "snapshot.empty_backup1",
          dirName);
      assertTrue(
          dirName + " doesn't exist in expected location for backup " + backupName,
          new File(backupDir, dirName).exists());
    }

    index(leaderClient, "id", "1", "name", "foo");

    { // second backup w/uncommited doc
      final String backupName = "empty_backup2";
      final GenericSolrRequest req =
          new GenericSolrRequest(
              SolrRequest.METHOD.GET,
              "/replication",
              params(
                  "command",
                  "backup",
                  "location",
                  backupDir.getAbsolutePath(),
                  "name",
                  backupName));
      final TimeOut timeout = new TimeOut(30, TimeUnit.SECONDS, TimeSource.NANO_TIME);
      final SimpleSolrResponse rsp = req.process(leaderClient);

      final String dirName = backupStatus.waitForBackupSuccess(backupName, timeout);
      assertEquals(
          "Did not get expected dir name for backup, did API change?",
          "snapshot.empty_backup2",
          dirName);
      assertTrue(
          dirName + " doesn't exist in expected location for backup " + backupName,
          new File(backupDir, dirName).exists());
    }

    // confirm backups really are empty
    for (int i = 1; i <= 2; i++) {
      final String name = "snapshot.empty_backup" + i;
      try (Directory dir = new NIOFSDirectory(new File(backupDir, name).toPath());
          IndexReader reader = DirectoryReader.open(dir)) {
        assertEquals(name + " is not empty", 0, reader.numDocs());
      }
    }
  }

  public void testGetBoolWithBackwardCompatibility() {
    assertTrue(ReplicationHandler.getBoolWithBackwardCompatibility(params(), "foo", "bar", true));
    assertFalse(ReplicationHandler.getBoolWithBackwardCompatibility(params(), "foo", "bar", false));
    assertTrue(
        ReplicationHandler.getBoolWithBackwardCompatibility(
            params("foo", "true"), "foo", "bar", false));
    assertTrue(
        ReplicationHandler.getBoolWithBackwardCompatibility(
            params("bar", "true"), "foo", "bar", false));
    assertTrue(
        ReplicationHandler.getBoolWithBackwardCompatibility(
            params("foo", "true", "bar", "false"), "foo", "bar", false));
  }

  public void testGetObjectWithBackwardCompatibility() {
    assertEquals(
        "aaa",
        ReplicationHandler.getObjectWithBackwardCompatibility(params(), "foo", "bar", "aaa"));
    assertEquals(
        "bbb",
        ReplicationHandler.getObjectWithBackwardCompatibility(
            params("foo", "bbb"), "foo", "bar", "aaa"));
    assertEquals(
        "bbb",
        ReplicationHandler.getObjectWithBackwardCompatibility(
            params("bar", "bbb"), "foo", "bar", "aaa"));
    assertEquals(
        "bbb",
        ReplicationHandler.getObjectWithBackwardCompatibility(
            params("foo", "bbb", "bar", "aaa"), "foo", "bar", "aaa"));
    assertNull(ReplicationHandler.getObjectWithBackwardCompatibility(params(), "foo", "bar", null));
  }

  public void testGetObjectWithBackwardCompatibilityFromNL() {
    NamedList<Object> nl = new NamedList<>();
    assertNull(ReplicationHandler.getObjectWithBackwardCompatibility(nl, "foo", "bar"));
    nl.add("bar", "bbb");
    assertEquals("bbb", ReplicationHandler.getObjectWithBackwardCompatibility(nl, "foo", "bar"));
    nl.add("foo", "aaa");
    assertEquals("aaa", ReplicationHandler.getObjectWithBackwardCompatibility(nl, "foo", "bar"));
  }

  private static class AddExtraDocs implements Runnable {

    SolrClient leaderClient;
    int startId;

    public AddExtraDocs(SolrClient leaderClient, int startId) {
      this.leaderClient = leaderClient;
      this.startId = startId;
    }

    @Override
    public void run() {
      final int totalDocs = TestUtil.nextInt(random(), 1, 10);
      for (int i = 0; i < totalDocs; i++) {
        try {
          index(
              leaderClient,
              "id",
              i + startId,
              "name",
              TestUtil.randomSimpleString(random(), 1000, 5000));
        } catch (Exception e) {
          // Do nothing. Wasn't able to add doc.
        }
      }
      try {
        leaderClient.commit();
      } catch (Exception e) {
        // Do nothing. No extra doc got committed.
      }
    }
  }

  private UpdateResponse emptyUpdate(SolrClient client, String... params)
      throws SolrServerException, IOException {

    UpdateRequest req = new UpdateRequest();
    req.setParams(params(params));
    return req.process(client);
  }

  /**
   * Polls the SolrCore stats using the specified jettySolrRunner until the "startTime" time for
   * collection is after the specified "min". Will loop for at most "timeout" milliseconds before
   * throwing an assertion failure.
   *
   * @param jettySolrRunner The JettySolrRunner to poll
   * @param min the startTime value must exceed this value before the method will return, if null
   *     this method will return the first startTime value encountered.
   * @return the startTime value of collection
   */
  @SuppressWarnings("unchecked")
  private Date watchCoreStartAt(JettySolrRunner jettySolrRunner, final Date min)
      throws InterruptedException, IOException, SolrServerException {
    final long sleepInterval = 200;
    long timeSlept = 0;

    try (SolrClient adminClient = adminClient(jettySolrRunner)) {
      SolrParams p = params("action", "status", "core", "collection1");
      while (timeSlept < TIMEOUT) {
        QueryRequest req = new QueryRequest(p);
        req.setPath("/admin/cores");
        try {
          NamedList<Object> data = adminClient.request(req);
          for (String k : new String[] {"status", "collection1"}) {
            Object o = data.get(k);
            assertNotNull("core status rsp missing key: " + k, o);
            data = (NamedList<Object>) o;
          }
          Date startTime = (Date) data.get("startTime");
          assertNotNull("core has null startTime", startTime);
          if (null == min || startTime.after(min)) {
            return startTime;
          }
        } catch (SolrException e) {
          // workarround for SOLR-4668
          if (500 != e.code()) {
            throw e;
          } // else server possibly from the core reload in progress...
        }

        timeSlept += sleepInterval;
        Thread.sleep(sleepInterval);
      }
      fail("timed out waiting for collection1 startAt time to exceed: " + min);
      return min; // compilation neccessity
    }
  }

  private void assertReplicationResponseSucceeded(NamedList<?> response) {
    assertNotNull("null response from server", response);
    assertNotNull("Expected replication response to have 'status' field", response.get("status"));
    assertEquals("OK", response.get("status"));
  }

  public static String buildUrl(int port) {
    return buildUrl(port, context);
  }
}
