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

import java.io.File;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.net.URL;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.util.Constants;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.lucene.util.TestUtil;

import org.apache.solr.BaseDistributedSearchTestCase;
import org.apache.solr.SolrTestCaseJ4.SuppressSSL;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.client.solrj.response.SimpleSolrResponse;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.security.AllowListUrlChecker;
import org.apache.solr.util.TestInjection;
import org.apache.solr.util.TimeOut;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.matchers.JUnitMatchers.containsString;

/**
 * Test for ReplicationHandler
 *
 *
 * @since 1.4
 */
@Slow
@SuppressSSL     // Currently unknown why SSL does not work with this test
// commented 20-July-2018 @LuceneTestCase.BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // 12-Jun-2018
// commented out on: 24-Dec-2018 @LuceneTestCase.BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // added 23-Aug-2018
public class TestReplicationHandler extends AbstractReplicationTestBase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

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

  @Test
  public void doTestHandlerPathUnchanged() throws Exception {
    assertEquals("/replication", ReplicationHandler.PATH);
  }

  @Test
  public void testUrlAllowList() throws Exception {
    // Run another test with URL allow-list enabled and allow-list is empty.
    // Expect an exception because the leader URL is not allowed.
    systemClearPropertySolrDisableUrlAllowList();
    SolrException e = expectThrows(SolrException.class, this::doTestDetails);
    assertTrue(e.getMessage().contains("nor in the configured '" + AllowListUrlChecker.URL_ALLOW_LIST + "'"));

    // Set the allow-list to allow the leader URL.
    // Expect the same test to pass now.
    System.setProperty(TEST_URL_ALLOW_LIST, leaderJetty.getBaseUrl() + "," + followerJetty.getBaseUrl());
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
    leaderClient = createNewSolrClient(leaderJetty.getLocalPort());
    followerClient = createNewSolrClient(followerJetty.getLocalPort());
    
    clearIndexWithReplication();
    { 
      NamedList<Object> details = getDetails(leaderClient);
      
      assertEquals("leader isLeader?",
                   "true", details.get("isLeader"));
      assertEquals("leader isFollower?",
                   "false", details.get("isFollower"));
      assertNotNull("leader has leader section",
                    details.get("leader"));
    }

    // check details on the follower a couple of times before & after fetching
    for (int i = 0; i < 3; i++) {
      NamedList<Object> details = getDetails(followerClient);
      assertNotNull(i + ": " + details);
      assertNotNull(i + ": " + details.toString(), details.get("follower"));

      if (i > 0) {
        rQuery(i, "*:*", followerClient);
        List<?> replicatedAtCount = (List<?>) ((NamedList<?>) details.get("follower")).get("indexReplicatedAtList");
        int tries = 0;
        while ((replicatedAtCount == null || replicatedAtCount.size() < i) && tries++ < 5) {
          Thread.sleep(1000);
          details = getDetails(followerClient);
          replicatedAtCount = (List<?>) ((NamedList<?>) details.get("follower")).get("indexReplicatedAtList");
        }
        
        assertNotNull("Expected to see that the follower has replicated" + i + ": " + details.toString(), replicatedAtCount);
        
        // we can have more replications than we added docs because a replication can legally fail and try 
        // again (sometimes we cannot merge into a live index and have to try again)
        assertTrue("i:" + i + " replicationCount:" + replicatedAtCount.size(), replicatedAtCount.size() >= i); 
      }

      assertEquals(i + ": " + "follower isLeader?", "false", details.get("isLeader"));
      assertEquals(i + ": " + "follower isFollower?", "true", details.get("isFollower"));
      assertNotNull(i + ": " + "follower has follower section", details.get("follower"));
      // SOLR-2677: assert not false negatives
      Object timesFailed = ((NamedList)details.get("follower")).get(IndexFetcher.TIMES_FAILED);
      // SOLR-7134: we can have a fail because some mock index files have no checksum, will
      // always be downloaded, and may not be able to be moved into the existing index
      assertTrue(i + ": " + "follower has fetch error count: " + timesFailed, timesFailed == null || ((Number) timesFailed).intValue() == 1);

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
      repeater = new SolrInstance(createTempDir("solr-instance").toFile(), "repeater", leaderJetty.getLocalPort());
      repeater.setUp();
      repeaterJetty = createAndStartJetty(repeater);
      repeaterClient = createNewSolrClient(repeaterJetty.getLocalPort());

      
      NamedList<Object> details = getDetails(repeaterClient);
      
      assertEquals("repeater isLeader?",
                   "true", details.get("isLeader"));
      assertEquals("repeater isFollower?",
                   "true", details.get("isFollower"));
      assertNotNull("repeater has leader section",
                    details.get("leader"));
      assertNotNull("repeater has follower section",
                    details.get("follower"));

    } finally {
      try { 
        if (repeaterJetty != null) repeaterJetty.stop(); 
      } catch (Exception e) { /* :NOOP: */ }
      if (repeaterClient != null) repeaterClient.close();
    }
  }
  
  @Test
  public void testLegacyConfiguration() throws Exception {
    SolrInstance solrInstance = null;
    JettySolrRunner instanceJetty = null;
    SolrClient client = null;
    try {
      solrInstance = new SolrInstance(createTempDir("solr-instance").toFile(), "replication-legacy", leaderJetty.getLocalPort());
      solrInstance.setUp();
      instanceJetty = createAndStartJetty(solrInstance);
      client = createNewSolrClient(instanceJetty.getLocalPort());

      
      NamedList<Object> details = getDetails(client);
      
      assertEquals("repeater isLeader?",
                   "true", details.get("isLeader"));
      assertEquals("repeater isFollower?",
                   "true", details.get("isFollower"));
      assertNotNull("repeater has leader section",
                    details.get("leader"));
      assertNotNull("repeater has follower section",
                    details.get("follower"));

    } finally {
      if (instanceJetty != null) {
        instanceJetty.stop();
      }
      if (client != null) client.close();
    }
  }


  /**
   * Verify that empty commits and/or commits with openSearcher=false
   * on the leader do not cause subsequent replication problems on the follower
   */
  public void testEmptyCommits() throws Exception {
    clearIndexWithReplication();
    
    // add a doc to leader and commit
    index(leaderClient, "id", "1", "name", "empty1");
    emptyUpdate(leaderClient, "commit", "true");
    // force replication
    pullFromLeaderToFollower();
    // verify doc is on follower
    rQuery(1, "name:empty1", followerClient);
    assertVersions(leaderClient, followerClient);

    // do a completely empty commit on leader and force replication
    emptyUpdate(leaderClient, "commit", "true");
    pullFromLeaderToFollower();

    // add another doc and verify follower gets it
    index(leaderClient, "id", "2", "name", "empty2");
    emptyUpdate(leaderClient, "commit", "true");
    // force replication
    pullFromLeaderToFollower();

    rQuery(1, "name:empty2", followerClient);
    assertVersions(leaderClient, followerClient);

    // add a third doc but don't open a new searcher on leader
    index(leaderClient, "id", "3", "name", "empty3");
    emptyUpdate(leaderClient, "commit", "true", "openSearcher", "false");
    pullFromLeaderToFollower();
    
    // verify follower can search the doc, but leader doesn't
    rQuery(0, "name:empty3", leaderClient);
    rQuery(1, "name:empty3", followerClient);

    // final doc with hard commit, follower and leader both showing all docs
    index(leaderClient, "id", "4", "name", "empty4");
    emptyUpdate(leaderClient, "commit", "true");
    pullFromLeaderToFollower();

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

    invokeReplicationCommand(leaderJetty.getLocalPort(), "disableReplication");
    invokeReplicationCommand(followerJetty.getLocalPort(), "disablepoll");
    
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

    //this doc is added to follower so it should show an item w/ that result
    assertEquals(1, numFound(rQuery(1, "id:555", followerClient)));

    //Let's fetch the index rather than rely on the polling.
    invokeReplicationCommand(leaderJetty.getLocalPort(), "enablereplication");
    invokeReplicationCommand(followerJetty.getLocalPort(), "fetchindex");

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
    for (int i = 0; i < nDocs; i++)
      index(leaderClient, "id", i, "name", "name = " + i);

    leaderClient.commit();

    NamedList<Object> leaderQueryRsp = rQuery(nDocs, "*:*", leaderClient);
    SolrDocumentList leaderQueryResult = (SolrDocumentList) leaderQueryRsp.get("response");
    assertEquals(nDocs, numFound(leaderQueryRsp));

    //get docs from follower and check if number is equal to leader
    NamedList<Object> followerQueryRsp = rQuery(nDocs, "*:*", followerClient);
    SolrDocumentList followerQueryResult = (SolrDocumentList) followerQueryRsp.get("response");
    assertEquals(nDocs, numFound(followerQueryRsp));

    //compare results
    String cmp = BaseDistributedSearchTestCase.compare(leaderQueryResult, followerQueryResult, 0, null);
    assertNull(cmp);
    
    assertVersions(leaderClient, followerClient);

    //start config files replication test
    leaderClient.deleteByQuery("*:*");
    leaderClient.commit();

    //change the schema on leader
    leader.copyConfigFile(CONF_DIR + "schema-replication2.xml", "schema.xml");

    leaderJetty.stop();

    leaderJetty = createAndStartJetty(leader);
    leaderClient.close();
    leaderClient = createNewSolrClient(leaderJetty.getLocalPort());

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
    followerClient = createNewSolrClient(followerJetty.getLocalPort());
    //add a doc with new field and commit on leader to trigger index fetch from follower.
    index(leaderClient, "id", "2000", "name", "name = " + 2000, "newname", "newname = " + 2000);
    leaderClient.commit();

    assertEquals(1, numFound( rQuery(1, "*:*", leaderClient)));
    
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
    for (int i = 0; i < nDocs; i++)
      index(leaderClient, "id", i, "name", "name = " + i);

    leaderClient.commit();

    NamedList<Object> leaderQueryRsp = rQuery(nDocs, "*:*", leaderClient);
    SolrDocumentList leaderQueryResult = (SolrDocumentList) leaderQueryRsp.get("response");
    assertEquals(nDocs, numFound(leaderQueryRsp));

    //get docs from follower and check if number is equal to leader
    NamedList<Object> followerQueryRsp = rQuery(nDocs, "*:*", followerClient);
    SolrDocumentList followerQueryResult = (SolrDocumentList) followerQueryRsp.get("response");
    assertEquals(nDocs, numFound(followerQueryRsp));

    //compare results
    String cmp = BaseDistributedSearchTestCase.compare(leaderQueryResult, followerQueryResult, 0, null);
    assertNull(cmp);

    // start stop polling test
    invokeReplicationCommand(followerJetty.getLocalPort(), "disablepoll");
    
    index(leaderClient, "id", 501, "name", "name = " + 501);
    leaderClient.commit();

    //get docs from leader and check if number is equal to leader
    assertEquals(nDocs+1, numFound(rQuery(nDocs+1, "*:*", leaderClient)));
    
    // NOTE: this test is wierd, we want to verify it DOESNT replicate...
    // for now, add a sleep for this.., but the logic is wierd.
    Thread.sleep(3000);
    
    //get docs from follower and check if number is not equal to leader; polling is disabled
    assertEquals(nDocs, numFound(rQuery(nDocs, "*:*", followerClient)));

    // re-enable replication
    invokeReplicationCommand(followerJetty.getLocalPort(), "enablepoll");

    assertEquals(nDocs+1, numFound(rQuery(nDocs+1, "*:*", followerClient)));
  }

  /**
   * We assert that if leader is down for more than poll interval,
   * the follower doesn't re-fetch the whole index from leader again if
   * the index hasn't changed. See SOLR-9036
   */
  @Test
  public void doTestIndexFetchOnLeaderRestart() throws Exception  {
    useFactory(null);
    try {
      clearIndexWithReplication();
      // change solrconfig having 'replicateAfter startup' option on leader
      leader.copyConfigFile(CONF_DIR + "solrconfig-leader2.xml",
          "solrconfig.xml");

      leaderJetty.stop();
      leaderJetty.start();

      // close and re-create leader client because its connection pool has stale connections
      leaderClient.close();
      leaderClient = createNewSolrClient(leaderJetty.getLocalPort());

      nDocs--;
      for (int i = 0; i < nDocs; i++)
        index(leaderClient, "id", i, "name", "name = " + i);

      leaderClient.commit();

      NamedList<Object> leaderQueryRsp = rQuery(nDocs, "*:*", leaderClient);
      SolrDocumentList leaderQueryResult = (SolrDocumentList) leaderQueryRsp.get("response");
      assertEquals(nDocs, numFound(leaderQueryRsp));

      //get docs from follower and check if number is equal to leader
      NamedList<Object> followerQueryRsp = rQuery(nDocs, "*:*", followerClient);
      SolrDocumentList followerQueryResult = (SolrDocumentList) followerQueryRsp.get("response");
      assertEquals(nDocs, numFound(followerQueryRsp));

      //compare results
      String cmp = BaseDistributedSearchTestCase.compare(leaderQueryResult, followerQueryResult, 0, null);
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
        // Sometimes replication will fail because leader's core is still loading; make sure there was one success
        assertEquals(1, timesReplicated - previousTimesFailed);

      }

      leaderJetty.stop();

      final TimeOut waitForLeaderToShutdown = new TimeOut(300, TimeUnit.SECONDS, TimeSource.NANO_TIME);
      waitForLeaderToShutdown.waitFor
        ("Gave up after waiting an obscene amount of time for leader to shut down",
         () -> leaderJetty.isStopped() );
        
      for(int retries=0; ;retries++) { 

        Thread.yield(); // might not be necessary at all
        // poll interval on follower is 1 second, so we just sleep for a few seconds
        Thread.sleep(2000);
        
        NamedList<Object> followerDetails=null;
        try {
          followerDetails = getFollowerDetails();
          int failed = Integer.parseInt(getStringOrNull(followerDetails,"timesFailed"));
          if (previousTimesFailed != null) {
            assertTrue(failed > previousTimesFailed);
          }
          assertEquals(1, Integer.parseInt(getStringOrNull(followerDetails,"timesIndexReplicated")) - failed);
          break;
        } catch (NumberFormatException | AssertionError notYet) {
          if (log.isInfoEnabled()) {
            log.info("{}th attempt failure on {} details are {}", retries + 1, notYet, followerDetails); // nowarn
          }
          if (retries>9) {
            log.error("giving up: ", notYet);
            throw notYet;
          } 
        }
      }
      
      leaderJetty.start();

      // poll interval on follower is 1 second, so we just sleep for a few seconds
      Thread.sleep(2000);
      //get docs from follower and assert that they are still the same as before
      followerQueryRsp = rQuery(nDocs, "*:*", followerClient);
      followerQueryResult = (SolrDocumentList) followerQueryRsp.get("response");
      assertEquals(nDocs, numFound(followerQueryRsp));

    } finally {
      resetFactory();
    }
  }

  @Test
  public void doTestIndexFetchWithLeaderUrl() throws Exception {
    //change solrconfig on follower
    //this has no entry for pollinginterval
    follower.setTestPort(leaderJetty.getLocalPort());
    follower.copyConfigFile(CONF_DIR + "solrconfig-follower1.xml", "solrconfig.xml");
    followerJetty.stop();
    followerJetty = createAndStartJetty(follower);
    followerClient.close();
    followerClient = createNewSolrClient(followerJetty.getLocalPort());

    leaderClient.deleteByQuery("*:*");
    followerClient.deleteByQuery("*:*");
    followerClient.commit();
    nDocs--;
    for (int i = 0; i < nDocs; i++)
      index(leaderClient, "id", i, "name", "name = " + i);

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
    String leaderUrl = buildUrl(followerJetty.getLocalPort()) + "/" + DEFAULT_TEST_CORENAME + ReplicationHandler.PATH+"?command=fetchindex&" + urlKey + "=";
    leaderUrl += buildUrl(leaderJetty.getLocalPort()) + "/" + DEFAULT_TEST_CORENAME + ReplicationHandler.PATH;
    URL url = new URL(leaderUrl);
    InputStream stream = url.openStream();
    stream.close();
    
    //get docs from follower and check if number is equal to leader
    NamedList<Object> followerQueryRsp = rQuery(nDocs, "*:*", followerClient);
    SolrDocumentList followerQueryResult = (SolrDocumentList) followerQueryRsp.get("response");
    assertEquals(nDocs, followerQueryResult.getNumFound());
    //compare results
    String cmp = BaseDistributedSearchTestCase.compare(leaderQueryResult, followerQueryResult, 0, null);
    assertEquals(null, cmp);

    // index fetch from the follower to the leader
    
    for (int i = nDocs; i < nDocs + 3; i++)
      index(followerClient, "id", i, "name", "name = " + i);

    followerClient.commit();
    
    pullFromFollowerToLeader();
    rQuery(nDocs + 3, "*:*", leaderClient);
    
    //get docs from follower and check if number is equal to leader
    followerQueryRsp = rQuery(nDocs + 3, "*:*", followerClient);
    followerQueryResult = (SolrDocumentList) followerQueryRsp.get("response");
    assertEquals(nDocs + 3, followerQueryResult.getNumFound());
    //compare results
    leaderQueryRsp = rQuery(nDocs + 3, "*:*", leaderClient);
    leaderQueryResult = (SolrDocumentList) leaderQueryRsp.get("response");
    cmp = BaseDistributedSearchTestCase.compare(leaderQueryResult, followerQueryResult, 0, null);
    assertEquals(null, cmp);

    assertVersions(leaderClient, followerClient);
    
    pullFromFollowerToLeader();
    
    //get docs from follower and check if number is equal to leader
    followerQueryRsp = rQuery(nDocs + 3, "*:*", followerClient);
    followerQueryResult = (SolrDocumentList) followerQueryRsp.get("response");
    assertEquals(nDocs + 3, followerQueryResult.getNumFound());
    //compare results
    leaderQueryRsp = rQuery(nDocs + 3, "*:*", leaderClient);
    leaderQueryResult = (SolrDocumentList) leaderQueryRsp.get("response");
    cmp = BaseDistributedSearchTestCase.compare(leaderQueryResult, followerQueryResult, 0, null);
    assertEquals(null, cmp);
    
    assertVersions(leaderClient, followerClient);
    
    // now force a new index directory
    for (int i = nDocs + 3; i < nDocs + 7; i++)
      index(leaderClient, "id", i, "name", "name = " + i);
    
    leaderClient.commit();
    
    pullFromFollowerToLeader();
    rQuery((int) followerQueryResult.getNumFound(), "*:*", leaderClient);
    
    //get docs from follower and check if number is equal to leader
    followerQueryRsp = rQuery(nDocs + 3, "*:*", followerClient);
    followerQueryResult = (SolrDocumentList) followerQueryRsp.get("response");
    assertEquals(nDocs + 3, followerQueryResult.getNumFound());
    //compare results
    leaderQueryRsp = rQuery(nDocs + 3, "*:*", leaderClient);
    leaderQueryResult = (SolrDocumentList) leaderQueryRsp.get("response");
    cmp = BaseDistributedSearchTestCase.compare(leaderQueryResult, followerQueryResult, 0, null);
    assertEquals(null, cmp);
    
    assertVersions(leaderClient, followerClient);
    pullFromFollowerToLeader();
    
    //get docs from follower and check if number is equal to leader
    followerQueryRsp = rQuery(nDocs + 3, "*:*", followerClient);
    followerQueryResult = (SolrDocumentList) followerQueryRsp.get("response");
    assertEquals(nDocs + 3, followerQueryResult.getNumFound());
    //compare results
    leaderQueryRsp = rQuery(nDocs + 3, "*:*", leaderClient);
    leaderQueryResult = (SolrDocumentList) leaderQueryRsp.get("response");
    cmp = BaseDistributedSearchTestCase.compare(leaderQueryResult, followerQueryResult, 0, null);
    assertEquals(null, cmp);
    
    assertVersions(leaderClient, followerClient);
    
    NamedList<Object> details = getDetails(leaderClient);
   
    details = getDetails(followerClient);
    
    checkForSingleIndex(leaderJetty);
    checkForSingleIndex(followerJetty);
  }
  
  
  @Test
  //commented 20-Sep-2018  @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // added 17-Aug-2018
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
      follower.copyConfigFile(CONF_DIR +"solrconfig-follower1.xml", "solrconfig.xml");
      follower.copyConfigFile(CONF_DIR +followerSchema, "schema.xml");
      followerJetty.stop();
      followerJetty = createAndStartJetty(follower);
      followerClient.close();
      followerClient = createNewSolrClient(followerJetty.getLocalPort());

      leader.copyConfigFile(CONF_DIR + "solrconfig-leader3.xml",
          "solrconfig.xml");
      leaderJetty.stop();
      leaderJetty = createAndStartJetty(leader);
      leaderClient.close();
      leaderClient = createNewSolrClient(leaderJetty.getLocalPort());
      
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

          followerSchema = followerSchema.equals(FOLLOWER_SCHEMA_1) ?
            FOLLOWER_SCHEMA_2 : FOLLOWER_SCHEMA_1;
          leader.copyConfigFile(CONF_DIR + followerSchema, "schema.xml");
        }
        
        int docs = random().nextInt(maxDocs) + 1;
        for (int i = 0; i < docs; i++) {
          index(leaderClient, "id", id++, "name", "name = " + i);
        }
        
        totalDocs += docs;
        leaderClient.commit();
        
        NamedList<Object> leaderQueryRsp = rQuery(totalDocs, "*:*", leaderClient);
        SolrDocumentList leaderQueryResult = (SolrDocumentList) leaderQueryRsp
            .get("response");
        assertEquals(totalDocs, leaderQueryResult.getNumFound());
        
        // index fetch
        Date followerCoreStart = watchCoreStartAt(followerClient, 30*1000, null);
        pullFromLeaderToFollower();
        if (confCoreReload) {
          watchCoreStartAt(followerClient, 30*1000, followerCoreStart);
        }

        // get docs from follower and check if number is equal to leader
        NamedList<Object> followerQueryRsp = rQuery(totalDocs, "*:*", followerClient);
        SolrDocumentList followerQueryResult = (SolrDocumentList) followerQueryRsp
            .get("response");
        assertEquals(totalDocs, followerQueryResult.getNumFound());
        // compare results
        String cmp = BaseDistributedSearchTestCase.compare(leaderQueryResult,
            followerQueryResult, 0, null);
        assertEquals(null, cmp);
        
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

  @Test
  public void doTestRepeater() throws Exception {
    // no polling
    follower.setTestPort(leaderJetty.getLocalPort());
    follower.copyConfigFile(CONF_DIR + "solrconfig-follower1.xml", "solrconfig.xml");
    followerJetty.stop();
    followerJetty = createAndStartJetty(follower);
    followerClient.close();
    followerClient = createNewSolrClient(followerJetty.getLocalPort());

    try {
      repeater = new SolrInstance(createTempDir("solr-instance").toFile(), "repeater", leaderJetty.getLocalPort());
      repeater.setUp();
      repeater.copyConfigFile(CONF_DIR + "solrconfig-repeater.xml",
          "solrconfig.xml");
      repeaterJetty = createAndStartJetty(repeater);
      if (repeaterClient != null) {
        repeaterClient.close();
      }
      repeaterClient = createNewSolrClient(repeaterJetty.getLocalPort());
      
      for (int i = 0; i < 3; i++)
        index(leaderClient, "id", i, "name", "name = " + i);

      leaderClient.commit();
      
      pullFromTo(leaderJetty, repeaterJetty);
      
      rQuery(3, "*:*", repeaterClient);
      
      pullFromTo(repeaterJetty, followerJetty);
      
      rQuery(3, "*:*", followerClient);
      
      assertVersions(leaderClient, repeaterClient);
      assertVersions(repeaterClient, followerClient);
      
      for (int i = 0; i < 4; i++)
        index(repeaterClient, "id", i, "name", "name = " + i);
      repeaterClient.commit();
      
      pullFromTo(leaderJetty, repeaterJetty);
      
      rQuery(3, "*:*", repeaterClient);
      
      pullFromTo(repeaterJetty, followerJetty);
      
      rQuery(3, "*:*", followerClient);
      
      for (int i = 3; i < 6; i++)
        index(leaderClient, "id", i, "name", "name = " + i);
      
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
    //stop follower
    followerJetty.stop();

    nDocs--;
    leaderClient.deleteByQuery("*:*");

    leaderClient.commit();



    //change solrconfig having 'replicateAfter startup' option on leader
    leader.copyConfigFile(CONF_DIR + "solrconfig-leader2.xml",
                          "solrconfig.xml");

    leaderJetty.stop();

    leaderJetty = createAndStartJetty(leader);
    leaderClient.close();
    leaderClient = createNewSolrClient(leaderJetty.getLocalPort());
    
    for (int i = 0; i < nDocs; i++)
      index(leaderClient, "id", i, "name", "name = " + i);

    leaderClient.commit();
    
    NamedList<Object> leaderQueryRsp = rQuery(nDocs, "*:*", leaderClient);
    SolrDocumentList leaderQueryResult = (SolrDocumentList) leaderQueryRsp.get("response");
    assertEquals(nDocs, leaderQueryResult.getNumFound());
    

    follower.setTestPort(leaderJetty.getLocalPort());
    follower.copyConfigFile(follower.getSolrConfigFile(), "solrconfig.xml");

    //start follower
    followerJetty = createAndStartJetty(follower);
    followerClient.close();
    followerClient = createNewSolrClient(followerJetty.getLocalPort());

    //get docs from follower and check if number is equal to leader
    NamedList<Object> followerQueryRsp = rQuery(nDocs, "*:*", followerClient);
    SolrDocumentList followerQueryResult = (SolrDocumentList) followerQueryRsp.get("response");
    assertEquals(nDocs, followerQueryResult.getNumFound());

    //compare results
    String cmp = BaseDistributedSearchTestCase.compare(leaderQueryResult, followerQueryResult, 0, null);
    assertEquals(null, cmp);

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
      leader.copyConfigFile(CONF_DIR + "solrconfig-leader2.xml",
          "solrconfig.xml");
      
      leaderJetty.stop();
      
      leaderJetty = createAndStartJetty(leader);
      leaderClient.close();
      leaderClient = createNewSolrClient(leaderJetty.getLocalPort());
      
      for (int i = 0; i < nDocs; i++)
        index(leaderClient, "id", i, "name", "name = " + i);
      
      leaderClient.commit();
      
      // now we restart to test what happens with no activity before the follower
      // tries to
      // replicate
      leaderJetty.stop();
      leaderJetty.start();
      
      // leaderClient = createNewSolrClient(leaderJetty.getLocalPort());
      
      NamedList<Object> leaderQueryRsp = rQuery(nDocs, "*:*", leaderClient);
      SolrDocumentList leaderQueryResult = (SolrDocumentList) leaderQueryRsp
          .get("response");
      assertEquals(nDocs, leaderQueryResult.getNumFound());
      
      follower.setTestPort(leaderJetty.getLocalPort());
      follower.copyConfigFile(follower.getSolrConfigFile(), "solrconfig.xml");
      
      // start follower
      followerJetty = createAndStartJetty(follower);
      followerClient.close();
      followerClient = createNewSolrClient(followerJetty.getLocalPort());
      
      // get docs from follower and check if number is equal to leader
      NamedList<Object> followerQueryRsp = rQuery(nDocs, "*:*", followerClient);
      SolrDocumentList followerQueryResult = (SolrDocumentList) followerQueryRsp
          .get("response");
      assertEquals(nDocs, followerQueryResult.getNumFound());
      
      // compare results
      String cmp = BaseDistributedSearchTestCase.compare(leaderQueryResult,
          followerQueryResult, 0, null);
      assertEquals(null, cmp);
      
    } finally {
      resetFactory();
    }
  }

  @Test
  public void doTestReplicateAfterCoreReload() throws Exception {
    int docs = TEST_NIGHTLY ? 200000 : 10;
    
    //stop follower
    followerJetty.stop();


    //change solrconfig having 'replicateAfter startup' option on leader
    leader.copyConfigFile(CONF_DIR + "solrconfig-leader3.xml",
                          "solrconfig.xml");

    leaderJetty.stop();

    leaderJetty = createAndStartJetty(leader);
    leaderClient.close();
    leaderClient = createNewSolrClient(leaderJetty.getLocalPort());

    leaderClient.deleteByQuery("*:*");
    for (int i = 0; i < docs; i++)
      index(leaderClient, "id", i, "name", "name = " + i);

    leaderClient.commit();

    NamedList<Object> leaderQueryRsp = rQuery(docs, "*:*", leaderClient);
    SolrDocumentList leaderQueryResult = (SolrDocumentList) leaderQueryRsp.get("response");
    assertEquals(docs, leaderQueryResult.getNumFound());
    
    follower.setTestPort(leaderJetty.getLocalPort());
    follower.copyConfigFile(follower.getSolrConfigFile(), "solrconfig.xml");

    //start follower
    followerJetty = createAndStartJetty(follower);
    followerClient.close();
    followerClient = createNewSolrClient(followerJetty.getLocalPort());
    
    //get docs from follower and check if number is equal to leader
    NamedList<Object> followerQueryRsp = rQuery(docs, "*:*", followerClient);
    SolrDocumentList followerQueryResult = (SolrDocumentList) followerQueryRsp.get("response");
    assertEquals(docs, followerQueryResult.getNumFound());
    
    //compare results
    String cmp = BaseDistributedSearchTestCase.compare(leaderQueryResult, followerQueryResult, 0, null);
    assertEquals(null, cmp);
    
    Object version = getIndexVersion(leaderClient).get("indexversion");
    
    reloadCore(leaderClient, "collection1");
    
    assertEquals(version, getIndexVersion(leaderClient).get("indexversion"));
    
    index(leaderClient, "id", docs + 10, "name", "name = 1");
    index(leaderClient, "id", docs + 20, "name", "name = 2");

    leaderClient.commit();
    
    NamedList<Object> resp = rQuery(docs + 2, "*:*", leaderClient);
    leaderQueryResult = (SolrDocumentList) resp.get("response");
    assertEquals(docs + 2, leaderQueryResult.getNumFound());
    
    //get docs from follower and check if number is equal to leader
    followerQueryRsp = rQuery(docs + 2, "*:*", followerClient);
    followerQueryResult = (SolrDocumentList) followerQueryRsp.get("response");
    assertEquals(docs + 2, followerQueryResult.getNumFound());
    
  }

  @Test
  // 12-Jun-2018 @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // 04-May-2018
  public void doTestIndexAndConfigAliasReplication() throws Exception {
    clearIndexWithReplication();

    nDocs--;
    for (int i = 0; i < nDocs; i++)
      index(leaderClient, "id", i, "name", "name = " + i);

    leaderClient.commit();

    NamedList<Object> leaderQueryRsp = rQuery(nDocs, "*:*", leaderClient);
    SolrDocumentList leaderQueryResult = (SolrDocumentList) leaderQueryRsp.get("response");
    assertEquals(nDocs, leaderQueryResult.getNumFound());

    //get docs from follower and check if number is equal to leader
    NamedList<Object> followerQueryRsp = rQuery(nDocs, "*:*", followerClient);
    SolrDocumentList followerQueryResult = (SolrDocumentList) followerQueryRsp.get("response");

    assertEquals(nDocs, followerQueryResult.getNumFound());

    //compare results
    String cmp = BaseDistributedSearchTestCase.compare(leaderQueryResult, followerQueryResult, 0, null);
    assertEquals(null, cmp);

    //start config files replication test
    //clear leader index
    leaderClient.deleteByQuery("*:*");
    leaderClient.commit();
    rQuery(0, "*:*", leaderClient); // sanity check w/retry

    //change solrconfig on leader
    leader.copyConfigFile(CONF_DIR + "solrconfig-leader1.xml",
                          "solrconfig.xml");

    //change schema on leader
    leader.copyConfigFile(CONF_DIR + "schema-replication2.xml",
                          "schema.xml");

    //keep a copy of the new schema
    leader.copyConfigFile(CONF_DIR + "schema-replication2.xml",
                          "schema-replication2.xml");

    leaderJetty.stop();

    leaderJetty = createAndStartJetty(leader);
    leaderClient.close();
    leaderClient = createNewSolrClient(leaderJetty.getLocalPort());

    follower.setTestPort(leaderJetty.getLocalPort());
    follower.copyConfigFile(follower.getSolrConfigFile(), "solrconfig.xml");

    followerJetty.stop();
    followerJetty = createAndStartJetty(follower);
    followerClient.close();
    followerClient = createNewSolrClient(followerJetty.getLocalPort());

    followerClient.deleteByQuery("*:*");
    followerClient.commit();
    rQuery(0, "*:*", followerClient); // sanity check w/retry
    
    // record collection1's start time on follower
    final Date followerStartTime = watchCoreStartAt(followerClient, 30*1000, null);

    //add a doc with new field and commit on leader to trigger index fetch from follower.
    index(leaderClient, "id", "2000", "name", "name = " + 2000, "newname", "n2000");
    leaderClient.commit();
    rQuery(1, "newname:n2000", leaderClient);  // sanity check

    // wait for follower to reload core by watching updated startTime
    watchCoreStartAt(followerClient, 30*1000, followerStartTime);

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

    //clean index
    leaderClient.deleteByQuery("*:*");
    followerClient.deleteByQuery("*:*");
    leaderClient.commit();
    followerClient.commit();

    leaderJetty.stop();
    followerJetty.stop();

    //Start leader with the new solrconfig
    leader.copyConfigFile(CONF_DIR + "solrconfig-leader-throttled.xml", "solrconfig.xml");
    useFactory(null);
    leaderJetty = createAndStartJetty(leader);
    leaderClient.close();
    leaderClient = createNewSolrClient(leaderJetty.getLocalPort());

    //index docs
    final int totalDocs = TestUtil.nextInt(random(), 17, 53);
    for (int i = 0; i < totalDocs; i++)
      index(leaderClient, "id", i, "name", TestUtil.randomSimpleString(random(), 1000 , 5000));

    leaderClient.commit();

    //Check Index Size
    String dataDir = leader.getDataDir();
    leaderClient.close();
    leaderJetty.stop();

    Directory dir = FSDirectory.open(Paths.get(dataDir).resolve("index"));
    String[] files = dir.listAll();
    long totalBytes = 0;
    for(String file : files) {
      totalBytes += dir.fileLength(file);
    }

    float approximateTimeInSeconds = Math.round( totalBytes/1024/1024/0.1 ); // maxWriteMBPerSec=0.1 in solrconfig

    //Start again and replicate the data
    useFactory(null);
    leaderJetty = createAndStartJetty(leader);
    leaderClient = createNewSolrClient(leaderJetty.getLocalPort());

    //start follower
    follower.setTestPort(leaderJetty.getLocalPort());
    follower.copyConfigFile(CONF_DIR + "solrconfig-follower1.xml", "solrconfig.xml");
    followerJetty = createAndStartJetty(follower);
    followerClient.close();
    followerClient = createNewSolrClient(followerJetty.getLocalPort());

    long startTime = System.nanoTime();

    pullFromLeaderToFollower();

    //Add a few more docs in the leader. Just to make sure that we are replicating the correct index point
    //These extra docs should not get replicated
    new Thread(new AddExtraDocs(leaderClient, totalDocs)).start();

    //Wait and make sure that it actually replicated correctly.
    NamedList<Object> followerQueryRsp = rQuery(totalDocs, "*:*", followerClient);
    SolrDocumentList followerQueryResult = (SolrDocumentList) followerQueryRsp.get("response");
    assertEquals(totalDocs, followerQueryResult.getNumFound());

    long timeTaken = System.nanoTime() - startTime;

    long timeTakenInSeconds = TimeUnit.SECONDS.convert(timeTaken, TimeUnit.NANOSECONDS);

    //Let's make sure it took more than approximateTimeInSeconds to make sure that it was throttled
    log.info("approximateTimeInSeconds = {} timeTakenInSeconds = {}"
        , approximateTimeInSeconds, timeTakenInSeconds);
    assertTrue(timeTakenInSeconds - approximateTimeInSeconds > 0);
  }

  @Test
  public void doTestIllegalFilePaths() throws Exception {
    // Loop through the file=, cf=, tlogFile= params and prove that it throws exception for path traversal attempts
    String absFile = Paths.get("foo").toAbsolutePath().toString();
    List<String> illegalFilenames = Arrays.asList(absFile, "../dir/traversal", "illegal\rfile\nname\t");
    List<String> params = Arrays.asList(ReplicationHandler.FILE, ReplicationHandler.CONF_FILE_SHORT, ReplicationHandler.TLOG_FILE);
    for (String param : params) {
      for (String filename : illegalFilenames) {
        expectThrows(Exception.class, () ->
            invokeReplicationCommand(leaderJetty.getLocalPort(), "filecontent&" + param + "=" + filename));
      }
    }
  }

  @Test
  public void testFileListShouldReportErrorsWhenTheyOccur() throws Exception {
    SolrQuery q = new SolrQuery();
    q.add("qt", "/replication")
        .add("wt", "json")
        .add("command", "filelist")
        .add("generation", "-2"); // A 'generation' value not matching any commit point should cause error.
    QueryResponse response = followerClient.query(q);
    NamedList<Object> resp = response.getResponse();
    assertNotNull(resp);
    assertEquals("ERROR", resp.get("status"));
    assertEquals("invalid index generation", resp.get("message"));
  }

  @Test
  public void testFetchIndexShouldReportErrorsWhenTheyOccur() throws Exception  {
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
    assertEquals("Fetch index with wait=true should have returned an error response", "ERROR", resp.get("status"));
  }

  @Test
  public void testShouldReportErrorWhenRequiredCommandArgMissing() throws Exception {
    SolrQuery q = new SolrQuery();
    q.add("qt", "/replication")
        .add("wt", "json");
    SolrException thrown = expectThrows(SolrException.class, () -> {
      followerClient.query(q);
    });
    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, thrown.code());
    assertThat(thrown.getMessage(), containsString("Missing required parameter: command"));
  }

  @Test
  public void testShouldReportErrorWhenDeletingBackupButNameMissing() {
    SolrQuery q = new SolrQuery();
    q.add("qt", "/replication")
        .add("wt", "json")
        .add("command", "deletebackup");
    SolrException thrown = expectThrows(SolrException.class, () -> {
      followerClient.query(q);
    });
    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, thrown.code());
    assertThat(thrown.getMessage(), containsString("Missing required parameter: name"));
  }

  @Test
  public void testEmptyBackups() throws Exception {
    final File backupDir = createTempDir().toFile();
    final BackupStatusChecker backupStatus = new BackupStatusChecker(leaderClient);

    leaderJetty.getCoreContainer().getAllowPaths().add(backupDir.toPath());

    { // initial request w/o any committed docs
      final String backupName = "empty_backup1";
      final GenericSolrRequest req = new GenericSolrRequest
        (SolrRequest.METHOD.GET, "/replication",
         params("command", "backup",
                "location", backupDir.getAbsolutePath(),
                "name", backupName));
      final TimeOut timeout = new TimeOut(30, TimeUnit.SECONDS, TimeSource.NANO_TIME);
      final SimpleSolrResponse rsp = req.process(leaderClient);

      final String dirName = backupStatus.waitForBackupSuccess(backupName, timeout);
      assertEquals("Did not get expected dir name for backup, did API change?",
                   "snapshot.empty_backup1", dirName);
      assertTrue(dirName + " doesn't exist in expected location for backup " + backupName,
                 new File(backupDir, dirName).exists());
    }
    
    index(leaderClient, "id", "1", "name", "foo");
    
    { // second backup w/uncommited doc
      final String backupName = "empty_backup2";
      final GenericSolrRequest req = new GenericSolrRequest
        (SolrRequest.METHOD.GET, "/replication",
         params("command", "backup",
                "location", backupDir.getAbsolutePath(),
                "name", backupName));
      final TimeOut timeout = new TimeOut(30, TimeUnit.SECONDS, TimeSource.NANO_TIME);
      final SimpleSolrResponse rsp = req.process(leaderClient);
      
      final String dirName = backupStatus.waitForBackupSuccess(backupName, timeout);
      assertEquals("Did not get expected dir name for backup, did API change?",
                   "snapshot.empty_backup2", dirName);
      assertTrue(dirName + " doesn't exist in expected location for backup " + backupName,
                 new File(backupDir, dirName).exists());
    }

    // confirm backups really are empty
    for (int i = 1; i <=2; i++) {
      final String name = "snapshot.empty_backup"+i;
      try (Directory dir = new NIOFSDirectory(new File(backupDir, name).toPath());
           IndexReader reader = DirectoryReader.open(dir)) {
        assertEquals(name + " is not empty", 0, reader.numDocs());
      }
    }
  }
  
  public void testGetBoolWithBackwardCompatibility() {
    assertTrue(ReplicationHandler.getBoolWithBackwardCompatibility(params(), "foo", "bar", true));
    assertFalse(ReplicationHandler.getBoolWithBackwardCompatibility(params(), "foo", "bar", false));
    assertTrue(ReplicationHandler.getBoolWithBackwardCompatibility(params("foo", "true"), "foo", "bar", false));
    assertTrue(ReplicationHandler.getBoolWithBackwardCompatibility(params("bar", "true"), "foo", "bar", false));
    assertTrue(ReplicationHandler.getBoolWithBackwardCompatibility(params("foo", "true", "bar", "false"), "foo", "bar", false));
  }
  
  public void testGetObjectWithBackwardCompatibility() {
    assertEquals("aaa", ReplicationHandler.getObjectWithBackwardCompatibility(params(), "foo", "bar", "aaa"));
    assertEquals("bbb", ReplicationHandler.getObjectWithBackwardCompatibility(params("foo", "bbb"), "foo", "bar", "aaa"));
    assertEquals("bbb", ReplicationHandler.getObjectWithBackwardCompatibility(params("bar", "bbb"), "foo", "bar", "aaa"));
    assertEquals("bbb", ReplicationHandler.getObjectWithBackwardCompatibility(params("foo", "bbb", "bar", "aaa"), "foo", "bar", "aaa"));
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
  
  
  private class AddExtraDocs implements Runnable {

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
          index(leaderClient, "id", i + startId, "name", TestUtil.randomSimpleString(random(), 1000 , 5000));
        } catch (Exception e) {
          //Do nothing. Wasn't able to add doc.
        }
      }
      try {
        leaderClient.commit();
      } catch (Exception e) {
        //Do nothing. No extra doc got committed.
      }
    }
  }
}
