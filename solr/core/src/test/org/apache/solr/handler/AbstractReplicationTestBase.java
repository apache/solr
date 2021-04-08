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

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettyConfig;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.CachingDirectoryFactory;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.StandardDirectoryFactory;
import org.apache.solr.core.snapshots.SolrSnapshotMetaDataManager;
import org.apache.solr.util.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.invoke.MethodHandles;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.*;


/**
 * Base code for orchestrating legacy replication based tests
 *
 *
 */
public abstract class AbstractReplicationTestBase extends SolrTestCaseJ4 {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  protected static final String CONF_DIR = "solr"
      + File.separator + "collection1" + File.separator + "conf"
      + File.separator;

  protected JettySolrRunner leaderJetty, followerJetty, repeaterJetty;
  protected HttpSolrClient leaderClient, followerClient, repeaterClient;
  protected SolrInstance leader = null, follower = null, repeater = null;

  protected static String context = "/solr";

  // number of docs to index... decremented for each test case to tell if we accidentally reuse
  // index from previous test method
  protected static int nDocs = 500;
  
  /* For testing backward compatibility, remove for 10.x */
  protected static boolean useLegacyParams = false;
  
  @BeforeClass
  public static void beforeClass() {
    useLegacyParams = rarely();

  }
  
  @Before
  public void setUp() throws Exception {
    super.setUp();
    leader = new SolrInstance(createTempDir("solr-instance").toFile(), "leader", null);
    leader.setUp();
    leaderJetty = createAndStartJetty(leader);
    leaderClient = createNewSolrClient(leaderJetty.getLocalPort(), true);

    follower = new SolrInstance(createTempDir("solr-instance").toFile(), "follower", leaderJetty.getLocalPort());
    follower.setUp();
    followerJetty = createAndStartJetty(follower);
    followerClient = createNewSolrClient(followerJetty.getLocalPort(), true);

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

  protected static JettySolrRunner createAndStartJetty(SolrInstance instance) throws Exception {
    FileUtils.copyFile(new File(SolrTestCaseJ4.TEST_HOME(), "solr.xml"), new File(instance.getHomeDir(), "solr.xml"));
    Properties nodeProperties = new Properties();
    nodeProperties.setProperty("solr.data.dir", instance.getDataDir());
    JettyConfig jettyConfig = JettyConfig.builder().setContext("/solr").setPort(0).build();
    JettySolrRunner jetty = new JettySolrRunner(instance.getHomeDir(), nodeProperties, jettyConfig);
    jetty.start();
    return jetty;
  }

  protected static HttpSolrClient createNewSolrClient(int port) {
    return createNewSolrClient(port, true);
  }
  protected static HttpSolrClient createNewSolrClient(int port, boolean includeCore) {
    try {
      // setup the client...
      String baseUrl = buildUrl(port);
      if(includeCore) {
        baseUrl += "/" + DEFAULT_TEST_CORENAME;
      }
      HttpSolrClient client = getHttpSolrClient(baseUrl, 15000, 90000);
      return client;
    }
    catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  protected static int index(SolrClient s, Object... fields) throws Exception {
    SolrInputDocument doc = new SolrInputDocument();
    for (int i = 0; i < fields.length; i += 2) {
      doc.addField((String) (fields[i]), fields[i + 1]);
    }
    return s.add(doc).getStatus();
  }

  @SuppressWarnings({"rawtypes"})
  protected NamedList query(String query, SolrClient s) throws SolrServerException, IOException {
    ModifiableSolrParams params = new ModifiableSolrParams();

    params.add("q", query);
    params.add("sort","id desc");

    QueryResponse qres = s.query(params);
    return qres.getResponse();
  }

  /** will sleep up to 30 seconds, looking for expectedDocCount */
  @SuppressWarnings({"rawtypes"})
  protected NamedList rQuery(int expectedDocCount, String query, SolrClient client) throws Exception {
    int timeSlept = 0;
    NamedList res = query(query, client);
    while (expectedDocCount != numFound(res)
           && timeSlept < 30000) {
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

  protected  long numFound(@SuppressWarnings({"rawtypes"})NamedList res) {
    return ((SolrDocumentList) res.get("response")).getNumFound();
  }

  protected  NamedList<Object> getDetails(SolrClient s) throws Exception {

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("command","details");
    params.set("_trace","getDetails");
    params.set("qt",ReplicationHandler.PATH);
    QueryRequest req = new QueryRequest(params);

    NamedList<Object> res = s.request(req);
    assertReplicationResponseSucceeded(res);

    @SuppressWarnings("unchecked") NamedList<Object> details 
      = (NamedList<Object>) res.get("details");

    assertNotNull("null details", details);

    return details;
  }

  protected NamedList<Object> getIndexVersion(SolrClient s) throws Exception {
    
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("command","indexversion");
    params.set("_trace","getIndexVersion");
    params.set("qt",ReplicationHandler.PATH);
    QueryRequest req = new QueryRequest(params);

    NamedList<Object> res = s.request(req);
    assertReplicationResponseSucceeded(res);

    return res;
  }

  protected NamedList<Object> reloadCore(SolrClient s, String core) throws Exception {

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("action","reload");
    params.set("core", core);
    params.set("qt","/admin/cores");
    QueryRequest req = new QueryRequest(params);

    try (HttpSolrClient adminClient = adminClient(s)) {
      NamedList<Object> res = adminClient.request(req);
      assertNotNull("null response from server", res);
      return res;
    }

  }

  protected HttpSolrClient adminClient(SolrClient client) {
    String adminUrl = ((HttpSolrClient)client).getBaseURL().replace("/collection1", "");
    return getHttpSolrClient(adminUrl);
  }


  //Simple function to wrap the invocation of replication commands on the various
  //jetty servers.
  protected static void invokeReplicationCommand(int pJettyPort, String pCommand) throws IOException
  {
    String leaderUrl = buildUrl(pJettyPort) + "/" + DEFAULT_TEST_CORENAME + ReplicationHandler.PATH+"?command=" + pCommand;
    URL u = new URL(leaderUrl);
    InputStream stream = u.openStream();
    stream.close();
  }
  

  protected String getFollowerDetails(String keyName) throws SolrServerException, IOException {
    NamedList<Object> details = getFollowerDetails();
    return getStringOrNull(details, keyName);
  }

  protected String getStringOrNull(NamedList<Object> details, String keyName) {
    Object o = details.get(keyName);
    return o != null ? o.toString() : null;
  }

  protected NamedList<Object> getFollowerDetails() throws SolrServerException, IOException {
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

  protected CachingDirectoryFactory getCachingDirectoryFactory(SolrCore core) {
    return (CachingDirectoryFactory) core.getDirectoryFactory();
  }

  protected void checkForSingleIndex(JettySolrRunner jetty) {
    checkForSingleIndex(jetty, false);
  }

  protected void checkForSingleIndex(JettySolrRunner jetty, boolean afterReload) {
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
          assertTrue(livePaths.toString() + ":" + livePaths.size(), 3 == livePaths.size() || 4 == livePaths.size());
        } else {
          assertTrue(livePaths.toString() + ":" + livePaths.size(), 3 == livePaths.size());
        }

        // :TODO: assert that one of the paths is a subpath of hte other
      }
      if (dirFactory instanceof StandardDirectoryFactory) {
        System.out.println(Arrays.asList(new File(ddir).list()));
        // we also allow one extra index dir - it may not be removed until the core is closed
        int cnt = indexDirCount(ddir);
        // if after reload, there may be 2 index dirs while the reloaded SolrCore closes.
        if (afterReload) {
          assertTrue("found:" + cnt + Arrays.asList(new File(ddir).list()).toString(), 1 == cnt || 2 == cnt);
        } else {
          assertTrue("found:" + cnt + Arrays.asList(new File(ddir).list()).toString(), 1 == cnt);
        }

      }
    }
  }

  protected int indexDirCount(String ddir) {
    String[] list = new File(ddir).list(new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        File f = new File(dir, name);
        return f.isDirectory() && !SolrSnapshotMetaDataManager.SNAPSHOT_METADATA_DIR.equals(name);
      }
    });
    return list.length;
  }

  protected void pullFromLeaderToFollower() throws MalformedURLException,
      IOException {
    pullFromTo(leaderJetty, followerJetty);
  }

  protected void assertVersions(SolrClient client1, SolrClient client2) throws Exception {
    NamedList<Object> details = getDetails(client1);
    @SuppressWarnings({"unchecked"})
    ArrayList<NamedList<Object>> commits = (ArrayList<NamedList<Object>>) details.get("commits");
    Long maxVersionClient1 = getVersion(client1);
    Long maxVersionClient2 = getVersion(client2);

    if (maxVersionClient1 > 0 && maxVersionClient2 > 0) {
      assertEquals(maxVersionClient1, maxVersionClient2);
    }
    
    // check vs /replication?command=indexversion call
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("qt", ReplicationHandler.PATH);
    params.set("_trace", "assertVersions");
    params.set("command", "indexversion");
    QueryRequest req = new QueryRequest(params);
    NamedList<Object> resp = client1.request(req);
    assertReplicationResponseSucceeded(resp);
    Long version = (Long) resp.get("indexversion");
    assertEquals(maxVersionClient1, version);
    
    // check vs /replication?command=indexversion call
    resp = client2.request(req);
    assertReplicationResponseSucceeded(resp);
    version = (Long) resp.get("indexversion");
    assertEquals(maxVersionClient2, version);
  }

  @SuppressWarnings({"unchecked"})
  protected Long getVersion(SolrClient client) throws Exception {
    NamedList<Object> details;
    ArrayList<NamedList<Object>> commits;
    details = getDetails(client);
    commits = (ArrayList<NamedList<Object>>) details.get("commits");
    Long maxVersionFollower= 0L;
    for(NamedList<Object> commit : commits) {
      Long version = (Long) commit.get("indexVersion");
      maxVersionFollower = Math.max(version, maxVersionFollower);
    }
    return maxVersionFollower;
  }

  protected void pullFromFollowerToLeader() throws MalformedURLException,
      IOException {
    pullFromTo(followerJetty, leaderJetty);
  }

  protected void pullFromTo(JettySolrRunner from, JettySolrRunner to) throws IOException {
    String leaderUrl;
    URL url;
    InputStream stream;
    leaderUrl = buildUrl(to.getLocalPort())
        + "/" + DEFAULT_TEST_CORENAME
        + ReplicationHandler.PATH+"?wait=true&command=fetchindex&leaderUrl="
        + buildUrl(from.getLocalPort())
        + "/" + DEFAULT_TEST_CORENAME + ReplicationHandler.PATH;
    url = new URL(leaderUrl);
    stream = url.openStream();
    stream.close();
  }


  /**
   * character copy of file using UTF-8. If port is non-null, will be substituted any time "TEST_PORT" is found.
   */
  protected static void copyFile(File src, File dst, Integer port, boolean internalCompression) throws IOException {
    try (BufferedReader in = new BufferedReader(new InputStreamReader(new FileInputStream(src), StandardCharsets.UTF_8));
         Writer out = new OutputStreamWriter(new FileOutputStream(dst), StandardCharsets.UTF_8)) {

      for (String line = in.readLine(); null != line; line = in.readLine()) {
        if (null != port) {
          line = line.replace("TEST_PORT", port.toString());
        }
        line = line.replace("COMPRESSION", internalCompression ? "internal" : "false");
        out.write(line);
      }
    }
  }

  protected UpdateResponse emptyUpdate(SolrClient client, String... params)
    throws SolrServerException, IOException {

    UpdateRequest req = new UpdateRequest();
    req.setParams(params(params));
    return req.process(client);
  }

  /**
   * Polls the SolrCore stats using the specified client until the "startTime" 
   * time for collection is after the specified "min".  Will loop for 
   * at most "timeout" milliseconds before throwing an assertion failure.
   * 
   * @param client The SolrClient to poll
   * @param timeout the max milliseconds to continue polling for
   * @param min the startTime value must exceed this value before the method will return, if null this method will return the first startTime value encountered.
   * @return the startTime value of collection
   */
  protected Date watchCoreStartAt(SolrClient client, final long timeout,
                                final Date min) throws InterruptedException, IOException, SolrServerException {
    final long sleepInterval = 200;
    long timeSlept = 0;

    try (HttpSolrClient adminClient = adminClient(client)) {
      SolrParams p = params("action", "status", "core", "collection1");
      while (timeSlept < timeout) {
        QueryRequest req = new QueryRequest(p);
        req.setPath("/admin/cores");
        try {
          @SuppressWarnings({"rawtypes"})
          NamedList data = adminClient.request(req);
          for (String k : new String[]{"status", "collection1"}) {
            Object o = data.get(k);
            assertNotNull("core status rsp missing key: " + k, o);
            data = (NamedList) o;
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

  protected void assertReplicationResponseSucceeded(@SuppressWarnings({"rawtypes"})NamedList response) {
    assertNotNull("null response from server", response);
    assertNotNull("Expected replication response to have 'status' field", response.get("status"));
    assertEquals("OK", response.get("status"));
  }

  protected static String buildUrl(int port) {
    return buildUrl(port, context);
  }

  static class SolrInstance {

    private String name;
    private Integer testPort;
    private File homeDir;
    private File confDir;
    private File dataDir;

    /**
     * @param homeDir Base directory to build solr configuration and index in
     * @param name used to pick which
     *        "solrconfig-${name}.xml" file gets copied
     *        to solrconfig.xml in new conf dir.
     * @param testPort if not null, used as a replacement for
     *        TEST_PORT in the cloned config files.
     */
    public SolrInstance(File homeDir, String name, Integer testPort) {
      this.homeDir = homeDir;
      this.name = name;
      this.testPort = testPort;
    }

    public String getHomeDir() {
      return homeDir.toString();
    }

    public String getSchemaFile() {
      return CONF_DIR + "schema-replication1.xml";
    }

    public String getConfDir() {
      return confDir.toString();
    }

    public String getDataDir() {
      return dataDir.getAbsolutePath();
    }

    public String getSolrConfigFile() {
      return CONF_DIR + "solrconfig-"+name+".xml";
    }
    
    /** If it needs to change */
    public void setTestPort(Integer testPort) {
      this.testPort = testPort;
    }

    public void setUp() throws Exception {
      System.setProperty("solr.test.sys.prop1", "propone");
      System.setProperty("solr.test.sys.prop2", "proptwo");

      Properties props = new Properties();
      props.setProperty("name", "collection1");

      writeCoreProperties(homeDir.toPath().resolve("collection1"), props, "TestReplicationHandler");

      dataDir = new File(homeDir + "/collection1", "data");
      confDir = new File(homeDir + "/collection1", "conf");

      homeDir.mkdirs();
      dataDir.mkdirs();
      confDir.mkdirs();

      copyConfigFile(getSolrConfigFile(), "solrconfig.xml");
      copyConfigFile(getSchemaFile(), "schema.xml");
      copyConfigFile(CONF_DIR + "solrconfig.snippet.randomindexconfig.xml", 
                     "solrconfig.snippet.randomindexconfig.xml");
    }

    public void copyConfigFile(String srcFile, String destFile) 
      throws IOException {
      copyFile(getFile(srcFile), 
               new File(confDir, destFile),
               testPort, random().nextBoolean());
    }

  }
}
