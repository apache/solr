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
package org.apache.solr.cloud;

import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;

import java.io.FileWriter;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Properties;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.cloud.SecurityAwareZkACLProvider;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.VMParamsAllAndReadonlyDigestZkACLProvider;
import org.apache.solr.common.cloud.VMParamsSingleSetCredentialsDigestZkCredentialsProvider;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException.NoAuthException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AbstractVMParamsZkACLAndCredentialsProvidersTestBase extends SolrTestCaseJ4 {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final Charset DATA_ENCODING = StandardCharsets.UTF_8;

  protected ZkTestServer zkServer;

  protected Path zkDir;

  @BeforeClass
  public static void beforeClass() {
    System.setProperty("solrcloud.skip.autorecovery", "true");
  }

  @AfterClass
  public static void afterClass() throws InterruptedException {
    System.clearProperty("solrcloud.skip.autorecovery");
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    if (log.isInfoEnabled()) {
      log.info("####SETUP_START {}", getTestName());
    }
    createTempDir();

    zkDir = createTempDir().resolve("zookeeper/server1/data");
    log.info("ZooKeeper dataDir:{}", zkDir);
    zkServer = new ZkTestServer(zkDir);
    zkServer.run(false);

    System.setProperty("zkHost", zkServer.getZkAddress());

    setSecuritySystemProperties();

    SolrZkClient zkClient =
        new SolrZkClient(
            zkServer.getZkHost(), AbstractZkTestCase.TIMEOUT, AbstractZkTestCase.TIMEOUT);
    zkClient.makePath("/solr", false, true);
    zkClient.close();

    zkClient = new SolrZkClient(zkServer.getZkAddress(), AbstractZkTestCase.TIMEOUT);
    zkClient.create(
        "/protectedCreateNode", "content".getBytes(DATA_ENCODING), CreateMode.PERSISTENT, false);
    zkClient.makePath(
        "/protectedMakePathNode", "content".getBytes(DATA_ENCODING), CreateMode.PERSISTENT, false);

    zkClient.create(
        SecurityAwareZkACLProvider.SECURITY_ZNODE_PATH,
        "content".getBytes(DATA_ENCODING),
        CreateMode.PERSISTENT,
        false);
    zkClient.close();

    clearSecuritySystemProperties();

    zkClient = new SolrZkClient(zkServer.getZkAddress(), AbstractZkTestCase.TIMEOUT);
    // Currently no credentials on ZK connection, because those same VM-params are used for adding
    // ACLs, and here we want
    // no (or completely open) ACLs added. Therefore hack your way into being authorized for
    // creating anyway
    zkClient
        .getZooKeeper()
        .addAuthInfo(
            "digest",
            ("connectAndAllACLUsername:connectAndAllACLPassword").getBytes(StandardCharsets.UTF_8));
    zkClient.create(
        "/unprotectedCreateNode", "content".getBytes(DATA_ENCODING), CreateMode.PERSISTENT, false);
    zkClient.makePath(
        "/unprotectedMakePathNode",
        "content".getBytes(DATA_ENCODING),
        CreateMode.PERSISTENT,
        false);
    zkClient.close();

    if (log.isInfoEnabled()) {
      log.info("####SETUP_END {}", getTestName());
    }
  }

  @Override
  public void tearDown() throws Exception {
    zkServer.shutdown();

    clearSecuritySystemProperties();

    super.tearDown();
  }

  @Test
  public void testNoCredentials() throws Exception {
    useNoCredentials();

    try (SolrZkClient zkClient =
        new SolrZkClient(zkServer.getZkAddress(), AbstractZkTestCase.TIMEOUT)) {
      doTest(zkClient, false, false, false, false, false, false, false, false, false, false);
    }
  }

  @Test
  public void testWrongCredentials() throws Exception {
    useWrongCredentials();

    try (SolrZkClient zkClient =
        new SolrZkClient(zkServer.getZkAddress(), AbstractZkTestCase.TIMEOUT)) {
      doTest(zkClient, false, false, false, false, false, false, false, false, false, false);
    }
  }

  @Test
  public void testAllCredentials() throws Exception {
    useAllCredentials();

    try (SolrZkClient zkClient =
        new SolrZkClient(zkServer.getZkAddress(), AbstractZkTestCase.TIMEOUT)) {
      doTest(zkClient, true, true, true, true, true, true, true, true, true, true);
    }
  }

  @Test
  public void testReadonlyCredentials() throws Exception {
    useReadonlyCredentials();

    try (SolrZkClient zkClient =
        new SolrZkClient(zkServer.getZkAddress(), AbstractZkTestCase.TIMEOUT)) {
      doTest(zkClient, true, true, false, false, false, false, false, false, false, false);
    }
  }

  @Test
  public void testReadonlyCredentialsFromFile() throws Exception {
    useReadonlyCredentialsFromFile();

    try (SolrZkClient zkClient =
        new SolrZkClient(zkServer.getZkAddress(), AbstractZkTestCase.TIMEOUT)) {
      doTest(zkClient, true, true, false, false, false, false, false, false, false, false);
    }
  }

  @Test
  public void testAllCredentialsFromFile() throws Exception {
    useAllCredentialsFromFile();

    try (SolrZkClient zkClient =
        new SolrZkClient(zkServer.getZkAddress(), AbstractZkTestCase.TIMEOUT)) {
      doTest(zkClient, true, true, true, true, true, true, true, true, true, true);
    }
  }

  @Test
  public void testRepairACL() throws Exception {
    clearSecuritySystemProperties();
    try (SolrZkClient zkClient =
        new SolrZkClient(zkServer.getZkAddress(), AbstractZkTestCase.TIMEOUT)) {
      // Currently no credentials on ZK connection, because those same VM-params are used for adding
      // ACLs, and here we want
      // no (or completely open) ACLs added. Therefore hack your way into being authorized for
      // creating anyway
      zkClient
          .getZooKeeper()
          .addAuthInfo(
              "digest",
              ("connectAndAllACLUsername:connectAndAllACLPassword")
                  .getBytes(StandardCharsets.UTF_8));

      zkClient.create(
          "/security.json", "{}".getBytes(StandardCharsets.UTF_8), CreateMode.PERSISTENT, false);
      assertEquals(OPEN_ACL_UNSAFE, zkClient.getACL("/security.json", null, false));
    }

    setSecuritySystemProperties();
    try (SolrZkClient zkClient =
        new SolrZkClient(zkServer.getZkAddress(), AbstractZkTestCase.TIMEOUT)) {
      ZkController.createClusterZkNodes(zkClient);
      assertNotEquals(OPEN_ACL_UNSAFE, zkClient.getACL("/security.json", null, false));
    }

    useReadonlyCredentials();
    try (SolrZkClient zkClient =
        new SolrZkClient(zkServer.getZkAddress(), AbstractZkTestCase.TIMEOUT)) {
      NoAuthException e =
          assertThrows(
              NoAuthException.class, () -> zkClient.getData("/security.json", null, null, false));
      assertEquals("/security.json", e.getPath());
    }
  }

  public static void doTest(
      SolrZkClient zkClient,
      boolean getData,
      boolean list,
      boolean create,
      boolean setData,
      boolean delete,
      boolean secureGet,
      boolean secureList,
      boolean secureCreate,
      boolean secureSet,
      boolean secureDelete)
      throws Exception {
    doTest(zkClient, "/protectedCreateNode", getData, list, create, setData, delete);
    doTest(zkClient, "/protectedMakePathNode", getData, list, create, setData, delete);
    doTest(zkClient, "/unprotectedCreateNode", true, true, true, true, delete);
    doTest(zkClient, "/unprotectedMakePathNode", true, true, true, true, delete);
    doTest(
        zkClient,
        SecurityAwareZkACLProvider.SECURITY_ZNODE_PATH,
        secureGet,
        secureList,
        secureCreate,
        secureSet,
        secureDelete);
  }

  protected static void doTest(
      SolrZkClient zkClient,
      String path,
      boolean getData,
      boolean list,
      boolean create,
      boolean setData,
      boolean delete)
      throws Exception {
    doTest(getData, () -> zkClient.getData(path, null, null, false));
    doTest(list, () -> zkClient.getChildren(path, null, false));

    doTest(
        create,
        () -> {
          zkClient.create(path + "/subnode", null, CreateMode.PERSISTENT, false);
          zkClient.delete(path + "/subnode", -1, false);
        });
    doTest(
        create,
        () -> {
          zkClient.makePath(path + "/subnode/subsubnode", false);
          zkClient.delete(path + "/subnode/subsubnode", -1, false);
          zkClient.delete(path + "/subnode", -1, false);
        });

    doTest(setData, () -> zkClient.setData(path, (byte[]) null, false));

    // Actually about the ACLs on /solr, but that is protected
    doTest(delete, () -> zkClient.delete(path, -1, false));
  }

  interface ExceptingRunnable {
    void run() throws Exception;
  }

  private static void doTest(boolean shouldSucceed, ExceptingRunnable action) throws Exception {
    if (shouldSucceed) {
      action.run();
    } else {
      expectThrows(NoAuthException.class, action::run);
    }
  }

  private void useNoCredentials() {
    clearSecuritySystemProperties();
  }

  private void useWrongCredentials() {
    clearSecuritySystemProperties();

    System.setProperty(
        SolrZkClient.ZK_CRED_PROVIDER_CLASS_NAME_VM_PARAM_NAME,
        VMParamsSingleSetCredentialsDigestZkCredentialsProvider.class.getName());
    System.setProperty(
        VMParamsSingleSetCredentialsDigestZkCredentialsProvider
            .DEFAULT_DIGEST_USERNAME_VM_PARAM_NAME,
        "connectAndAllACLUsername");
    System.setProperty(
        VMParamsSingleSetCredentialsDigestZkCredentialsProvider
            .DEFAULT_DIGEST_PASSWORD_VM_PARAM_NAME,
        "connectAndAllACLPasswordWrong");
  }

  private void useAllCredentials() {
    clearSecuritySystemProperties();

    System.setProperty(
        SolrZkClient.ZK_CRED_PROVIDER_CLASS_NAME_VM_PARAM_NAME,
        VMParamsSingleSetCredentialsDigestZkCredentialsProvider.class.getName());
    System.setProperty(
        VMParamsSingleSetCredentialsDigestZkCredentialsProvider
            .DEFAULT_DIGEST_USERNAME_VM_PARAM_NAME,
        "connectAndAllACLUsername");
    System.setProperty(
        VMParamsSingleSetCredentialsDigestZkCredentialsProvider
            .DEFAULT_DIGEST_PASSWORD_VM_PARAM_NAME,
        "connectAndAllACLPassword");
  }

  private void useReadonlyCredentials() {
    clearSecuritySystemProperties();

    System.setProperty(
        SolrZkClient.ZK_CRED_PROVIDER_CLASS_NAME_VM_PARAM_NAME,
        VMParamsSingleSetCredentialsDigestZkCredentialsProvider.class.getName());
    System.setProperty(
        VMParamsSingleSetCredentialsDigestZkCredentialsProvider
            .DEFAULT_DIGEST_USERNAME_VM_PARAM_NAME,
        "readonlyACLUsername");
    System.setProperty(
        VMParamsSingleSetCredentialsDigestZkCredentialsProvider
            .DEFAULT_DIGEST_PASSWORD_VM_PARAM_NAME,
        "readonlyACLPassword");
  }

  private void useReadonlyCredentialsFromFile() throws IOException {
    useCredentialsFromFile("readonlyACLUsername", "readonlyACLPassword");
  }

  private void useAllCredentialsFromFile() throws IOException {
    useCredentialsFromFile("connectAndAllACLUsername", "connectAndAllACLPassword");
  }

  private void useCredentialsFromFile(String username, String password) throws IOException {
    clearSecuritySystemProperties();

    System.setProperty(
        SolrZkClient.ZK_CRED_PROVIDER_CLASS_NAME_VM_PARAM_NAME,
        VMParamsSingleSetCredentialsDigestZkCredentialsProvider.class.getName());

    Properties props = new Properties();
    props.setProperty(
        VMParamsSingleSetCredentialsDigestZkCredentialsProvider
            .DEFAULT_DIGEST_USERNAME_VM_PARAM_NAME,
        username);
    props.setProperty(
        VMParamsSingleSetCredentialsDigestZkCredentialsProvider
            .DEFAULT_DIGEST_PASSWORD_VM_PARAM_NAME,
        password);
    saveCredentialsFile(props);
  }

  private void saveCredentialsFile(Properties props) throws IOException {
    Path tmp = createTempFile("zk-creds", "properties");
    try (FileWriter w = new FileWriter(tmp.toFile(), StandardCharsets.UTF_8)) {
      props.store(w, "test");
    }
    System.setProperty(
        VMParamsSingleSetCredentialsDigestZkCredentialsProvider.DEFAULT_DIGEST_FILE_VM_PARAM_NAME,
        tmp.toAbsolutePath().toString());
  }

  private void setSecuritySystemProperties() {
    System.setProperty(
        SolrZkClient.ZK_ACL_PROVIDER_CLASS_NAME_VM_PARAM_NAME,
        VMParamsAllAndReadonlyDigestZkACLProvider.class.getName());
    System.setProperty(
        SolrZkClient.ZK_CRED_PROVIDER_CLASS_NAME_VM_PARAM_NAME,
        VMParamsSingleSetCredentialsDigestZkCredentialsProvider.class.getName());
    System.setProperty(
        VMParamsSingleSetCredentialsDigestZkCredentialsProvider
            .DEFAULT_DIGEST_USERNAME_VM_PARAM_NAME,
        "connectAndAllACLUsername");
    System.setProperty(
        VMParamsSingleSetCredentialsDigestZkCredentialsProvider
            .DEFAULT_DIGEST_PASSWORD_VM_PARAM_NAME,
        "connectAndAllACLPassword");
    System.setProperty(
        VMParamsAllAndReadonlyDigestZkACLProvider.DEFAULT_DIGEST_READONLY_USERNAME_VM_PARAM_NAME,
        "readonlyACLUsername");
    System.setProperty(
        VMParamsAllAndReadonlyDigestZkACLProvider.DEFAULT_DIGEST_READONLY_PASSWORD_VM_PARAM_NAME,
        "readonlyACLPassword");
  }

  private void clearSecuritySystemProperties() {
    System.clearProperty(SolrZkClient.ZK_ACL_PROVIDER_CLASS_NAME_VM_PARAM_NAME);
    System.clearProperty(SolrZkClient.ZK_CRED_PROVIDER_CLASS_NAME_VM_PARAM_NAME);
    System.clearProperty(
        VMParamsSingleSetCredentialsDigestZkCredentialsProvider
            .DEFAULT_DIGEST_USERNAME_VM_PARAM_NAME);
    System.clearProperty(
        VMParamsSingleSetCredentialsDigestZkCredentialsProvider
            .DEFAULT_DIGEST_PASSWORD_VM_PARAM_NAME);
    System.clearProperty(
        VMParamsAllAndReadonlyDigestZkACLProvider.DEFAULT_DIGEST_READONLY_USERNAME_VM_PARAM_NAME);
    System.clearProperty(
        VMParamsAllAndReadonlyDigestZkACLProvider.DEFAULT_DIGEST_READONLY_PASSWORD_VM_PARAM_NAME);
    System.clearProperty(
        VMParamsSingleSetCredentialsDigestZkCredentialsProvider.DEFAULT_DIGEST_FILE_VM_PARAM_NAME);
  }
}
