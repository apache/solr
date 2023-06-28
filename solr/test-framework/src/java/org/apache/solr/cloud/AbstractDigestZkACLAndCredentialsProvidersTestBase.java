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

import static org.apache.solr.common.cloud.VMParamsZkCredentialsInjector.DEFAULT_DIGEST_FILE_VM_PARAM_NAME;
import static org.apache.solr.common.cloud.VMParamsZkCredentialsInjector.DEFAULT_DIGEST_PASSWORD_VM_PARAM_NAME;
import static org.apache.solr.common.cloud.VMParamsZkCredentialsInjector.DEFAULT_DIGEST_USERNAME_VM_PARAM_NAME;
import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;

import java.io.FileWriter;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.cloud.DigestZkACLProvider;
import org.apache.solr.common.cloud.DigestZkCredentialsProvider;
import org.apache.solr.common.cloud.SecurityAwareZkACLProvider;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.VMParamsZkCredentialsInjector;
import org.apache.solr.common.cloud.ZkCredentialsInjector;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException.NoAuthException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AbstractDigestZkACLAndCredentialsProvidersTestBase extends SolrTestCaseJ4 {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final Charset DATA_ENCODING = StandardCharsets.UTF_8;

  private static final String ALL_USERNAME = "connectAndAllACLUsername";
  private static final String ALL_PASSWORD = "connectAndAllACLPassword";
  private static final String READONLY_USERNAME = "readonlyACLUsername";
  private static final String READONLY_PASSWORD = "readonlyACLPassword";

  protected ZkTestServer zkServer;

  protected Path zkDir;

  @BeforeClass
  public static void beforeClass() {
    System.setProperty("solrcloud.skip.autorecovery", "true");
  }

  @AfterClass
  public static void afterClass() {
    System.clearProperty("solrcloud.skip.autorecovery");
  }

  @Override
  public void setUp() throws Exception {
    // TODO: Does all of this setup need to happen for each test case, or can it be done once for
    // the class? (i.e. @BeforeClass) and maybe some minor reset logic in setup, instead of full
    // startup and teardown of a new ZkTestServer in each cycle?
    super.setUp();
    if (log.isInfoEnabled()) {
      log.info("####SETUP_START {}", getTestName());
    }
    createTempDir();

    zkDir = createTempDir().resolve("zookeeper/server1/data");
    log.info("ZooKeeper dataDir:{}", zkDir);
    setSecuritySystemProperties();
    zkServer = new ZkTestServer(zkDir);
    zkServer.run(false);

    System.setProperty("zkHost", zkServer.getZkAddress());

    setDigestZkSystemProps();
    System.setProperty(
        SolrZkClient.ZK_CREDENTIALS_INJECTOR_CLASS_NAME_VM_PARAM_NAME,
        AllAndReadonlyCredentialZkCredentialsInjector.class.getName());

    SolrZkClient zkClient =
        new SolrZkClient.Builder()
            .withUrl(zkServer.getZkHost())
            .withTimeout(AbstractZkTestCase.TIMEOUT, TimeUnit.MILLISECONDS)
            .withConnTimeOut(AbstractZkTestCase.TIMEOUT, TimeUnit.MILLISECONDS)
            .build();

    zkClient.makePath("/solr", false, true);
    zkClient.close();

    zkClient =
        new SolrZkClient.Builder()
            .withUrl(zkServer.getZkAddress())
            .withTimeout(AbstractZkTestCase.TIMEOUT, TimeUnit.MILLISECONDS)
            .build();
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

    zkClient =
        new SolrZkClient.Builder()
            .withUrl(zkServer.getZkAddress())
            .withTimeout(AbstractZkTestCase.TIMEOUT, TimeUnit.MILLISECONDS)
            .build();
    // Currently, no credentials on ZK connection, because those same VM-params are used for adding
    // ACLs, and here we want
    // no (or completely open) ACLs added. Therefore, hack your way into being authorized for
    // creating anyway
    zkClient
        .getZooKeeper()
        .addAuthInfo(
            "digest", (ALL_USERNAME + ":" + ALL_PASSWORD).getBytes(StandardCharsets.UTF_8));
    zkClient.create(
        "/unprotectedCreateNode", "content".getBytes(DATA_ENCODING), CreateMode.PERSISTENT, false);
    zkClient.makePath(
        "/unprotectedMakePathNode",
        "content".getBytes(DATA_ENCODING),
        CreateMode.PERSISTENT,
        false);
    zkClient.close();

    setDigestZkSystemProps();
    if (log.isInfoEnabled()) {
      log.info("####SETUP_END {}", getTestName());
    }
  }

  private void setDigestZkSystemProps() {
    System.setProperty(
        SolrZkClient.ZK_CRED_PROVIDER_CLASS_NAME_VM_PARAM_NAME,
        DigestZkCredentialsProvider.class.getName());
    System.setProperty(
        SolrZkClient.ZK_ACL_PROVIDER_CLASS_NAME_VM_PARAM_NAME, DigestZkACLProvider.class.getName());
  }

  @Override
  public void tearDown() throws Exception {
    zkServer.shutdown();

    clearSecuritySystemProperties();

    super.tearDown();
  }

  @Test
  public void testNoCredentials() throws Exception {
    List<TestZkCredentialsInjector> testZkCredentialsInjectors =
        List.of(
            new TestZkCredentialsInjector(NoCredentialZkCredentialsInjector.class),
            new TestZkCredentialsInjector(VMParamsZkCredentialsInjector.class));

    testInjectors(
        testZkCredentialsInjectors,
        false,
        false,
        false,
        false,
        false,
        false,
        false,
        false,
        false,
        false);
  }

  @Test
  public void testWrongCredentials() throws Exception {
    List<TestZkCredentialsInjector> testZkCredentialsInjectors =
        List.of(
            new TestZkCredentialsInjector(WrongAllCredentialZkCredentialsInjector.class),
            new TestZkCredentialsInjector(
                VMParamsZkCredentialsInjector.class,
                List.of(
                    DEFAULT_DIGEST_USERNAME_VM_PARAM_NAME, DEFAULT_DIGEST_PASSWORD_VM_PARAM_NAME),
                List.of(ALL_USERNAME, ALL_PASSWORD + "Wrong")));

    testInjectors(
        testZkCredentialsInjectors,
        false,
        false,
        false,
        false,
        false,
        false,
        false,
        false,
        false,
        false);
  }

  @Test
  public void testAllCredentials() throws Exception {
    List<TestZkCredentialsInjector> testZkCredentialsInjectors =
        List.of(
            new TestZkCredentialsInjector(AllCredentialZkCredentialsInjector.class),
            new TestZkCredentialsInjector(
                VMParamsZkCredentialsInjector.class,
                List.of(
                    DEFAULT_DIGEST_USERNAME_VM_PARAM_NAME, DEFAULT_DIGEST_PASSWORD_VM_PARAM_NAME),
                List.of(ALL_USERNAME, ALL_PASSWORD)));

    testInjectors(
        testZkCredentialsInjectors, true, true, true, true, true, true, true, true, true, true);
  }

  @Test
  public void testReadonlyCredentials() throws Exception {
    List<TestZkCredentialsInjector> testZkCredentialsInjectors =
        List.of(
            new TestZkCredentialsInjector(ConnectWithReadonlyCredsInjector.class),
            new TestZkCredentialsInjector(
                VMParamsZkCredentialsInjector.class,
                List.of(
                    DEFAULT_DIGEST_USERNAME_VM_PARAM_NAME, DEFAULT_DIGEST_PASSWORD_VM_PARAM_NAME),
                List.of(READONLY_USERNAME, READONLY_PASSWORD)));
    testInjectors(
        testZkCredentialsInjectors,
        true,
        true,
        false,
        false,
        false,
        false,
        false,
        false,
        false,
        false);
  }

  protected void testInjectors(
      List<TestZkCredentialsInjector> testZkCredentialsInjectors,
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
    for (TestZkCredentialsInjector testZkCredentialsInjector : testZkCredentialsInjectors) {
      tearDown();
      setUp();
      testZkCredentialsInjector.setSystemProps();
      try (SolrZkClient zkClient =
          new SolrZkClient.Builder()
              .withUrl(zkServer.getZkAddress())
              .withTimeout(AbstractZkTestCase.TIMEOUT, TimeUnit.MILLISECONDS)
              .build()) {
        doTest(
            zkClient,
            getData,
            list,
            create,
            setData,
            delete,
            secureGet,
            secureList,
            secureCreate,
            secureSet,
            secureDelete);
      }
    }
  }

  @Test
  public void testRepairACL() throws Exception {
    clearSecuritySystemProperties();
    try (SolrZkClient zkClient =
        new SolrZkClient.Builder()
            .withUrl(zkServer.getZkAddress())
            .withTimeout(AbstractZkTestCase.TIMEOUT, TimeUnit.MILLISECONDS)
            .build()) {
      // Currently, no credentials on ZK connection, because those same VM-params are used for
      // adding ACLs, and here we want
      // no (or completely open) ACLs added. Therefore, hack your way into being authorized for
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
        new SolrZkClient.Builder()
            .withUrl(zkServer.getZkAddress())
            .withTimeout(AbstractZkTestCase.TIMEOUT, TimeUnit.MILLISECONDS)
            .build()) {
      ZkController.createClusterZkNodes(zkClient);
      assertNotEquals(OPEN_ACL_UNSAFE, zkClient.getACL("/security.json", null, false));
    }

    useZkCredentialsInjector(ConnectWithReadonlyCredsInjector.class);
    // useReadonlyCredentials();
    try (SolrZkClient zkClient =
        new SolrZkClient.Builder()
            .withUrl(zkServer.getZkAddress())
            .withTimeout(AbstractZkTestCase.TIMEOUT, TimeUnit.MILLISECONDS)
            .build()) {
      NoAuthException e =
          assertThrows(
              NoAuthException.class, () -> zkClient.getData("/security.json", null, null, false));
      assertEquals("/security.json", e.getPath());
    }
  }

  private void useZkCredentialsInjector(Class<?> zkCredentialsInjectorClass) {
    clearSecuritySystemProperties();
    setDigestZkSystemProps();
    System.setProperty(
        SolrZkClient.ZK_CREDENTIALS_INJECTOR_CLASS_NAME_VM_PARAM_NAME,
        zkCredentialsInjectorClass.getName());
  }

  private void setSecuritySystemProperties() {
    System.setProperty(
        SolrZkClient.ZK_CRED_PROVIDER_CLASS_NAME_VM_PARAM_NAME,
        DigestZkCredentialsProvider.class.getName());
    System.setProperty(
        SolrZkClient.ZK_ACL_PROVIDER_CLASS_NAME_VM_PARAM_NAME, DigestZkACLProvider.class.getName());
    System.setProperty(
        SolrZkClient.ZK_CREDENTIALS_INJECTOR_CLASS_NAME_VM_PARAM_NAME,
        AllAndReadonlyCredentialZkCredentialsInjector.class.getName());
  }

  private void clearSecuritySystemProperties() {
    System.clearProperty(SolrZkClient.ZK_CRED_PROVIDER_CLASS_NAME_VM_PARAM_NAME);
    System.clearProperty(SolrZkClient.ZK_ACL_PROVIDER_CLASS_NAME_VM_PARAM_NAME);
    System.clearProperty(SolrZkClient.ZK_CREDENTIALS_INJECTOR_CLASS_NAME_VM_PARAM_NAME);
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

  @Test
  public void testVMParamsAllCredentialsFromFile() throws Exception {
    useVMParamsAllCredentialsFromFile();

    try (SolrZkClient zkClient =
        new SolrZkClient.Builder()
            .withUrl(zkServer.getZkAddress())
            .withTimeout(AbstractZkTestCase.TIMEOUT, TimeUnit.MILLISECONDS)
            .build()) {
      doTest(zkClient, true, true, true, true, true, true, true, true, true, true);
    }
  }

  @Test
  public void testVMParamsReadonlyCredentialsFromFile() throws Exception {
    useVMParamsReadonlyCredentialsFromFile();

    try (SolrZkClient zkClient =
        new SolrZkClient.Builder()
            .withUrl(zkServer.getZkAddress())
            .withTimeout(AbstractZkTestCase.TIMEOUT, TimeUnit.MILLISECONDS)
            .build()) {
      doTest(zkClient, true, true, false, false, false, false, false, false, false, false);
    }
  }

  private void useVMParamsAllCredentialsFromFile() throws IOException {
    useVMParamsCredentialsFromFile("connectAndAllACLUsername", "connectAndAllACLPassword");
  }

  private void useVMParamsReadonlyCredentialsFromFile() throws IOException {
    useVMParamsCredentialsFromFile("readonlyACLUsername", "readonlyACLPassword");
  }

  private void useVMParamsCredentialsFromFile(String username, String password) throws IOException {
    Properties props = new Properties();
    props.setProperty(
        VMParamsZkCredentialsInjector.DEFAULT_DIGEST_USERNAME_VM_PARAM_NAME, username);
    props.setProperty(
        VMParamsZkCredentialsInjector.DEFAULT_DIGEST_PASSWORD_VM_PARAM_NAME, password);
    String credsFile = saveCredentialsFile(props);

    useZkCredentialsInjector(VMParamsZkCredentialsInjector.class);
    System.setProperty(DEFAULT_DIGEST_FILE_VM_PARAM_NAME, credsFile);
  }

  private String saveCredentialsFile(Properties props) throws IOException {
    Path tmp = createTempFile("zk-creds", "properties");
    try (FileWriter w = new FileWriter(tmp.toFile(), StandardCharsets.UTF_8)) {
      props.store(w, "test");
    }
    return tmp.toAbsolutePath().toString();
  }

  public static class NoCredentialZkCredentialsInjector implements ZkCredentialsInjector {
    @Override
    public List<ZkCredential> getZkCredentials() {
      return Collections.emptyList();
    }
  }

  public static class AllAndReadonlyCredentialZkCredentialsInjector
      implements ZkCredentialsInjector {
    @Override
    public List<ZkCredential> getZkCredentials() {
      return List.of(
          new ZkCredential(ALL_USERNAME, ALL_PASSWORD, ZkCredential.Perms.ALL),
          new ZkCredential(READONLY_USERNAME, READONLY_PASSWORD, ZkCredential.Perms.READ));
    }
  }

  public static class ConnectWithReadonlyCredsInjector implements ZkCredentialsInjector {
    @Override
    public List<ZkCredential> getZkCredentials() {
      return List.of(
          new ZkCredential(READONLY_USERNAME, READONLY_PASSWORD, ZkCredential.Perms.ALL));
    }
  }

  public static class AllCredentialZkCredentialsInjector implements ZkCredentialsInjector {
    @Override
    public List<ZkCredential> getZkCredentials() {
      return List.of(new ZkCredential(ALL_USERNAME, ALL_PASSWORD, ZkCredential.Perms.ALL));
    }
  }

  public static class WrongAllCredentialZkCredentialsInjector implements ZkCredentialsInjector {
    @Override
    public List<ZkCredential> getZkCredentials() {
      return List.of(
          new ZkCredential(ALL_USERNAME, ALL_PASSWORD + "Wrong", ZkCredential.Perms.ALL));
    }
  }

  class TestZkCredentialsInjector {
    private final Class<?> zkCredentialsInjectorClass;
    private final List<String> systemPropsKeys;
    private final List<String> systemPropsValues;

    public TestZkCredentialsInjector(Class<?> zkCredentialsInjectorClass) {
      this(zkCredentialsInjectorClass, Collections.emptyList(), Collections.emptyList());
    }

    public TestZkCredentialsInjector(
        Class<?> zkCredentialsInjectorClass,
        List<String> systemPropsKeys,
        List<String> systemPropsValues) {
      this.zkCredentialsInjectorClass = zkCredentialsInjectorClass;
      this.systemPropsKeys = systemPropsKeys;
      this.systemPropsValues = systemPropsValues;
    }

    private void setSystemProps() {
      clearSecuritySystemProperties();
      setDigestZkSystemProps();
      System.setProperty(
          SolrZkClient.ZK_CREDENTIALS_INJECTOR_CLASS_NAME_VM_PARAM_NAME,
          zkCredentialsInjectorClass.getName());
      int i = 0;
      for (String key : systemPropsKeys) {
        System.setProperty(key, systemPropsValues.get(i++));
      }
    }
  }
}
