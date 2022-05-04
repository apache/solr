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
package org.apache.solr.secret.zk;

import static org.apache.solr.common.cloud.acl.SecretCredentialInjector.*;
import static org.apache.solr.secret.zk.AWSSecretCredentialsProvider.SecretMultiCredentials;

import java.lang.invoke.MethodHandles;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.*;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.cloud.AbstractZkTestCase;
import org.apache.solr.cloud.ZkTestServer;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.acl.DigestZkACLProvider;
import org.apache.solr.common.cloud.acl.DigestZkCredentialsProvider;
import org.apache.solr.common.cloud.acl.SecretCredentialInjector;
import org.apache.solr.common.cloud.acl.SecurityAwareZkACLProvider;
import org.apache.solr.common.cloud.acl.ZkCredentialsInjector;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException.NoAuthException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AWSSecretCredentialsProviderTest extends SolrTestCaseJ4 {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final Charset DATA_ENCODING = StandardCharsets.UTF_8;

  private static final String ALL_USERNAME = "connectAndAllACLUsername";
  private static final String ALL_PASSWORD = "connectAndAllACLPassword";
  private static final String READONLY_USERNAME = "readonlyACLUsername";
  private static final String READONLY_PASSWORD = "readonlyACLPassword";

  public static final String SECRET_NAME = "zkSecretCredentialSecretName";

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
    super.setUp();
    assumeWorkingMockito();
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

    setSecretDigestZkSystemProps();
    System.setProperty(
        SolrZkClient.ZK_CREDENTIALS_INJECTOR_CLASS_NAME_VM_PARAM_NAME,
        AllAndReadonlyCredentialZkCredentialsInjector.class.getName());

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

    setSecretDigestZkSystemProps();
    if (log.isInfoEnabled()) {
      log.info("####SETUP_END {}", getTestName());
    }
  }

  private void setSecretDigestZkSystemProps() {
    System.setProperty(
        SolrZkClient.ZK_CRED_PROVIDER_CLASS_NAME_VM_PARAM_NAME,
        DigestZkCredentialsProvider.class.getName());
    System.setProperty(
        SolrZkClient.ZK_ACL_PROVIDER_CLASS_NAME_VM_PARAM_NAME, DigestZkACLProvider.class.getName());
    System.setProperty(
        SolrZkClient.ZK_CREDENTIALS_INJECTOR_CLASS_NAME_VM_PARAM_NAME,
        SecretCredentialInjector.class.getName());
    System.setProperty(SECRET_CREDENTIAL_PROVIDER_SECRET_NAME_VM_PARAM, SECRET_NAME);
  }

  @Override
  public void tearDown() throws Exception {
    zkServer.shutdown();
    clearSecuritySystemProperties();
    super.tearDown();
  }

  @Test
  public void whenPassedSecretCredentials_thenReturnSameZkCredentials() {
    final List<ZkCredential> zkCredentials =
        new ArrayList<>() {
          {
            add(new ZkCredential(ALL_USERNAME, ALL_PASSWORD, ZkCredential.Perms.all));
            add(new ZkCredential(READONLY_USERNAME, READONLY_PASSWORD, ZkCredential.Perms.read));
          }
        };

    final SecretMultiCredentials secretMultiCredentials = new SecretMultiCredentials(zkCredentials);
    AWSSecretCredentialsProvider awsSecretCredentialsProvider =
        Mockito.spy(new AWSSecretCredentialsProvider());
    Mockito.doReturn(secretMultiCredentials)
        .when(awsSecretCredentialsProvider)
        .createSecretMultiCredential(SECRET_NAME);
    assertEquals(
        secretMultiCredentials.getZkCredentials(),
        awsSecretCredentialsProvider.getZkCredentials(SECRET_NAME));
  }

  @Test
  public void testSecretJsonFormat() {
    final AWSSecretCredentialsProvider awsSecretCredentialsProvider =
        Mockito.spy(new AWSSecretCredentialsProvider());

    final String secretEmpty = "";
    Mockito.doReturn(secretEmpty).when(awsSecretCredentialsProvider).getSecretValue(SECRET_NAME);
    assertThrows(
        IllegalArgumentException.class,
        () -> awsSecretCredentialsProvider.createSecretMultiCredential(SECRET_NAME));

    final String secretInvalidJson = "invalid json";
    Mockito.doReturn(secretInvalidJson)
        .when(awsSecretCredentialsProvider)
        .getSecretValue(SECRET_NAME);
    assertThrows(
        IllegalArgumentException.class,
        () -> awsSecretCredentialsProvider.createSecretMultiCredential(SECRET_NAME));

    final String secretWrongFormat =
        "{"
            + "   \"zkCredential\": ["
            + "      {\"username\": \"admin-user\", \"password\": \"ADMIN-PASSWORD\", \"perms\": \"all\"},"
            + "      {\"username\": \"readonly-user\", \"password\": \"READONLY-PASSWORD\", \"perms\": \"read\"}"
            + "   ]"
            + "}";
    Mockito.doReturn(secretWrongFormat)
        .when(awsSecretCredentialsProvider)
        .getSecretValue(SECRET_NAME);
    assertThrows(
        IllegalArgumentException.class,
        () -> awsSecretCredentialsProvider.createSecretMultiCredential(SECRET_NAME));

    final String secretRightFormat =
        "{"
            + "   \"zkCredentials\": ["
            + "      {\"username\": \"admin-user\", \"password\": \"ADMIN-PASSWORD\", \"perms\": \"all\"},"
            + "      {\"username\": \"readonly-user\", \"password\": \"READONLY-PASSWORD\", \"perms\": \"read\"}"
            + "   ]"
            + "}";
    Mockito.doReturn(secretRightFormat)
        .when(awsSecretCredentialsProvider)
        .getSecretValue(SECRET_NAME);
    awsSecretCredentialsProvider.createSecretMultiCredential(SECRET_NAME);
  }

  @Test
  public void testAllCredentials() throws Exception {
    System.setProperty(
        SECRET_CREDENTIAL_PROVIDER_CLASS_VM_PARAM,
        AllAndReadonlyAWSSecretCredentialsProvider.class.getName());

    try (SolrZkClient zkClient =
        new SolrZkClient(zkServer.getZkAddress(), AbstractZkTestCase.TIMEOUT)) {
      doTest(zkClient, true, true, true, true, true, true, true, true, true, true);
    }
  }

  @Test
  public void testNoCredentials() throws Exception {
    System.setProperty(
        SECRET_CREDENTIAL_PROVIDER_CLASS_VM_PARAM, NoAWSSecretCredentialsProvider.class.getName());

    try (SolrZkClient zkClient =
        new SolrZkClient(zkServer.getZkAddress(), AbstractZkTestCase.TIMEOUT)) {
      doTest(zkClient, false, false, false, false, false, false, false, false, false, false);
    }
  }

  @Test
  public void testWrongCredentials0() throws Exception {
    System.setProperty(
        SECRET_CREDENTIAL_PROVIDER_CLASS_VM_PARAM,
        WrongAllAWSSecretCredentialsProvider.class.getName());

    try (SolrZkClient zkClient =
        new SolrZkClient(zkServer.getZkAddress(), AbstractZkTestCase.TIMEOUT)) {
      doTest(zkClient, false, false, false, false, false, false, false, false, false, false);
    }
  }

  @Test
  public void testReadonlyCredentials() throws Exception {
    System.setProperty(
        SECRET_CREDENTIAL_PROVIDER_CLASS_VM_PARAM,
        ConnectWithReadonlyAWSSecretCredentialsProvider.class.getName());

    try (SolrZkClient zkClient =
        new SolrZkClient(zkServer.getZkAddress(), AbstractZkTestCase.TIMEOUT)) {
      doTest(zkClient, true, true, false, false, false, false, false, false, false, false);
    }
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
    System.clearProperty(SECRET_CREDENTIAL_PROVIDER_SECRET_NAME_VM_PARAM);
  }

  protected static void doTest(
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

  public static class NoAWSSecretCredentialsProvider extends AWSSecretCredentialsProvider {

    @Override
    protected SecretMultiCredentials createSecretMultiCredential(String secretName) {
      return new SecretMultiCredentials(Collections.emptyList());
    }
  }

  public static class AllAndReadonlyAWSSecretCredentialsProvider
      extends AWSSecretCredentialsProvider {

    @Override
    protected SecretMultiCredentials createSecretMultiCredential(String secretName) {
      return new SecretMultiCredentials(
          new ArrayList<>() {
            {
              add(new ZkCredential(ALL_USERNAME, ALL_PASSWORD, ZkCredential.Perms.all));
              add(new ZkCredential(READONLY_USERNAME, READONLY_PASSWORD, ZkCredential.Perms.read));
            }
          });
    }
  }

  public static class ConnectWithReadonlyAWSSecretCredentialsProvider
      extends AWSSecretCredentialsProvider {

    @Override
    protected SecretMultiCredentials createSecretMultiCredential(String secretName) {
      return new SecretMultiCredentials(
          new ArrayList<>() {
            {
              // uses readonly creds to connect to zookeeper, hence "all"
              add(new ZkCredential(READONLY_USERNAME, READONLY_PASSWORD, ZkCredential.Perms.all));
            }
          });
    }
  }

  public static class WrongAllAWSSecretCredentialsProvider extends AWSSecretCredentialsProvider {

    @Override
    protected SecretMultiCredentials createSecretMultiCredential(String secretName) {
      return new SecretMultiCredentials(
          new ArrayList<>() {
            {
              add(new ZkCredential(ALL_USERNAME, ALL_PASSWORD + "Wrong", ZkCredential.Perms.all));
            }
          });
    }
  }

  public static class AllAndReadonlyCredentialZkCredentialsInjector
      implements ZkCredentialsInjector {
    @Override
    public List<ZkCredential> getZkCredentials() {
      List<ZkCredential> zkCredentials = new ArrayList<>(2);
      ZkCredential allCreds = new ZkCredential(ALL_USERNAME, ALL_PASSWORD, ZkCredential.Perms.all);
      ZkCredential readCreds =
          new ZkCredential(READONLY_USERNAME, READONLY_PASSWORD, ZkCredential.Perms.read);
      zkCredentials.add(allCreds);
      zkCredentials.add(readCreds);
      return zkCredentials;
    }
  }
}
