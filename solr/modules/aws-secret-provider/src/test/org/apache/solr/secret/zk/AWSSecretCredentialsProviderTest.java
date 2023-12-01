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

import static org.apache.solr.cloud.AbstractDigestZkACLAndCredentialsProvidersTestBase.ALL_PASSWORD;
import static org.apache.solr.cloud.AbstractDigestZkACLAndCredentialsProvidersTestBase.ALL_USERNAME;
import static org.apache.solr.cloud.AbstractDigestZkACLAndCredentialsProvidersTestBase.READONLY_PASSWORD;
import static org.apache.solr.cloud.AbstractDigestZkACLAndCredentialsProvidersTestBase.READONLY_USERNAME;
import static org.apache.solr.cloud.AbstractDigestZkACLAndCredentialsProvidersTestBase.doTest;
import static org.apache.solr.cloud.AbstractDigestZkACLAndCredentialsProvidersTestBase.setUpZKNodes;
import static org.apache.solr.common.cloud.SolrZkClient.ZK_ACL_PROVIDER_CLASS_NAME_VM_PARAM_NAME;
import static org.apache.solr.common.cloud.SolrZkClient.ZK_CREDENTIALS_INJECTOR_CLASS_NAME_VM_PARAM_NAME;
import static org.apache.solr.common.cloud.SolrZkClient.ZK_CRED_PROVIDER_CLASS_NAME_VM_PARAM_NAME;
import static org.apache.solr.secret.zk.AWSSecretManagerCredentialsInjector.AWS_SM_CREDENTIALS_SECRET_NAME_VM_PARAM;
import static org.apache.solr.secret.zk.AWSSecretManagerCredentialsInjector.SecretMultiCredentials;

import java.lang.invoke.MethodHandles;
import java.nio.file.Path;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.cloud.AbstractZkTestCase;
import org.apache.solr.cloud.ZkTestServer;
import org.apache.solr.common.cloud.DigestZkACLProvider;
import org.apache.solr.common.cloud.DigestZkCredentialsProvider;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkCredentialsInjector.ZkCredential;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AWSSecretCredentialsProviderTest extends SolrTestCaseJ4 {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final String SECRET_NAME = "AWS_SM_DEV_SECRET_NAME";

  private static final String SECRET_ALL_AND_READY_VALID_JSON =
      String.format(
          Locale.ROOT,
          "{\"zkCredentials\": ["
              + "    {\"username\": \"%s\", \"password\": \"%s\", \"perms\": \"ALL\"},"
              + "    {\"username\": \"%s\", \"password\": \"%s\", \"perms\": \"READ\"}"
              + "  ]"
              + "}",
          ALL_USERNAME,
          ALL_PASSWORD,
          READONLY_USERNAME,
          READONLY_PASSWORD);

  // SolrZkClient uses ALL 'user' to connect to ZK, hence perms=ALL. Still, we want to test the
  // read-only creds, i.e. connect to ZK with read-only creds
  private static final String SECRET_READY_ONLY_VALID_JSON =
      String.format(
          Locale.ROOT,
          "{\"zkCredentials\": ["
              + "    {\"username\": \"%s\", \"password\": \"%s\", \"perms\": \"ALL\"}"
              + "  ]"
              + "}",
          READONLY_USERNAME,
          READONLY_PASSWORD);

  protected ZkTestServer zkServer;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    assumeWorkingMockito();
    if (log.isInfoEnabled()) {
      log.info("####SETUP_START {}", getTestName());
    }

    Path zkDir = createTempDir().resolve("zookeeper/server1/data");
    log.info("ZooKeeper dataDir:{}", zkDir);
    zkServer = new ZkTestServer(zkDir);
    zkServer.run(false);
    System.setProperty("zkHost", zkServer.getZkAddress());
    setUpZKNodes(zkServer);
    setDigestZkSystemProps();
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
  public void testReturnTheExpectedZkCredentials() {
    System.setProperty(AWS_SM_CREDENTIALS_SECRET_NAME_VM_PARAM, SECRET_NAME);
    final List<ZkCredential> zkCredentials =
        List.of(
            new ZkCredential(ALL_USERNAME, ALL_PASSWORD, ZkCredential.Perms.ALL),
            new ZkCredential(READONLY_USERNAME, READONLY_PASSWORD, ZkCredential.Perms.READ));
    final SecretMultiCredentials secretMultiCredentials = new SecretMultiCredentials(zkCredentials);

    AWSSecretManagerCredentialsInjector awsSecretCredentialsInjector =
        Mockito.spy(new AWSSecretManagerCredentialsInjector());
    Mockito.doReturn(SECRET_ALL_AND_READY_VALID_JSON)
        .when(awsSecretCredentialsInjector)
        .getSecretValue(SECRET_NAME);

    assertEquals(
        secretMultiCredentials.getZkCredentials().toString(),
        awsSecretCredentialsInjector.getZkCredentials().toString());
  }

  @Test
  public void testSecretJsonFormat() {
    final AWSSecretManagerCredentialsInjector awsSecretCredentialsProvider =
        Mockito.spy(new AWSSecretManagerCredentialsInjector());

    final String secretEmpty = "";
    Mockito.doReturn(secretEmpty).when(awsSecretCredentialsProvider).getSecretValue(SECRET_NAME);
    assertThrows(
        IllegalArgumentException.class,
        () -> awsSecretCredentialsProvider.createSecretMultiCredentials(SECRET_NAME));

    final String secretInvalidJson = "invalid json";
    Mockito.doReturn(secretInvalidJson)
        .when(awsSecretCredentialsProvider)
        .getSecretValue(SECRET_NAME);
    assertThrows(
        IllegalArgumentException.class,
        () -> awsSecretCredentialsProvider.createSecretMultiCredentials(SECRET_NAME));

    final String secretWrongFormat =
        String.format(
            Locale.ROOT,
            "{\"zkCredential\": ["
                + "    {\"username\": \"%s\", \"password\": \"%s\", \"perms\": \"ALL\"},"
                + "    {\"username\": \"%s\", \"password\": \"%s\", \"perms\": \"READ\"}"
                + "  ]"
                + "}",
            ALL_USERNAME,
            ALL_PASSWORD,
            READONLY_USERNAME,
            READONLY_PASSWORD);

    Mockito.doReturn(secretWrongFormat)
        .when(awsSecretCredentialsProvider)
        .getSecretValue(SECRET_NAME);
    assertThrows(
        IllegalArgumentException.class,
        () -> awsSecretCredentialsProvider.createSecretMultiCredentials(SECRET_NAME));

    Mockito.doReturn(SECRET_ALL_AND_READY_VALID_JSON)
        .when(awsSecretCredentialsProvider)
        .getSecretValue(SECRET_NAME);
    awsSecretCredentialsProvider.createSecretMultiCredentials(SECRET_NAME);
  }

  @Test
  public void testAllCredentials() throws Exception {
    System.setProperty(
        ZK_CREDENTIALS_INJECTOR_CLASS_NAME_VM_PARAM_NAME,
        AllAndReadonlyAWSSecretCredentialsProvider.class.getName());

    try (SolrZkClient zkClient =
        new SolrZkClient.Builder()
            .withUrl(zkServer.getZkAddress())
            .withConnTimeOut(AbstractZkTestCase.TIMEOUT, TimeUnit.MILLISECONDS)
            .build()) {
      doTest(zkClient, true, true, true, true, true, true, true, true, true, true);
    }
  }

  @Test
  public void testReadonlyCredentials() throws Exception {
    System.setProperty(
        ZK_CREDENTIALS_INJECTOR_CLASS_NAME_VM_PARAM_NAME,
        ConnectWithReadonlyAWSSecretCredentialsProvider.class.getName());

    try (SolrZkClient zkClient =
        new SolrZkClient.Builder()
            .withUrl(zkServer.getZkAddress())
            .withConnTimeOut(AbstractZkTestCase.TIMEOUT, TimeUnit.MILLISECONDS)
            .build()) {
      doTest(zkClient, true, true, false, false, false, false, false, false, false, false);
    }
  }

  @Test
  public void testWrongCredentials() throws Exception {
    System.setProperty(
        ZK_CREDENTIALS_INJECTOR_CLASS_NAME_VM_PARAM_NAME,
        WrongAllAWSSecretCredentialsProvider.class.getName());

    try (SolrZkClient zkClient =
        new SolrZkClient.Builder()
            .withUrl(zkServer.getZkAddress())
            .withConnTimeOut(AbstractZkTestCase.TIMEOUT, TimeUnit.MILLISECONDS)
            .build()) {
      doTest(zkClient, false, false, false, false, false, false, false, false, false, false);
    }
  }

  @Test
  public void testNoCredentials() throws Exception {
    System.setProperty(
        ZK_CREDENTIALS_INJECTOR_CLASS_NAME_VM_PARAM_NAME,
        NoAWSSecretCredentialsProvider.class.getName());

    try (SolrZkClient zkClient =
        new SolrZkClient.Builder()
            .withUrl(zkServer.getZkAddress())
            .withConnTimeOut(AbstractZkTestCase.TIMEOUT, TimeUnit.MILLISECONDS)
            .build()) {
      doTest(zkClient, false, false, false, false, false, false, false, false, false, false);
    }
  }

  private void setDigestZkSystemProps() {
    System.setProperty(
        SolrZkClient.ZK_CRED_PROVIDER_CLASS_NAME_VM_PARAM_NAME,
        DigestZkCredentialsProvider.class.getName());
    System.setProperty(
        SolrZkClient.ZK_ACL_PROVIDER_CLASS_NAME_VM_PARAM_NAME, DigestZkACLProvider.class.getName());
  }

  private void clearSecuritySystemProperties() {
    System.clearProperty(ZK_CRED_PROVIDER_CLASS_NAME_VM_PARAM_NAME);
    System.clearProperty(ZK_ACL_PROVIDER_CLASS_NAME_VM_PARAM_NAME);
    System.clearProperty(ZK_CREDENTIALS_INJECTOR_CLASS_NAME_VM_PARAM_NAME);
    System.clearProperty(AWS_SM_CREDENTIALS_SECRET_NAME_VM_PARAM);
  }

  public static class AllAndReadonlyAWSSecretCredentialsProvider
      extends AWSSecretManagerCredentialsInjector {
    @Override
    protected String getSecretValue(String secretName) {
      return SECRET_ALL_AND_READY_VALID_JSON;
    }
  }

  public static class ConnectWithReadonlyAWSSecretCredentialsProvider
      extends AWSSecretManagerCredentialsInjector {
    @Override
    protected String getSecretValue(String secretName) {
      return SECRET_READY_ONLY_VALID_JSON;
    }
  }

  public static class WrongAllAWSSecretCredentialsProvider
      extends AWSSecretManagerCredentialsInjector {
    @Override
    protected String getSecretValue(String secretName) {
      String secretAllWrongCredsJson =
          String.format(
              Locale.ROOT,
              "{\"zkCredentials\": ["
                  + "    {\"username\": \"%s\", \"password\": \"%s\", \"perms\": \"ALL\"}"
                  + "  ]"
                  + "}",
              ALL_USERNAME,
              "WRONG_PWD");
      return secretAllWrongCredsJson;
    }
  }

  public static class NoAWSSecretCredentialsProvider extends AWSSecretManagerCredentialsInjector {
    @Override
    protected String getSecretValue(String secretName) {
      return "{\"zkCredentials\": []}";
    }
  }
}
