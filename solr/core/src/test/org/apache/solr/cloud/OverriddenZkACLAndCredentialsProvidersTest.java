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

import static org.apache.solr.common.cloud.VMParamsZkCredentialsInjector.DEFAULT_DIGEST_PASSWORD_VM_PARAM_NAME;
import static org.apache.solr.common.cloud.VMParamsZkCredentialsInjector.DEFAULT_DIGEST_READONLY_PASSWORD_VM_PARAM_NAME;
import static org.apache.solr.common.cloud.VMParamsZkCredentialsInjector.DEFAULT_DIGEST_READONLY_USERNAME_VM_PARAM_NAME;
import static org.apache.solr.common.cloud.VMParamsZkCredentialsInjector.DEFAULT_DIGEST_USERNAME_VM_PARAM_NAME;
import static org.apache.solr.common.cloud.ZkCredentialsInjector.ZkCredential.Perms;

import java.lang.invoke.MethodHandles;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.cloud.DigestZkACLProvider;
import org.apache.solr.common.cloud.DigestZkCredentialsProvider;
import org.apache.solr.common.cloud.SecurityAwareZkACLProvider;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.VMParamsZkCredentialsInjector;
import org.apache.solr.common.cloud.ZkACLProvider;
import org.apache.solr.common.cloud.ZkCredentialsInjector;
import org.apache.solr.common.cloud.ZkCredentialsProvider;
import org.apache.zookeeper.CreateMode;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OverriddenZkACLAndCredentialsProvidersTest extends SolrTestCaseJ4 {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final Charset DATA_ENCODING = StandardCharsets.UTF_8;

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
    if (log.isInfoEnabled()) {
      log.info("####SETUP_START {}", getTestName());
    }
    createTempDir();

    zkDir = createTempDir().resolve("zookeeper/server1/data");
    log.info("ZooKeeper dataDir:{}", zkDir);
    zkServer = new ZkTestServer(zkDir);
    zkServer.run(false);

    System.setProperty("zkHost", zkServer.getZkAddress());

    SolrZkClient zkClient =
        new SolrZkClientFactoryUsingCompletelyNewProviders(
                "connectAndAllACLUsername",
                "connectAndAllACLPassword",
                "readonlyACLUsername",
                "readonlyACLPassword")
            .getSolrZkClient(zkServer.getZkHost(), AbstractZkTestCase.TIMEOUT);
    zkClient.makePath("/solr", false, true);
    zkClient.close();

    zkClient =
        new SolrZkClientFactoryUsingCompletelyNewProviders(
                "connectAndAllACLUsername",
                "connectAndAllACLPassword",
                "readonlyACLUsername",
                "readonlyACLPassword")
            .getSolrZkClient(zkServer.getZkAddress(), AbstractZkTestCase.TIMEOUT);
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

    zkClient =
        new SolrZkClientFactoryUsingCompletelyNewProviders(null, null, null, null)
            .getSolrZkClient(zkServer.getZkAddress(), AbstractZkTestCase.TIMEOUT);
    zkClient
        .getZooKeeper()
        .addAuthInfo(
            "digest",
            ("connectAndAllACLUsername:connectAndAllACLPassword").getBytes(DATA_ENCODING));
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
  public void testNoCredentialsSolrZkClientFactoryUsingCompletelyNewProviders() throws Exception {
    SolrZkClient zkClient =
        new SolrZkClientFactoryUsingCompletelyNewProviders(null, null, null, null)
            .getSolrZkClient(zkServer.getZkAddress(), AbstractZkTestCase.TIMEOUT);
    try {
      AbstractDigestZkACLAndCredentialsProvidersTestBase.doTest(
          zkClient, false, false, false, false, false, false, false, false, false, false);
    } finally {
      zkClient.close();
    }
  }

  @Test
  public void testWrongCredentialsSolrZkClientFactoryUsingCompletelyNewProviders()
      throws Exception {
    SolrZkClient zkClient =
        new SolrZkClientFactoryUsingCompletelyNewProviders(
                "connectAndAllACLUsername", "connectAndAllACLPasswordWrong", null, null)
            .getSolrZkClient(zkServer.getZkAddress(), AbstractZkTestCase.TIMEOUT);
    try {
      AbstractDigestZkACLAndCredentialsProvidersTestBase.doTest(
          zkClient, false, false, false, false, false, false, false, false, false, false);
    } finally {
      zkClient.close();
    }
  }

  @Test
  public void testAllCredentialsSolrZkClientFactoryUsingCompletelyNewProviders() throws Exception {
    SolrZkClient zkClient =
        new SolrZkClientFactoryUsingCompletelyNewProviders(
                "connectAndAllACLUsername", "connectAndAllACLPassword", null, null)
            .getSolrZkClient(zkServer.getZkAddress(), AbstractZkTestCase.TIMEOUT);
    try {
      AbstractDigestZkACLAndCredentialsProvidersTestBase.doTest(
          zkClient, true, true, true, true, true, true, true, true, true, true);
    } finally {
      zkClient.close();
    }
  }

  @Test
  public void testReadonlyCredentialsSolrZkClientFactoryUsingCompletelyNewProviders()
      throws Exception {
    SolrZkClient zkClient =
        new SolrZkClientFactoryUsingCompletelyNewProviders(
                "readonlyACLUsername", "readonlyACLPassword", null, null)
            .getSolrZkClient(zkServer.getZkAddress(), AbstractZkTestCase.TIMEOUT);
    try {
      AbstractDigestZkACLAndCredentialsProvidersTestBase.doTest(
          zkClient, true, true, false, false, false, false, false, false, false, false);
    } finally {
      zkClient.close();
    }
  }

  @Test
  public void
      testNoCredentialsSolrZkClientFactoryUsingVMParamsProvidersButWithDifferentVMParamsNames()
          throws Exception {
    useNoCredentials();

    SolrZkClient zkClient =
        new SolrZkClientUsingVMParamsProvidersButWithDifferentVMParamsNames(
            new SolrZkClient.Builder()
                .withUrl(zkServer.getZkAddress())
                .withTimeout(AbstractZkTestCase.TIMEOUT, TimeUnit.MILLISECONDS));
    try {
      AbstractDigestZkACLAndCredentialsProvidersTestBase.doTest(
          zkClient, false, false, false, false, false, false, false, false, false, false);
    } finally {
      zkClient.close();
    }
  }

  @Test
  public void
      testWrongCredentialsSolrZkClientFactoryUsingVMParamsProvidersButWithDifferentVMParamsNames()
          throws Exception {
    useWrongCredentials();

    SolrZkClient zkClient =
        new SolrZkClientUsingVMParamsProvidersButWithDifferentVMParamsNames(
            new SolrZkClient.Builder()
                .withUrl(zkServer.getZkAddress())
                .withTimeout(AbstractZkTestCase.TIMEOUT, TimeUnit.MILLISECONDS));
    try {
      AbstractDigestZkACLAndCredentialsProvidersTestBase.doTest(
          zkClient, false, false, false, false, false, false, false, false, false, false);
    } finally {
      zkClient.close();
    }
  }

  @Test
  public void
      testAllCredentialsSolrZkClientFactoryUsingVMParamsProvidersButWithDifferentVMParamsNames()
          throws Exception {
    useAllCredentials();

    SolrZkClient zkClient =
        new SolrZkClientUsingVMParamsProvidersButWithDifferentVMParamsNames(
            new SolrZkClient.Builder()
                .withUrl(zkServer.getZkAddress())
                .withTimeout(AbstractZkTestCase.TIMEOUT, TimeUnit.MILLISECONDS));
    try {
      AbstractDigestZkACLAndCredentialsProvidersTestBase.doTest(
          zkClient, true, true, true, true, true, true, true, true, true, true);
    } finally {
      zkClient.close();
    }
  }

  @Test
  public void
      testReadonlyCredentialsSolrZkClientFactoryUsingVMParamsProvidersButWithDifferentVMParamsNames()
          throws Exception {
    useReadonlyCredentials();

    SolrZkClient zkClient =
        new SolrZkClientUsingVMParamsProvidersButWithDifferentVMParamsNames(
            new SolrZkClient.Builder()
                .withUrl(zkServer.getZkAddress())
                .withTimeout(AbstractZkTestCase.TIMEOUT, TimeUnit.MILLISECONDS));
    try {
      AbstractDigestZkACLAndCredentialsProvidersTestBase.doTest(
          zkClient, true, true, false, false, false, false, false, false, false, false);
    } finally {
      zkClient.close();
    }
  }

  private static class SolrZkClientFactoryUsingCompletelyNewProviders {

    final String digestUsername;
    final String digestPassword;
    final String digestReadonlyUsername;
    final String digestReadonlyPassword;
    private final ZkCredentialsInjector zkCredentialsInjector;

    public SolrZkClientFactoryUsingCompletelyNewProviders(
        final String digestUsername,
        final String digestPassword,
        final String digestReadonlyUsername,
        final String digestReadonlyPassword) {
      this.digestUsername = digestUsername;
      this.digestPassword = digestPassword;
      this.digestReadonlyUsername = digestReadonlyUsername;
      this.digestReadonlyPassword = digestReadonlyPassword;
      zkCredentialsInjector =
          () -> {
            List<ZkCredentialsInjector.ZkCredential> zkCredentials = new ArrayList<>(2);
            ZkCredentialsInjector.ZkCredential allCreds =
                new ZkCredentialsInjector.ZkCredential(digestUsername, digestPassword, Perms.ALL);
            ZkCredentialsInjector.ZkCredential readCreds =
                new ZkCredentialsInjector.ZkCredential(
                    digestReadonlyUsername, digestReadonlyPassword, Perms.READ);
            zkCredentials.add(allCreds);
            zkCredentials.add(readCreds);
            return zkCredentials;
          };
    }

    public SolrZkClient getSolrZkClient(String zkServerAddress, int zkClientTimeout) {

      return new SolrZkClient(
          new SolrZkClient.Builder()
              .withUrl(zkServerAddress)
              .withTimeout(zkClientTimeout, TimeUnit.MILLISECONDS)) {

        @Override
        protected ZkCredentialsProvider createZkCredentialsToAddAutomatically() {
          return new DigestZkCredentialsProvider(zkCredentialsInjector);
        }

        @Override
        public ZkACLProvider createZkACLProvider() {
          return new DigestZkACLProvider(zkCredentialsInjector);
        }
      };
    }
  }

  private static class SolrZkClientUsingVMParamsProvidersButWithDifferentVMParamsNames
      extends SolrZkClient {

    private static final VMParamsZkCredentialsInjector vmParamsZkCredentialsInjector =
        new VMParamsZkCredentialsInjector(
            "alternative" + DEFAULT_DIGEST_USERNAME_VM_PARAM_NAME,
            "alternative" + DEFAULT_DIGEST_PASSWORD_VM_PARAM_NAME,
            "alternative" + DEFAULT_DIGEST_READONLY_USERNAME_VM_PARAM_NAME,
            "alternative" + DEFAULT_DIGEST_READONLY_PASSWORD_VM_PARAM_NAME);

    public SolrZkClientUsingVMParamsProvidersButWithDifferentVMParamsNames(
        SolrZkClient.Builder builder) {
      super(builder);
    }

    @Override
    protected ZkCredentialsInjector createZkCredentialsInjector() {
      return vmParamsZkCredentialsInjector;
    }

    @Override
    protected ZkCredentialsProvider createZkCredentialsToAddAutomatically() {
      return new DigestZkCredentialsProvider(vmParamsZkCredentialsInjector);
    }

    @Override
    public ZkACLProvider createZkACLProvider() {
      return new DigestZkACLProvider(vmParamsZkCredentialsInjector);
    }
  }

  public void useNoCredentials() {
    clearSecuritySystemProperties();
  }

  public void useWrongCredentials() {
    clearSecuritySystemProperties();

    System.setProperty(
        "alternative" + DEFAULT_DIGEST_USERNAME_VM_PARAM_NAME, "connectAndAllACLUsername");
    System.setProperty(
        "alternative" + DEFAULT_DIGEST_PASSWORD_VM_PARAM_NAME, "connectAndAllACLPasswordWrong");
  }

  public void useAllCredentials() {
    clearSecuritySystemProperties();

    System.setProperty(
        "alternative" + DEFAULT_DIGEST_USERNAME_VM_PARAM_NAME, "connectAndAllACLUsername");
    System.setProperty(
        "alternative" + DEFAULT_DIGEST_PASSWORD_VM_PARAM_NAME, "connectAndAllACLPassword");
  }

  public void useReadonlyCredentials() {
    clearSecuritySystemProperties();

    System.setProperty(
        "alternative" + DEFAULT_DIGEST_USERNAME_VM_PARAM_NAME, "readonlyACLUsername");
    System.setProperty(
        "alternative" + DEFAULT_DIGEST_PASSWORD_VM_PARAM_NAME, "readonlyACLPassword");
  }

  public void setSecuritySystemProperties() {
    System.setProperty(
        "alternative" + DEFAULT_DIGEST_USERNAME_VM_PARAM_NAME, "connectAndAllACLUsername");
    System.setProperty(
        "alternative" + DEFAULT_DIGEST_PASSWORD_VM_PARAM_NAME, "connectAndAllACLPassword");
    System.setProperty(
        "alternative" + DEFAULT_DIGEST_READONLY_USERNAME_VM_PARAM_NAME, "readonlyACLUsername");
    System.setProperty(
        "alternative" + DEFAULT_DIGEST_READONLY_PASSWORD_VM_PARAM_NAME, "readonlyACLPassword");
  }

  public void clearSecuritySystemProperties() {
    System.clearProperty("alternative" + DEFAULT_DIGEST_USERNAME_VM_PARAM_NAME);
    System.clearProperty("alternative" + DEFAULT_DIGEST_PASSWORD_VM_PARAM_NAME);
    System.clearProperty("alternative" + DEFAULT_DIGEST_READONLY_USERNAME_VM_PARAM_NAME);
    System.clearProperty("alternative" + DEFAULT_DIGEST_READONLY_PASSWORD_VM_PARAM_NAME);
  }
}
