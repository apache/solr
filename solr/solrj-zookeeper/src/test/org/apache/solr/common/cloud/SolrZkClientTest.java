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
package org.apache.solr.common.cloud;

import java.io.File;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.NoSuchAlgorithmException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.AbstractZkTestCase;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.cloud.ZkTestServer;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.util.ExternalPaths;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.server.auth.DigestAuthenticationProvider;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SolrZkClientTest extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final String ROOT = "/";
  private static final String PATH = "/collections/collection1";

  protected ZkTestServer zkServer;

  SolrZkClient aclClient;
  SolrZkClient credentialsClient;
  SolrZkClient defaultClient;
  private CloudSolrClient solrClient;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    configureCluster(1)
        .addConfig("_default", new File(ExternalPaths.DEFAULT_CONFIGSET).toPath())
        .configure();
    solrClient =
        new RandomizingCloudSolrClientBuilder(
                Collections.singletonList(cluster.getZkServer().getZkAddress()), Optional.empty())
            .build();

    final String SCHEME = "digest";
    final String AUTH = "user:pass";

    Path zkDir = createTempDir();
    log.info("ZooKeeper dataDir:{}", zkDir);
    zkServer = new ZkTestServer(zkDir);
    zkServer.run();

    try (SolrZkClient client =
        new SolrZkClient.Builder()
            .withUrl(zkServer.getZkHost())
            .withTimeout(AbstractZkTestCase.TIMEOUT, TimeUnit.MILLISECONDS)
            .build()) {
      // Set up chroot
      client.makePath("/solr", false, true);
    }

    defaultClient =
        new SolrZkClient.Builder()
            .withUrl(zkServer.getZkAddress())
            .withTimeout(AbstractZkTestCase.TIMEOUT, TimeUnit.MILLISECONDS)
            .build();
    defaultClient.makePath(PATH, true);

    aclClient =
        new SolrZkClient(
            new SolrZkClient.Builder()
                .withUrl(zkServer.getZkAddress())
                .withTimeout(AbstractZkTestCase.TIMEOUT, TimeUnit.MILLISECONDS)) {
          @Override
          protected ZkACLProvider createZkACLProvider() {
            return new DefaultZkACLProvider() {
              @Override
              protected List<ACL> createGlobalACLsToAdd() {
                try {
                  Id id = new Id(SCHEME, DigestAuthenticationProvider.generateDigest(AUTH));
                  return Collections.singletonList(new ACL(ZooDefs.Perms.ALL, id));
                } catch (NoSuchAlgorithmException e) {
                  throw new RuntimeException(e);
                }
              }
            };
          }
        };

    credentialsClient =
        new SolrZkClient(
            new SolrZkClient.Builder()
                .withUrl(zkServer.getZkAddress())
                .withTimeout(AbstractZkTestCase.TIMEOUT, TimeUnit.MILLISECONDS)) {
          @Override
          protected ZkCredentialsProvider createZkCredentialsToAddAutomatically() {
            return new DefaultZkCredentialsProvider() {
              @Override
              protected Collection<ZkCredentials> createCredentials() {
                return Collections.singleton(
                    new ZkCredentials(SCHEME, AUTH.getBytes(StandardCharsets.UTF_8)));
              }
            };
          }
        };
  }

  @Override
  public void tearDown() throws Exception {
    aclClient.close();
    credentialsClient.close();
    defaultClient.close();
    zkServer.shutdown();
    solrClient.close();
    cluster.shutdown();
    super.tearDown();
  }

  @Test
  public void testSimpleUpdateACLs() throws KeeperException, InterruptedException {
    assertTrue(
        "Initial create was in secure mode; please check the test", canRead(defaultClient, PATH));
    assertTrue(
        "Credentialed client should always be able to read", canRead(credentialsClient, PATH));

    // convert to secure
    aclClient.updateACLs(ROOT);
    assertFalse(
        "Default client should not be able to read root in secure mode",
        canRead(defaultClient, ROOT));
    assertFalse(
        "Default client should not be able to read children in secure mode",
        canRead(defaultClient, PATH));
    assertTrue(
        "Credentialed client should always be able to read root in secure mode",
        canRead(credentialsClient, ROOT));
    assertTrue(
        "Credentialed client should always be able to read in secure mode",
        canRead(credentialsClient, PATH));

    // convert to non-secure
    credentialsClient.updateACLs(ROOT);
    assertTrue(
        "Default client should work again after clearing ACLs", canRead(defaultClient, PATH));
    assertTrue(
        "Credentialed client should always be able to read", canRead(credentialsClient, PATH));

    // convert a subtree to secure
    aclClient.updateACLs("/collections");
    assertTrue("Default client should read unaffected paths", canRead(defaultClient, ROOT));
    assertFalse("Default client should not read secure children", canRead(defaultClient, PATH));
  }

  @Test
  // SOLR-13491
  public void testWrappingWatches() throws Exception {
    AtomicInteger calls = new AtomicInteger(0);
    Semaphore semA = new Semaphore(0);
    Semaphore semB = new Semaphore(0);
    Watcher watcherA =
        new Watcher() {
          @Override
          public void process(WatchedEvent event) {
            calls.getAndIncrement();
            semA.release();
          }
        };
    Watcher watcherB =
        new Watcher() {
          @Override
          public void process(WatchedEvent event) {
            calls.getAndDecrement();
            semB.release();
          }
        };
    Watcher wrapped1A = defaultClient.wrapWatcher(watcherA);
    Watcher wrapped2A = defaultClient.wrapWatcher(watcherA);
    Watcher wrappedB = defaultClient.wrapWatcher(watcherB);
    assertEquals(wrapped1A, wrapped2A);
    assertEquals(wrapped2A, wrapped1A);
    assertNotEquals(wrapped1A, wrappedB);
    assertEquals(wrapped1A.hashCode(), wrapped2A.hashCode());

    CollectionAdminRequest.createCollection(getSaferTestName(), "_default", 1, 1)
        .process(solrClient);

    CollectionAdminRequest.setCollectionProperty(getSaferTestName(), "foo", "bar")
        .process(solrClient);

    // Thread.sleep(600000);

    ZkStateReader.from(solrClient)
        .getZkClient()
        .getData(
            "/collections/" + getSaferTestName() + "/collectionprops.json", wrapped1A, null, true);
    ZkStateReader.from(solrClient)
        .getZkClient()
        .getData(
            "/collections/" + getSaferTestName() + "/collectionprops.json", wrapped2A, null, true);

    CollectionAdminRequest.setCollectionProperty(getSaferTestName(), "baz", "bam")
        .process(solrClient);

    assertTrue("Watch A didn't trigger", semA.tryAcquire(5, TimeUnit.SECONDS));
    if (TEST_NIGHTLY) {
      // give more time in nightly tests to ensure no extra watch calls
      Thread.sleep(500);
    }
    assertEquals(1, calls.get()); // same wrapped watch set twice, only invoked once

    ZkStateReader.from(solrClient)
        .getZkClient()
        .getData(
            "/collections/" + getSaferTestName() + "/collectionprops.json", wrapped1A, null, true);
    ZkStateReader.from(solrClient)
        .getZkClient()
        .getData(
            "/collections/" + getSaferTestName() + "/collectionprops.json", wrappedB, null, true);

    CollectionAdminRequest.setCollectionProperty(getSaferTestName(), "baz", "bang")
        .process(solrClient);

    assertTrue("Watch A didn't trigger", semA.tryAcquire(5, TimeUnit.SECONDS));
    assertTrue("Watch B didn't trigger", semB.tryAcquire(5, TimeUnit.SECONDS));
    if (TEST_NIGHTLY) {
      // give more time in nightly tests to ensure no extra watch calls
      Thread.sleep(500);
    }
    assertEquals(1, calls.get()); // offsetting watches, no change
  }

  private static boolean canRead(SolrZkClient zkClient, String path)
      throws KeeperException, InterruptedException {
    try {
      zkClient.getData(path, null, null, true);
      return true;
    } catch (KeeperException.NoAuthException e) {
      return false;
    }
  }

  @Test
  public void getConfig() {
    // As the embedded ZK is hardcoded to standalone, there is no way to test actual config data
    // here
    assertEquals("", defaultClient.getConfig());
  }

  @Test
  public void testCheckInterrupted() {
    assertFalse(Thread.currentThread().isInterrupted());
    SolrZkClient.checkInterrupted(new RuntimeException());
    assertFalse(Thread.currentThread().isInterrupted());
    SolrZkClient.checkInterrupted(new InterruptedException());
    assertTrue(Thread.currentThread().isInterrupted());
  }

  @Test
  public void testInstantiationWithSolrResourceLoader() {
    enableCustomCredentialsProvider();
    SolrResourceLoader solrResourceLoader =
        new SolrResourceLoader(Path.of("."), ClassLoader.getSystemClassLoader());
    new SolrZkClient.Builder()
        .withUrl(zkServer.getZkHost())
        .withTimeout(AbstractZkTestCase.TIMEOUT, TimeUnit.MILLISECONDS)
        .withSolrClassLoader(solrResourceLoader)
        .build()
        .close(); // no more tests needed. We only test class instantiation
  }

  private static void enableCustomCredentialsProvider() {
    System.setProperty(
        SolrZkClient.ZK_CRED_PROVIDER_CLASS_NAME_VM_PARAM_NAME,
        DigestZkCredentialsProvider.class.getName());
    System.setProperty(
        SolrZkClient.ZK_ACL_PROVIDER_CLASS_NAME_VM_PARAM_NAME, DigestZkACLProvider.class.getName());
    System.setProperty(
        SolrZkClient.ZK_CREDENTIALS_INJECTOR_CLASS_NAME_VM_PARAM_NAME,
        CustomZkCredentialsInjector.class.getName());
  }

  public static class CustomZkCredentialsInjector implements ZkCredentialsInjector {
    @Override
    public List<ZkCredential> getZkCredentials() {
      return List.of(new ZkCredential("someuser", "somepass", ZkCredential.Perms.READ));
    }
  }
}
