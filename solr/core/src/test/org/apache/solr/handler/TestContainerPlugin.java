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

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.apache.solr.client.solrj.SolrRequest.METHOD.GET;
import static org.apache.solr.filestore.TestDistribFileStore.readFile;
import static org.apache.solr.filestore.TestDistribFileStore.uploadKey;
import static org.hamcrest.Matchers.containsString;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Phaser;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.util.ResourceLoader;
import org.apache.lucene.util.ResourceLoaderAware;
import org.apache.solr.api.Command;
import org.apache.solr.api.ConfigurablePlugin;
import org.apache.solr.api.ContainerPluginsRegistry;
import org.apache.solr.api.EndPoint;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.BaseHttpSolrClient.RemoteExecutionException;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.V2Request;
import org.apache.solr.client.solrj.request.beans.PackagePayload;
import org.apache.solr.client.solrj.request.beans.PluginMeta;
import org.apache.solr.client.solrj.response.V2Response;
import org.apache.solr.cloud.ClusterSingleton;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.annotation.JsonProperty;
import org.apache.solr.common.util.ReflectMapWriter;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.embedded.JettySolrRunner;
import org.apache.solr.filestore.FileStoreAPI;
import org.apache.solr.filestore.TestDistribFileStore;
import org.apache.solr.filestore.TestDistribFileStore.Fetcher;
import org.apache.solr.pkg.PackageAPI;
import org.apache.solr.pkg.PackageListeners;
import org.apache.solr.pkg.SolrPackageLoader;
import org.apache.solr.pkg.TestPackages;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.security.PermissionNameProvider;
import org.apache.solr.util.ErrorLogMuter;
import org.hamcrest.MatcherAssert;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestContainerPlugin extends SolrCloudTestCase {
  private Phaser phaser;

  private CountingListener listener;

  private boolean forceV2;

  /**
   * A package listener that will count how many times it has been triggered. Useful to wait for
   * changes accross multiple cores.
   *
   * <p>Use by calling {@link #reset()} before the API calls, and then {@link #waitFor(int)} to
   * block until <code>num</code> cores have been notified.
   */
  static class CountingListener implements PackageListeners.Listener {
    private Semaphore changeCalled = new Semaphore(0);

    @Override
    public String packageName() {
      return null; // will fire on all package changes
    }

    @Override
    public Map<String, PackageAPI.PkgVersion> packageDetails() {
      return null; // only used to print meta information
    }

    @Override
    public void changed(SolrPackageLoader.SolrPackage pkg, Ctx ctx) {
      changeCalled.release();
    }

    public void reset() {
      changeCalled.drainPermits();
    }

    public boolean waitFor(int num) throws InterruptedException {
      return changeCalled.tryAcquire(num, 10, TimeUnit.SECONDS);
    }
  }

  @Before
  public void setup() throws Exception {
    System.setProperty("enable.packages", "true");
    phaser = new Phaser();
    forceV2 = random().nextBoolean();

    int nodes = TEST_NIGHTLY ? 4 : 2;
    cluster =
        configureCluster(nodes)
            .addConfig("conf1", configset("cloud-minimal"))
            .withJettyConfig(jetty -> jetty.enableV2(true))
            .configure();

    String coll = getSaferTestName();
    CollectionAdminRequest.createCollection(coll, "conf1", 1, nodes)
        .process(cluster.getSolrClient());
    cluster.waitForActiveCollection(coll, 1, nodes); // 1 replica per node

    listener = new CountingListener();
    for (JettySolrRunner jetty : cluster.getJettySolrRunners()) {
      CoreContainer cc = jetty.getCoreContainer();
      cc.getContainerPluginsRegistry().setPhaser(phaser);
      cc.getCores()
          .forEach(
              c -> {
                c.getPackageListeners().addListener(listener);
              });
    }
  }

  @After
  public void teardown() throws Exception {
    shutdownCluster();
    System.clearProperty("enable.packages");
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testApi() throws Exception {
    int version = phaser.getPhase();

    PluginMeta plugin = new PluginMeta();
    V2Request addPlugin = postPlugin(singletonMap("add", plugin));

    // test with an invalid class
    try (ErrorLogMuter errors = ErrorLogMuter.substring("TestContainerPlugin$C2")) {
      plugin.name = "testplugin";
      plugin.klass = C2.class.getName();
      expectError(addPlugin, "No method with @Command in class");
      assertEquals(1, errors.getCount());
    }

    // test with a valid class. This should succeed now
    plugin.klass = C3.class.getName();
    addPlugin.process(cluster.getSolrClient());

    version = phaser.awaitAdvanceInterruptibly(version, 10, TimeUnit.SECONDS);

    // just check if the plugin is indeed registered
    Callable<V2Response> readPluginState = getPlugin("/cluster/plugin");
    V2Response rsp = readPluginState.call();
    assertEquals(C3.class.getName(), rsp._getStr("/plugin/testplugin/class", null));

    // let's test the plugin
    TestDistribFileStore.assertResponseValues(
        getPlugin("/plugin/my/plugin"), Map.of("/testkey", "testval"));

    // now remove the plugin
    postPlugin("{remove : testplugin}").process(cluster.getSolrClient());

    version = phaser.awaitAdvanceInterruptibly(version, 10, TimeUnit.SECONDS);

    // verify it is removed
    rsp = readPluginState.call();
    assertNull(rsp._get("/plugin/testplugin/class", null));

    try (ErrorLogMuter errors = ErrorLogMuter.substring("TestContainerPlugin$C4")) {
      // test with a class  @EndPoint methods. This also uses a template in the path name
      plugin.klass = C4.class.getName();
      plugin.name = "collections";
      plugin.pathPrefix = "collections";
      expectError(addPlugin, "path must not have a prefix: collections");
      assertEquals(1, errors.getCount());
    }

    plugin.name = "my-random-name";
    plugin.pathPrefix = "my-random-prefix";

    addPlugin.process(cluster.getSolrClient());
    version = phaser.awaitAdvanceInterruptibly(version, 10, TimeUnit.SECONDS);

    // let's test the plugin
    TestDistribFileStore.assertResponseValues(
        getPlugin("/my-random-name/my/plugin"), Map.of("/method.name", "m1"));

    TestDistribFileStore.assertResponseValues(
        getPlugin("/my-random-prefix/their/plugin"), Map.of("/method.name", "m2"));
    // now remove the plugin
    postPlugin("{remove : my-random-name}").process(cluster.getSolrClient());

    version = phaser.awaitAdvanceInterruptibly(version, 10, TimeUnit.SECONDS);

    RemoteExecutionException e =
        assertThrows(
            RemoteExecutionException.class,
            () -> getPlugin("/my-random-prefix/their/plugin").call());
    assertEquals(404, e.code());

    final String msg = (String) ((Map<String, Object>) (e.getMetaData().get("error"))).get("msg");
    MatcherAssert.assertThat(msg, containsString("Cannot find API for the path"));

    // test ClusterSingleton plugin
    CConfig c6Cfg = new CConfig();
    c6Cfg.strVal = "added";
    plugin.name = "clusterSingleton";
    plugin.klass = C6.class.getName();
    plugin.config = c6Cfg;
    addPlugin.process(cluster.getSolrClient());
    version = phaser.awaitAdvanceInterruptibly(version, 10, TimeUnit.SECONDS);

    // just check if the plugin is indeed registered
    rsp = readPluginState.call();
    assertEquals(C6.class.getName(), rsp._getStr("/plugin/clusterSingleton/class", null));

    assertTrue("ccProvided", C6.ccProvided);
    assertTrue("startCalled", C6.startCalled);
    assertFalse("stopCalled", C6.stopCalled);

    // update the clusterSingleton config
    c6Cfg.strVal = "updated";
    postPlugin(singletonMap("update", plugin)).process(cluster.getSolrClient());
    version = phaser.awaitAdvanceInterruptibly(version, 10, TimeUnit.SECONDS);

    assertTrue("stopCalled", C6.stopCalled);

    // Clear stopCalled, it will be verified again when the Overseer Jetty is killed
    C6.stopCalled = false;

    assertEquals(CConfig.class, ContainerPluginsRegistry.getConfigClass(new CC()));
    assertEquals(CConfig.class, ContainerPluginsRegistry.getConfigClass(new CC1()));
    assertEquals(CConfig.class, ContainerPluginsRegistry.getConfigClass(new CC2()));

    CConfig cfg = new CConfig();
    cfg.boolVal = Boolean.TRUE;
    cfg.strVal = "Something";
    cfg.longVal = 1234L;
    PluginMeta p = new PluginMeta();
    p.name = "hello";
    p.klass = CC.class.getName();
    p.config = cfg;

    postPlugin(singletonMap("add", p)).process(cluster.getSolrClient());

    version = phaser.awaitAdvanceInterruptibly(version, 10, TimeUnit.SECONDS);

    TestDistribFileStore.assertResponseValues(
        getPlugin("hello/plugin"),
        Map.of(
            "/config/boolVal", "true", "/config/strVal", "Something", "/config/longVal", "1234"));

    cfg.strVal = "Something else";
    postPlugin(singletonMap("update", p)).process(cluster.getSolrClient());
    version = phaser.awaitAdvanceInterruptibly(version, 10, TimeUnit.SECONDS);

    TestDistribFileStore.assertResponseValues(
        getPlugin("hello/plugin"),
        Map.of("/config/boolVal", "true", "/config/strVal", cfg.strVal, "/config/longVal", "1234"));

    // kill the Overseer leader
    for (JettySolrRunner jetty : cluster.getJettySolrRunners()) {
      if (!jetty.getCoreContainer().getZkController().getOverseer().isClosed()) {
        cluster.stopJettySolrRunner(jetty);
        cluster.waitForJettyToStop(jetty);
      }
    }
    assertTrue("stopCalled", C6.stopCalled);
  }

  @Test
  public void testApiFromPackage() throws Exception {
    String FILE1 = "/myplugin/v1.jar";
    String FILE2 = "/myplugin/v2.jar";

    int version = phaser.getPhase();

    byte[] derFile = readFile("cryptokeys/pub_key512.der");
    uploadKey(derFile, FileStoreAPI.KEYS_DIR + "/pub_key512.der", cluster);
    TestPackages.postFileAndWait(
        cluster,
        "runtimecode/containerplugin.v.1.jar.bin",
        FILE1,
        "pmrmWCDafdNpYle2rueAGnU2J6NYlcAey9mkZYbqh+5RdYo2Ln+llLF9voyRj+DDivK9GV1XdtKvD9rgCxlD7Q==");
    TestPackages.postFileAndWait(
        cluster,
        "runtimecode/containerplugin.v.2.jar.bin",
        FILE2,
        "StR3DmqaUSL7qjDOeVEiCqE+ouiZAkW99fsL48F9oWG047o7NGgwwZ36iGgzDC3S2tPaFjRAd9Zg4UK7OZLQzg==");

    // We have two versions of the plugin in 2 different jar files. they are already uploaded to
    // the package store
    listener.reset();
    PackagePayload.AddVersion add = new PackagePayload.AddVersion();
    add.version = "1.0";
    add.pkg = "mypkg";
    add.files = singletonList(FILE1);
    V2Request addPkgVersionReq =
        new V2Request.Builder("/cluster/package")
            .forceV2(forceV2)
            .POST()
            .withPayload(singletonMap("add", add))
            .build();
    addPkgVersionReq.process(cluster.getSolrClient());
    assertTrue(
        "core package listeners did not notify",
        listener.waitFor(cluster.getJettySolrRunners().size()));

    waitForAllNodesToSync(
        "/cluster/package",
        Map.of(
            ":result:packages:mypkg[0]:version",
            "1.0",
            ":result:packages:mypkg[0]:files[0]",
            FILE1));

    // Now let's create a plugin using v1 jar file
    PluginMeta plugin = new PluginMeta();
    plugin.name = "myplugin";
    plugin.klass = "mypkg:org.apache.solr.handler.MyPlugin";
    plugin.version = add.version;
    final V2Request addPluginReq = postPlugin(singletonMap("add", plugin));
    addPluginReq.process(cluster.getSolrClient());
    version = phaser.awaitAdvanceInterruptibly(version, 10, TimeUnit.SECONDS);

    // verify the plugin creation
    TestDistribFileStore.assertResponseValues(
        getPlugin("/cluster/plugin"),
        Map.of(
            "/plugin/myplugin/class", plugin.klass,
            "/plugin/myplugin/version", plugin.version));
    // let's test this now
    Callable<V2Response> invokePlugin = getPlugin("/plugin/my/path");
    TestDistribFileStore.assertResponseValues(invokePlugin, Map.of("/myplugin.version", "1.0"));

    // now let's upload the jar file for version 2.0 of the plugin
    add.version = "2.0";
    add.files = singletonList(FILE2);
    addPkgVersionReq.process(cluster.getSolrClient());

    // here the plugin version is updated
    plugin.version = add.version;
    postPlugin(singletonMap("update", plugin)).process(cluster.getSolrClient());
    version = phaser.awaitAdvanceInterruptibly(version, 10, TimeUnit.SECONDS);

    // now verify if it is indeed updated
    TestDistribFileStore.assertResponseValues(
        getPlugin("/cluster/plugin"),
        Map.of("/plugin/myplugin/class", plugin.klass, "/plugin/myplugin/version", "2.0"));
    // invoke the plugin and test thye output
    TestDistribFileStore.assertResponseValues(invokePlugin, Map.of("/myplugin.version", "2.0"));

    plugin.name = "plugin2";
    plugin.klass = "mypkg:" + C5.class.getName();
    plugin.version = "2.0";
    addPluginReq.process(cluster.getSolrClient());
    version = phaser.awaitAdvanceInterruptibly(version, 10, TimeUnit.SECONDS);
    assertNotNull(C5.classData);
    assertEquals(1452, C5.classData.limit());
  }

  public static class CC1 extends CC {}

  public static class CC2 extends CC1 {}

  public static class CC implements ConfigurablePlugin<CConfig> {
    private CConfig cfg;

    @Override
    public void configure(CConfig cfg) {
      this.cfg = cfg;
    }

    @EndPoint(
        method = GET,
        path = "/hello/plugin",
        permission = PermissionNameProvider.Name.READ_PERM)
    public void m2(SolrQueryRequest req, SolrQueryResponse rsp) {
      rsp.add("config", cfg);
    }
  }

  public static class CConfig implements ReflectMapWriter {

    @JsonProperty public String strVal;

    @JsonProperty public Long longVal;

    @JsonProperty public Boolean boolVal;
  }

  public static class C6 implements ClusterSingleton, ConfigurablePlugin<CConfig> {
    static boolean startCalled = false;
    static boolean stopCalled = false;
    static boolean ccProvided = false;

    private State state = State.STOPPED;

    public C6(CoreContainer cc) {
      if (cc != null) {
        ccProvided = true;
      }
    }

    @Override
    public String getName() {
      return "C6";
    }

    @Override
    public void start() {
      state = State.STARTING;
      startCalled = true;
      state = State.RUNNING;
    }

    @Override
    public State getState() {
      return state;
    }

    @Override
    public void stop() {
      state = State.STOPPING;
      stopCalled = true;
      state = State.STOPPED;
    }

    @Override
    public void configure(CConfig cfg) {}
  }

  public static class C5 implements ResourceLoaderAware {
    static ByteBuffer classData;
    private SolrResourceLoader resourceLoader;

    @Override
    public void inform(ResourceLoader loader) {
      this.resourceLoader = (SolrResourceLoader) loader;
      try (InputStream is = resourceLoader.openResource("org/apache/solr/handler/MyPlugin.class")) {
        byte[] buf = new byte[1024 * 5];
        int sz = is.read(buf);
        classData = ByteBuffer.wrap(buf, 0, sz);
      } catch (IOException e) {
        // do not do anything
      }
    }

    @EndPoint(
        method = GET,
        path = "/$plugin-name/m2",
        permission = PermissionNameProvider.Name.COLL_READ_PERM)
    public void m2() {}
  }

  public static class C1 {}

  @EndPoint(
      method = GET,
      path = "/plugin/my/plugin",
      permission = PermissionNameProvider.Name.COLL_READ_PERM)
  public static class C2 {}

  @EndPoint(
      method = GET,
      path = "/plugin/my/plugin",
      permission = PermissionNameProvider.Name.COLL_READ_PERM)
  public static class C3 {
    @Command
    public void read(SolrQueryRequest req, SolrQueryResponse rsp) {
      rsp.add("testkey", "testval");
    }
  }

  public static class C4 {

    @EndPoint(
        method = GET,
        path = "$plugin-name/my/plugin",
        permission = PermissionNameProvider.Name.READ_PERM)
    public void m1(SolrQueryRequest req, SolrQueryResponse rsp) {
      rsp.add("method.name", "m1");
    }

    @EndPoint(
        method = GET,
        path = "$path-prefix/their/plugin",
        permission = PermissionNameProvider.Name.READ_PERM)
    public void m2(SolrQueryRequest req, SolrQueryResponse rsp) {
      rsp.add("method.name", "m2");
    }
  }

  private Callable<V2Response> getPlugin(String path) {
    V2Request req = new V2Request.Builder(path).forceV2(forceV2).GET().build();
    return () -> {
      V2Response rsp = req.process(cluster.getSolrClient());
      return rsp;
    };
  }

  private V2Request postPlugin(Object payload) {
    return new V2Request.Builder("/cluster/plugin")
        .forceV2(forceV2)
        .POST()
        .withPayload(payload)
        .build();
  }

  public void waitForAllNodesToSync(String path, Map<String, Object> expected) throws Exception {
    for (JettySolrRunner jettySolrRunner : cluster.getJettySolrRunners()) {
      String baseUrl = jettySolrRunner.getBaseUrl().toString().replace("/solr", "/api");
      String url = baseUrl + path + "?wt=javabin";
      TestDistribFileStore.assertResponseValues(1, new Fetcher(url, jettySolrRunner), expected);
    }
  }

  private void expectError(V2Request req, String expectErrorMsg) {
    String errPath = "/error/details[0]/errorMessages[0]";
    expectError(req, cluster.getSolrClient(), errPath, expectErrorMsg);
  }

  private static void expectError(
      V2Request req, SolrClient client, String errPath, String expectErrorMsg) {
    RemoteExecutionException e =
        expectThrows(RemoteExecutionException.class, () -> req.process(client));
    String msg = e.getMetaData()._getStr(errPath, "");
    assertTrue(expectErrorMsg, msg.contains(expectErrorMsg));
  }
}
