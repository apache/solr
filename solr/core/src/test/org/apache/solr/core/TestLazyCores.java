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
package org.apache.solr.core;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.handler.admin.CoreAdminHandler;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrQueryRequestBase;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.CommitUpdateCommand;
import org.apache.solr.update.UpdateHandler;
import org.apache.solr.util.ReadOnlyCoresLocator;
import org.junit.Test;

public class TestLazyCores extends SolrTestCaseJ4 {

  private Path solrHomeDirectory;

  private static CoreDescriptor makeCoreDescriptor(
      CoreContainer cc, String coreName, String loadOnStartup) {
    return new CoreDescriptor(
        coreName,
        cc.getCoreRootDirectory().resolve(coreName),
        cc,
        CoreDescriptor.CORE_LOADONSTARTUP,
        loadOnStartup);
  }

  private static final CoresLocator testCores =
      new ReadOnlyCoresLocator() {
        @Override
        public List<CoreDescriptor> discover(CoreContainer cc) {
          return List.of(
              makeCoreDescriptor(cc, "collection1", "true"),
              makeCoreDescriptor(cc, "collection4", "false"),
              makeCoreDescriptor(cc, "collection5", "true"));
        }
      };

  private CoreContainer init() throws Exception {
    solrHomeDirectory = createTempDir();

    for (int idx = 1; idx < 10; ++idx) {
      copyMinConf(solrHomeDirectory.resolve("collection" + idx));
    }

    NodeConfig cfg = NodeConfig.loadNodeConfig(solrHomeDirectory, null);
    return createCoreContainer(cfg, testCores);
  }

  @Test
  public void testLazyLoad() throws Exception {
    CoreContainer cc = init();
    try {

      // NOTE: This checks the initial state for loading, no need to do this elsewhere.
      checkLoadedCores(cc, "collection1", "collection5");
      checkCoresNotLoaded(cc, "collection4");

      SolrCore core1 = cc.getCore("collection1");
      assertTrue("core1 should be loadable", core1.getCoreDescriptor().isLoadOnStartup());
      assertNotNull(core1.getSolrConfig());

      SolrCore core4 = cc.getCore("collection4");
      assertFalse("core4 should not be loadable", core4.getCoreDescriptor().isLoadOnStartup());

      SolrCore core5 = cc.getCore("collection5");
      assertTrue("core5 should be loadable", core5.getCoreDescriptor().isLoadOnStartup());

      core1.close();
      core4.close();
      core5.close();
    } finally {
      cc.shutdown();
    }
  }

  // This is a little weak. I'm not sure how to test that lazy core2 is loaded automagically. The
  // getCore will, of course, load it.

  private void checkSearch(SolrCore core) throws IOException {
    addLazy(core, "id", "0");
    addLazy(core, "id", "1", "v_t", "Hello Dude");
    addLazy(core, "id", "2", "v_t", "Hello Yonik");
    addLazy(core, "id", "3", "v_s", "{!literal}");
    addLazy(core, "id", "4", "v_s", "other stuff");
    addLazy(core, "id", "5", "v_f", "3.14159");
    addLazy(core, "id", "6", "v_f", "8983");

    SolrQueryRequest req = makeReq(core);
    CommitUpdateCommand cmtCmd = new CommitUpdateCommand(req, false);
    core.getUpdateHandler().commit(cmtCmd);

    // Just get a couple of searches to work!
    assertQ(
        "test prefix query",
        makeReq(core, "q", "{!prefix f=v_t}hel", "wt", "xml"),
        "//result[@numFound='2']");

    assertQ(
        "test raw query",
        makeReq(core, "q", "{!raw f=v_t}hello", "wt", "xml"),
        "//result[@numFound='2']");

    // no analysis is done, so these should match nothing
    assertQ(
        "test raw query",
        makeReq(core, "q", "{!raw f=v_t}Hello", "wt", "xml"),
        "//result[@numFound='0']");
    assertQ(
        "test raw query",
        makeReq(core, "q", "{!raw f=v_f}1.5", "wt", "xml"),
        "//result[@numFound='0']");
  }

  @Test
  public void testLazySearch() throws Exception {
    CoreContainer cc = init();
    try {
      // Make sure collection4 isn't loaded. Should be loaded on the get
      checkCoresNotLoaded(cc, "collection4");
      SolrCore core4 = cc.getCore("collection4");

      checkSearch(core4);

      // Now just ensure that the normal searching on "collection1" finds _0_ on the same query that
      // found _2_ above. Use of makeReq above and req below is tricky, very tricky.
      SolrCore collection1 = cc.getCore("collection1");
      assertQ(
          "test raw query",
          makeReq(collection1, "q", "{!raw f=v_t}hello", "wt", "xml"),
          "//result[@numFound='0']");

      checkLoadedCores(cc, "collection1", "collection4", "collection5");

      core4.close();
      collection1.close();
    } finally {
      cc.shutdown();
    }
  }

  // Test case for SOLR-4300

  @Test
  public void testRace() throws Exception {
    final List<SolrCore> theCores = new ArrayList<>();
    final CoreContainer cc = init();
    try {

      Thread[] threads = new Thread[15];
      for (int idx = 0; idx < threads.length; idx++) {
        threads[idx] =
            new Thread(
                () -> {
                  SolrCore core = cc.getCore("collection4");
                  synchronized (theCores) {
                    theCores.add(core);
                  }
                });
        threads[idx].start();
      }
      for (Thread thread : threads) {
        thread.join();
      }
      for (int idx = 0; idx < theCores.size() - 1; ++idx) {
        assertEquals("Cores should be the same!", theCores.get(idx), theCores.get(idx + 1));
      }
      for (SolrCore core : theCores) {
        core.close();
      }

    } finally {
      cc.shutdown();
    }
  }

  private void tryCreateFail(CoreAdminHandler admin, String name, String dataDir, String... errs) {
    SolrException thrown =
        expectThrows(
            SolrException.class,
            () -> {
              SolrQueryResponse resp = new SolrQueryResponse();

              SolrQueryRequest request =
                  req(
                      CoreAdminParams.ACTION,
                      CoreAdminParams.CoreAdminAction.CREATE.toString(),
                      CoreAdminParams.DATA_DIR,
                      dataDir,
                      CoreAdminParams.NAME,
                      name,
                      "schema",
                      "schema.xml",
                      "config",
                      "solrconfig.xml");

              admin.handleRequestBody(request, resp);
            });
    assertEquals("Exception code should be 500", 500, thrown.code());
    for (String err : errs) {
      assertTrue(
          "Should have seen an exception containing the an error",
          thrown.getMessage().contains(err));
    }
  }

  @Test
  public void testCreateSame() throws Exception {
    final CoreContainer cc = init();
    try {
      final CoreAdminHandler admin = new CoreAdminHandler(cc);
      SolrCore lc4 = cc.getCore("collection4");
      SolrCore lc5 = cc.getCore("collection5");

      // Should fail with the same name
      tryCreateFail(admin, "collection4", "t14", "Core with name", "collection4", "already exists");
      tryCreateFail(admin, "collection5", "t15", "Core with name", "collection5", "already exists");

      lc4.close();
      lc5.close();

    } finally {
      cc.shutdown();
    }
  }

  private void unloadViaAdmin(CoreContainer cc, String name) throws Exception {

    try (final CoreAdminHandler admin = new CoreAdminHandler(cc)) {
      SolrQueryResponse resp = new SolrQueryResponse();
      admin.handleRequestBody(
          req(
              CoreAdminParams.ACTION,
              CoreAdminParams.CoreAdminAction.UNLOAD.toString(),
              CoreAdminParams.CORE,
              name),
          resp);
    }
  }

  // Test that transient cores
  // 1> produce errors as appropriate when the config or schema files are foo'd
  // 2> "self-heal". That is, if the problem is corrected can the core be reloaded and used?
  // 3> that OK cores can be searched even when some cores failed to load.
  // 4> that having no solr.xml entry for transient cache handler correctly uses the default.
  @Test
  public void testBadConfigsGenerateErrors() throws Exception {
    final CoreContainer cc =
        initGoodAndBad(
            Arrays.asList("core1", "core2"),
            Arrays.asList("badSchema1", "badSchema2"),
            Arrays.asList("badConfig1", "badConfig2"));

    try {
      // first, did the two good cores load successfully?
      checkLoadedCores(cc, "core1", "core2");

      // Did the bad cores fail to load?
      checkFailedCores(cc, "badSchema1", "badSchema2", "badConfig1", "badConfig2");

      //  Can we still search the "good" cores even though there were core init failures?
      SolrCore core1 = cc.getCore("core1");
      checkSearch(core1);

      // Did we get the expected message for each of the cores that failed to load? Make sure we
      // don't run afoul of the dreaded slash/backslash difference on Windows and *nix machines.
      testMessage(
          cc.getCoreInitFailures(), Path.of("badConfig1", "conf", "solrconfig.xml").toString());
      testMessage(
          cc.getCoreInitFailures(), Path.of("badConfig2", "conf", "solrconfig.xml").toString());
      testMessage(cc.getCoreInitFailures(), Path.of("badSchema1", "conf", "schema.xml").toString());
      testMessage(cc.getCoreInitFailures(), Path.of("badSchema2", "conf", "schema.xml").toString());

      // Status should report that there are failure messages for the bad cores and none for the
      // good cores.
      checkStatus(cc, true, "core1");
      checkStatus(cc, true, "core2");
      checkStatus(cc, false, "badSchema1");
      checkStatus(cc, false, "badSchema2");
      checkStatus(cc, false, "badConfig1");
      checkStatus(cc, false, "badConfig2");

      // Copy good config and schema files in and see if you can then load them (they are transient
      // after all)
      copyGoodConf("badConfig1", "solrconfig-minimal.xml", "solrconfig.xml");
      copyGoodConf("badConfig2", "solrconfig-minimal.xml", "solrconfig.xml");
      copyGoodConf("badSchema1", "schema-tiny.xml", "schema.xml");
      copyGoodConf("badSchema2", "schema-tiny.xml", "schema.xml");

      // Reload the cores and ensure that
      // 1> they pick up the new configs
      // 2> they don't fail again b/c they still have entries in loadFailure in core container.
      cc.reload("badConfig1");
      cc.reload("badConfig2");
      cc.reload("badSchema1");
      cc.reload("badSchema2");
      SolrCore bc1 = cc.getCore("badConfig1");
      SolrCore bc2 = cc.getCore("badConfig2");
      SolrCore bs1 = cc.getCore("badSchema1");
      SolrCore bs2 = cc.getCore("badSchema2");

      // all the cores should be found in the list now.
      checkLoadedCores(
          cc, "core1", "core2", "badSchema1", "badSchema2", "badConfig1", "badConfig2");

      // Did we clear out the errors by putting good files in place? And the cores that never were
      // bad should be OK too.
      checkStatus(cc, true, "core1");
      checkStatus(cc, true, "core2");
      checkStatus(cc, true, "badSchema1");
      checkStatus(cc, true, "badSchema2");
      checkStatus(cc, true, "badConfig1");
      checkStatus(cc, true, "badConfig2");

      // Are the formerly bad cores now searchable? Testing one of each should do.
      checkSearch(core1);
      checkSearch(bc1);
      checkSearch(bs1);

      core1.close();
      bc1.close();
      bc2.close();
      bs1.close();
      bs2.close();
    } finally {
      cc.shutdown();
    }
  }

  // See fi the message you expect is in the list of failures
  private void testMessage(Map<String, CoreContainer.CoreLoadFailure> failures, String lookFor) {
    List<String> messages = new ArrayList<>();
    for (CoreContainer.CoreLoadFailure e : failures.values()) {
      String message = e.exception.getCause().getMessage();
      messages.add(message);
      if (message.contains(lookFor)) return;
    }
    fail(
        "Should have found message containing these tokens "
            + lookFor
            + " in the failure messages: "
            + messages);
  }

  // Just localizes writing a configuration rather than repeating it for good and bad files.
  private void writeCustomConfig(String coreName, String config, String schema, String rand_snip)
      throws IOException {

    Path coreRoot = solrHomeDirectory.resolve(coreName);
    Path subHome = coreRoot.resolve("conf");
    Files.createDirectories(subHome);
    // Write the file for core discovery
    Files.writeString(
        coreRoot.resolve("core.properties"),
        "name="
            + coreName
            + System.getProperty("line.separator")
            + "transient=true"
            + System.getProperty("line.separator")
            + "loadOnStartup=true",
        StandardCharsets.UTF_8);

    Files.writeString(
        subHome.resolve("solrconfig.snippet.randomindexconfig.xml"),
        rand_snip,
        StandardCharsets.UTF_8);

    Files.writeString(subHome.resolve("solrconfig.xml"), config, StandardCharsets.UTF_8);

    Files.writeString(subHome.resolve("schema.xml"), schema, StandardCharsets.UTF_8);
  }

  // Write out the cores' config files, both bad schema files, bad config files and some good
  // cores.
  private CoreContainer initGoodAndBad(
      List<String> goodCores, List<String> badSchemaCores, List<String> badConfigCores)
      throws Exception {
    solrHomeDirectory = createTempDir();

    // Don't pollute the log with exception traces when they're expected.
    ignoreException(Pattern.quote("SAXParseException"));

    // Create the cores that should be fine.
    for (String coreName : goodCores) {
      Path coreRoot = solrHomeDirectory.resolve(coreName);
      copyMinConf(coreRoot, "name=" + coreName);
    }

    // Collect the files that we'll write to the config directories.
    Path top = SolrTestCaseJ4.TEST_HOME().resolve("collection1").resolve("conf");
    String min_schema = Files.readString(top.resolve("schema-tiny.xml"), StandardCharsets.UTF_8);
    String min_config =
        Files.readString(top.resolve("solrconfig-minimal.xml"), StandardCharsets.UTF_8);
    String rand_snip =
        Files.readString(
            top.resolve("solrconfig.snippet.randomindexconfig.xml"), StandardCharsets.UTF_8);

    // Now purposely mess up the config files, introducing stupid syntax errors.
    String bad_config = min_config.replace("<requestHandler", "<reqsthalr");
    String bad_schema = min_schema.replace("<field", "<filed");

    // Create the cores with bad configs
    for (String coreName : badConfigCores) {
      writeCustomConfig(coreName, bad_config, min_schema, rand_snip);
    }

    // Create the cores with bad schemas.
    for (String coreName : badSchemaCores) {
      writeCustomConfig(coreName, min_config, bad_schema, rand_snip);
    }

    NodeConfig config = SolrXmlConfig.fromString(solrHomeDirectory, "<solr/>");

    // OK this should succeed, but at the end we should have recorded a series of errors.
    return createCoreContainer(config, new CorePropertiesLocator(config));
  }

  // We want to see that the core "heals itself" if an un-corrupted file is written to the
  // directory.
  private void copyGoodConf(String coreName, String srcName, String dstName) throws IOException {
    Path coreRoot = solrHomeDirectory.resolve(coreName);
    Path subHome = coreRoot.resolve("conf");
    Path top = SolrTestCaseJ4.TEST_HOME().resolve("collection1").resolve("conf");
    Files.copy(top.resolve(srcName), subHome.resolve(dstName), StandardCopyOption.REPLACE_EXISTING);
  }

  // If ok==true, we shouldn't be seeing any failure cases.
  // if ok==false, the core being examined should have a failure in the list.
  private void checkStatus(CoreContainer cc, Boolean ok, String core) throws Exception {
    SolrQueryResponse resp = new SolrQueryResponse();
    try (final CoreAdminHandler admin = new CoreAdminHandler(cc)) {
      admin.handleRequestBody(
          req(
              CoreAdminParams.ACTION,
              CoreAdminParams.CoreAdminAction.STATUS.toString(),
              CoreAdminParams.CORE,
              core),
          resp);
    }

    @SuppressWarnings({"unchecked"})
    Map<String, Exception> failures = (Map<String, Exception>) resp.getValues().get("initFailures");

    if (ok) {
      if (failures.size() != 0) {
        fail("Should have cleared the error, but there are failures " + failures);
      }
    } else {
      if (failures.size() == 0) {
        fail("Should have had errors here but the status return has no failures!");
      }
    }
  }

  public static void checkCoresNotLoaded(CoreContainer cc, String... coreNames) {
    checkSomeCoresNotLoaded(cc, coreNames.length, coreNames);
  }

  public static void checkSomeCoresNotLoaded(
      CoreContainer cc, int numNotLoaded, String... coreNames) {
    Collection<String> loadedCoreNames = cc.getLoadedCoreNames();
    List<String> notLoadedCoreNames = new ArrayList<>();
    for (String coreName : coreNames) {
      if (!loadedCoreNames.contains(coreName)) {
        notLoadedCoreNames.add(coreName);
      }
    }
    assertEquals(
        "Expected "
            + numNotLoaded
            + " not loaded cores but found "
            + notLoadedCoreNames.size()
            + ", coreNames="
            + Arrays.asList(coreNames)
            + ", notLoadedCoreNames="
            + notLoadedCoreNames
            + ", loadedCoreNames="
            + loadedCoreNames,
        numNotLoaded,
        notLoadedCoreNames.size());

    // All transient cores are listed in allCoreNames.
    Collection<String> allCoreNames = cc.getAllCoreNames();
    for (String coreName : coreNames) {
      assertTrue(
          "Core " + coreName + " should have been found in the list of all known core names",
          allCoreNames.contains(coreName));
    }

    checkCoreNamesAndDescriptors(cc);
  }

  private static void checkCoreNamesAndDescriptors(CoreContainer cc) {
    Collection<String> allNames = cc.getAllCoreNames();
    List<CoreDescriptor> descriptors = cc.getCoreDescriptors();

    // Every core that has not failed to load should be in coreDescriptors.
    assertEquals(
        "There should be as many coreDescriptors as coreNames",
        allNames.size(),
        descriptors.size());
    for (CoreDescriptor desc : descriptors) {
      assertTrue(
          "Each coreName should have a corresponding coreDescriptor",
          allNames.contains(desc.getName()));
    }

    // All loaded cores are in allNames.
    for (String name : cc.getLoadedCoreNames()) {
      assertTrue(
          "Loaded core " + name + " should have been found in the list of all possible core names",
          allNames.contains(name));
    }
  }

  private static void checkFailedCores(CoreContainer cc, String... failedCoreNames) {
    // Failed cores should not be in allCoreNames.
    Collection<String> allNames = cc.getAllCoreNames();
    for (String name : failedCoreNames) {
      assertFalse(
          "Failed core "
              + name
              + " should not have been found in the list of all possible core names",
          allNames.contains(name));
    }
  }

  public static void checkLoadedCores(CoreContainer cc, String... coreNames) {
    checkSomeLoadedCores(cc, coreNames.length, coreNames);
  }

  public static void checkSomeLoadedCores(CoreContainer cc, int numLoaded, String... coreNames) {
    Collection<String> loadedCoreNames = cc.getLoadedCoreNames();
    List<String> loadedListedCoreNames = new ArrayList<>();
    for (String coreName : coreNames) {
      if (loadedCoreNames.contains(coreName)) {
        loadedListedCoreNames.add(coreName);
      }
    }
    assertEquals(
        "Expected "
            + numLoaded
            + " loaded cores but found "
            + loadedListedCoreNames.size()
            + ", coreNames="
            + Arrays.asList(coreNames)
            + ", loadedListedCoreNames="
            + loadedListedCoreNames
            + ", loadedCoreNames="
            + loadedCoreNames,
        numLoaded,
        loadedListedCoreNames.size());
  }

  private void addLazy(SolrCore core, String... fieldValues) throws IOException {
    UpdateHandler updater = core.getUpdateHandler();
    AddUpdateCommand cmd = new AddUpdateCommand(makeReq(core));
    cmd.solrDoc = sdoc((Object[]) fieldValues);
    updater.addDoc(cmd);
  }

  private SolrQueryRequestBase makeReq(SolrCore core, String... paramPairs) {
    return new SolrQueryRequestBase(core, params(paramPairs));
  }

  @Test
  public void testMidUseUnload() throws Exception {
    // sleep for up to 10 s Must add 1 because using
    final int maximumSleepMillis = random().nextInt(9999) + 1;
    // this as a seed will rea few lines down will
    // throw an exception if this is zero
    if (VERBOSE) {
      System.out.println("TestLazyCores.testMidUseUnload maximumSleepMillis=" + maximumSleepMillis);
    }

    class TestThread extends Thread {

      SolrCore core_to_use = null;

      @Override
      public void run() {

        final int sleep_millis = random().nextInt(maximumSleepMillis);
        try {
          if (sleep_millis > 0) {
            if (VERBOSE) {
              System.out.println(
                  "TestLazyCores.testMidUseUnload Thread.run sleeping for " + sleep_millis + " ms");
            }
            Thread.sleep(sleep_millis);
          }
        } catch (InterruptedException ie) {
          if (VERBOSE) {
            System.out.println(
                "TestLazyCores.testMidUseUnload Thread.run caught "
                    + ie
                    + " whilst sleeping for "
                    + sleep_millis
                    + " ms");
          }
        }

        // not closed since we are still using it and hold a reference
        assertFalse(core_to_use.isClosed());
        // now give up our reference to the core
        core_to_use.close();
      }
    }

    CoreContainer cc = init();

    try {
      TestThread thread = new TestThread();

      thread.core_to_use = cc.getCore("collection1");
      assertNotNull(thread.core_to_use);
      assertFalse(thread.core_to_use.isClosed()); // freshly-in-use core is not closed
      thread.start();

      unloadViaAdmin(cc, "collection1");
      assertTrue(thread.core_to_use.isClosed()); // after unload-ing the core is closed

      thread.join();
    } finally {
      cc.shutdown();
    }
  }
}
