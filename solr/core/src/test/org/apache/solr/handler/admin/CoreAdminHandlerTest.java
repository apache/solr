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
package org.apache.solr.handler.admin;

import static org.apache.solr.handler.admin.CoreAdminHandler.CoreAdminAsyncTracker.COMPLETED;

import com.fasterxml.jackson.core.type.TypeReference;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.io.file.PathUtils;
import org.apache.lucene.util.Constants;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.api.model.CoreStatusResponse;
import org.apache.solr.client.solrj.JacksonContentWriter;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.core.SolrCore;
import org.apache.solr.embedded.JettyConfig;
import org.apache.solr.embedded.JettySolrRunner;
import org.apache.solr.handler.admin.CoreAdminHandler.CoreAdminAsyncTracker;
import org.apache.solr.handler.admin.CoreAdminHandler.CoreAdminAsyncTracker.TaskObject;
import org.apache.solr.response.SolrQueryResponse;
import org.junit.BeforeClass;
import org.junit.Test;

public class CoreAdminHandlerTest extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml", "schema.xml");
  }

  public String getCoreName() {
    return this.getClass().getName() + "_sys_vars";
  }

  @Test
  public void testCreateWithSysVars() throws Exception {
    useFactory(null); // I require FS-based indexes for this test.

    final Path workDir = createTempDir(getCoreName());

    String coreName = "with_sys_vars";
    Path instDir = workDir.resolve(coreName);
    Path subHome = instDir.resolve("conf");
    Files.createDirectories(subHome);

    // Be sure we pick up sysvars when we create this
    String srcDir = SolrTestCaseJ4.TEST_HOME() + "/collection1/conf";
    Files.copy(Path.of(srcDir, "schema-tiny.xml"), subHome.resolve("schema_ren.xml"));
    Files.copy(Path.of(srcDir, "solrconfig-minimal.xml"), subHome.resolve("solrconfig_ren.xml"));
    Files.copy(
        Path.of(srcDir, "solrconfig.snippet.randomindexconfig.xml"),
        subHome.resolve("solrconfig.snippet.randomindexconfig.xml"));

    final CoreContainer cores = h.getCoreContainer();
    cores.getAllowPaths().add(workDir);

    final CoreAdminHandler admin = new CoreAdminHandler(cores);

    // create a new core (using CoreAdminHandler) w/ properties
    System.setProperty("INSTDIR_TEST", instDir.toAbsolutePath().toString());
    System.setProperty("CONFIG_TEST", "solrconfig_ren.xml");
    System.setProperty("SCHEMA_TEST", "schema_ren.xml");

    Path dataDir = workDir.resolve("data_diff");
    System.setProperty("DATA_TEST", dataDir.toAbsolutePath().toString());

    SolrQueryResponse resp = new SolrQueryResponse();
    admin.handleRequestBody(
        req(
            CoreAdminParams.ACTION,
            CoreAdminParams.CoreAdminAction.CREATE.toString(),
            CoreAdminParams.NAME,
            getCoreName(),
            CoreAdminParams.INSTANCE_DIR,
            "${INSTDIR_TEST}",
            CoreAdminParams.CONFIG,
            "${CONFIG_TEST}",
            CoreAdminParams.SCHEMA,
            "${SCHEMA_TEST}",
            CoreAdminParams.DATA_DIR,
            "${DATA_TEST}"),
        resp);
    assertNull("Exception on create", resp.getException());

    // Now assert that certain values are properly dereferenced in the process of creating the core,
    // see SOLR-4982.

    // Should NOT be a datadir named ${DATA_TEST} (literal). This is the bug after all
    Path badDir = instDir.resolve("${DATA_TEST}");
    assertFalse(
        "Should have substituted the sys var, found file " + badDir.toAbsolutePath(),
        Files.exists(badDir));

    // For the other 3 vars, we couldn't get past creating the core fi dereferencing didn't work
    // correctly.

    // Should have segments in the directory pointed to by the ${DATA_TEST}.
    Path test = dataDir.resolve("index");
    assertTrue("Should have found index dir at " + test.toAbsolutePath(), Files.exists(test));
    admin.close();
  }

  @Test
  public void testCoreAdminHandler() throws Exception {
    final Path workDir = createTempDir();

    final CoreContainer cores = h.getCoreContainer();
    cores.getAllowPaths().add(workDir);

    final CoreAdminHandler admin = new CoreAdminHandler(cores);

    Path instDir;
    try (SolrCore template = cores.getCore("collection1")) {
      assertNotNull(template);
      instDir = template.getCoreDescriptor().getInstanceDir();
    }

    assertTrue("instDir doesn't exist: " + instDir, Files.exists(instDir));
    final Path instProp = workDir.resolve("instProp");
    PathUtils.copyDirectory(instDir, instProp);

    SolrQueryResponse resp = new SolrQueryResponse();
    // Sneaking in a test for using a bad core name
    SolrException se =
        expectThrows(
            SolrException.class,
            () -> {
              admin.handleRequestBody(
                  req(
                      CoreAdminParams.ACTION,
                      CoreAdminParams.CoreAdminAction.CREATE.toString(),
                      CoreAdminParams.INSTANCE_DIR,
                      instProp.toAbsolutePath().toString(),
                      CoreAdminParams.NAME,
                      "ugly$core=name"),
                  new SolrQueryResponse());
            });
    assertTrue("Expected error message for bad core name.", se.toString().contains("Invalid core"));

    CoreDescriptor cd = cores.getCoreDescriptor("ugly$core=name");
    assertNull("Should NOT have added this core!", cd);

    // create a new core (using CoreAdminHandler) w/ properties

    admin.handleRequestBody(
        req(
            CoreAdminParams.ACTION,
            CoreAdminParams.CoreAdminAction.CREATE.toString(),
            CoreAdminParams.INSTANCE_DIR,
            instProp.toAbsolutePath().toString(),
            CoreAdminParams.NAME,
            "props",
            CoreAdminParams.PROPERTY_PREFIX + "hoss",
            "man",
            CoreAdminParams.PROPERTY_PREFIX + "foo",
            "baz"),
        resp);
    assertNull("Exception on create", resp.getException());

    cd = cores.getCoreDescriptor("props");
    assertNotNull("Core not added!", cd);
    assertEquals(cd.getCoreProperty("hoss", null), "man");
    assertEquals(cd.getCoreProperty("foo", null), "baz");

    // attempt to create a bogus core and confirm failure
    ignoreException("Could not load config");
    se =
        expectThrows(
            SolrException.class,
            () -> {
              admin.handleRequestBody(
                  req(
                      CoreAdminParams.ACTION,
                      CoreAdminParams.CoreAdminAction.CREATE.toString(),
                      CoreAdminParams.NAME,
                      "bogus_dir_core",
                      CoreAdminParams.INSTANCE_DIR,
                      "dir_does_not_exist_127896"),
                  new SolrQueryResponse());
            });
    // :NOOP:
    // :TODO: CoreAdminHandler's exception messages are terrible, otherwise we could assert
    // something useful here

    unIgnoreException("Could not load config");

    // check specifically for status of the failed core name
    resp = new SolrQueryResponse();
    admin.handleRequestBody(
        req(
            CoreAdminParams.ACTION,
            CoreAdminParams.CoreAdminAction.STATUS.toString(),
            CoreAdminParams.CORE,
            "bogus_dir_core"),
        resp);
    @SuppressWarnings("unchecked")
    Map<String, Exception> failures = (Map<String, Exception>) resp.getValues().get("initFailures");
    assertNotNull("core failures is null", failures);

    final var statusByCore =
        JacksonContentWriter.DEFAULT_MAPPER.convertValue(
            resp.getValues().get("status"),
            new TypeReference<Map<String, CoreStatusResponse.SingleCoreData>>() {});
    assertNotNull("core status is null", statusByCore);

    assertEquals("wrong number of core failures", 1, failures.size());
    Exception fail = failures.get("bogus_dir_core");
    assertNotNull("null failure for test core", fail);
    assertTrue(
        "init failure doesn't mention problem: " + fail.getCause().getMessage(),
        0 < fail.getCause().getMessage().indexOf("dir_does_not_exist"));

    assertTrue("bogus_dir_core status isn't empty", statusByCore.containsKey("bogus_dir_core"));
    final var bogusDirCoreStatus = statusByCore.get("bogus_dir_core");
    assertNull(bogusDirCoreStatus.name);
    assertNull(bogusDirCoreStatus.config);

    // Try renaming the core, we should fail
    // First assert that the props core exists
    cd = cores.getCoreDescriptor("props");
    assertNotNull("Core disappeared!", cd);

    // now rename it something else just for kicks since we don't actually test this that I could
    // find.
    admin.handleRequestBody(
        req(
            CoreAdminParams.ACTION,
            CoreAdminParams.CoreAdminAction.RENAME.toString(),
            CoreAdminParams.CORE,
            "props",
            CoreAdminParams.OTHER,
            "rename_me"),
        resp);

    cd = cores.getCoreDescriptor("rename_me");
    assertNotNull("Core should have been renamed!", cd);

    // Rename it something bogus and see if you get an exception, the old core is still there and
    // the bogus one isn't
    se =
        expectThrows(
            SolrException.class,
            () -> {
              admin.handleRequestBody(
                  req(
                      CoreAdminParams.ACTION,
                      CoreAdminParams.CoreAdminAction.RENAME.toString(),
                      CoreAdminParams.CORE,
                      "rename_me",
                      CoreAdminParams.OTHER,
                      "bad$name"),
                  new SolrQueryResponse());
            });
    assertTrue(
        "Expected error message for bad core name.", se.getMessage().contains("Invalid core"));

    cd = cores.getCoreDescriptor("bad$name");
    assertNull("Core should NOT exist!", cd);

    cd = cores.getCoreDescriptor("rename_me");
    assertNotNull("Core should have been renamed!", cd);

    // :TODO: because of SOLR-3665 we can't ask for status from all cores
    admin.close();
  }

  @Test
  public void testDeleteInstanceDir() throws Exception {
    Path solrHomeDirectory = createTempDir("solr-home");
    copySolrHomeToTemp(solrHomeDirectory, "corex");
    Path corex = solrHomeDirectory.resolve("corex");
    Files.writeString(corex.resolve("core.properties"), "", StandardCharsets.UTF_8);

    copySolrHomeToTemp(solrHomeDirectory, "corerename");

    Path coreRename = solrHomeDirectory.resolve("corerename");
    Path renamePropFile = coreRename.resolve("core.properties");
    Files.writeString(renamePropFile, "", StandardCharsets.UTF_8);

    JettySolrRunner runner =
        new JettySolrRunner(
            solrHomeDirectory.toAbsolutePath().toString(), JettyConfig.builder().build());
    runner.start();

    try (SolrClient client =
        new HttpSolrClient.Builder(runner.getBaseUrl().toString())
            .withDefaultCollection("corex")
            .withConnectionTimeout(DEFAULT_CONNECTION_TIMEOUT, TimeUnit.MILLISECONDS)
            .withSocketTimeout(DEFAULT_CONNECTION_TIMEOUT, TimeUnit.MILLISECONDS)
            .build()) {
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField("id", "123");
      client.add(doc);
      client.commit();
    }

    try (SolrClient client =
        new HttpSolrClient.Builder(runner.getBaseUrl().toString())
            .withConnectionTimeout(DEFAULT_CONNECTION_TIMEOUT, TimeUnit.MILLISECONDS)
            .withSocketTimeout(DEFAULT_CONNECTION_TIMEOUT, TimeUnit.MILLISECONDS)
            .build()) {
      CoreAdminRequest.Unload req = new CoreAdminRequest.Unload(false);
      req.setDeleteInstanceDir(true);
      req.setCoreName("corex");
      req.process(client);
    }

    // Make sure a renamed core
    // 1> has the property persisted (SOLR-11783)
    // 2> is deleted after rename properly.

    try (SolrClient client =
        new HttpSolrClient.Builder(runner.getBaseUrl().toString())
            .withConnectionTimeout(DEFAULT_CONNECTION_TIMEOUT, TimeUnit.MILLISECONDS)
            .withSocketTimeout(DEFAULT_CONNECTION_TIMEOUT, TimeUnit.MILLISECONDS)
            .build()) {
      CoreAdminRequest.renameCore("corerename", "brand_new_core_name", client);
      Properties props = new Properties();
      try (Reader is = Files.newBufferedReader(renamePropFile, StandardCharsets.UTF_8)) {
        props.load(is);
      }
      assertEquals(
          "Name should have been persisted!", "brand_new_core_name", props.getProperty("name"));
    }

    try (SolrClient client =
        new HttpSolrClient.Builder(runner.getBaseUrl().toString())
            .withConnectionTimeout(DEFAULT_CONNECTION_TIMEOUT, TimeUnit.MILLISECONDS)
            .withSocketTimeout(DEFAULT_CONNECTION_TIMEOUT, TimeUnit.MILLISECONDS)
            .build()) {
      CoreAdminRequest.Unload req = new CoreAdminRequest.Unload(false);
      req.setDeleteInstanceDir(true);
      req.setCoreName("brand_new_core_name");
      req.process(client);
    }

    runner.stop();

    assertFalse(
        "Instance directory exists after core unload with deleteInstanceDir=true : " + corex,
        Files.exists(corex));

    assertFalse(
        "Instance directory exists after core unload with deleteInstanceDir=true : " + coreRename,
        Files.exists(coreRename));
  }

  @Test
  public void testUnloadForever() throws Exception {
    Path solrHomeDirectory = createTempDir("solr-home");
    copySolrHomeToTemp(solrHomeDirectory, "corex");
    Path corex = solrHomeDirectory.resolve("corex");
    Files.writeString(corex.resolve("core.properties"), "", StandardCharsets.UTF_8);
    JettySolrRunner runner =
        new JettySolrRunner(
            solrHomeDirectory.toAbsolutePath().toString(), JettyConfig.builder().build());
    runner.start();

    try (SolrClient client =
        new HttpSolrClient.Builder(runner.getBaseUrl().toString())
            .withDefaultCollection("corex")
            .withConnectionTimeout(DEFAULT_CONNECTION_TIMEOUT, TimeUnit.MILLISECONDS)
            .withSocketTimeout(DEFAULT_CONNECTION_TIMEOUT, TimeUnit.MILLISECONDS)
            .build()) {
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField("id", "123");
      client.add(doc);
      client.commit();
    }

    try (SolrClient client =
        new HttpSolrClient.Builder(runner.getBaseUrl().toString())
            .withDefaultCollection("corex")
            .withConnectionTimeout(DEFAULT_CONNECTION_TIMEOUT, TimeUnit.MILLISECONDS)
            .withSocketTimeout(DEFAULT_CONNECTION_TIMEOUT, TimeUnit.MILLISECONDS)
            .build()) {
      QueryResponse result = client.query(new SolrQuery("id:*"));
      assertEquals(1, result.getResults().getNumFound());
    }

    try (SolrClient client =
        new HttpSolrClient.Builder(runner.getBaseUrl().toString())
            .withConnectionTimeout(DEFAULT_CONNECTION_TIMEOUT, TimeUnit.MILLISECONDS)
            .withSocketTimeout(DEFAULT_CONNECTION_TIMEOUT, TimeUnit.MILLISECONDS)
            .build()) {
      CoreAdminRequest.Unload req = new CoreAdminRequest.Unload(false);
      req.setDeleteInstanceDir(false); // random().nextBoolean());
      req.setCoreName("corex");
      req.process(client);
    }

    SolrClient.RemoteSolrException rse =
        expectThrows(
            SolrClient.RemoteSolrException.class,
            () -> {
              try (SolrClient client =
                  new HttpSolrClient.Builder(runner.getBaseUrl().toString())
                      .withDefaultCollection("corex")
                      .withConnectionTimeout(DEFAULT_CONNECTION_TIMEOUT, TimeUnit.MILLISECONDS)
                      .withSocketTimeout(DEFAULT_CONNECTION_TIMEOUT * 1000, TimeUnit.MILLISECONDS)
                      .build()) {
                client.query(new SolrQuery("id:*"));
              } finally {
                runner.stop();
              }
            });
    assertEquals("Should have received a 404 error", 404, rse.code());
  }

  @Test
  public void testDeleteInstanceDirAfterCreateFailure() throws Exception {
    assumeFalse(
        "Ignore test on windows because it does not delete data directory immediately after unload",
        Constants.WINDOWS);
    Path solrHomeDirectory = createTempDir("solr-home");
    copySolrHomeToTemp(solrHomeDirectory, "corex");
    Path corex = solrHomeDirectory.resolve("corex");
    Files.writeString(corex.resolve("core.properties"), "", StandardCharsets.UTF_8);
    JettySolrRunner runner =
        new JettySolrRunner(
            solrHomeDirectory.toAbsolutePath().toString(), JettyConfig.builder().build());
    runner.start();

    try (SolrClient client =
        new HttpSolrClient.Builder(runner.getBaseUrl().toString())
            .withDefaultCollection("corex")
            .withConnectionTimeout(DEFAULT_CONNECTION_TIMEOUT, TimeUnit.MILLISECONDS)
            .withSocketTimeout(DEFAULT_CONNECTION_TIMEOUT, TimeUnit.MILLISECONDS)
            .build()) {
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField("id", "123");
      client.add(doc);
      client.commit();
    }

    Path dataDir = null;
    try (SolrClient client = getHttpSolrClient(runner.getBaseUrl().toString())) {
      final var status = CoreAdminRequest.getCoreStatus("corex", true, client);
      String dataDirectory = status.dataDir;
      dataDir = Path.of(dataDirectory);
      assertTrue(Files.exists(dataDir));
    }

    Path subHome = solrHomeDirectory.resolve("corex").resolve("conf");
    String top = SolrTestCaseJ4.TEST_HOME() + "/collection1/conf";
    Files.copy(
        Path.of(top, "bad-error-solrconfig.xml"),
        subHome.resolve("solrconfig.xml"),
        StandardCopyOption.REPLACE_EXISTING);

    try (SolrClient client =
        new HttpSolrClient.Builder(runner.getBaseUrl().toString())
            .withConnectionTimeout(DEFAULT_CONNECTION_TIMEOUT, TimeUnit.MILLISECONDS)
            .withSocketTimeout(DEFAULT_CONNECTION_TIMEOUT, TimeUnit.MILLISECONDS)
            .build()) {
      // this is expected because we put a bad solrconfig -- ignore
      expectThrows(Exception.class, () -> CoreAdminRequest.reloadCore("corex", client));

      CoreAdminRequest.Unload req = new CoreAdminRequest.Unload(false);
      req.setDeleteDataDir(true);
      // important because the data directory is inside the instance directory
      req.setDeleteInstanceDir(false);
      req.setCoreName("corex");
      req.process(client);
    }

    runner.stop();

    assertTrue(
        "The data directory was not cleaned up on unload after a failed core reload",
        Files.notExists(dataDir));
  }

  @Test
  public void testNonexistentCoreReload() throws Exception {
    final CoreAdminHandler admin = new CoreAdminHandler(h.getCoreContainer());
    SolrQueryResponse resp = new SolrQueryResponse();

    SolrException e =
        expectThrows(
            SolrException.class,
            () -> {
              admin.handleRequestBody(
                  req(
                      CoreAdminParams.ACTION,
                      CoreAdminParams.CoreAdminAction.RELOAD.toString(),
                      CoreAdminParams.CORE,
                      "non-existent-core"),
                  resp);
            });
    assertEquals(
        "Expected error message for non-existent core.",
        "No such core: non-existent-core",
        e.getMessage());

    // test null core
    e =
        expectThrows(
            SolrException.class,
            () -> {
              admin.handleRequestBody(
                  req(CoreAdminParams.ACTION, CoreAdminParams.CoreAdminAction.RELOAD.toString()),
                  resp);
            });
    assertEquals(
        "Expected error message for non-existent core.",
        "Missing required parameter: core",
        e.getMessage());
    admin.close();
  }

  @Test
  public void testTrackedRequestExpiration() throws Exception {
    // Create a tracker with controlled clock, relative to 0
    AtomicLong clock = new AtomicLong(0L);
    CoreAdminAsyncTracker asyncTracker = new CoreAdminAsyncTracker(clock::get, 100L, 10L);
    try {
      Set<TaskObject> tasks =
          Set.of(
              new TaskObject("id1", "ACTION", false, SolrQueryResponse::new),
              new TaskObject("id2", "ACTION", false, SolrQueryResponse::new));

      // Submit all tasks and wait for internal status to be COMPLETED
      tasks.forEach(asyncTracker::submitAsyncTask);
      while (!tasks.stream().allMatch(t -> COMPLETED.equals(t.getStatus()))) {
        Thread.sleep(10L);
      }

      // Timeout for running tasks is 100n, so status can be retrieved after 20n.
      // But timeout for complete tasks is 10n once we polled the status at least once, so status
      // is not available anymore 20n later.
      clock.set(20);
      assertEquals(COMPLETED, asyncTracker.getAsyncRequestForStatus("id1").getStatus());
      clock.set(40L);
      assertNull(asyncTracker.getAsyncRequestForStatus("id1"));

      // Move the clock after the running timeout.
      // Status of second task is not available anymore, even if it wasn't retrieved yet
      clock.set(110L);
      assertNull(asyncTracker.getAsyncRequestForStatus("id2"));

    } finally {
      asyncTracker.shutdown();
    }
  }

  /** Check we reject a task is the async ID already exists. */
  @Test
  public void testDuplicatedRequestId() {

    // Different tasks but with same ID
    TaskObject task1 = new TaskObject("id1", "ACTION", false, null);
    TaskObject task2 = new TaskObject("id1", "ACTION", false, null);

    CoreAdminAsyncTracker asyncTracker = new CoreAdminAsyncTracker();
    try {
      asyncTracker.submitAsyncTask(task1);
      try {
        asyncTracker.submitAsyncTask(task2);
        fail("Task should have been rejected.");
      } catch (SolrException e) {
        assertEquals("Duplicate request with the same requestid found.", e.getMessage());
      }

      assertNotNull(task1.getStatus());
      assertNull(task2.getStatus());
    } finally {
      asyncTracker.shutdown();
    }
  }
}
