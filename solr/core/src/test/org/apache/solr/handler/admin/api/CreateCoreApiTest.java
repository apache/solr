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
package org.apache.solr.handler.admin.api;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.api.model.CreateCoreRequestBody;
import org.apache.solr.common.SolrException;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.admin.CoreAdminHandler;
import org.apache.solr.response.SolrQueryResponse;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/** Test for {@link CreateCore} */
public class CreateCoreApiTest extends SolrTestCaseJ4 {
  private CoreContainer coreContainer;
  private CreateCore createCore;
  private final String CREATE_CORE_NAME = "demo1";
  private Path instDir;
  private CoreAdminHandler coreAdminHandler;

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml", "schema.xml");
  }

  public String getCoreWorkDir() {
    return this.getClass().getName();
  }

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    final Path workDir = createTempDir(getCoreWorkDir());
    instDir = workDir.resolve(CREATE_CORE_NAME);
    Path subHome = instDir.resolve("conf");
    Files.createDirectories(subHome);
    String srcDir = SolrTestCaseJ4.TEST_HOME() + "/collection1/conf";
    Files.copy(Path.of(srcDir, "schema-tiny.xml"), subHome.resolve("schema_ren.xml"));
    Files.copy(Path.of(srcDir, "solrconfig-minimal.xml"), subHome.resolve("solrconfig_ren.xml"));
    Files.copy(
        Path.of(srcDir, "solrconfig.snippet.randomindexconfig.xml"),
        subHome.resolve("solrconfig.snippet.randomindexconfig.xml"));
    final CoreContainer cores = h.getCoreContainer();
    cores.getAllowPaths().add(workDir);
    // cores.getAllowPaths().add(workDir);
    coreAdminHandler = new CoreAdminHandler(cores);
    SolrQueryResponse response = new SolrQueryResponse();
    createCore =
        new CreateCore(
            coreAdminHandler.getCoreContainer(),
            coreAdminHandler.getCoreAdminAsyncTracker(),
            req(),
            response);
  }

  @Test
  public void test_createCore_with_existent_configSet() {
    CreateCoreRequestBody createCoreRequestBody = new CreateCoreRequestBody();
    createCoreRequestBody.name = coreName;
    createCoreRequestBody.configSet = "_default";
    createCoreRequestBody.dataDir = "data_demo";
    createCoreRequestBody.instanceDir = instDir.toAbsolutePath().toString();
    createCore.createCore(createCoreRequestBody, CREATE_CORE_NAME);
    Path dataDir = instDir.resolve("data_demo");
    assertTrue(Files.exists(dataDir));
  }

  @Test
  public void testReportError_createCore_non_existent_configSet() {
    CreateCoreRequestBody createCoreRequestBody = new CreateCoreRequestBody();
    createCoreRequestBody.name = coreName;
    createCoreRequestBody.configSet = "_default_non_existent_configSet";
    createCoreRequestBody.instanceDir = instDir.toAbsolutePath().toString();
    assertThrows(
        "Could not load configuration from directory",
        SolrException.class,
        () -> createCore.createCore(createCoreRequestBody, CREATE_CORE_NAME));
  }

  @Test
  public void testReportError_two_thread_simultaneously_createCore() throws InterruptedException {
    final CreateCoreRequestBody createCoreRequestBody = new CreateCoreRequestBody();
    createCoreRequestBody.name = coreName;
    createCoreRequestBody.instanceDir = instDir.toAbsolutePath().toString();
    createCoreRequestBody.config = "solrconfig_ren.xml";
    createCoreRequestBody.schema = "schema_ren.xml";
    createCoreRequestBody.dataDir = "data_demo";
    final AtomicReference<Exception> exp = new AtomicReference<>();
    final Runnable coreCreationCmd =
        () -> {
          try {
            createCore.createCore(createCoreRequestBody, CREATE_CORE_NAME);
          } catch (Exception e) {
            exp.set(e);
          }
        };

    Thread firstCoreCreationThread = new Thread(coreCreationCmd);
    Thread secCoreCreationThread = new Thread(coreCreationCmd);
    firstCoreCreationThread.start();
    secCoreCreationThread.start();
    firstCoreCreationThread.join();
    secCoreCreationThread.join();
    // Test for failed creation cmd
    assertNotNull("Exception must be thrown as two threads creating core simultaneously", exp);
  }

  @Test
  public void test_buildCoreParams() {
    final var unknownKey = "UNKNOWN_KEY";
    final var configKeyWithBlankVal = "config";

    Map<String, Object> params = Map.of(unknownKey, "TEST_VAL", configKeyWithBlankVal, "");
    Map<String, String> coreParams = CreateCore.buildCoreParams(params);
    assertFalse(coreParams.containsKey("unknownKey"));
    assertFalse(coreParams.containsKey(configKeyWithBlankVal));

    final var configKeyWithNullVal = "config";
    params = new HashMap<>();
    params.put(configKeyWithNullVal, null);

    coreParams = CreateCore.buildCoreParams(params);
    assertFalse(coreParams.containsKey(configKeyWithNullVal));
  }

  @After
  public void close() throws Exception {
    coreAdminHandler.close();
  }
}
