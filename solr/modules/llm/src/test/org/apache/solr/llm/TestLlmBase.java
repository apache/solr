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
package org.apache.solr.llm;

import org.apache.commons.io.file.PathUtils;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.llm.embedding.EmbeddingModel;
import org.apache.solr.llm.store.EmbeddingModelException;
import org.apache.solr.llm.store.rest.ManagedEmbeddingModelStore;
import org.apache.solr.util.RestTestBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

public class TestLlmBase extends RestTestBase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  protected static final SolrResourceLoader solrResourceLoader =
      new SolrResourceLoader(Path.of("").toAbsolutePath());

  protected static Path tmpSolrHome;
  protected static Path tmpConfDir;

  public static final String MODEL_FILE_NAME = "_schema_embedding-model-store.json";
  protected static final String COLLECTION = "collection1";
  protected static final String CONF_DIR = COLLECTION + "/conf";

  protected static Path mstorefile = null;


  protected static void setuptest(boolean bulkIndex) throws Exception {
    setuptest("solrconfig-llm.xml", "schema.xml");
    if (bulkIndex) prepareIndex();
  }

  protected static void setupPersistenttest(boolean bulkIndex) throws Exception {
    setupPersistentTest("solrconfig-llm.xml", "schema.xml");
    if (bulkIndex) prepareIndex();
  }

  public static ManagedEmbeddingModelStore getManagedModelStore() {
    try (SolrCore core = solrClientTestRule.getCoreContainer().getCore(DEFAULT_TEST_CORENAME)) {
      return ManagedEmbeddingModelStore.getManagedModelStore(core);
    }
  }

  protected static void setupTestInit(String solrconfig, String schema, boolean isPersistent)
      throws Exception {
    tmpSolrHome = createTempDir();
    tmpConfDir = tmpSolrHome.resolve(CONF_DIR);
    tmpConfDir.toFile().deleteOnExit();
    PathUtils.copyDirectory(TEST_PATH(), tmpSolrHome.toAbsolutePath());

    final Path mstore = tmpConfDir.resolve(MODEL_FILE_NAME);

    if (isPersistent) {
      mstorefile = mstore;
    }
    
    if (Files.exists(mstore)) {
      if (log.isInfoEnabled()) {
        log.info("remove model store config file in {}", mstore.toAbsolutePath());
      }
      Files.delete(mstore);
    }
    if (!solrconfig.equals("solrconfig-llm.xml")) {
      Files.copy(
          tmpSolrHome.resolve(CONF_DIR).resolve(solrconfig),
          tmpSolrHome.resolve(CONF_DIR).resolve("solrconfig-llm.xml"));
    }
    if (!schema.equals("schema.xml")) {
      Files.copy(
          tmpSolrHome.resolve(CONF_DIR).resolve(schema),
          tmpSolrHome.resolve(CONF_DIR).resolve("schema.xml"));
    }

    System.setProperty("managed.schema.mutable", "true");
  }

  public static void setuptest(String solrconfig, String schema) throws Exception {

    setupTestInit(solrconfig, schema, false);
    System.setProperty("enable.update.log", "false");

    createJettyAndHarness(
        tmpSolrHome.toAbsolutePath().toString(), solrconfig, schema, "/solr", true, null);
  }

  public static void setupPersistentTest(String solrconfig, String schema) throws Exception {

    setupTestInit(solrconfig, schema, true);

    createJettyAndHarness(
        tmpSolrHome.toAbsolutePath().toString(), solrconfig, schema, "/solr", true, null);
  }

  protected static void aftertest() throws Exception {
    if (null != restTestHarness) {
      restTestHarness.close();
      restTestHarness = null;
    }
    solrClientTestRule.reset();
    if (null != tmpSolrHome) {
      PathUtils.deleteDirectory(tmpSolrHome);
      tmpSolrHome = null;
    }
    System.clearProperty("managed.schema.mutable");
  }

  public static void makeRestTestHarnessNull() {
    restTestHarness = null;
  }

  /** produces a model encoded in json * */
  public static String getModelInJson(
      String name, String className, String params) {
    final StringBuilder sb = new StringBuilder();
    sb.append("{\n");
    sb.append("\"name\":").append('"').append(name).append('"').append(",\n");
    sb.append("\"class\":").append('"').append(className).append('"').append(",\n");
    if (params != null) {
      sb.append(",\n");
      sb.append("\"params\":").append(params);
    }
    sb.append("\n}\n");
    return sb.toString();
  }

  protected static void loadModel(
      String name, String className, String params) throws Exception {
    final String model = getModelInJson(name, className, params);
    log.info("loading model \n{} ", model);
    assertJPut(ManagedEmbeddingModelStore.REST_END_POINT, model, "/responseHeader/status==0");
  }

  public static void loadModels(String fileName) throws Exception {
    final URL url = TestLlmBase.class.getResource("/modelExamples/" + fileName);
    final String multipleModels = Files.readString(Path.of(url.toURI()), StandardCharsets.UTF_8);

    assertJPut(ManagedEmbeddingModelStore.REST_END_POINT, multipleModels, "/responseHeader/status==0");
  }

  public static EmbeddingModel createModelFromFiles(String modelFileName, String featureFileName)
      throws EmbeddingModelException, Exception {
    return createModelFromFiles(
        modelFileName, featureFileName);
  }

  public static EmbeddingModel createModelFromFiles(
      String modelFileName)
      throws Exception {
    URL url = TestLlmBase.class.getResource("/modelExamples/" + modelFileName);
    final String modelJson = Files.readString(Path.of(url.toURI()), StandardCharsets.UTF_8);
    final ManagedEmbeddingModelStore ms = getManagedModelStore();

    final EmbeddingModel model =
        ManagedEmbeddingModelStore.fromEmbeddingModelMap(
            solrResourceLoader, mapFromJson(modelJson));
    ms.addModel(model);
    return model;
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> mapFromJson(String json) throws EmbeddingModelException {
    Object parsedJson = null;
    try {
      parsedJson = Utils.fromJSONString(json);
    } catch (final Exception ioExc) {
      throw new EmbeddingModelException("ObjectBuilder failed parsing json", ioExc);
    }
    return (Map<String, Object>) parsedJson;
  }


  protected static void prepareIndex() throws Exception {
    assertU(
        adoc(
            "title",
            "bloomberg different bla",
            "description",
            "bloomberg",
            "id",
            "6",
            "popularity",
            "1"));
    assertU(
        adoc(
            "title",
            "bloomberg bloomberg ",
            "description",
            "bloomberg",
            "id",
            "7",
            "popularity",
            "2"));
    assertU(
        adoc(
            "title",
            "bloomberg bloomberg bloomberg",
            "description",
            "bloomberg",
            "id",
            "8",
            "popularity",
            "3"));
    assertU(
        adoc(
            "title",
            "bloomberg bloomberg bloomberg bloomberg",
            "description",
            "bloomberg",
            "id",
            "9",
            "popularity",
            "5"));
    assertU(commit());
  }
}
