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
package org.apache.solr.ltr;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.file.PathUtils;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.ltr.feature.Feature;
import org.apache.solr.ltr.feature.FeatureException;
import org.apache.solr.ltr.feature.ValueFeature;
import org.apache.solr.ltr.model.LTRScoringModel;
import org.apache.solr.ltr.model.ModelException;
import org.apache.solr.ltr.store.FeatureStore;
import org.apache.solr.ltr.store.rest.ManagedFeatureStore;
import org.apache.solr.ltr.store.rest.ManagedModelStore;
import org.apache.solr.util.RestTestBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestRerankBase extends RestTestBase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  protected static final SolrResourceLoader solrResourceLoader =
      new SolrResourceLoader(Path.of("").toAbsolutePath());

  protected static Path tmpSolrHome;
  protected static Path tmpConfDir;

  public static final String FEATURE_FILE_NAME = "_schema_feature-store.json";
  public static final String MODEL_FILE_NAME = "_schema_model-store.json";
  public static final String PARENT_ENDPOINT = "/schema/*";

  protected static final String COLLECTION = "collection1";
  protected static final String CONF_DIR = COLLECTION + "/conf";

  protected static Path fstorefile = null;
  protected static Path mstorefile = null;

  private static final String SYSTEM_PROPERTY_SOLR_LTR_TRANSFORMER_FV_DEFAULTFORMAT =
      "solr.ltr.transformer.fv.defaultFormat";
  private static String defaultFeatureFormat;

  protected String chooseDefaultFeatureVector(String dense, String sparse) {
    if (defaultFeatureFormat == null) {
      // to match <code><str
      // name="defaultFormat">${solr.ltr.transformer.fv.defaultFormat:dense}</str></code> snippet
      return dense;
    } else if ("dense".equals(defaultFeatureFormat)) {
      return dense;
    } else if ("sparse".equals(defaultFeatureFormat)) {
      return sparse;
    } else {
      fail("unexpected feature format choice: " + defaultFeatureFormat);
      return null;
    }
  }

  protected static void chooseDefaultFeatureFormat() throws Exception {
    switch (random().nextInt(3)) {
      case 0:
        defaultFeatureFormat = null;
        break;
      case 1:
        defaultFeatureFormat = "dense";
        break;
      case 2:
        defaultFeatureFormat = "sparse";
        break;
      default:
        fail("unexpected feature format choice");
        break;
    }
    if (defaultFeatureFormat != null) {
      System.setProperty(
          SYSTEM_PROPERTY_SOLR_LTR_TRANSFORMER_FV_DEFAULTFORMAT, defaultFeatureFormat);
    }
  }

  protected static void unchooseDefaultFeatureFormat() {
    System.clearProperty(SYSTEM_PROPERTY_SOLR_LTR_TRANSFORMER_FV_DEFAULTFORMAT);
  }

  protected static void setuptest(boolean bulkIndex) throws Exception {
    chooseDefaultFeatureFormat();
    setuptest("solrconfig-ltr.xml", "schema.xml");
    if (bulkIndex) bulkIndex();
  }

  protected static void setupPersistenttest(boolean bulkIndex) throws Exception {
    chooseDefaultFeatureFormat();
    setupPersistentTest("solrconfig-ltr.xml", "schema.xml");
    if (bulkIndex) bulkIndex();
  }

  public static ManagedFeatureStore getManagedFeatureStore() {
    try (SolrCore core = solrClientTestRule.getCoreContainer().getCore(DEFAULT_TEST_CORENAME)) {
      return ManagedFeatureStore.getManagedFeatureStore(core);
    }
  }

  public static ManagedModelStore getManagedModelStore() {
    try (SolrCore core = solrClientTestRule.getCoreContainer().getCore(DEFAULT_TEST_CORENAME)) {
      return ManagedModelStore.getManagedModelStore(core);
    }
  }

  protected static void setupTestInit(String solrconfig, String schema, boolean isPersistent)
      throws Exception {
    tmpSolrHome = createTempDir();
    tmpConfDir = tmpSolrHome.resolve(CONF_DIR);
    PathUtils.copyDirectory(TEST_PATH(), tmpSolrHome);

    final Path fstore = tmpConfDir.resolve(FEATURE_FILE_NAME);
    final Path mstore = tmpConfDir.resolve(MODEL_FILE_NAME);

    if (isPersistent) {
      fstorefile = fstore;
      mstorefile = mstore;
    }

    if (Files.exists(fstore)) {
      if (log.isInfoEnabled()) {
        log.info("remove feature store config file in {}", fstore);
      }
      Files.delete(fstore);
    }
    if (Files.exists(mstore)) {
      if (log.isInfoEnabled()) {
        log.info("remove model store config file in {}", mstore);
      }
      Files.delete(mstore);
    }
    if (!solrconfig.equals("solrconfig.xml")) {
      Files.copy(
          tmpSolrHome.resolve(CONF_DIR).resolve(solrconfig),
          tmpSolrHome.resolve(CONF_DIR).resolve("solrconfig.xml"));
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

    createJettyAndHarness(tmpSolrHome, solrconfig, schema, "/solr", true, null);
  }

  public static void setupPersistentTest(String solrconfig, String schema) throws Exception {

    setupTestInit(solrconfig, schema, true);

    createJettyAndHarness(tmpSolrHome, solrconfig, schema, "/solr", true, null);
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
    // System.clearProperty("enable.update.log");
    unchooseDefaultFeatureFormat();
  }

  public static void makeRestTestHarnessNull() {
    restTestHarness = null;
  }

  /** produces a model encoded in json * */
  public static String getModelInJson(
      String name, String type, String[] features, String fstore, String params) {
    final StringBuilder sb = new StringBuilder();
    sb.append("{\n");
    sb.append("\"name\":").append('"').append(name).append('"').append(",\n");
    sb.append("\"store\":").append('"').append(fstore).append('"').append(",\n");
    sb.append("\"class\":").append('"').append(type).append('"').append(",\n");
    sb.append("\"features\":").append('[');
    if (features.length > 0) {
      for (final String feature : features) {
        sb.append("\n\t{ ");
        sb.append("\"name\":").append('"').append(feature).append('"').append("},");
      }
      sb.deleteCharAt(sb.length() - 1);
    }
    sb.append("\n]\n");
    if (params != null) {
      sb.append(",\n");
      sb.append("\"params\":").append(params);
    }
    sb.append("\n}\n");
    return sb.toString();
  }

  /** produces a model encoded in json * */
  public static String getFeatureInJson(String name, String type, String fstore, String params) {
    final StringBuilder sb = new StringBuilder();
    sb.append("{\n");
    sb.append("\"name\":").append('"').append(name).append('"').append(",\n");
    sb.append("\"store\":").append('"').append(fstore).append('"').append(",\n");
    sb.append("\"class\":").append('"').append(type).append('"');
    if (params != null) {
      sb.append(",\n");
      sb.append("\"params\":").append(params);
    }
    sb.append("\n}\n");
    return sb.toString();
  }

  /** Load a feature from the test fstore and verify that it succeeded */
  protected static void loadFeature(String name, String type, String params) throws IOException {
    loadFeature(name, type, "test", params, 0);
  }

  /** Load a feature and expect a given status code (i.e. testing for failure) */
  protected static void loadFeature(String name, String type, String params, int responseCode)
      throws IOException {
    loadFeature(name, type, "test", params, responseCode);
  }

  /** Load a feature from a custom fstore and verify that it succeeded */
  protected static void loadFeature(String name, String type, String fstore, String params)
      throws Exception {
    loadFeature(name, type, fstore, params, 0);
  }

  /**
   * Load a feature from a custom fstore and expect a given status code (i.e. testing for failure)
   */
  protected static void loadFeature(
      String name, String type, String fstore, String params, int responseCode) throws IOException {
    final String feature = getFeatureInJson(name, type, fstore, params);
    log.info("loading feauture \n{} ", feature);
    assertJPut(
        ManagedFeatureStore.REST_END_POINT, feature, "/responseHeader/status==" + responseCode);
  }

  protected static void loadModel(String name, String type, String[] features, String params)
      throws Exception {
    loadModel(name, type, features, "test", params);
  }

  protected static void loadModel(
      String name, String type, String[] features, String fstore, String params) throws Exception {
    final String model = getModelInJson(name, type, features, fstore, params);
    log.info("loading model \n{} ", model);
    assertJPut(ManagedModelStore.REST_END_POINT, model, "/responseHeader/status==0");
  }

  public static void loadModels(String fileName) throws Exception {
    final URL url = TestRerankBase.class.getResource("/modelExamples/" + fileName);
    final String multipleModels = Files.readString(Path.of(url.toURI()), StandardCharsets.UTF_8);

    assertJPut(ManagedModelStore.REST_END_POINT, multipleModels, "/responseHeader/status==0");
  }

  public static LTRScoringModel createModelFromFiles(String modelFileName, String featureFileName)
      throws ModelException, Exception {
    return createModelFromFiles(
        modelFileName, featureFileName, FeatureStore.DEFAULT_FEATURE_STORE_NAME);
  }

  public static LTRScoringModel createModelFromFiles(
      String modelFileName, String featureFileName, String featureStoreName)
      throws ModelException, Exception {
    URL url = TestRerankBase.class.getResource("/modelExamples/" + modelFileName);
    final String modelJson = Files.readString(Path.of(url.toURI()), StandardCharsets.UTF_8);
    final ManagedModelStore ms = getManagedModelStore();

    url = TestRerankBase.class.getResource("/featureExamples/" + featureFileName);
    final String featureJson = Files.readString(Path.of(url.toURI()), StandardCharsets.UTF_8);

    Object parsedFeatureJson = null;
    try {
      parsedFeatureJson = Utils.fromJSONString(featureJson);
    } catch (final Exception ioExc) {
      throw new ModelException("ObjectBuilder failed parsing json", ioExc);
    }

    final ManagedFeatureStore fs = getManagedFeatureStore();
    // fs.getFeatureStore(null).clear();
    fs.doDeleteChild(null, featureStoreName); // is this safe??
    // based on my need to call this I dont think that
    // "getNewManagedFeatureStore()"
    // is actually returning a new feature store each time
    fs.applyUpdatesToManagedData(parsedFeatureJson);
    ms.setManagedFeatureStore(fs); // can we skip this and just use fs directly below?

    final LTRScoringModel ltrScoringModel =
        ManagedModelStore.fromLTRScoringModelMap(
            solrResourceLoader, mapFromJson(modelJson), ms.getManagedFeatureStore());
    ms.addModel(ltrScoringModel);
    return ltrScoringModel;
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> mapFromJson(String json) throws ModelException {
    Object parsedJson = null;
    try {
      parsedJson = Utils.fromJSONString(json);
    } catch (final Exception ioExc) {
      throw new ModelException("ObjectBuilder failed parsing json", ioExc);
    }
    return (Map<String, Object>) parsedJson;
  }

  public static void loadFeatures(String fileName) throws Exception {
    final URL url = TestRerankBase.class.getResource("/featureExamples/" + fileName);
    final String multipleFeatures = Files.readString(Path.of(url.toURI()), StandardCharsets.UTF_8);
    log.info("send \n{}", multipleFeatures);

    assertJPut(ManagedFeatureStore.REST_END_POINT, multipleFeatures, "/responseHeader/status==0");
  }

  protected List<Feature> getFeatures(List<String> names) throws FeatureException {
    final List<Feature> features = new ArrayList<>();
    int pos = 0;
    for (final String name : names) {
      final Map<String, Object> params = new HashMap<String, Object>();
      params.put("value", 10);
      final Feature f =
          Feature.getInstance(
              solrResourceLoader, ValueFeature.class.getCanonicalName(), name, params);
      f.setIndex(pos);
      features.add(f);
      ++pos;
    }
    return features;
  }

  protected List<Feature> getFeatures(String[] names) throws FeatureException {
    return getFeatures(Arrays.asList(names));
  }

  protected static void bulkIndex() throws Exception {
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

  protected static void doTestParamsToMap(
      String featureClassName, LinkedHashMap<String, Object> featureParams) throws Exception {

    // start with default parameters
    final LinkedHashMap<String, Object> paramsA = new LinkedHashMap<String, Object>();
    final Object defaultValue;
    switch (random().nextInt(6)) {
      case 0:
        defaultValue = null;
        break;
      case 1:
        defaultValue = "1.2";
        break;
      case 2:
        defaultValue = Double.valueOf(3.4d);
        break;
      case 3:
        defaultValue = Float.valueOf(0.5f);
        break;
      case 4:
        defaultValue = Integer.valueOf(67);
        break;
      case 5:
        defaultValue = Long.valueOf(89);
        break;
      default:
        defaultValue = null;
        fail("unexpected defaultValue choice");
        break;
    }
    if (defaultValue != null) {
      paramsA.put("defaultValue", defaultValue);
    }

    // then add specific parameters
    paramsA.putAll(featureParams);

    // next choose a random feature name
    final String featureName = "randomFeatureName" + random().nextInt(10);

    // create a feature from the parameters
    final Feature featureA =
        Feature.getInstance(solrResourceLoader, featureClassName, featureName, paramsA);

    // turn the feature back into parameters
    final LinkedHashMap<String, Object> paramsB = featureA.paramsToMap();

    // create feature B from feature A's parameters
    final Feature featureB =
        Feature.getInstance(solrResourceLoader, featureClassName, featureName, paramsB);

    // check that feature A and feature B are identical
    assertEquals(featureA, featureB);
  }
}
