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

import static org.hamcrest.Matchers.startsWith;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.Utils;
import org.apache.solr.util.RestTestHarness;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * This class has multiple test methods that each create the same collection, using the same
 * properties, but differ in how/when they set those properties, and then use {@link
 * #checkCollection} to confirm each collection had the expected final config
 */
public class TestSetPropertyConfigApis extends SolrCloudTestCase {

  // Re-used in each test
  public static String CONFIGSET_NAME = "cloud-minimal-userproperties";

  /**
   * Props that can be re-used in multiple tests that want to make simple assertions
   *
   * @see #checkCollectionUsingSimpleProps
   */
  private static Map<String, String> SIMPLE_PROPS =
      Map.of(
          "my.custom.prop", "foo",
          "custom.echoParams", "all",
          "update.autoCreateFields", "false",
          "solr.commitwithin.softcommit", "false");

  /**
   * For simplicity we use a single replica of a single shard on a single jetty So we know exactly
   * where/who we have to wait on for any reloads
   */
  private static int SINGLE_CORE = 1;

  /**
   * @see #SINGLE_CORE
   * @see SolrCore#close
   */
  private static SolrCore getCore(final String collectionName) {
    final CoreContainer cc = cluster.getRandomJetty(random()).getCoreContainer();
    final CoreDescriptor coreDesc =
        cc.getCoreDescriptors().stream()
            .filter(cd -> collectionName.equals(cd.getCollectionName()))
            .findAny()
            .get();
    return cc.getCore(coreDesc.getName());
  }

  @BeforeClass
  public static void createCluster() throws Exception {
    configureCluster(SINGLE_CORE).addConfig(CONFIGSET_NAME, configset(CONFIGSET_NAME)).configure();
  }

  @After
  public void cleanupAllCollectionsAndConfigSideEffects() throws Exception {
    cluster.deleteAllCollections();

    // parsed config files can be cached in the CoreContainer
    cluster.getRandomJetty(random()).getCoreContainer().getObjectCache().clear();

    // In spite of what the API may say: the Config-API modifies the configset,
    // not the collection -- so we need to ensure no remnants of any changes.
    //
    // https://issues.apache.org/jira/browse/SOLR-17862
    cluster.deleteAllConfigSets();
    cluster.uploadConfigSet(configset(CONFIGSET_NAME), CONFIGSET_NAME);
  }

  /**
   * @see #checkCollection
   * @see #SIMPLE_PROPS
   */
  private void checkCollectionUsingSimpleProps(final String collectionName) throws Exception {
    checkCollection(collectionName, true, "foo", false, false);
  }

  /**
   * Makes some very targeted assertions about a collection created with {@link #CONFIGSET_NAME}
   *
   * @see #checkCollectionUsingSimpleProps
   */
  private void checkCollection(
      final String collectionName,
      final boolean echoAllParams,
      final String customProp,
      final boolean commitWithinSoft,
      final boolean customChainIsDefault)
      throws Exception {

    // Check the actual params used by the /select handler
    final NamedList<Object> header =
        cluster.getSolrClient().query(collectionName, params("q", "*:*")).getHeader();

    @SuppressWarnings("unchecked")
    final NamedList<String> params = (NamedList<String>) header.get("params");

    assertEquals(
        collectionName + " echoParams=all?", echoAllParams, "all".equals(params.get("echoParams")));
    if (echoAllParams) {
      assertThat(
          params.get("dummy1"),
          // Solr assigned implicit property
          startsWith(collectionName));
      // this tests that both keys and values were substituted correctly
      assertEquals(collectionName + " customPropKey?", "custom-key-value", params.get(customProp));
      assertEquals(collectionName + " customPropValue?", customProp, params.get("dummy2"));
    }

    // Introspect the update handler & processors of a random core of our collection.
    try (SolrCore core = getCore(collectionName)) {
      assertNotNull(core);

      assertEquals(
          collectionName + " commitWithinSoft?",
          commitWithinSoft,
          core.getSolrConfig().getUpdateHandlerInfo().commitWithinSoftCommit);

      assertNotNull(
          collectionName + " customChainName exists?", core.getUpdateProcessingChain(customProp));

      assertEquals(
          collectionName + " customChainNameIsDefault?",
          customChainIsDefault,
          core.getUpdateProcessingChain(customProp).equals(core.getUpdateProcessingChain(null)));
    }
  }

  @Test
  public void testAllSystemProperties() throws Exception {
    final String collectionName = getSaferTestName();
    SIMPLE_PROPS.entrySet().stream().forEach(e -> System.setProperty(e.getKey(), e.getValue()));

    processAndAssertSuccess(
        CollectionAdminRequest.createCollection(
            collectionName, CONFIGSET_NAME, SINGLE_CORE, SINGLE_CORE));

    cluster.waitForActiveCollection(collectionName, SINGLE_CORE, SINGLE_CORE);

    checkCollectionUsingSimpleProps(collectionName);
  }

  @Test
  public void testAllCollectionCreationProperties() throws Exception {
    final String collectionName = getSaferTestName();

    final CollectionAdminRequest.Create op =
        CollectionAdminRequest.createCollection(
            collectionName, CONFIGSET_NAME, SINGLE_CORE, SINGLE_CORE);
    SIMPLE_PROPS.entrySet().stream().forEach(e -> op.withProperty(e.getKey(), e.getValue()));
    processAndAssertSuccess(op);

    cluster.waitForActiveCollection(collectionName, SINGLE_CORE, SINGLE_CORE);

    checkCollectionUsingSimpleProps(collectionName);
  }

  @Test
  public void testConfigApiAfterCollectionCreation() throws Exception {
    final String collectionName = getSaferTestName();
    assertNull(System.getProperty("my.custom.prop"));

    // We set this one system property, to a value that is garunteed to *NOT* match any
    // of our expectations, before creating the collection, to ensure collection
    // creation options take precendent over system properties
    final String bogusInitialProp = "BOGUS__" + SIMPLE_PROPS.get("my.custom.prop");
    System.setProperty("my.custom.prop", bogusInitialProp);

    processAndAssertSuccess(
        CollectionAdminRequest.createCollection(
                collectionName, CONFIGSET_NAME, SINGLE_CORE, SINGLE_CORE)
            // This should take precedence over the system property
            // (and over any Config-API set value later)
            .withProperty("my.custom.prop", "qqq"));

    cluster.waitForActiveCollection(collectionName, SINGLE_CORE, SINGLE_CORE);

    // Check that our initial collection matches our special startup situation
    // (most of these come from the solrconfig.xml defaults since the props aren't set)
    checkCollection(collectionName, false, "qqq", true, true);

    // Now use the Config API to set *ALL* of the properties to their expected
    // "simple" values and check that our collection is in good shape
    // NOTE: custom 'qqq' should still override 'foo'
    setUserPropertiesAndWaitForReload(collectionName, SIMPLE_PROPS);
    checkCollection(collectionName, true, "qqq", false, false);
  }

  @Test
  public void testTwoCollectionsWithDifferentProps() throws Exception {
    final String collectionX = getSaferTestName() + "_x";
    final String collectionY = getSaferTestName() + "_y";

    // first create both collections

    processAndAssertSuccess(
        CollectionAdminRequest.createCollection(
                collectionX, CONFIGSET_NAME, SINGLE_CORE, SINGLE_CORE)
            .withProperty("my.custom.prop", "xxx")
            .withProperty("custom.echoParams", "all"));

    processAndAssertSuccess(
        CollectionAdminRequest.createCollection(
                collectionY, CONFIGSET_NAME, SINGLE_CORE, SINGLE_CORE)
            .withProperty("my.custom.prop", "yyy")
            .withProperty("solr.commitwithin.softcommit", "true"));

    cluster.waitForActiveCollection(collectionX, SINGLE_CORE, SINGLE_CORE);
    cluster.waitForActiveCollection(collectionY, SINGLE_CORE, SINGLE_CORE);

    // now check both collections against the props we expected (or their defaults)
    checkCollection(collectionX, true, "xxx", true, true);
    checkCollection(collectionY, false, "yyy", true, true);

    // In spite of what the API may say: the Config-API modifies the configset,
    // not the collection.  So if we modify the user-properties of Y,
    // we should see those changes in X as well...
    //
    // https://issues.apache.org/jira/browse/SOLR-17862
    //
    // ... EXCEPT!!! ... collection creation props take precedence over
    // configset props.
    final CountDownLatch firstReloadOfXandY = createSolrCoreCloseLatch(collectionX, collectionY);
    setUserProperties(
        collectionY,
        Map.of(
            "my.custom.prop", "zzz",
            "custom.echoParams", "all",
            "solr.commitwithin.softcommit", "false",
            "update.autoCreateFields", "false"));
    assertTrue(
        "Gave up after waiting an excessive amount of time for both collections to reload (first time)",
        firstReloadOfXandY.await(60, TimeUnit.SECONDS));

    checkCollection(collectionX, true, "xxx", false, false);
    checkCollection(collectionY, true, "yyy", true, false);

    // modify config (via "X" this time) and check *both* collections again
    final CountDownLatch secondReloadOfXandY = createSolrCoreCloseLatch(collectionX, collectionY);
    setUserProperties(
        collectionX,
        Map.of(
            "custom.echoParams", "explicit",
            "solr.commitwithin.softcommit", "true",
            "update.autoCreateFields", "true"));
    assertTrue(
        "Gave up after waiting an excessive amount of time for both collections to reload (again)",
        secondReloadOfXandY.await(60, TimeUnit.SECONDS));
    checkCollection(collectionX, true, "xxx", true, true);
    checkCollection(collectionY, false, "yyy", true, true);
  }

  private static void processAndAssertSuccess(final CollectionAdminRequest.Create op)
      throws Exception {
    op.setWaitForFinalState(true);
    assertTrue(op.process(cluster.getSolrClient()).isSuccess());
  }

  /**
   * Use the Config API to set *ALL* of the properties to their expected values via a single REST
   * command.
   */
  private static void setUserProperties(
      final String collectionName, final Map<String, String> props) throws Exception {

    try (RestTestHarness harness = makeRestHarness(collectionName)) {
      final String cmd =
          "{ 'set-user-property' : { "
              + props.entrySet().stream()
                  .map(e -> "'" + e.getKey() + "':'" + e.getValue() + "'")
                  .collect(Collectors.joining(","))
              + "}} ";
      runConfigCommand(harness, cmd);
    }
  }

  /**
   * Use the Config API to set *ALL* of the properties to their expected values via a single REST
   * command and wait for the core reload
   *
   * @see #SINGLE_CORE
   */
  private static void setUserPropertiesAndWaitForReload(
      final String collectionName, final Map<String, String> props) throws Exception {

    final CountDownLatch reloadLatch = createSolrCoreCloseLatch(collectionName);

    setUserProperties(collectionName, props);
    assertTrue(
        "Gave up after waiting an excessive amount of time for the core reload",
        reloadLatch.await(60, TimeUnit.SECONDS));
  }

  /**
   * Given some collection names, registers a {@link CloseHook} on the current core of each
   * collection, and returns a CountDownLatch that will fire when all of those cores have closed
   * (ie: wait for reload or shutdown)
   *
   * @see #SINGLE_CORE
   * @see SolrCore#addCloseHook
   */
  private static final CountDownLatch createSolrCoreCloseLatch(final String... collectionNames) {
    final CountDownLatch latch = new CountDownLatch(collectionNames.length);
    for (final String name : collectionNames) {
      try (SolrCore core = getCore(name)) {
        assertNotNull(core);

        core.addCloseHook(
            new CloseHook() {
              @Override
              public void postClose(SolrCore core) {
                latch.countDown();
              }
            });
      }
    }
    return latch;
  }

  private static RestTestHarness makeRestHarness(final String collectionName) {
    return new RestTestHarness(
        () -> cluster.getRandomJetty(random()).getBaseUrl().toString() + "/" + collectionName);
  }

  private static void runConfigCommand(RestTestHarness harness, String json) throws IOException {
    String response = harness.post("/config", json);
    Map<?, ?> map = (Map<?, ?>) Utils.fromJSONString(response);
    assertNull(response, map.get("errorMessages"));
    assertNull(response, map.get("errors"));
  }
}
