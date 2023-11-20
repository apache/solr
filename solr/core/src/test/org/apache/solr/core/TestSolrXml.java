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

import static org.hamcrest.core.StringContains.containsString;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.exec.OS;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.search.CacheConfig;
import org.apache.solr.search.CaffeineCache;
import org.apache.solr.search.SolrCache;
import org.apache.solr.search.TestThinCache;
import org.apache.solr.search.ThinCache;
import org.apache.solr.update.UpdateShardHandlerConfig;
import org.hamcrest.MatcherAssert;
import org.junit.Before;
import org.junit.Ignore;

public class TestSolrXml extends SolrTestCaseJ4 {
  // tmp dir, cleaned up automatically.
  private Path solrHome;

  @Before
  public void doBefore() {
    solrHome = createTempDir();
  }

  public void testAllInfoPresent() throws IOException {
    Path testSrcRoot = TEST_PATH();
    Files.copy(testSrcRoot.resolve("solr-50-all.xml"), solrHome.resolve("solr.xml"));

    System.setProperty(
        "solr.allowPaths", OS.isFamilyWindows() ? "C:\\tmp,C:\\home\\john" : "/tmp,/home/john");
    NodeConfig cfg = SolrXmlConfig.fromSolrHome(solrHome, new Properties());
    CloudConfig ccfg = cfg.getCloudConfig();
    UpdateShardHandlerConfig ucfg = cfg.getUpdateShardHandlerConfig();
    PluginInfo[] backupRepoConfigs = cfg.getBackupRepositoryPlugins();

    assertEquals("maxBooleanClauses", (Integer) 42, cfg.getBooleanQueryMaxClauseCount());
    assertEquals("core admin handler class", "testAdminHandler", cfg.getCoreAdminHandlerClass());
    assertEquals(
        "core admin handler actions",
        Map.of(
            "action1",
            "testCoreAdminHandlerAction1",
            "action2",
            "testCoreAdminHandlerAction2",
            "action3",
            "testCoreAdminHandlerAction3"),
        cfg.getCoreAdminHandlerActions());
    assertEquals(
        "collection handler class", "testCollectionsHandler", cfg.getCollectionsHandlerClass());
    assertEquals("info handler class", "testInfoHandler", cfg.getInfoHandlerClass());
    assertEquals(
        "config set handler class", "testConfigSetsHandler", cfg.getConfigSetsHandlerClass());
    assertEquals("cores locator class", "testCoresLocator", cfg.getCoresLocatorClass());
    assertEquals("core load threads", 11, cfg.getCoreLoadThreadCount(false));
    assertEquals("replay update threads", 100, cfg.getReplayUpdatesThreads());
    MatcherAssert.assertThat(
        "core root dir",
        cfg.getCoreRootDirectory().toString(),
        containsString("testCoreRootDirectory"));
    assertEquals(
        "distrib conn timeout",
        22,
        cfg.getUpdateShardHandlerConfig().getDistributedConnectionTimeout());
    assertEquals(
        "distrib socket timeout",
        33,
        cfg.getUpdateShardHandlerConfig().getDistributedSocketTimeout());
    assertEquals("max update conn", 3, cfg.getUpdateShardHandlerConfig().getMaxUpdateConnections());
    assertEquals(
        "max update conn/host",
        37,
        cfg.getUpdateShardHandlerConfig().getMaxUpdateConnectionsPerHost());
    assertEquals("distrib conn timeout", 22, ucfg.getDistributedConnectionTimeout());
    assertEquals("distrib socket timeout", 33, ucfg.getDistributedSocketTimeout());
    assertEquals("max update conn", 3, ucfg.getMaxUpdateConnections());
    assertEquals("max update conn/host", 37, ucfg.getMaxUpdateConnectionsPerHost());
    assertEquals("host", "testHost", ccfg.getHost());
    assertEquals("zk host context", "testHostContext", ccfg.getSolrHostContext());
    assertEquals("solr host port", 44, ccfg.getSolrHostPort());
    assertEquals("leader vote wait", 55, ccfg.getLeaderVoteWait());
    assertEquals("logging class", "testLoggingClass", cfg.getLogWatcherConfig().getLoggingClass());
    assertTrue("log watcher", cfg.getLogWatcherConfig().isEnabled());
    assertEquals("log watcher size", 88, cfg.getLogWatcherConfig().getWatcherSize());
    assertEquals("log watcher thresh", "99", cfg.getLogWatcherConfig().getWatcherThreshold());
    assertEquals("manage path", "testManagementPath", cfg.getManagementPath());
    assertEquals("shardLib", "testSharedLib", cfg.getSharedLibDirectory());
    assertTrue("schema cache", cfg.hasSchemaCache());
    assertEquals("trans cache size", 66, cfg.getTransientCacheSize());
    assertEquals("zk client timeout", 77, ccfg.getZkClientTimeout());
    assertEquals("zk host", "testZkHost", ccfg.getZkHost());
    assertEquals("zk ACL provider", "DefaultZkACLProvider", ccfg.getZkACLProviderClass());
    assertEquals(
        "zk credentials provider",
        "DefaultZkCredentialsProvider",
        ccfg.getZkCredentialsProviderClass());
    assertEquals(1, backupRepoConfigs.length);
    assertEquals("local", backupRepoConfigs[0].name);
    assertEquals("a.b.C", backupRepoConfigs[0].className);
    assertEquals("true", backupRepoConfigs[0].attributes.get("default"));
    assertEquals(0, backupRepoConfigs[0].initArgs.size());
    assertTrue(
        "allowPaths",
        cfg.getAllowPaths()
            .containsAll(
                OS.isFamilyWindows()
                    ? Set.of("C:\\tmp", "C:\\home\\john").stream()
                        .map(s -> Path.of(s))
                        .collect(Collectors.toSet())
                    : Set.of("/tmp", "/home/john").stream()
                        .map(s -> Path.of(s))
                        .collect(Collectors.toSet())));
    assertTrue("hideStackTrace", cfg.hideStackTraces());
    System.clearProperty("solr.allowPaths");

    PluginInfo replicaPlacementFactoryConfig = cfg.getReplicaPlacementFactoryConfig();
    assertEquals(
        "org.apache.solr.cluster.placement.plugins.AffinityPlacementFactory",
        replicaPlacementFactoryConfig.className);
    assertEquals(1, replicaPlacementFactoryConfig.initArgs.size());
    assertEquals(10, replicaPlacementFactoryConfig.initArgs.get("minimalFreeDiskGB"));
  }

  // Test  a few property substitutions that happen to be in solr-50-all.xml.
  public void testPropertySub() throws IOException {

    System.setProperty("coreRootDirectory", "myCoreRoot" + File.separator);
    System.setProperty("hostPort", "8888");
    System.setProperty("shareSchema", "false");
    System.setProperty("socketTimeout", "220");
    System.setProperty("connTimeout", "200");

    Path testSrcRoot = TEST_PATH();
    Files.copy(testSrcRoot.resolve("solr-50-all.xml"), solrHome.resolve("solr.xml"));

    NodeConfig cfg = SolrXmlConfig.fromSolrHome(solrHome, new Properties());
    MatcherAssert.assertThat(cfg.getCoreRootDirectory().toString(), containsString("myCoreRoot"));
    assertEquals("solr host port", 8888, cfg.getCloudConfig().getSolrHostPort());
    assertFalse("schema cache", cfg.hasSchemaCache());
  }

  public void testNodeLevelCache() {
    NodeConfig cfg =
        SolrXmlConfig.fromString(createTempDir(), TestThinCache.SOLR_NODE_LEVEL_CACHE_XML);
    Map<String, CacheConfig> cachesConfig = cfg.getCachesConfig();
    SolrCache<?, ?> nodeLevelCache = cachesConfig.get("myNodeLevelCache").newInstance();
    assertTrue(nodeLevelCache instanceof CaffeineCache);
    SolrCache<?, ?> nodeLevelCacheThin = cachesConfig.get("myNodeLevelCacheThin").newInstance();
    assertTrue(nodeLevelCacheThin instanceof ThinCache.NodeLevelCache);
  }

  public void testExplicitNullGivesDefaults() {
    System.setProperty("jetty.port", "8000");
    String solrXml =
        "<solr>"
            + "<null name=\"maxBooleanClauses\"/>"
            + "<solrcloud>"
            + "<str name=\"host\">host</str>"
            + "<int name=\"hostPort\">0</int>"
            + "<str name=\"hostContext\">solr</str>"
            + "<null name=\"leaderVoteWait\"/>"
            + "</solrcloud></solr>";

    NodeConfig cfg = SolrXmlConfig.fromString(solrHome, solrXml);
    assertNull("maxBooleanClauses", cfg.getBooleanQueryMaxClauseCount()); // default is null
    assertEquals("leaderVoteWait", 180000, cfg.getCloudConfig().getLeaderVoteWait());
    assertEquals("hostPort", 8000, cfg.getCloudConfig().getSolrHostPort());
  }

  public void testIntAsLongBad() {
    String bad = "" + TestUtil.nextLong(random(), Integer.MAX_VALUE, Long.MAX_VALUE);
    String solrXml =
        "<solr><updateshardhandler>"
            + "<long name=\"maxUpdateConnections\">"
            + bad
            + "</long>"
            + "</updateshardhandler></solr>";
    SolrException thrown =
        assertThrows(SolrException.class, () -> SolrXmlConfig.fromString(solrHome, solrXml));
    assertEquals(
        "Error parsing 'maxUpdateConnections', value '" + bad + "' cannot be parsed as int",
        thrown.getMessage());
  }

  public void testIntAsLongOk() {
    int ok = random().nextInt();
    String solrXml =
        "<solr><updateshardhandler>"
            + "<long name=\"maxUpdateConnections\">"
            + ok
            + "</long>"
            + "</updateshardhandler></solr>";
    NodeConfig cfg = SolrXmlConfig.fromString(solrHome, solrXml);
    assertEquals(ok, cfg.getUpdateShardHandlerConfig().getMaxUpdateConnections());
  }

  public void testMultiCloudSectionError() {
    String solrXml =
        "<solr>"
            + "<solrcloud><bool name=\"genericCoreNodeNames\">true</bool></solrcloud>"
            + "<solrcloud><bool name=\"genericCoreNodeNames\">false</bool></solrcloud>"
            + "</solr>";

    SolrException thrown =
        assertThrows(SolrException.class, () -> SolrXmlConfig.fromString(solrHome, solrXml));
    assertEquals("Multiple instances of solrcloud section found in solr.xml", thrown.getMessage());
  }

  public void testMultiLoggingSectionError() {
    String solrXml =
        "<solr>"
            + "<logging><str name=\"class\">foo</str></logging>"
            + "<logging><str name=\"class\">foo</str></logging>"
            + "</solr>";

    SolrException thrown =
        assertThrows(SolrException.class, () -> SolrXmlConfig.fromString(solrHome, solrXml));
    assertEquals("Multiple instances of logging section found in solr.xml", thrown.getMessage());
  }

  public void testMultiLoggingWatcherSectionError() {
    String solrXml =
        "<solr><logging>"
            + "<watcher><int name=\"threshold\">42</int></watcher>"
            + "<watcher><int name=\"threshold\">42</int></watcher>"
            + "<watcher><int name=\"threshold\">42</int></watcher>"
            + "</logging></solr>";

    SolrException thrown =
        assertThrows(SolrException.class, () -> SolrXmlConfig.fromString(solrHome, solrXml));
    assertEquals(
        "Multiple instances of logging/watcher section found in solr.xml", thrown.getMessage());
  }

  public void testMultiCoreAdminHandlerActionsSectionError() {
    String solrXml =
        "<solr>"
            + "<coreAdminHandlerActions><str name=\"action1\">testCoreAdminHandlerAction1</str></coreAdminHandlerActions>"
            + "<coreAdminHandlerActions><str name=\"action2\">testCoreAdminHandlerAction2</str></coreAdminHandlerActions>"
            + "</solr>";

    SolrException thrown =
        assertThrows(SolrException.class, () -> SolrXmlConfig.fromString(solrHome, solrXml));
    assertEquals(
        "Multiple instances of coreAdminHandlerActions section found in solr.xml",
        thrown.getMessage());
  }

  public void testValidStringValueWhenBoolTypeIsExpected() {
    boolean schemaCache = random().nextBoolean();
    String solrXml =
        String.format(Locale.ROOT, "<solr><str name=\"shareSchema\">%s</str></solr>", schemaCache);

    NodeConfig nodeConfig = SolrXmlConfig.fromString(solrHome, solrXml);
    assertEquals("gen core node names", schemaCache, nodeConfig.hasSchemaCache());
  }

  public void testValidStringValueWhenIntTypeIsExpected() {
    int maxUpdateConnections = random().nextInt();
    String solrXml =
        String.format(
            Locale.ROOT,
            "<solr><updateshardhandler><str name=\"maxUpdateConnections\">%d</str></updateshardhandler></solr>",
            maxUpdateConnections);
    NodeConfig nodeConfig = SolrXmlConfig.fromString(solrHome, solrXml);
    assertEquals(
        "max update conn",
        maxUpdateConnections,
        nodeConfig.getUpdateShardHandlerConfig().getMaxUpdateConnections());
  }

  public void testFailAtConfigParseTimeWhenIntTypeIsExpectedAndLongTypeIsGiven() {
    long val = TestUtil.nextLong(random(), Integer.MAX_VALUE, Long.MAX_VALUE);
    String solrXml =
        String.format(
            Locale.ROOT,
            "<solr><solrcloud><long name=\"maxUpdateConnections\">%d</long></solrcloud></solr>",
            val);

    SolrException thrown =
        assertThrows(SolrException.class, () -> SolrXmlConfig.fromString(solrHome, solrXml));
    assertEquals(
        "Error parsing 'maxUpdateConnections', value '" + val + "' cannot be parsed as int",
        thrown.getMessage());
  }

  public void testFailAtConfigParseTimeWhenBoolTypeIsExpectedAndValueIsInvalidString() {
    String solrXml =
        "<solr><solrcloud><bool name=\"genericCoreNodeNames\">FOO</bool></solrcloud></solr>";

    SolrException thrown =
        assertThrows(SolrException.class, () -> SolrXmlConfig.fromString(solrHome, solrXml));
    assertEquals("invalid boolean value: FOO", thrown.getMessage());
  }

  public void testFailAtConfigParseTimeWhenIntTypeIsExpectedAndBoolTypeIsGiven() {
    // given:
    boolean randomBoolean = random().nextBoolean();
    String solrXml =
        String.format(
            Locale.ROOT,
            "<solr><logging><int name=\"unknown-option\">%s</int></logging></solr>",
            randomBoolean);

    SolrException thrown =
        assertThrows(SolrException.class, () -> SolrXmlConfig.fromString(solrHome, solrXml));
    assertEquals(
        String.format(
            Locale.ROOT,
            "Value of 'unknown-option' can not be parsed as 'int': \"%s\"",
            randomBoolean),
        thrown.getMessage());
  }

  public void testFailAtConfigParseTimeWhenUnrecognizedSolrCloudOptionWasFound() {
    String solrXml =
        "<solr><solrcloud><str name=\"host\">host</str><int name=\"hostPort\">8983</int><str name=\"hostContext\"></str><bool name=\"unknown-option\">true</bool></solrcloud></solr>";

    SolrException thrown =
        assertThrows(SolrException.class, () -> SolrXmlConfig.fromString(solrHome, solrXml));
    assertEquals(
        "Unknown configuration parameter in <solrcloud> section of solr.xml: unknown-option",
        thrown.getMessage());
  }

  public void testFailAtConfigParseTimeWhenUnrecognizedSolrOptionWasFound() {
    String solrXml = "<solr><bool name=\"unknown-bool-option\">true</bool></solr>";

    SolrException thrown =
        assertThrows(SolrException.class, () -> SolrXmlConfig.fromString(solrHome, solrXml));
    assertEquals(
        "Unknown configuration value in solr.xml: unknown-bool-option", thrown.getMessage());
  }

  public void testFailAtConfigParseTimeWhenUnrecognizedLoggingOptionWasFound() {
    String solrXml =
        String.format(
            Locale.ROOT,
            "<solr><logging><bool name=\"unknown-option\">%s</bool></logging></solr>",
            random().nextBoolean());

    SolrException thrown =
        assertThrows(SolrException.class, () -> SolrXmlConfig.fromString(solrHome, solrXml));
    assertEquals("Unknown value in logwatcher config: unknown-option", thrown.getMessage());
  }

  public void testFailAtConfigParseTimeWhenLoggingConfigParamsAreDuplicated() {
    String v1 = "" + random().nextInt();
    String v2 = "" + random().nextInt();
    String solrXml =
        String.format(
            Locale.ROOT,
            "<solr><logging>"
                + "<str name=\"class\">%s</str>"
                + "<str name=\"class\">%s</str>"
                + "</logging></solr>",
            v1,
            v2);

    SolrException thrown =
        assertThrows(SolrException.class, () -> SolrXmlConfig.fromString(solrHome, solrXml));
    assertEquals("<logging> section of solr.xml contains duplicated 'class'", thrown.getMessage());
  }

  public void testFailAtConfigParseTimeWhenSolrCloudConfigParamsAreDuplicated() {
    String v1 = "" + random().nextInt();
    String v2 = "" + random().nextInt();
    String v3 = "" + random().nextInt();
    String solrXml =
        String.format(
            Locale.ROOT,
            "<solr><solrcloud>"
                + "<int name=\"zkClientTimeout\">%s</int>"
                + "<int name=\"zkClientTimeout\">%s</int>"
                + "<str name=\"zkHost\">foo</str>"
                + // other ok val in middle
                "<int name=\"zkClientTimeout\">%s</int>"
                + "</solrcloud></solr>",
            v1,
            v2,
            v3);

    SolrException thrown =
        assertThrows(SolrException.class, () -> SolrXmlConfig.fromString(solrHome, solrXml));
    assertEquals(
        "<solrcloud> section of solr.xml contains duplicated 'zkClientTimeout'",
        thrown.getMessage());
  }

  @Ignore
  public void testFailAtConfigParseTimeWhenSolrConfigParamsAreDuplicated() {
    String v1 = "" + random().nextInt();
    String v2 = "" + random().nextInt();
    String solrXml =
        String.format(
            Locale.ROOT,
            "<solr>"
                + "<int name=\"coreLoadThreads\">%s</int>"
                + "<str name=\"coreLoadThreads\">%s</str>"
                + "</solr>",
            v1,
            v2);
    SolrException thrown =
        assertThrows(SolrException.class, () -> SolrXmlConfig.fromString(solrHome, solrXml));
    assertEquals(
        "Main section of solr.xml contains duplicated 'coreLoadThreads'", thrown.getMessage());
  }

  public void testFailAtConfigParseTimeWhenCoreAdminHandlerActionsConfigParamsAreDuplicated() {
    String v1 = "" + random().nextInt();
    String v2 = "" + random().nextInt();
    String solrXml =
        String.format(
            Locale.ROOT,
            "<solr><coreAdminHandlerActions>"
                + "<str name=\"action\">%s</str>"
                + "<str name=\"action\">%s</str>"
                + "</coreAdminHandlerActions></solr>",
            v1,
            v2);

    SolrException thrown =
        assertThrows(SolrException.class, () -> SolrXmlConfig.fromString(solrHome, solrXml));
    assertEquals(
        "<coreAdminHandlerActions> section of solr.xml contains duplicated 'action'",
        thrown.getMessage());
  }

  public void testCloudConfigRequiresHostPort() {
    SolrException thrown =
        assertThrows(
            SolrException.class,
            () -> SolrXmlConfig.fromString(solrHome, "<solr><solrcloud></solrcloud></solr>"));
    assertEquals("solrcloud section missing required entry 'hostPort'", thrown.getMessage());
  }

  public void testCloudConfigRequiresHost() {
    SolrException thrown =
        assertThrows(
            SolrException.class,
            () ->
                SolrXmlConfig.fromString(
                    solrHome,
                    "<solr><solrcloud><int name=\"hostPort\">8983</int></solrcloud></solr>"));
    assertEquals("solrcloud section missing required entry 'host'", thrown.getMessage());
  }

  public void testCloudConfigRequiresHostContext() {
    String solrXml =
        "<solr><solrcloud><str name=\"host\">host</str><int name=\"hostPort\">8983</int></solrcloud></solr>";
    SolrException thrown =
        assertThrows(SolrException.class, () -> SolrXmlConfig.fromString(solrHome, solrXml));
    assertEquals("solrcloud section missing required entry 'hostContext'", thrown.getMessage());
  }

  public void testMultiBackupSectionError() {
    String solrXml = "<solr><backup></backup><backup></backup></solr>";
    SolrException thrown =
        assertThrows(SolrException.class, () -> SolrXmlConfig.fromString(solrHome, solrXml));
    assertEquals("Multiple instances of backup section found in solr.xml", thrown.getMessage());
  }
}
