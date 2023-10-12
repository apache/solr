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

import static org.apache.solr.common.params.CommonParams.NAME;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.function.Consumer;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.management.MBeanServer;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.common.ConfigNode;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.DOMUtil;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.util.Utils;
import org.apache.solr.logging.LogWatcherConfig;
import org.apache.solr.metrics.reporters.SolrJmxReporter;
import org.apache.solr.search.CacheConfig;
import org.apache.solr.servlet.SolrDispatchFilter;
import org.apache.solr.update.UpdateShardHandlerConfig;
import org.apache.solr.util.DOMConfigNode;
import org.apache.solr.util.DataConfigNode;
import org.apache.solr.util.JmxUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.InputSource;

/** Loads {@code solr.xml}. */
public class SolrXmlConfig {

  // TODO should these from* methods return a NodeConfigBuilder so that the caller (a test) can make
  // further manipulations like add properties and set the CorePropertiesLocator and "async" mode?

  public static final String ZK_HOST = "zkHost";
  public static final String SOLR_XML_FILE = "solr.xml";
  public static final String SOLR_DATA_HOME = "solr.data.home";

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final Pattern COMMA_SEPARATED_PATTERN = Pattern.compile("\\s*,\\s*");

  /**
   * Given some node Properties, checks if non-null and a 'zkHost' is already included. If so, the
   * Properties are returned as is. If not, then the returned value will be a new Properties,
   * wrapping the original Properties, with the 'zkHost' value set based on the value of the
   * corresponding System property (if set)
   *
   * <p>In theory we only need this logic once, ideally in SolrDispatchFilter, but we put it here to
   * re-use redundantly because of how much surface area our API has for various tests to poke at
   * us.
   */
  public static Properties wrapAndSetZkHostFromSysPropIfNeeded(final Properties props) {
    if (null != props && StrUtils.isNotNullOrEmpty(props.getProperty(ZK_HOST))) {
      // nothing to do...
      return props;
    }
    // we always wrap if we might set a property -- never mutate the original props
    final Properties results = (null == props ? new Properties() : new Properties(props));
    final String sysprop = System.getProperty(ZK_HOST);
    if (StrUtils.isNotNullOrEmpty(sysprop)) {
      results.setProperty(ZK_HOST, sysprop);
    }
    return results;
  }

  public static NodeConfig fromConfig(
      Path solrHome,
      Properties substituteProperties,
      boolean fromZookeeper,
      ConfigNode root,
      SolrResourceLoader loader) {

    checkForIllegalConfig(root);

    // sanity check: if our config came from zookeeper, then there *MUST* be Node Properties that
    // tell us what zkHost was used to read it (either via webapp context attribute, or that
    // SolrDispatchFilter filled in for us from system properties)
    assert ((!fromZookeeper)
        || (null != substituteProperties && null != substituteProperties.getProperty(ZK_HOST)));

    // Regardless of where/how we this XmlConfigFile was loaded from, if it contains a zkHost
    // property, we're going to use that as our "default" and only *directly* check the system
    // property if it's not specified.
    //
    // (checking the sys prop here is really just for tests that by-pass SolrDispatchFilter. In
    // non-test situations, SolrDispatchFilter will check the system property if needed in order to
    // try and load solr.xml from ZK, and should have put the sys prop value in the node properties
    // for us)
    final String defaultZkHost =
        wrapAndSetZkHostFromSysPropIfNeeded(substituteProperties).getProperty(ZK_HOST);

    CloudConfig cloudConfig = null;
    UpdateShardHandlerConfig deprecatedUpdateConfig = null;

    if (root.get("solrcloud").exists()) {
      NamedList<Object> cloudSection =
          readNodeListAsNamedList(root.get("solrcloud"), "<solrcloud>");
      deprecatedUpdateConfig = loadUpdateConfig(cloudSection, false);
      cloudConfig = fillSolrCloudSection(cloudSection, defaultZkHost);
    }

    NamedList<Object> entries = readNodeListAsNamedList(root, "<solr>");
    String nodeName = (String) entries.remove("nodeName");
    if (StrUtils.isNullOrEmpty(nodeName) && cloudConfig != null) nodeName = cloudConfig.getHost();

    // It should go inside the fillSolrSection method but
    // since it is arranged as a separate section it is placed here
    Map<String, String> coreAdminHandlerActions =
        readNodeListAsNamedList(root.get("coreAdminHandlerActions"), "<coreAdminHandlerActions>")
            .asMap()
            .entrySet()
            .stream()
            .collect(Collectors.toMap(item -> item.getKey(), item -> item.getValue().toString()));

    UpdateShardHandlerConfig updateConfig;
    if (deprecatedUpdateConfig == null) {
      updateConfig =
          loadUpdateConfig(
              readNodeListAsNamedList(root.get("updateshardhandler"), "<updateshardhandler>"),
              true);
    } else {
      updateConfig =
          loadUpdateConfig(
              readNodeListAsNamedList(root.get("updateshardhandler"), "<updateshardhandler>"),
              false);
      if (updateConfig != null) {
        throw new SolrException(
            SolrException.ErrorCode.SERVER_ERROR,
            "UpdateShardHandler configuration defined twice in solr.xml");
      }
      updateConfig = deprecatedUpdateConfig;
    }

    NodeConfig.NodeConfigBuilder configBuilder =
        new NodeConfig.NodeConfigBuilder(nodeName, solrHome);
    configBuilder.setSolrResourceLoader(loader);
    configBuilder.setUpdateShardHandlerConfig(updateConfig);
    configBuilder.setShardHandlerFactoryConfig(getPluginInfo(root.get("shardHandlerFactory")));
    configBuilder.setTracerConfig(getPluginInfo(root.get("tracerConfig")));
    configBuilder.setLogWatcherConfig(loadLogWatcherConfig(root.get("logging")));
    configBuilder.setSolrProperties(loadProperties(root, substituteProperties));
    if (cloudConfig != null) configBuilder.setCloudConfig(cloudConfig);
    configBuilder.setBackupRepositoryPlugins(
        getBackupRepositoryPluginInfos(root.get("backup").getAll("repository")));
    // <metrics><hiddenSysProps></metrics> will be removed in Solr 10, but until then, use it if a
    // <hiddenSysProps> is not provided under <solr>.
    // Remove this line in 10.0
    configBuilder.setHiddenSysProps(getHiddenSysProps(root.get("metrics")));
    configBuilder.setMetricsConfig(getMetricsConfig(root.get("metrics")));
    configBuilder.setCachesConfig(getCachesConfig(loader, root.get("caches")));
    configBuilder.setFromZookeeper(fromZookeeper);
    configBuilder.setDefaultZkHost(defaultZkHost);
    configBuilder.setCoreAdminHandlerActions(coreAdminHandlerActions);
    return fillSolrSection(configBuilder, root);
  }

  public static NodeConfig fromFile(Path solrHome, Path configFile, Properties substituteProps) {
    if (!Files.exists(configFile)) {
      if (Boolean.getBoolean("solr.solrxml.required")) {
        throw new SolrException(
            SolrException.ErrorCode.SERVER_ERROR,
            "solr.xml does not exist in " + configFile.getParent() + " cannot start Solr");
      }
      log.info("solr.xml not found in SOLR_HOME, using built-in default");
      String solrInstallDir = System.getProperty(SolrDispatchFilter.SOLR_INSTALL_DIR_ATTRIBUTE);
      if (solrInstallDir == null) {
        throw new SolrException(
            SolrException.ErrorCode.SERVER_ERROR,
            "Could not find default solr.xml file due to missing "
                + SolrDispatchFilter.SOLR_INSTALL_DIR_ATTRIBUTE);
      }
      configFile = Path.of(solrInstallDir).resolve("server").resolve("solr").resolve("solr.xml");
    }

    log.info("Loading solr.xml from {}", configFile);
    try (InputStream inputStream = Files.newInputStream(configFile)) {
      return fromInputStream(solrHome, inputStream, substituteProps);
    } catch (SolrException exc) {
      throw exc;
    } catch (Exception exc) {
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR, "Could not load SOLR configuration", exc);
    }
  }

  /** TEST-ONLY */
  public static NodeConfig fromString(Path solrHome, String xml) {
    return fromInputStream(
        solrHome, new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)), new Properties());
  }

  public static NodeConfig fromInputStream(
      Path solrHome, InputStream is, Properties substituteProps) {
    return fromInputStream(solrHome, is, substituteProps, false);
  }

  public static NodeConfig fromInputStream(
      Path solrHome, InputStream is, Properties substituteProps, boolean fromZookeeper) {
    SolrResourceLoader loader = new SolrResourceLoader(solrHome);
    if (substituteProps == null) {
      substituteProps = new Properties();
    }
    try {
      byte[] buf = is.readAllBytes();
      try (ByteArrayInputStream dup = new ByteArrayInputStream(buf)) {
        XmlConfigFile config =
            new XmlConfigFile(loader, null, new InputSource(dup), null, substituteProps);
        return fromConfig(
            solrHome,
            substituteProps,
            fromZookeeper,
            new DataConfigNode(new DOMConfigNode(config.getDocument().getDocumentElement())),
            loader);
      }
    } catch (SolrException exc) {
      throw exc;
    } catch (Exception e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }
  }

  public static NodeConfig fromSolrHome(Path solrHome, Properties substituteProps) {
    return fromFile(solrHome, solrHome.resolve(SOLR_XML_FILE), substituteProps);
  }

  private static void checkForIllegalConfig(ConfigNode root) {
    failIfFound(root.attr("coreLoadThreads"), "solr/@coreLoadThreads");
    failIfFound(root.attr("persistent"), "solr/@persistent");
    failIfFound(root.attr("sharedLib"), "solr/@sharedLib");
    failIfFound(root.attr("zkHost"), "solr/@zkHost");
    failIfFound(root.attr("zkHost"), "solr/@zkHost");
    failIfFound(root.get("cores").exists() ? "" : null, "solr/cores");

    assertSingleInstance(root.getAll("solrcloud"), "solrcloud");
    assertSingleInstance(root.getAll("logging"), "logging");
    assertSingleInstance(root.get("logging").getAll("watcher"), "logging/watcher");
    assertSingleInstance(root.getAll("backup"), "backup");
    assertSingleInstance(root.getAll("coreAdminHandlerActions"), "coreAdminHandlerActions");
  }

  private static void assertSingleInstance(List<ConfigNode> l, String section) {
    if (l.size() > 1)
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR,
          "Multiple instances of " + section + " section found in solr.xml");
  }

  private static void failIfFound(String val, String xPath) {

    if (val != null) {
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR,
          "Should not have found "
              + xPath
              + "\n. Please upgrade your solr.xml: https://solr.apache.org/guide/solr/latest/configuration-guide/configuring-solr-xml.html");
    }
  }

  private static Properties loadProperties(ConfigNode cfg, Properties substituteProperties) {
    Properties properties = new Properties(substituteProperties);
    cfg.forEachChild(
        it -> {
          if (it.name().equals("property")) {
            properties.setProperty(it.attr(NAME), it.attr("value"));
          }
          return Boolean.TRUE;
        });

    return properties;
  }

  private static NamedList<Object> readNodeListAsNamedList(ConfigNode cfg, String section) {
    NamedList<Object> nl = DOMUtil.readNamedListChildren(cfg);
    Set<String> keys = new HashSet<>();
    for (Map.Entry<String, Object> entry : nl) {
      if (!keys.add(entry.getKey()))
        throw new SolrException(
            SolrException.ErrorCode.SERVER_ERROR,
            section + " section of solr.xml contains duplicated '" + entry.getKey() + "'");
    }
    return nl;
  }

  private static int parseInt(String field, String value) {
    try {
      return Integer.parseInt(value);
    } catch (NumberFormatException e) {
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR,
          "Error parsing '" + field + "', value '" + value + "' cannot be parsed as int");
    }
  }

  private static NodeConfig fillSolrSection(NodeConfig.NodeConfigBuilder builder, ConfigNode root) {

    forEachNamedListEntry(
        root,
        it -> {
          if (it.name().equals("null")) return;
          try {
            switch (it.attr(NAME)) {
              case "adminHandler":
                builder.setCoreAdminHandlerClass(it.txt());
                break;
              case "collectionsHandler":
                builder.setCollectionsAdminHandlerClass(it.txt());
                break;
              case "healthCheckHandler":
                builder.setHealthCheckHandlerClass(it.txt());
                break;
              case "infoHandler":
                builder.setInfoHandlerClass(it.txt());
                break;
              case "configSetsHandler":
                builder.setConfigSetsHandlerClass(it.txt());
                break;
              case "configSetService":
                builder.setConfigSetServiceClass(it.txt());
                break;
              case "coresLocator":
                builder.setCoresLocatorClass(it.txt());
                break;
              case "coreRootDirectory":
                builder.setCoreRootDirectory(it.txt());
                break;
              case "solrDataHome":
                builder.setSolrDataHome(it.txt());
                break;
              case "maxBooleanClauses":
                builder.setBooleanQueryMaxClauseCount(it.intVal(-1));
                break;
              case "managementPath":
                builder.setManagementPath(it.txt());
                break;
              case "sharedLib":
                builder.setSharedLibDirectory(it.txt());
                break;
              case "modules":
                builder.setModules(it.txt());
                break;
              case "hiddenSysProps":
                builder.setHiddenSysProps(it.txt());
                break;
              case "allowPaths":
                builder.setAllowPaths(separatePaths(it.txt()));
                break;
              case "hideStackTrace":
                builder.setHideStackTrace(it.boolVal(false));
                break;
              case "configSetBaseDir":
                builder.setConfigSetBaseDirectory(it.txt());
                break;
              case "shareSchema":
                builder.setUseSchemaCache(it.boolVal(false));
                break;
              case "coreLoadThreads":
                builder.setCoreLoadThreads(it.intVal(-1));
                break;
              case "replayUpdatesThreads":
                builder.setReplayUpdatesThreads(it.intVal(-1));
                break;
              case "transientCacheSize":
                log.warn("solr.xml transientCacheSize -- transient cores is deprecated");
                builder.setTransientCacheSize(it.intVal(-1));
                break;
              case "allowUrls":
                builder.setAllowUrls(separateStrings(it.txt()));
                break;
              default:
                throw new SolrException(
                    SolrException.ErrorCode.SERVER_ERROR,
                    "Unknown configuration value in solr.xml: " + it.attr(NAME));
            }
          } catch (NumberFormatException e) {
            throw new SolrException(
                SolrException.ErrorCode.SERVER_ERROR,
                "Error parsing '" + it.attr(NAME) + "', value '" + it.txt() + "' cannot be parsed");
          }
        });

    return builder.build();
  }

  private static List<String> separateStrings(String commaSeparatedString) {
    if (StrUtils.isNullOrEmpty(commaSeparatedString)) {
      return Collections.emptyList();
    }
    return Arrays.asList(COMMA_SEPARATED_PATTERN.split(commaSeparatedString));
  }

  private static Set<String> separateStringsToSet(String commaSeparatedString) {
    if (StrUtils.isNullOrEmpty(commaSeparatedString)) {
      return Collections.emptySet();
    }
    return Set.of(COMMA_SEPARATED_PATTERN.split(commaSeparatedString));
  }

  private static Set<Path> separatePaths(String commaSeparatedString) {
    if (StrUtils.isNullOrEmpty(commaSeparatedString)) {
      return Collections.emptySet();
    }
    // Parse the list of paths. The special values '*' and '_ALL_' mean all paths.
    String[] pathStrings = COMMA_SEPARATED_PATTERN.split(commaSeparatedString);
    SolrPaths.AllowPathBuilder allowPathBuilder = new SolrPaths.AllowPathBuilder();
    for (String p : pathStrings) {
      allowPathBuilder.addPath(p);
    }
    return allowPathBuilder.build();
  }

  private static UpdateShardHandlerConfig loadUpdateConfig(
      NamedList<Object> nl, boolean alwaysDefine) {

    if (nl == null && !alwaysDefine) return null;

    if (nl == null) return UpdateShardHandlerConfig.DEFAULT;

    boolean defined = false;

    int maxUpdateConnections = HttpClientUtil.DEFAULT_MAXCONNECTIONS;
    int maxUpdateConnectionsPerHost = HttpClientUtil.DEFAULT_MAXCONNECTIONSPERHOST;
    int distributedSocketTimeout = HttpClientUtil.DEFAULT_SO_TIMEOUT;
    int distributedConnectionTimeout = HttpClientUtil.DEFAULT_CONNECT_TIMEOUT;
    String metricNameStrategy = UpdateShardHandlerConfig.DEFAULT_METRICNAMESTRATEGY;
    int maxRecoveryThreads = UpdateShardHandlerConfig.DEFAULT_MAXRECOVERYTHREADS;

    Object muc = nl.remove("maxUpdateConnections");
    if (muc != null) {
      maxUpdateConnections = parseInt("maxUpdateConnections", muc.toString());
      defined = true;
    }

    Object mucph = nl.remove("maxUpdateConnectionsPerHost");
    if (mucph != null) {
      maxUpdateConnectionsPerHost = parseInt("maxUpdateConnectionsPerHost", mucph.toString());
      defined = true;
    }

    Object dst = nl.remove("distribUpdateSoTimeout");
    if (dst != null) {
      distributedSocketTimeout = parseInt("distribUpdateSoTimeout", dst.toString());
      defined = true;
    }

    Object dct = nl.remove("distribUpdateConnTimeout");
    if (dct != null) {
      distributedConnectionTimeout = parseInt("distribUpdateConnTimeout", dct.toString());
      defined = true;
    }

    Object mns = nl.remove("metricNameStrategy");
    if (mns != null) {
      metricNameStrategy = mns.toString();
      defined = true;
    }

    Object mrt = nl.remove("maxRecoveryThreads");
    if (mrt != null) {
      maxRecoveryThreads = parseInt("maxRecoveryThreads", mrt.toString());
      defined = true;
    }

    if (!defined && !alwaysDefine) return null;

    return new UpdateShardHandlerConfig(
        maxUpdateConnections,
        maxUpdateConnectionsPerHost,
        distributedSocketTimeout,
        distributedConnectionTimeout,
        metricNameStrategy,
        maxRecoveryThreads);
  }

  private static String removeValue(NamedList<Object> nl, String key) {
    Object value = nl.remove(key);
    if (value == null) return null;
    return value.toString();
  }

  private static String required(String section, String key, String value) {
    if (value != null) return value;
    throw new SolrException(
        SolrException.ErrorCode.SERVER_ERROR,
        section + " section missing required entry '" + key + "'");
  }

  private static CloudConfig fillSolrCloudSection(NamedList<Object> nl, String defaultZkHost) {

    int hostPort =
        parseInt("hostPort", required("solrcloud", "hostPort", removeValue(nl, "hostPort")));
    if (hostPort <= 0) {
      // Default to the port that jetty is listening on, or 8983 if that is not provided.
      hostPort = parseInt("jetty.port", System.getProperty("jetty.port", "8983"));
    }
    String hostName = required("solrcloud", "host", removeValue(nl, "host"));
    String hostContext = required("solrcloud", "hostContext", removeValue(nl, "hostContext"));

    CloudConfig.CloudConfigBuilder builder =
        new CloudConfig.CloudConfigBuilder(hostName, hostPort, hostContext);
    // set the defaultZkHost until/unless it's overridden in the "cloud section" (below)...
    builder.setZkHost(defaultZkHost);

    for (Map.Entry<String, Object> entry : nl) {
      String name = entry.getKey();
      if (entry.getValue() == null) continue;
      String value = entry.getValue().toString();
      switch (name) {
        case "leaderVoteWait":
          builder.setLeaderVoteWait(parseInt(name, value));
          break;
        case "leaderConflictResolveWait":
          builder.setLeaderConflictResolveWait(parseInt(name, value));
          break;
        case "zkClientTimeout":
          builder.setZkClientTimeout(parseInt(name, value));
          break;
        case "zkHost":
          builder.setZkHost(value);
          break;
        case "genericCoreNodeNames":
          builder.setUseGenericCoreNames(Boolean.parseBoolean(value));
          break;
        case "zkACLProvider":
          builder.setZkACLProviderClass(value);
          break;
        case "zkCredentialsProvider":
          builder.setZkCredentialsProviderClass(value);
          break;
        case "zkCredentialsInjector":
          builder.setZkCredentialsInjectorClass(value);
          break;
        case "createCollectionWaitTimeTillActive":
          builder.setCreateCollectionWaitTimeTillActive(parseInt(name, value));
          break;
        case "createCollectionCheckLeaderActive":
          builder.setCreateCollectionCheckLeaderActive(Boolean.parseBoolean(value));
          break;
        case "pkiHandlerPrivateKeyPath":
          builder.setPkiHandlerPrivateKeyPath(value);
          break;
        case "pkiHandlerPublicKeyPath":
          builder.setPkiHandlerPublicKeyPath(value);
          break;
        case "distributedClusterStateUpdates":
          builder.setUseDistributedClusterStateUpdates(Boolean.parseBoolean(value));
          break;
        case "distributedCollectionConfigSetExecution":
          builder.setUseDistributedCollectionConfigSetExecution(Boolean.parseBoolean(value));
          break;
        case "minStateByteLenForCompression":
          builder.setMinStateByteLenForCompression(parseInt(name, value));
          break;
        case "stateCompressor":
          builder.setStateCompressorClass(value);
          break;
        default:
          throw new SolrException(
              SolrException.ErrorCode.SERVER_ERROR,
              "Unknown configuration parameter in <solrcloud> section of solr.xml: " + name);
      }
    }

    return builder.build();
  }

  private static LogWatcherConfig loadLogWatcherConfig(ConfigNode logging) {

    String loggingClass = null;
    boolean enabled = true;
    int watcherQueueSize = 50;
    String watcherThreshold = null;

    for (Map.Entry<String, Object> entry : readNodeListAsNamedList(logging, "<logging>")) {
      String name = entry.getKey();
      String value = entry.getValue().toString();
      switch (name) {
        case "class":
          loggingClass = value;
          break;
        case "enabled":
          enabled = Boolean.parseBoolean(value);
          break;
        default:
          throw new SolrException(
              SolrException.ErrorCode.SERVER_ERROR, "Unknown value in logwatcher config: " + name);
      }
    }

    for (Map.Entry<String, Object> entry :
        readNodeListAsNamedList(logging.get("watcher"), "<watcher>")) {
      String name = entry.getKey();
      String value = entry.getValue().toString();
      switch (name) {
        case "size":
          watcherQueueSize = parseInt(name, value);
          break;
        case "threshold":
          watcherThreshold = value;
          break;
        default:
          throw new SolrException(
              SolrException.ErrorCode.SERVER_ERROR, "Unknown value in logwatcher config: " + name);
      }
    }

    return new LogWatcherConfig(enabled, loggingClass, watcherThreshold, watcherQueueSize);
  }

  static void forEachNamedListEntry(ConfigNode cfg, Consumer<ConfigNode> fun) {
    cfg.forEachChild(
        it -> {
          if (DOMUtil.NL_TAGS.contains(it.name())) {
            fun.accept(it);
          }
          return Boolean.TRUE;
        });
  }

  private static PluginInfo[] getBackupRepositoryPluginInfos(List<ConfigNode> cfg) {
    if (cfg.isEmpty()) {
      return new PluginInfo[0];
    }

    PluginInfo[] configs = new PluginInfo[cfg.size()];
    for (int i = 0; i < cfg.size(); i++) {
      ConfigNode c = cfg.get(i);
      configs[i] = new PluginInfo(c, "BackupRepositoryFactory", true, true);
    }

    return configs;
  }

  private static MetricsConfig getMetricsConfig(ConfigNode metrics) {
    MetricsConfig.MetricsConfigBuilder builder = new MetricsConfig.MetricsConfigBuilder();
    boolean enabled = metrics.boolAttr("enabled", true);
    builder.setEnabled(enabled);
    if (!enabled) {
      log.info("Metrics collection is disabled.");
      return builder.build();
    }

    builder.setCounterSupplier(getPluginInfo(metrics.get("suppliers").get("counter")));
    builder.setMeterSupplier(getPluginInfo(metrics.get("suppliers").get("meter")));
    builder.setTimerSupplier(getPluginInfo(metrics.get("suppliers").get("timer")));
    builder.setHistogramSupplier(getPluginInfo(metrics.get("suppliers").get("histogram")));

    if (metrics.get("missingValues").exists()) {
      NamedList<Object> missingValues = DOMUtil.childNodesToNamedList(metrics.get("missingValues"));
      builder.setNullNumber(decodeNullValue(missingValues.get("nullNumber")));
      builder.setNotANumber(decodeNullValue(missingValues.get("notANumber")));
      builder.setNullString(decodeNullValue(missingValues.get("nullString")));
      builder.setNullObject(decodeNullValue(missingValues.get("nullObject")));
    }

    ConfigNode caching = metrics.get("solr/metrics/caching");
    if (caching != null) {
      Object threadsCachingIntervalSeconds =
          DOMUtil.childNodesToNamedList(caching).get("threadsIntervalSeconds", null);
      builder.setCacheConfig(
          new MetricsConfig.CacheConfig(
              threadsCachingIntervalSeconds == null
                  ? null
                  : Integer.parseInt(threadsCachingIntervalSeconds.toString())));
    }

    PluginInfo[] reporterPlugins = getMetricReporterPluginInfos(metrics);
    return builder.setMetricReporterPlugins(reporterPlugins).build();
  }

  private static Map<String, CacheConfig> getCachesConfig(
      SolrResourceLoader loader, ConfigNode caches) {
    Map<String, CacheConfig> ret =
        CacheConfig.getMultipleConfigs(loader, null, null, caches.getAll("cache"));
    for (CacheConfig c : ret.values()) {
      if (c.getRegenerator() != null) {
        throw new SolrException(
            SolrException.ErrorCode.SERVER_ERROR,
            "node-level caches should not be configured with a regenerator!");
      }
    }
    return Collections.unmodifiableMap(ret);
  }

  private static Object decodeNullValue(Object o) {
    if (o instanceof String) { // check if it's a JSON object
      String str = (String) o;
      if (!str.isBlank() && (str.startsWith("{") || str.startsWith("["))) {
        try {
          o = Utils.fromJSONString((String) o);
        } catch (Exception e) {
          // ignore
        }
      }
    }
    return o;
  }

  private static PluginInfo[] getMetricReporterPluginInfos(ConfigNode metrics) {
    List<PluginInfo> configs = new ArrayList<>();
    boolean hasJmxReporter = false;
    for (ConfigNode node : metrics.getAll("reporter")) {
      PluginInfo info = getPluginInfo(node);
      String clazz = info.className;
      if (clazz != null && clazz.equals(SolrJmxReporter.class.getName())) {
        hasJmxReporter = true;
      }
      configs.add(info);
    }

    // if there's an MBean server running but there was no JMX reporter then add a default one
    MBeanServer mBeanServer = JmxUtil.findFirstMBeanServer();
    if (mBeanServer != null && !hasJmxReporter) {
      log.debug(
          "MBean server found: {}, but no JMX reporters were configured - adding default JMX reporter.",
          mBeanServer);
      Map<String, Object> attributes = new HashMap<>();
      attributes.put("name", "default");
      attributes.put("class", SolrJmxReporter.class.getName());
      PluginInfo defaultPlugin = new PluginInfo("reporter", attributes);
      configs.add(defaultPlugin);
    }
    return configs.toArray(new PluginInfo[0]);
  }

  /**
   * Deprecated as of 9.3, will be removed in 10.0
   *
   * @param metrics configNode for the metrics
   * @return a comma-separated list of hidden Sys Props
   */
  @Deprecated(forRemoval = true, since = "9.3")
  private static String getHiddenSysProps(ConfigNode metrics) {
    ConfigNode p = metrics.get("hiddenSysProps");
    if (!p.exists()) return null;
    Set<String> props = new HashSet<>();
    p.forEachChild(
        it -> {
          if (it.name().equals("str") && StrUtils.isNotNullOrEmpty(it.txt()))
            props.add(Pattern.quote(it.txt()));
          return Boolean.TRUE;
        });
    return String.join(",", props);
  }

  private static PluginInfo getPluginInfo(ConfigNode cfg) {
    if (cfg == null || !cfg.exists()) return null;
    return new PluginInfo(cfg, cfg.name(), false, true);
  }
}
