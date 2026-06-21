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

import com.google.common.base.Strings;
import net.sf.saxon.om.NodeInfo;
import org.apache.commons.io.IOUtils;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.common.ParWork;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.logging.LogWatcherConfig;
import org.apache.solr.update.UpdateShardHandlerConfig;
import org.apache.solr.util.DOMUtil;
import org.apache.solr.util.PropertiesUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.InputSource;

import static org.apache.solr.common.params.CommonParams.NAME;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;


/**
 * Loads {@code solr.xml}.
 */
public class SolrXmlConfig {
  // TODO should these from* methods return a NodeConfigBuilder so that the caller (a test) can make further
  //  manipulations like add properties and set the CorePropertiesLocator and "async" mode?

  public final static String SOLR_XML_FILE = "solr.xml";
  public final static String SOLR_DATA_HOME = "solr.data.home";

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  public static final PluginInfo[] EMPTY_PLUGIN_INFOS = new PluginInfo[0];


  public SolrXmlConfig() {

  }

  public static NodeConfig fromConfig(Path solrHome, XmlConfigFile config, boolean fromZookeeper) throws XPathExpressionException {

    checkForIllegalConfig(config);

    CloudConfig cloudConfig = null;
    UpdateShardHandlerConfig deprecatedUpdateConfig = null;

    if (config.getNodeList("solr/solrcloud", false).size() > 0) {
      NamedList<Object> cloudSection = readNodeListAsNamedList(config, "solr/solrcloud/*[@name]", "<solrcloud>");
      deprecatedUpdateConfig = loadUpdateConfig(cloudSection, false);
      cloudConfig = fillSolrCloudSection(cloudSection);
    }

    NamedList<Object> entries = readNodeListAsNamedList(config, "solr/*[@name]", "<solr>");
    String nodeName = (String) entries.remove("nodeName");
    if (Strings.isNullOrEmpty(nodeName) && cloudConfig != null)
      nodeName = cloudConfig.getHost();

    UpdateShardHandlerConfig updateConfig;
    if (deprecatedUpdateConfig == null) {
      updateConfig = loadUpdateConfig(readNodeListAsNamedList(config, "solr/updateshardhandler/*[@name]", "<updateshardhandler>"), true);
    }
    else {
      updateConfig = loadUpdateConfig(readNodeListAsNamedList(config, "solr/updateshardhandler/*[@name]", "<updateshardhandler>"), false);
      if (updateConfig != null) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "UpdateShardHandler configuration defined twice in solr.xml");
      }
      updateConfig = deprecatedUpdateConfig;
    }

    NodeConfig.NodeConfigBuilder configBuilder = new NodeConfig.NodeConfigBuilder(nodeName, solrHome);
    configBuilder.setSolrResourceLoader(config.getResourceLoader());
    configBuilder.setUpdateShardHandlerConfig(updateConfig);
    configBuilder.setShardHandlerFactoryConfig(getShardHandlerFactoryPluginInfo(config));
    configBuilder.setTracerConfig(getTracerPluginInfo(config));
    configBuilder.setLogWatcherConfig(loadLogWatcherConfig(config, "solr/logging/*[@name]", "solr/logging/watcher/*[@name]"));
    configBuilder.setSolrProperties(loadProperties(config));
    if (cloudConfig != null)
      configBuilder.setCloudConfig(cloudConfig);
    configBuilder.setBackupRepositoryPlugins(getBackupRepositoryPluginInfos(config));
    configBuilder.setMetricsConfig(getMetricsConfig(config));
    configBuilder.setFromZookeeper(fromZookeeper);
    return fillSolrSection(configBuilder, entries);
  }

  public static NodeConfig fromFile(Path solrHome, Path configFile, Properties substituteProps) throws IOException {
    log.info("Loading container configuration from {}", configFile);
    if (!Files.exists(configFile)) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "solr.xml does not exist in " + configFile.getParent() + " cannot start Solr");
    }

    try (InputStream inputStream = Files.newInputStream(configFile)) {
      return fromInputStream(solrHome, inputStream, substituteProps);
    } catch (SolrException exc) {
      throw exc;
    } catch (Exception exc) {
      ParWork.propagateInterrupt(exc);
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Could not load SOLR configuration", exc);
    }

  }

  /** TEST-ONLY */
  public static NodeConfig fromString(Path solrHome, String xml) {
    return fromInputStream(solrHome,
        new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)),
        new Properties());
  }

  public static NodeConfig fromInputStream(Path solrHome, InputStream is, Properties substituteProps) {
    return fromInputStream(solrHome, is, substituteProps, false);
  }

  public static NodeConfig fromInputStream(Path solrHome, InputStream is, Properties substituteProps, boolean fromZookeeper) {
    try (SolrResourceLoader loader = new SolrResourceLoader(solrHome, false)) {
      byte[] buf = IOUtils.toByteArray(is);
      try (ByteArrayInputStream dup = new ByteArrayInputStream(buf)) {
        XmlConfigFile config = new XmlConfigFile(loader, null, new InputSource(dup), null, substituteProps);
        return fromConfig(solrHome, config, fromZookeeper);
      }
    } catch (SolrException exc) {
      log.error("Exception reading config", exc);
      throw exc;
    } catch (Exception e) {
      ParWork.propagateInterrupt(e);
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }
  }

  public static NodeConfig fromSolrHome(Path solrHome, Properties substituteProps) throws IOException {
    return fromFile(solrHome, solrHome.resolve(SOLR_XML_FILE), substituteProps);
  }

  private static void checkForIllegalConfig(XmlConfigFile config) {
    // was resource killer - note: perhaps not as bad now that xml is more efficient?
    SolrResourceLoader loader = config.getResourceLoader();
    failIfFound(config, SolrResourceLoader.configXpathExpressions.coreLoadThreadsExp, ConfigXpathExpressions.coreLoadThreadsPath);
    failIfFound(config, SolrResourceLoader.configXpathExpressions.persistentExp, ConfigXpathExpressions.persistentPath);
    failIfFound(config, SolrResourceLoader.configXpathExpressions.sharedLibExp, ConfigXpathExpressions.sharedLibPath);
    failIfFound(config, SolrResourceLoader.configXpathExpressions.zkHostExp, ConfigXpathExpressions.zkHostPath);
    failIfFound(config, SolrResourceLoader.configXpathExpressions.coresExp, ConfigXpathExpressions.coresPath);

    assertSingleInstance("solrcloud", config);
    assertSingleInstance("logging", config);
    assertSingleInstance("logging/watcher", config);
    assertSingleInstance("backup", config);
  }

  private static void assertSingleInstance(String section, XmlConfigFile config) {
    if (config.getNodeList("/solr/" + section, false).size() > 1)
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Multiple instances of " + section + " section found in solr.xml");
  }

  private static void failIfFound(XmlConfigFile config, XPathExpression xPath, String path) {
    if (config.getVal(xPath, path, false) != null) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Should not have found " + xPath +
          "\n. Please upgrade your solr.xml: https://lucene.apache.org/solr/guide/format-of-solr-xml.html");
    }
  }

  private static Properties loadProperties(XmlConfigFile config) {
    try {
      NodeInfo node = (NodeInfo) ((ArrayList) config.evaluate(config.tree, "solr", XPathConstants.NODESET)).get(0);

      ArrayList<NodeInfo> props = (ArrayList) SolrResourceLoader.getXPath().evaluate("property", node, XPathConstants.NODESET);
      Properties properties = config.getSubstituteProperties();
      int sz = props.size();
      for (int i = 0; i < sz; i++) {
        NodeInfo prop = props.get(i);
        properties.setProperty(DOMUtil.getAttr(prop, NAME),
            PropertiesUtil.substituteProperty(DOMUtil.getAttr(prop, "value"), null));
      }
      return properties;
    }
    catch (XPathExpressionException e) {
      log.warn("Error parsing solr.xml: {}", e.getMessage());
      return null;
    }
  }

  private static NamedList<Object> readNodeListAsNamedList(XmlConfigFile config, String path, String section) {
    List<NodeInfo> nodes = config.getNodeList(path, false);
    if (nodes == null) {
      return null;
    }
    return checkForDuplicates(section, DOMUtil.nodesToNamedList(nodes));
  }

  private static NamedList<Object> checkForDuplicates(String section, NamedList<Object> nl) {
    Set<String> keys = new HashSet<>();
    for (Map.Entry<String, Object> entry : nl) {
      if (!keys.add(entry.getKey()))
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
            section + " section of solr.xml contains duplicated '" + entry.getKey() + "'");
    }
    return nl;
  }

  private static int parseInt(String field, String value) {
    try {
      return Integer.parseInt(value);
    }
    catch (NumberFormatException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
          "Error parsing '" + field + "', value '" + value + "' cannot be parsed as int");
    }
  }

  private static NodeConfig fillSolrSection(NodeConfig.NodeConfigBuilder builder, NamedList<Object> nl) {

    for (Map.Entry<String, Object> entry : nl) {
      String name = entry.getKey();
      if (entry.getValue() == null)
        continue;
      String value = entry.getValue().toString();
      switch (name) {
        case "adminHandler":
          builder.setCoreAdminHandlerClass(value);
          break;
        case "collectionsHandler":
          builder.setCollectionsAdminHandlerClass(value);
          break;
        case "healthCheckHandler":
          builder.setHealthCheckHandlerClass(value);
          break;
        case "infoHandler":
          builder.setInfoHandlerClass(value);
          break;
        case "configSetsHandler":
          builder.setConfigSetsHandlerClass(value);
          break;
        case "coreRootDirectory":
          builder.setCoreRootDirectory(value);
          break;
        case "solrDataHome":
          builder.setSolrDataHome(value);
          break;
        case "maxBooleanClauses":
          builder.setBooleanQueryMaxClauseCount(parseInt(name, value));
          break;
        case "managementPath":
          builder.setManagementPath(value);
          break;
        case "sharedLib":
          builder.setSharedLibDirectory(value);
          break;
        case "configSetBaseDir":
          builder.setConfigSetBaseDirectory(value);
          break;
        case "shareSchema":
          builder.setUseSchemaCache(Boolean.parseBoolean(value));
          break;
        case "coreLoadThreads":
          builder.setCoreLoadThreads(parseInt(name, value));
          break;
        case "replayUpdatesThreads":
          builder.setReplayUpdatesThreads(parseInt(name, value));
          break;
        default:
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Unknown configuration value in solr.xml: " + name);
      }
    }

    return builder.build();
  }

  private static UpdateShardHandlerConfig loadUpdateConfig(NamedList<Object> nl, boolean alwaysDefine) {

    if (nl == null && !alwaysDefine)
      return null;

    if (nl == null)
      return UpdateShardHandlerConfig.DEFAULT;

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
    if (mns != null)  {
      metricNameStrategy = mns.toString();
      defined = true;
    }

    Object mrt = nl.remove("maxRecoveryThreads");
    if (mrt != null)  {
      maxRecoveryThreads = parseInt("maxRecoveryThreads", mrt.toString());
      defined = true;
    }

    if (!defined && !alwaysDefine)
      return null;

    return new UpdateShardHandlerConfig(maxUpdateConnections, maxUpdateConnectionsPerHost, distributedSocketTimeout,
                                        distributedConnectionTimeout, metricNameStrategy, maxRecoveryThreads);

  }

  private static String removeValue(NamedList<Object> nl, String key) {
    Object value = nl.remove(key);
    if (value == null)
      return null;
    return value.toString();
  }

  private static String required(String section, String key, String value) {
    if (value != null)
      return value;
    throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, section + " section missing required entry '" + key + "'");
  }

  private static CloudConfig fillSolrCloudSection(NamedList<Object> nl) {

    String hostName = required("solrcloud", "host", removeValue(nl, "host"));
    int hostPort = parseInt("hostPort", required("solrcloud", "hostPort", removeValue(nl, "hostPort")));
    String hostContext = required("solrcloud", "hostContext", removeValue(nl, "hostContext"));

    CloudConfig.CloudConfigBuilder builder = new CloudConfig.CloudConfigBuilder(hostName, hostPort, hostContext);

    for (Map.Entry<String, Object> entry : nl) {
      String name = entry.getKey();
      if (entry.getValue() == null)
        continue;
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
        default:
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Unknown configuration parameter in <solrcloud> section of solr.xml: " + name);
      }
    }

    return builder.build();
  }

  private static LogWatcherConfig loadLogWatcherConfig(XmlConfigFile config, String loggingPath, String watcherPath) {

    String loggingClass = null;
    boolean enabled = true;
    int watcherQueueSize = 50;
    String watcherThreshold = null;

    for (Map.Entry<String, Object> entry : readNodeListAsNamedList(config, loggingPath, "<logging>")) {
      String name = entry.getKey();
      String value = entry.getValue().toString();
      switch (name) {
        case "class":
          loggingClass = value; break;
        case "enabled":
          enabled = Boolean.parseBoolean(value); break;
        default:
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Unknown value in logwatcher config: " + name);
      }
    }

    for (Map.Entry<String, Object> entry : readNodeListAsNamedList(config, watcherPath, "<watcher>")) {
      String name = entry.getKey();
      String value = entry.getValue().toString();
      switch (name) {
        case "size":
          watcherQueueSize = parseInt(name, value); break;
        case "threshold":
          watcherThreshold = value; break;
        default:
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Unknown value in logwatcher config: " + name);
      }
    }

    return new LogWatcherConfig(enabled, loggingClass, watcherThreshold, watcherQueueSize);

  }

  private static PluginInfo getShardHandlerFactoryPluginInfo(XmlConfigFile config) {
    NodeInfo node = config.getNode(SolrResourceLoader.configXpathExpressions.shardHandlerFactoryExp, ConfigXpathExpressions.shardHandlerFactoryPath, false);
    return (node == null) ? null : new PluginInfo(node, "shardHandlerFactory", false, true);
  }

  private static PluginInfo[] getBackupRepositoryPluginInfos(XmlConfigFile config) {
    ArrayList<NodeInfo> nodes = (ArrayList) config.evaluate(config.tree, "solr/backup/repository", XPathConstants.NODESET);
    if (nodes == null || nodes.size() == 0)
      return EMPTY_PLUGIN_INFOS;
    PluginInfo[] configs = new PluginInfo[nodes.size()];
    for (int i = 0; i < nodes.size(); i++) {
      configs[i] = new PluginInfo(nodes.get(i), "BackupRepositoryFactory", true, true);
    }
    return configs;
  }

  private static MetricsConfig getMetricsConfig(XmlConfigFile config) throws XPathExpressionException {
    MetricsConfig.MetricsConfigBuilder builder = new MetricsConfig.MetricsConfigBuilder();
    SolrResourceLoader loader = config.getResourceLoader();
    NodeInfo node = config.getNode(SolrResourceLoader.configXpathExpressions.counterExp, ConfigXpathExpressions.counterExpPath, false);
    if (node != null) {
      builder = builder.setCounterSupplier(new PluginInfo(node, "counterSupplier", false, false));
    }
    node = config.getNode(SolrResourceLoader.configXpathExpressions.meterExp, ConfigXpathExpressions.meterPath, false);
    if (node != null) {
      builder = builder.setMeterSupplier(new PluginInfo(node, "meterSupplier", false, false));
    }
    node = config.getNode(SolrResourceLoader.configXpathExpressions.timerExp, ConfigXpathExpressions.timerPath, false);
    if (node != null) {
      builder = builder.setTimerSupplier(new PluginInfo(node, "timerSupplier", false, false));
    }
    node = config.getNode(SolrResourceLoader.configXpathExpressions.histoExp, ConfigXpathExpressions.histoPath, false);
    if (node != null) {
      builder = builder.setHistogramSupplier(new PluginInfo(node, "histogramSupplier", false, false));
    }
    Set<String> hiddenSysProps = getHiddenSysProps(config);
    return builder
        .setHiddenSysProps(hiddenSysProps)
        .build();
  }

  private static Set<String> getHiddenSysProps(XmlConfigFile config) {
    ArrayList<NodeInfo> nodes = (ArrayList) config.evaluate(config.tree, "solr/metrics/hiddenSysProps/str", XPathConstants.NODESET);
    if (nodes == null || nodes.size() == 0) {
      return NodeConfig.NodeConfigBuilder.DEFAULT_HIDDEN_SYS_PROPS;
    }
    Set<String> props = new HashSet<>(nodes.size());
    for (int i = 0; i < nodes.size(); i++) {
      String prop = DOMUtil.getText(nodes.get(i));
      if (prop != null && !prop.trim().isEmpty()) {
        props.add(prop.trim());
      }
    }
    if (props.isEmpty()) {
      return NodeConfig.NodeConfigBuilder.DEFAULT_HIDDEN_SYS_PROPS;
    } else {
      return props;
    }
  }

  private static PluginInfo getTracerPluginInfo(XmlConfigFile config) {
    NodeInfo node = config.getNode(SolrResourceLoader.configXpathExpressions.tracerConfigExp, ConfigXpathExpressions.tracerConfigPath, false);
    return (node == null) ? null : new PluginInfo(node, "tracerConfig", false, true);
  }
}
