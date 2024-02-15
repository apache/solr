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
package org.apache.solr.cloud;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.lang.invoke.MethodHandles;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.solr.common.SolrException;
import org.apache.solr.servlet.SolrDispatchFilter;
import org.apache.solr.util.AddressUtils;
import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.quorum.QuorumPeer;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.apache.zookeeper.server.quorum.QuorumPeerMain;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SolrZkServer {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final String ZK_WHITELIST_PROPERTY = "zookeeper.4lw.commands.whitelist";

  String zkRun;
  String zkHost;

  int solrPort;
  Properties props;
  SolrZkServerProps zkProps;

  private Thread zkThread; // the thread running a zookeeper server, only if zkRun is set

  private File dataHome; // o.a.zookeeper.**.QuorumPeerConfig needs a File not a Path
  private String confHome;

  public SolrZkServer(String zkRun, String zkHost, File dataHome, String confHome, int solrPort) {
    this.zkRun = zkRun;
    this.zkHost = zkHost;
    this.dataHome = dataHome;
    this.confHome = confHome;
    this.solrPort = solrPort;
  }

  public String getClientString() {
    if (zkHost != null) return zkHost;

    if (zkProps == null) return null;

    // if the string wasn't passed as zkHost, then use the standalone server we started
    if (zkRun == null) return null;

    InetSocketAddress addr = zkProps.getClientPortAddress();
    String hostName;
    // We cannot advertise 0.0.0.0, so choose the best host to advertise
    // (the same that the Solr Node defaults to)
    if (addr.getAddress().isAnyLocalAddress()) {
      hostName = AddressUtils.getHostToAdvertise();
    } else {
      hostName = addr.getAddress().getHostAddress();
    }
    return hostName + ":" + addr.getPort();
  }

  public void parseConfig() {
    if (zkProps == null) {
      zkProps = new SolrZkServerProps();
      // set default data dir
      // TODO: use something based on IP+port???  support ensemble all from same solr home?
      zkProps.setDataDir(dataHome);
      zkProps.zkRun = zkRun;
      zkProps.solrPort = Integer.toString(solrPort);
    }

    var zooCfgPath = Path.of(confHome).resolve("zoo.cfg");
    if (!Files.exists(zooCfgPath)) {
      log.info("Zookeeper configuration not found in {}, using built-in default", confHome);
      String solrInstallDir = System.getProperty(SolrDispatchFilter.SOLR_INSTALL_DIR_ATTRIBUTE);
      if (solrInstallDir == null) {
        throw new SolrException(
            SolrException.ErrorCode.SERVER_ERROR,
            "Could not find default zoo.cfg file due to missing "
                + SolrDispatchFilter.SOLR_INSTALL_DIR_ATTRIBUTE);
      }
      zooCfgPath = Path.of(solrInstallDir).resolve("server").resolve("solr").resolve("zoo.cfg");
    }

    try {
      props = SolrZkServerProps.getProperties(zooCfgPath);
      SolrZkServerProps.injectServers(props, zkRun, zkHost);
      // This is the address that the embedded Zookeeper will bind to. Like Solr, it defaults to
      // "127.0.0.1".
      props.setProperty(
          "clientPortAddress", System.getProperty("solr.zk.embedded.host", "127.0.0.1"));
      if (props.getProperty("clientPort") == null) {
        props.setProperty("clientPort", Integer.toString(solrPort + 1000));
      }
      zkProps.parseProperties(props);
    } catch (QuorumPeerConfig.ConfigException | IOException e) {
      if (zkRun != null) throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }
  }

  public Map<Long, QuorumPeer.QuorumServer> getServers() {
    return zkProps.getServers();
  }

  public void start() {
    if (zkRun == null) return;

    if (System.getProperty(ZK_WHITELIST_PROPERTY) == null) {
      System.setProperty(ZK_WHITELIST_PROPERTY, "ruok, mntr, conf");
    }
    AtomicReference<Exception> zkException = new AtomicReference<>();
    zkThread =
        new Thread(
            () -> {
              try {
                if (zkProps.getServers().size() > 1) {
                  QuorumPeerMain zkServer = new QuorumPeerMain();
                  zkServer.runFromConfig(zkProps);
                } else {
                  ServerConfig sc = new ServerConfig();
                  sc.readFrom(zkProps);
                  ZooKeeperServerMain zkServer = new ZooKeeperServerMain();
                  zkServer.runFromConfig(sc);
                }
                log.info("ZooKeeper Server exited.");
              } catch (Exception e) {
                log.error("ZooKeeper Server ERROR", e);
                zkException.set(e);
              }
            },
            "embeddedZkServer");

    if (zkProps.getServers().size() > 1) {
      if (log.isInfoEnabled()) {
        log.info(
            "STARTING EMBEDDED ENSEMBLE ZOOKEEPER SERVER at port {}, listening on host {}",
            zkProps.getClientPortAddress().getPort(),
            zkProps.getClientPortAddress().getAddress().getHostAddress());
      }
    } else {
      if (log.isInfoEnabled()) {
        log.info(
            "STARTING EMBEDDED ENSEMBLE ZOOKEEPER SERVER at port {}, listening on host {}",
            zkProps.getClientPortAddress().getPort(),
            zkProps.getClientPortAddress().getAddress().getHostAddress());
      }
    }

    log.warn(
        "Embedded Zookeeper is not recommended in production environments. See Reference Guide for details.");

    zkThread.setDaemon(true);
    zkThread.start();
    try {
      // We don't have any way to hook into the ZK server object to check that it is running, so we
      // just wait and hope
      Thread.sleep(500); // pause for ZooKeeper to start
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR,
          "Interrupted while starting embedded zookeeper server",
          e);
    }

    if (zkException.get() != null) {
      log.info("Embedded ZK dataHome={}", dataHome);
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR,
          "Could not start embedded zookeeper server",
          zkException.get());
    }
  }

  public void stop() {
    if (zkRun == null) return;
    zkThread.interrupt();
  }
}

// Allows us to set a default for the data dir before parsing
// zoo.cfg (which validates that there is a dataDir)
class SolrZkServerProps extends QuorumPeerConfig {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  String solrPort; // port that Solr is listening on
  String zkRun;

  /**
   * Parse a ZooKeeper configuration file
   *
   * @param configPath the path of the configuration file
   * @throws IllegalArgumentException if a config file does not exist at the given path
   * @throws ConfigException error processing configuration
   */
  public static Properties getProperties(Path configPath) throws ConfigException {
    log.info("Reading configuration from: {}", configPath);

    if (!Files.exists(configPath)) {
      throw new IllegalArgumentException(configPath + " file is missing");
    }

    try (Reader reader = Files.newBufferedReader(configPath)) {
      Properties cfg = new Properties();
      cfg.load(reader);
      return cfg;
    } catch (IOException | IllegalArgumentException e) {
      throw new ConfigException("Error processing " + configPath, e);
    }
  }

  // Adds server.x if they don't exist, based on zkHost if it does exist.
  // Given zkHost=localhost:1111,localhost:2222 this will inject
  // server.0=localhost:1112:1113
  // server.1=localhost:2223:2224
  public static void injectServers(Properties props, String zkRun, String zkHost) {

    // if clientPort not already set, use zkRun
    if (zkRun != null && props.getProperty("clientPort") == null) {
      int portIdx = zkRun.lastIndexOf(':');
      if (portIdx > 0) {
        String portStr = zkRun.substring(portIdx + 1);
        props.setProperty("clientPort", portStr);
      }
    }

    boolean hasServers = hasServers(props);

    if (!hasServers && zkHost != null) {
      int alg = Integer.parseInt(props.getProperty("electionAlg", "3").trim());
      String[] hosts = zkHost.split(",");
      int serverNum = 0;
      for (String hostAndPort : hosts) {
        hostAndPort = hostAndPort.trim();
        int portIdx = hostAndPort.lastIndexOf(':');
        String clientPortStr = hostAndPort.substring(portIdx + 1);
        int clientPort = Integer.parseInt(clientPortStr);
        String host = hostAndPort.substring(0, portIdx);

        String serverStr = host + ':' + (clientPort + 1);
        // zk leader election algorithms other than 0 need an extra port for leader election.
        if (alg != 0) {
          serverStr = serverStr + ':' + (clientPort + 2);
        }

        props.setProperty("server." + serverNum, serverStr);
        serverNum++;
      }
    }
  }

  public static boolean hasServers(Properties props) {
    for (Object key : props.keySet()) if (((String) key).startsWith("server.")) return true;
    return false;
  }

  public void setDataDir(File dataDir) {
    this.dataDir = dataDir;
  }

  /**
   * Parse config from a Properties.
   *
   * @param zkProp Properties to parse from.
   */
  @Override
  public void parseProperties(Properties zkProp) throws IOException, ConfigException {
    super.parseProperties(zkProp);
  }
}
