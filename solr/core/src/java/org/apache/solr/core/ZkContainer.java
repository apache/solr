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

import static org.apache.solr.common.cloud.ZkStateReader.HTTPS;
import static org.apache.solr.common.cloud.ZkStateReader.HTTPS_PORT_PROP;

import io.opentelemetry.api.common.Attributes;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;
import org.apache.solr.client.solrj.impl.SolrZkClientTimeout;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterProperties;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.cloud.ZooKeeperException;
import org.apache.solr.common.util.EnvUtils;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.logging.MDCLoggingContext;
import org.apache.solr.metrics.SolrMetricProducer;
import org.apache.solr.metrics.SolrMetricsContext;
import org.apache.solr.metrics.otel.OtelUnit;
import org.apache.solr.servlet.CoreContainerProvider;
import org.apache.solr.util.AddressUtils;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.admin.ZooKeeperAdmin;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.embedded.ZooKeeperServerEmbedded;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Used by {@link CoreContainer} to hold ZooKeeper / SolrCloud info, especially {@link
 * ZkController}. Mainly it does some ZK initialization, and ensures a loading core registers in ZK.
 * Even when in standalone mode, perhaps surprisingly, an instance of this class exists. If {@link
 * #getZkController()} returns null then we're in standalone mode.
 */
public class ZkContainer {
  // NOTE DWS: It's debatable if this in-between class is needed instead of folding it all into
  // ZkController. ZKC is huge though.

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final String ZK_WHITELIST_PROPERTY = "zookeeper.4lw.commands.whitelist";
  public static final String ZK_MAX_CNXNS_PROPERTY = "zookeeper.maxCnxns";
  // Per ZooKeeper, "0" means no limit for max client connections.
  public static final String ZK_MAX_CNXNS_DEFAULT = "0";

  // How long to wait for a quick TCP probe when deciding whether a zkHost peer is already
  // serving (and thus whether we should join its ensemble via reconfig instead of bootstrapping).
  private static final long ZK_JOIN_PROBE_TIMEOUT_MS =
      EnvUtils.getPropertyAsLong("solr.zookeeper.server.join.probeTimeoutMs", 1500L);
  // How long to wait for our ZooKeeperAdmin client to connect to the existing ensemble when
  // joining it.
  private static final long ZK_JOIN_CONNECT_TIMEOUT_MS =
      EnvUtils.getPropertyAsLong("solr.zookeeper.server.join.connectTimeoutMs", 15000L);

  protected ZkController zkController;

  private volatile ZooKeeperServerEmbedded zkServerEmbedded;

  private ExecutorService coreZkRegister =
      ExecutorUtil.newMDCAwareCachedThreadPool(new SolrNamedThreadFactory("coreZkRegister"));

  private SolrMetricProducer metricProducer;

  public ZkContainer() {}

  public void initZooKeeper(final CoreContainer cc, CloudConfig config) {
    // zkServerEnabled is set whenever in solrCloud mode ('-c') but no explicit zkHost/ZK_HOST is
    // provided.
    final boolean zkServerEnabled =
        EnvUtils.getPropertyAsBool("solr.zookeeper.server.enabled", false);
    boolean zkQuorumNode = false;
    if (NodeRoles.MODE_ON.equals(cc.nodeRoles.getRoleMode(NodeRoles.Role.ZOOKEEPER_QUORUM))) {
      zkQuorumNode = true;
      log.info("Starting node in ZooKeeper Quorum role.");
    }

    if (zkServerEnabled && config == null) {
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR,
          "Cannot start Solr in cloud mode - no cloud config provided");
    }

    if (config == null) {
      log.info("Solr is running in standalone mode");
      return;
    }

    final boolean runAsQuorum = config.getZkHost() != null && zkQuorumNode;

    String zookeeperHost = config.getZkHost();
    final var solrHome = cc.getSolrHome();
    // runAsQuorum takes precedence: the zookeeper_quorum node role alone is sufficient to start
    // embedded ZK in quorum mode — no need to also set solr.zookeeper.server.enabled=true.
    // zkServerEnabled is only used for legacy standalone embedded ZK (bin/solr -c without ZK_HOST).
    if (runAsQuorum) {
      // ZK hardcodes /zookeeper/config (the dynamic membership znode) to a read-only ACL
      // regardless of our own ACL provider (ZOOKEEPER-2014), so an anonymous client can never
      // pass its authorization check for dynamic reconfiguration (used when a new node joins an
      // already-running ensemble below). Since this quorum already runs fully open
      // (OPEN_ACL_UNSAFE on every znode we control), skipACL just extends that same no-auth
      // posture to this one ZK-managed node too, rather than adding real security. This must be
      // set on every quorum member from its own startup — it's a JVM-wide, read-once-at-class-load
      // property, so a node that booted before this was set can't pick it up without restarting.
      Object skipAclPrev = System.getProperties().putIfAbsent("zookeeper.skipACL", "yes");
      if (skipAclPrev == null) {
        log.warn(
            "Setting system property zookeeper.skipACL=yes for the lifetime of this JVM so"
                + " dynamic ZK reconfiguration (used when another node later joins this quorum)"
                + " can succeed. This disables ACL enforcement for ANY ZooKeeper server or client"
                + " sharing this JVM, not just this quorum node -- avoid running other"
                + " security-sensitive ZK usage in the same process.");
      } else if (!"yes".equals(skipAclPrev)) {
        log.warn(
            "zookeeper.skipACL is already set to '{}' (not 'yes'); dynamic reconfiguration for"
                + " this quorum will fail with NoAuthException if another node later tries to"
                + " join it.",
            skipAclPrev);
      }

      // ZooKeeperServerEmbedded being used under the covers.
      // When running multiple quorum nodes on the same machine sharing a solrHome,
      // override the default data and config directories with unique ones
      final int zkClientPortOffset =
          EnvUtils.getPropertyAsInteger("solr.zookeeper.server.client.port.offset", 1000);
      final int zkQuorumPortOffset =
          EnvUtils.getPropertyAsInteger("solr.zookeeper.server.quorum.port.offset", 1010);
      final int zkElectionPortOffset =
          EnvUtils.getPropertyAsInteger("solr.zookeeper.server.election.port.offset", 1020);
      final int zkPort = config.getSolrHostPort() + zkClientPortOffset;
      final var zkDataDir =
          solrHome.resolve(EnvUtils.getProperty("solr.zookeeper.server.datadir", "zoo_data"));
      final var zkConfDir =
          solrHome.resolve(EnvUtils.getProperty("solr.zookeeper.server.confdir", ""));

      // Populate a zoo.cfg
      final String zooCfgTemplate =
          """
              tickTime=2000
              initLimit=10
              syncLimit=5
              dataDir=@@DATA_DIR@@
              4lw.commands.whitelist=mntr,conf,ruok
              admin.enableServer=false
              reconfigEnabled=true
              clientPort=@@ZK_CLIENT_PORT@@
              """;
      String zooCfgContents =
          zooCfgTemplate
              .replace("@@DATA_DIR@@", zkDataDir.toString())
              .replace("@@ZK_CLIENT_PORT@@", String.valueOf(zkPort));
      // A chroot (if any) is appended once to the whole connect string, e.g.
      // "host1:9983,host2:9983,host3:9983/solr" — strip it before splitting into per-host
      // entries, matching the stripChroot() behavior of the SolrZkServer this replaces.
      final String zkHostNoChroot = stripChroot(config.getZkHost());
      final String[] zkHosts = zkHostNoChroot.split(",");
      // TODO: myId detection uses exact string matching between config.getHost() and the host
      // portion of zkHost entries. This fails when zkHost uses "localhost" but Solr is
      // configured with "127.0.0.1" (or vice versa). Consider resolving hostnames/IPs before
      // matching, or matching by port alone (which is unique per node in a single-machine setup).
      final String targetConnStringSection = config.getHost() + ":" + zkPort;
      if (log.isInfoEnabled()) {
        log.info(
            "Trying to match {} against zkHostString {} to determine myid",
            targetConnStringSection,
            zkHostNoChroot);
      }

      final List<ZkQuorumPeer> peers = new ArrayList<>();
      int myId = -1;
      for (int i = 0; i < zkHosts.length; i++) {
        final String host = zkHosts[i].trim();
        final var hostComponents = host.split(":");
        if (hostComponents.length < 2) {
          throw new IllegalStateException(
              "Invalid zkHost entry (expected 'host:port'): '" + host + "'");
        }
        final var zkServer = hostComponents[0];
        final int zkClientPort;
        try {
          zkClientPort = Integer.parseInt(hostComponents[1]);
        } catch (NumberFormatException e) {
          throw new IllegalStateException(
              "Invalid port in zkHost entry '" + host + "': " + hostComponents[1], e);
        }
        final boolean isSelf = targetConnStringSection.equals(host);
        if (isSelf) {
          myId = i + 1;
        }
        // Derive quorum/election ports from the peer's ZK client port using the same offsets.
        // All nodes are assumed to use the same offset configuration.
        final var zkQuorumPort = zkClientPort - zkClientPortOffset + zkQuorumPortOffset;
        final var zkLeaderPort = zkClientPort - zkClientPortOffset + zkElectionPortOffset;
        peers.add(new ZkQuorumPeer(zkServer, zkClientPort, zkQuorumPort, zkLeaderPort, isSelf));
      }

      if (myId == -1) {
        throw new IllegalStateException(
            "Unable to determine ZK 'myid' for target " + targetConnStringSection);
      }

      // If any OTHER peer in zkHost is already a genuinely serving ensemble member, we're
      // joining an ensemble that's already up and running (e.g. growing it later) rather than
      // cold-bootstrapping a brand-new one alongside our peers (the typical case when all nodes
      // in zkHost start up together). Detection and join are done as a single atomic attempt per
      // peer -- a cold-starting sibling may have its ZK port open yet still be alone and unable
      // to serve requests (no leader elected among its own static peers yet), so a plain "is the
      // port open" probe can't reliably tell bootstrap and join apart; only a successful
      // getConfig() call (which requires a functioning, leader-having server) can.
      final ZkQuorumPeer self = peers.get(myId - 1);

      try {
        Files.createDirectories(zkDataDir);

        ZkJoinResult joined = null;
        for (ZkQuorumPeer peer : peers) {
          if (peer.isSelf()) {
            continue;
          }
          joined = tryJoinExistingEnsemble(peer, self);
          if (joined != null) {
            break;
          }
        }

        if (joined != null) {
          zooCfgContents = zooCfgContents + joined.serverLines();
          myId = joined.myId();
        } else {
          for (int i = 0; i < peers.size(); i++) {
            final ZkQuorumPeer peer = peers.get(i);
            zooCfgContents =
                zooCfgContents
                    + "server."
                    + (i + 1)
                    + "="
                    + peer.host()
                    + ":"
                    + peer.quorumPort()
                    + ":"
                    + peer.leaderPort()
                    + "\n";
          }
        }

        Files.writeString(zkDataDir.resolve("myid"), String.valueOf(myId));
        Properties zkProps = new Properties();
        zkProps.load(new StringReader(zooCfgContents));
        startZooKeeperServerEmbedded(zkPort, zkConfDir, zkProps);
      } catch (Exception e) {
        throw new ZooKeeperException(
            SolrException.ErrorCode.SERVER_ERROR,
            "IOException bootstrapping zk quorum instance",
            e);
      }
    } else if (zkServerEnabled) {
      try {
        final Path zkDataHome =
            solrHome.resolve(EnvUtils.getProperty("solr.zookeeper.server.datadir", "zoo_data"));
        final Path zkConfHome =
            solrHome.resolve(
                EnvUtils.getProperty("solr.zookeeper.server.confdir", solrHome.toString()));

        Path zooCfgPath = zkConfHome.resolve("zoo.cfg");
        if (!Files.exists(zooCfgPath)) {
          log.info("Zookeeper configuration not found in {}, using built-in default", zkConfHome);
          String solrInstallDir = System.getProperty(CoreContainerProvider.SOLR_INSTALL_DIR);
          if (solrInstallDir == null) {
            throw new SolrException(
                SolrException.ErrorCode.SERVER_ERROR,
                "Could not find default zoo.cfg file due to missing "
                    + CoreContainerProvider.SOLR_INSTALL_DIR);
          }
          zooCfgPath = Path.of(solrInstallDir).resolve("server").resolve("solr").resolve("zoo.cfg");
        }

        final Properties zkProps = new Properties();
        try (Reader reader = Files.newBufferedReader(zooCfgPath)) {
          zkProps.load(reader);
        }
        if (zkProps.getProperty("dataDir") == null) {
          zkProps.setProperty("dataDir", zkDataHome.toString());
        }
        final String clientPortAddress =
            EnvUtils.getProperty("solr.zookeeper.embedded.host", "127.0.0.1");
        zkProps.setProperty("clientPortAddress", clientPortAddress);
        if (zkProps.getProperty("clientPort") == null) {
          zkProps.setProperty("clientPort", Integer.toString(config.getSolrHostPort() + 1000));
        }
        if (System.getProperty(ZK_WHITELIST_PROPERTY) == null) {
          System.setProperty(ZK_WHITELIST_PROPERTY, "ruok, mntr, conf");
        }

        Files.createDirectories(zkDataHome);
        final int zkPort = Integer.parseInt(zkProps.getProperty("clientPort"));
        startZooKeeperServerEmbedded(zkPort, zkDataHome, zkProps);

        if (zookeeperHost == null) {
          // We cannot advertise 0.0.0.0, so choose the best host to advertise
          // (the same that the Solr Node defaults to)
          final String hostName =
              "0.0.0.0".equals(clientPortAddress)
                  ? AddressUtils.getHostToAdvertise()
                  : clientPortAddress;
          zookeeperHost = hostName + ":" + zkPort;
        }
      } catch (Exception e) {
        throw new ZooKeeperException(
            SolrException.ErrorCode.SERVER_ERROR,
            "Exception starting embedded zookeeper server",
            e);
      }
    }

    int zkClientConnectTimeout = SolrZkClientTimeout.DEFAULT_ZK_CONNECT_TIMEOUT;

    if (zookeeperHost != null) {

      // we are ZooKeeper enabled
      try {
        // If this is an ensemble, allow for a long connect time for other servers to come up
        if (runAsQuorum) {
          zkClientConnectTimeout = 24 * 60 * 60 * 1000; // 1 day for embedded quorum
          log.info("Zookeeper client={} (quorum mode)  Waiting for a quorum.", zookeeperHost);
        } else {
          log.info("Zookeeper client={}", zookeeperHost);
        }
        boolean createRoot = EnvUtils.getPropertyAsBool("solr.zookeeper.chroot.create", false);

        if (!ZkController.checkChrootPath(zookeeperHost, createRoot)) {
          throw new ZooKeeperException(
              SolrException.ErrorCode.SERVER_ERROR,
              "A chroot was specified in ZkHost but the znode doesn't exist. " + zookeeperHost);
        }

        ZkController zkController =
            new ZkController(cc, zookeeperHost, zkClientConnectTimeout, config);

        if (zkServerEnabled || runAsQuorum) {
          if (StrUtils.isNotNullOrEmpty(EnvUtils.getProperty(HTTPS_PORT_PROP))) {
            // Embedded ZK and probably running with SSL
            new ClusterProperties(zkController.getZkClient())
                .setClusterProperty(ZkStateReader.URL_SCHEME, HTTPS);
          }
        }

        this.zkController = zkController;

        metricProducer =
            new SolrMetricProducer() {
              SolrMetricsContext ctx;

              @Override
              public void initializeMetrics(
                  SolrMetricsContext parentContext, Attributes attributes) {
                ctx = parentContext.getChildContext(this);

                var metricsListener = zkController.getZkClient().getMetrics();

                ctx.observableLongCounter(
                    "solr.zk.ops",
                    "Total number of ZooKeeper operations",
                    measurement -> {
                      measurement.record(
                          metricsListener.getReads(),
                          attributes.toBuilder().put(OPERATION_ATTR, "read").build());
                      measurement.record(
                          metricsListener.getDeletes(),
                          attributes.toBuilder().put(OPERATION_ATTR, "delete").build());
                      measurement.record(
                          metricsListener.getWrites(),
                          attributes.toBuilder().put(OPERATION_ATTR, "write").build());
                      measurement.record(
                          metricsListener.getMultiOps(),
                          attributes.toBuilder().put(OPERATION_ATTR, "multi").build());
                      measurement.record(
                          metricsListener.getExistsChecks(),
                          attributes.toBuilder().put(OPERATION_ATTR, "exists").build());
                    });

                ctx.observableLongCounter(
                    "solr.zk.read",
                    "Total bytes read from ZooKeeper",
                    measurement -> {
                      measurement.record(metricsListener.getBytesRead(), attributes);
                    },
                    OtelUnit.BYTES);

                ctx.observableLongCounter(
                    "solr.zk.watches_fired",
                    "Total number of ZooKeeper watches fired",
                    measurement -> {
                      measurement.record(metricsListener.getWatchesFired(), attributes);
                    });

                ctx.observableLongCounter(
                    "solr.zk.written",
                    "Total bytes written to ZooKeeper",
                    measurement -> {
                      measurement.record(metricsListener.getBytesWritten(), attributes);
                    },
                    OtelUnit.BYTES);

                ctx.observableLongCounter(
                    "solr.zk.cumulative.multi_ops.total",
                    "Total cumulative multi-operations count",
                    measurement -> {
                      measurement.record(metricsListener.getCumulativeMultiOps(), attributes);
                    });

                ctx.observableLongCounter(
                    "solr.zk.get_children.ops",
                    "Total number of ZooKeeper getChildren calls",
                    measurement -> {
                      measurement.record(metricsListener.getChildFetches(), attributes);
                    });
              }

              @Override
              public SolrMetricsContext getSolrMetricsContext() {
                return ctx;
              }
            };
      } catch (InterruptedException e) {
        // Restore the interrupted status
        Thread.currentThread().interrupt();
        log.error("", e);
        throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR, "", e);
      } catch (TimeoutException e) {
        log.error("Could not connect to ZooKeeper", e);
        throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR, "", e);
      } catch (IOException | KeeperException e) {
        log.error("", e);
        throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR, "", e);
      }
    }
  }

  private void startZooKeeperServerEmbedded(int port, Path zkHomeDir, Properties p)
      throws Exception {
    ensureZkMaxCnxnsConfigured();
    zkServerEmbedded =
        ZooKeeperServerEmbedded.builder().baseDir(zkHomeDir).configuration(p).build();
    zkServerEmbedded.start();
    log.info("Started embedded ZooKeeper server on port {}", port);
  }

  /** Strips a trailing chroot (e.g. "host:2181/solr" -&gt; "host:2181") from a zkHost string. */
  private static String stripChroot(String zkHost) {
    if (zkHost == null || zkHost.trim().isEmpty() || zkHost.lastIndexOf('/') < 0) {
      return zkHost;
    }
    return zkHost.substring(0, zkHost.lastIndexOf('/'));
  }

  /** One entry parsed out of the quorum's zkHost connection string. */
  private record ZkQuorumPeer(
      String host, int clientPort, int quorumPort, int leaderPort, boolean isSelf) {}

  /** Outcome of joining an already-running ensemble via dynamic reconfiguration. */
  private record ZkJoinResult(int myId, String serverLines) {}

  /**
   * Attempts, in one atomic step, to detect whether {@code contact} is a genuinely live,
   * already-serving ensemble member and, if so, join it by asking it (via dynamic
   * reconfiguration) to add us as a new voting member. Returns {@code null} if {@code contact}
   * isn't reachable, or is reachable but not actually serving yet (e.g. a sibling that's itself
   * mid cold-bootstrap, with no leader elected among its own static peers) -- the caller should
   * treat that the same as "no live ensemble to join" and fall back to cold-bootstrapping
   * alongside every peer in zkHost instead. Requires the existing ensemble's members to have been
   * started with {@code reconfigEnabled=true} (the default for quorum nodes started by this
   * class).
   */
  private ZkJoinResult tryJoinExistingEnsemble(ZkQuorumPeer contact, ZkQuorumPeer self)
      throws Exception {
    final String connectString = contact.host() + ":" + contact.clientPort();
    final CountDownLatch connected = new CountDownLatch(1);
    final ZooKeeperAdmin admin =
        new ZooKeeperAdmin(
            connectString,
            (int) ZK_JOIN_CONNECT_TIMEOUT_MS, // ZK session timeout, once connected
            event -> {
              if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
                connected.countDown();
              }
            });
    try {
      // Bound how long we wait to find out whether this peer is reachable at all right now --
      // kept short so cold-bootstrap (where siblings aren't up yet) isn't needlessly delayed; a
      // genuinely live ensemble member will typically respond almost immediately.
      if (!connected.await(ZK_JOIN_PROBE_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
        return null;
      }

      final String currentConfig;
      try {
        currentConfig = new String(admin.getConfig(false, new Stat()), StandardCharsets.UTF_8);
      } catch (Exception e) {
        // Connected, but couldn't read the dynamic config -- most likely this peer is itself
        // mid cold-bootstrap and hasn't elected a leader yet, so it can't service requests.
        if (log.isDebugEnabled()) {
          log.debug(
              "Peer {} is reachable but not serving yet, not joining it: {}",
              connectString,
              e.toString());
        }
        return null;
      }

      // If we're already a member of this ensemble's current config -- e.g. restarting after
      // being stopped, with the same host/ports as before -- just reconnect as that existing
      // member rather than asking to be added as a new one. Asking to be added again is
      // unnecessary (our identity hasn't changed) and can be flatly rejected by ZK with
      // NewConfigNoQuorum if not enough of the other members happen to be synced at that exact
      // moment to approve a membership change.
      Integer existingId = findExistingServerId(currentConfig, self);
      if (existingId != null) {
        log.info(
            "Rejoining existing ZK ensemble at {} as already-configured server.{}",
            connectString,
            existingId);
        return new ZkJoinResult(existingId, extractServerLines(currentConfig));
      }

      int maxId = 0;
      for (String line : currentConfig.split("\n")) {
        if (line.startsWith("server.")) {
          maxId = Math.max(maxId, Integer.parseInt(line.substring(7, line.indexOf('='))));
        }
      }
      final int newId = maxId + 1;
      final String joiningServer =
          "server."
              + newId
              + "="
              + self.host()
              + ":"
              + self.quorumPort()
              + ":"
              + self.leaderPort()
              + ":participant;"
              + self.clientPort();

      log.info("Joining existing ZK ensemble at {} as server.{}", connectString, newId);
      final byte[] merged = admin.reconfigure(List.of(joiningServer), null, null, -1, new Stat());
      return new ZkJoinResult(newId, extractServerLines(new String(merged, StandardCharsets.UTF_8)));
    } finally {
      try {
        admin.close();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }

  /**
   * Returns the server id {@code self} is already configured under in a ZK dynamic-config string
   * (matched by host/quorumPort/leaderPort), or {@code null} if it isn't a member yet.
   */
  private static Integer findExistingServerId(String config, ZkQuorumPeer self) {
    final String hostAndPorts = self.host() + ":" + self.quorumPort() + ":" + self.leaderPort();
    for (String line : config.split("\n")) {
      if (!line.startsWith("server.")) {
        continue;
      }
      int eq = line.indexOf('=');
      if (eq < 0) {
        continue;
      }
      // rhs is "host:quorumPort:leaderPort[:role];clientPort"
      String hostPortsSection = line.substring(eq + 1).split(";", 2)[0];
      if (hostPortsSection.equals(hostAndPorts)
          || hostPortsSection.startsWith(hostAndPorts + ":")) {
        return Integer.parseInt(line.substring(7, eq));
      }
    }
    return null;
  }

  /** Extracts just the {@code server.N=...} lines from a ZK dynamic-config string. */
  private static String extractServerLines(String config) {
    StringBuilder serverLines = new StringBuilder();
    for (String line : config.split("\n")) {
      if (line.startsWith("server.")) {
        serverLines.append(line).append('\n');
      }
    }
    return serverLines.toString();
  }

  public static void ensureZkMaxCnxnsConfigured() {
    System.getProperties().putIfAbsent(ZK_MAX_CNXNS_PROPERTY, ZK_MAX_CNXNS_DEFAULT);
  }

  public static volatile Predicate<CoreDescriptor> testing_beforeRegisterInZk;

  public void registerInZk(final SolrCore core, boolean background, boolean skipRecovery) {
    if (zkController == null) {
      return;
    }

    CoreDescriptor cd = core.getCoreDescriptor(); // save this here - the core may not have it later
    Runnable r =
        () -> {
          MDCLoggingContext.setCore(core);
          try {
            try {
              if (testing_beforeRegisterInZk != null) {
                boolean didTrigger = testing_beforeRegisterInZk.test(cd);
                if (log.isDebugEnabled()) {
                  log.debug("{} pre-zk hook", (didTrigger ? "Ran" : "Skipped"));
                }
              }
              if (!core.getCoreContainer().isShutDown()) {
                zkController.register(core.getName(), cd, skipRecovery);
              }
            } catch (InterruptedException e) {
              // Restore the interrupted status
              Thread.currentThread().interrupt();
              log.error("Interrupted", e);
            } catch (KeeperException e) {
              log.error("KeeperException registering core {}", core.getName(), e);
            } catch (IllegalStateException ignore) {

            } catch (Exception e) {
              log.error("Exception registering core {}", core.getName(), e);
              try {
                zkController.publish(cd, Replica.State.DOWN);
              } catch (InterruptedException e1) {
                Thread.currentThread().interrupt();
                log.error("Interrupted", e1);
              } catch (Exception e1) {
                log.error("Exception publishing down state for core {}", core.getName(), e1);
              }
            }
          } finally {
            MDCLoggingContext.clear();
          }
        };

    if (background) {
      coreZkRegister.execute(r);
    } else {
      r.run();
    }
  }

  public ZkController getZkController() {
    return zkController;
  }

  public void close() {

    try {
      ExecutorUtil.shutdownAndAwaitTermination(coreZkRegister);
    } finally {
      try {
        if (zkController != null) {
          zkController.close();
        }
      } finally {
        if (zkServerEmbedded != null) {
          try {
            zkServerEmbedded.close();
            // ZooKeeperServerEmbedded.close() is asynchronous: ZK's QuorumCnxManager
            // WorkerSender/WorkerReceiver threads may still be running. Wait up to 5s.
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (System.nanoTime() < deadline) {
              boolean quorumThreadsRunning =
                  Thread.getAllStackTraces().keySet().stream()
                      .anyMatch(
                          t ->
                              t.getName().startsWith("WorkerSender")
                                  || t.getName().startsWith("WorkerReceiver"));
              if (!quorumThreadsRunning) break;
              try {
                Thread.sleep(100);
              } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                break;
              }
            }
            log.info("Closed embedded ZooKeeper server");
          } catch (Exception e) {
            log.error("Error closing embedded ZooKeeper server", e);
          }
        }
      }
      IOUtils.closeQuietly(metricProducer);
    }
  }

  public ExecutorService getCoreZkRegisterExecutorService() {
    return coreZkRegister;
  }

  public SolrMetricProducer getZkMetricsProducer() {
    return this.metricProducer;
  }
}
