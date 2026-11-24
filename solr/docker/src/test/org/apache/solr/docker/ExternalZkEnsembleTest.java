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

package org.apache.solr.docker;

import static org.apache.solr.docker.DockerHelpers.HEAP;
import static org.apache.solr.docker.DockerHelpers.MEMORY_LIMIT_BYTES;
import static org.apache.solr.docker.DockerHelpers.STARTUP_TIMEOUT;
import static org.apache.solr.docker.DockerHelpers.assertExec;
import static org.apache.solr.docker.DockerHelpers.exec;
import static org.apache.solr.docker.DockerHelpers.firstGroup;
import static org.apache.solr.docker.DockerHelpers.imageOverride;
import static org.apache.solr.docker.DockerHelpers.requireProperty;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.lucene.tests.util.QuickPatchThreadsFilter;
import org.apache.solr.SolrIgnoredThreadsFilter;
import org.apache.solr.SolrTestCase;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.util.TimeOut;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.Container.ExecResult;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;

/**
 * System integration test for SolrCloud against an external, three-server ZooKeeper ensemble: the
 * deployment mode where ZooKeeper runs outside Solr, as opposed to Solr's embedded ZooKeeper.
 *
 * <p>Both ZooKeeper and Solr run in containers on one private Docker network, so the addresses
 * ZooKeeper advertises in its ensemble config ({@code zk1:2181}, ...) resolve for Solr just as they
 * would in a real deployment. Solr's ZooKeeper client follows that config, which rules out running
 * Solr in this JVM against containers reached through mapped ports.
 *
 * <p>Runs only from the {@code dockerExternalZkEnsembleTest} Gradle task, which builds the Solr
 * image from the working tree. The ZooKeeper image can be overridden with {@code SOLR_ZK_IMAGE}.
 */
// ThreadLeakFilters are not additive, so SolrTestCase's own two are repeated here.
@ThreadLeakFilters(
    defaultFilters = true,
    filters = {
      SolrIgnoredThreadsFilter.class,
      QuickPatchThreadsFilter.class,
      DockerHelpers.TestcontainersThreadsFilter.class
    })
public class ExternalZkEnsembleTest extends SolrTestCase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final String DEFAULT_ZK_IMAGE = "zookeeper:3.9";

  private static final int ZK_SERVERS = 3;
  private static final int ZK_CLIENT_PORT = 2181;
  private static final int SOLR_PORT = 8983;
  private static final String COLLECTION = "external-zk";

  /** Every request is sent from inside the container it targets, so loopback reaches its node. */
  private static final String LOCAL_SOLR = "http://localhost:" + SOLR_PORT;

  /** How long to wait for ZooKeeper to elect a leader, or for Solr to reconnect after one dies. */
  private static final long CONVERGE_TIMEOUT_SECONDS = 60;

  @Test
  public void solrCloudOnExternalZkEnsemble() {
    assumeTrue("Docker is not available", DockerClientFactory.instance().isDockerAvailable());
    String solrImage = requireProperty("solr.docker.imageId", "dockerExternalZkEnsembleTest");
    String zkImage = imageOverride("SOLR_ZK_IMAGE", DEFAULT_ZK_IMAGE);

    List<GenericContainer<?>> zks = new ArrayList<>();
    List<GenericContainer<?>> solrs = new ArrayList<>();
    boolean failed = true;
    try (Network network = Network.newNetwork()) {
      try {
        for (int id = 1; id <= ZK_SERVERS; id++) {
          zks.add(zkContainer(zkImage, network, id));
        }
        Startables.deepStart(zks).join();
        awaitZkLeader(zks);

        String zkHost = zkHost();
        solrs.add(solrContainer(solrImage, network, "solr1", zkHost));
        solrs.add(solrContainer(solrImage, network, "solr2", zkHost));
        Startables.deepStart(solrs).join();
        GenericContainer<?> solr1 = solrs.get(0);
        GenericContainer<?> solr2 = solrs.get(1);
        waitFor("2 live Solr nodes", () -> liveNodeCount(solr1) == 2);

        assertExec(
            exec(
                solr1,
                "solr",
                "create",
                "-c",
                COLLECTION,
                "--shards",
                "2",
                "--solr-url",
                LOCAL_SOLR));
        awaitHealthy(solr1);

        String status = zkStatus(solr1);
        assertEquals(status, "ensemble", jsonString(status, "mode"));
        assertEquals(status, ZK_SERVERS, jsonInt(status, "ensembleSize"));
        assertEquals(status, "green", jsonString(status, "status"));

        assertTrue("Could not index before-failover", tryIndex(solr1, "before-failover"));
        assertEquals(1, numFound(solr2));

        // Losing the leader leaves 2 of 3 servers, still a quorum, so Solr must carry on.
        GenericContainer<?> leader = zkLeader(zks);
        leader.stop();

        // Succeeds once the two survivors have elected a new leader and Solr has reconnected.
        waitFor(
            "indexing through solr2 after the ZooKeeper leader stopped",
            () -> tryIndex(solr2, "after-failover"));
        waitFor("both documents visible from solr1", () -> numFound(solr1) == 2);
        assertEquals(2, liveNodeCount(solr1));

        String degraded = zkStatus(solr1);
        assertEquals(degraded, ZK_SERVERS, jsonInt(degraded, "ensembleSize"));
        assertEquals(degraded, "yellow", jsonString(degraded, "status"));

        failed = false;
      } finally {
        if (failed) {
          logContainers(zks);
          logContainers(solrs);
        }
        solrs.forEach(GenericContainer::stop);
        zks.forEach(GenericContainer::stop);
      }
    }
  }

  @SuppressWarnings("resource")
  private static GenericContainer<?> zkContainer(String image, Network network, int id) {
    String servers =
        IntStream.rangeClosed(1, ZK_SERVERS)
            .mapToObj(i -> "server." + i + "=zk" + i + ":2888:3888;" + ZK_CLIENT_PORT)
            .collect(Collectors.joining(" "));
    return new GenericContainer<>(DockerImageName.parse(image))
        .withNetwork(network)
        .withNetworkAliases("zk" + id)
        .withEnv("ZOO_MY_ID", String.valueOf(id))
        .withEnv("ZOO_SERVERS", servers)
        // Solr's ZK Status page reads these four-letter-word commands.
        .withEnv("ZOO_4LW_COMMANDS_WHITELIST", "mntr,conf,ruok")
        .withExposedPorts(ZK_CLIENT_PORT)
        .withStartupTimeout(STARTUP_TIMEOUT)
        .waitingFor(Wait.forListeningPort());
  }

  @SuppressWarnings("resource")
  private static GenericContainer<?> solrContainer(
      String image, Network network, String name, String zkHost) {
    return new GenericContainer<>(DockerImageName.parse(image))
        .withNetwork(network)
        .withNetworkAliases(name)
        .withCommand(
            "solr",
            "start",
            "-f",
            "-m",
            HEAP,
            "--host",
            name,
            "-p",
            String.valueOf(SOLR_PORT),
            "-z",
            zkHost)
        .withExposedPorts(SOLR_PORT)
        .withStartupTimeout(STARTUP_TIMEOUT)
        .waitingFor(Wait.forListeningPort())
        .withCreateContainerCmdModifier(cmd -> cmd.getHostConfig().withMemory(MEMORY_LIMIT_BYTES));
  }

  private static String zkHost() {
    return IntStream.rangeClosed(1, ZK_SERVERS)
        .mapToObj(i -> "zk" + i + ":" + ZK_CLIENT_PORT)
        .collect(Collectors.joining(","));
  }

  /** The ZooKeeper container currently leading, according to {@code zkServer.sh status}. */
  private static GenericContainer<?> zkLeader(List<GenericContainer<?>> zks) {
    for (GenericContainer<?> zk : zks) {
      if (exec(zk, "zkServer.sh", "status").getStdout().contains("Mode: leader")) {
        return zk;
      }
    }
    return null;
  }

  private static void awaitZkLeader(List<GenericContainer<?>> zks) {
    waitFor("a ZooKeeper leader among " + zks.size() + " servers", () -> zkLeader(zks) != null);
  }

  private static void awaitHealthy(GenericContainer<?> solr) {
    waitFor(
        COLLECTION + " to report healthy",
        () ->
            exec(solr, "solr", "healthcheck", "-c", COLLECTION, "--solr-url", LOCAL_SOLR)
                    .getExitCode()
                == 0);
  }

  private static boolean tryIndex(GenericContainer<?> solr, String id) {
    return exec(
                solr,
                "curl",
                "-fsS",
                "-H",
                "Content-Type: application/json",
                "--data-binary",
                "[{\"id\":\"" + id + "\"}]",
                LOCAL_SOLR + "/solr/" + COLLECTION + "/update?commit=true")
            .getExitCode()
        == 0;
  }

  private static int numFound(GenericContainer<?> solr) {
    ExecResult select =
        exec(
            solr,
            "curl",
            "-fsS",
            LOCAL_SOLR + "/solr/" + COLLECTION + "/select?q=*:*&rows=0&wt=json");
    assertExec(select);
    return jsonInt(select.getStdout(), "numFound");
  }

  private static String zkStatus(GenericContainer<?> solr) {
    ExecResult status =
        exec(solr, "curl", "-fsS", LOCAL_SOLR + "/solr/admin/zookeeper/status?wt=json");
    assertExec(status);
    return status.getStdout();
  }

  /** Live node count, or -1 while the node can't answer. */
  private static int liveNodeCount(GenericContainer<?> solr) {
    ExecResult clusterStatus =
        exec(
            solr,
            "curl",
            "-fsS",
            LOCAL_SOLR + "/solr/admin/collections?action=CLUSTERSTATUS&wt=json");
    return clusterStatus.getExitCode() == 0
        ? DockerHelpers.liveNodeCount(clusterStatus.getStdout())
        : -1;
  }

  private static String jsonString(String json, String key) {
    return firstGroup(json, "\"" + key + "\"\\s*:\\s*\"([^\"]*)\"");
  }

  private static int jsonInt(String json, String key) {
    return Integer.parseInt(firstGroup(json, "\"" + key + "\"\\s*:\\s*(\\d+)"));
  }

  private static void waitFor(String what, BooleanSupplier condition) {
    TimeOut timeout = new TimeOut(CONVERGE_TIMEOUT_SECONDS, TimeUnit.SECONDS, TimeSource.NANO_TIME);
    try {
      timeout.waitFor(what, condition::getAsBoolean);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted waiting for " + what, e);
    } catch (TimeoutException e) {
      throw new AssertionError(e.getMessage(), e);
    }
  }

  private static void logContainers(List<GenericContainer<?>> containers) {
    for (GenericContainer<?> container : containers) {
      if (container.isRunning()) {
        log.error(
            "Container {} ({}) logs:\n{}",
            container.getNetworkAliases(),
            container.getDockerImageName(),
            container.getLogs());
      }
    }
  }
}
