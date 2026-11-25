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

import com.carrotsearch.randomizedtesting.ThreadFilter;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import com.github.dockerjava.api.model.Bind;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
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
import org.testcontainers.containers.ExecConfig;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.ImagePullPolicy;
import org.testcontainers.images.PullPolicy;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;

/**
 * Port of {@code test_rolling_upgrade.bats}.
 *
 * <p>Upgrades from the latest release of the previous major version to an image of the Solr version
 * under test. Either end can be overridden, e.g. {@code export SOLR_BEGIN_IMAGE=solr:9.11-slim}.
 *
 * <p>Runs only from the {@code dockerRollingUpgradeTest} Gradle task, which builds the image from
 * the working tree and hands over the id {@code dockerBuild} wrote. That keeps the upgrade onto the
 * code in the tree rather than whatever {@code apache/solr:<version>} happens to be tagged on the
 * machine, and keeps a Docker- and network-using test out of ordinary builds — which is why there
 * is no {@code @Nightly} as well.
 *
 * <p>A {@code @ClassRule} is not an option here: replacing a running node with one on the new image
 * mid-test <em>is</em> the thing under test.
 *
 * <p>8983-8985 and 9983 below are container ports on a private Docker network, not host ports: this
 * class binds none of the four ports it owns, and Testcontainers' published wait-strategy port is
 * chosen by Docker, so a class running in parallel cannot collide with it.
 *
 * <p>Containers take Testcontainers' generated names and carry {@code solr-node1..3} as network
 * aliases, so a leftover container cannot make this run fail to create one. The volumes get a
 * per-run suffix, a Docker volume being global to the daemon, and are removed before use as well as
 * after.
 */
// ThreadLeakFilters are not additive, so SolrTestCase's own two are repeated here.
@ThreadLeakFilters(
    defaultFilters = true,
    filters = {
      SolrIgnoredThreadsFilter.class,
      QuickPatchThreadsFilter.class,
      RollingUpgradeTest.TestcontainersThreadsFilter.class
    })
public class RollingUpgradeTest extends SolrTestCase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /** Non-daemon threads Testcontainers and its Docker client leave behind. */
  @SuppressWarnings("NewClassNamingConvention")
  public static class TestcontainersThreadsFilter implements ThreadFilter {
    @Override
    public boolean reject(Thread t) {
      if (t == null || t.getName() == null) {
        return false;
      }
      String name = t.getName();
      return name.startsWith("testcontainers-")
          || name.startsWith("ducttape-")
          || name.startsWith("docker-java-")
          || name.startsWith("HttpClient-")
          || name.startsWith("OkHttp")
          || name.startsWith("Okio");
    }
  }

  /** A 200m heap in a 400m container. */
  private static final long MEMORY_LIMIT_BYTES = 400L * 1024 * 1024;

  private static final String HEAP = "200m";

  /** How long Testcontainers waits for a node's port to listen before giving up. */
  private static final Duration STARTUP_TIMEOUT = Duration.ofMinutes(2);

  private String beginImage;
  private String endImage;
  private Path artifactDir;

  @Test
  public void dockerSolrCloudRollingUpgrade() throws IOException {
    // The other container suites get this from their @ClassRule; this one has no rule to put it in,
    // because swapping a running node for one on the new image is the thing under test. It is the
    // only skip left in the suite: a begin image that cannot be pulled is a failure, not a skip.
    assumeTrue("Docker is not available", DockerClientFactory.instance().isDockerAvailable());
    resolveImages();

    boolean failed = true;
    // Networking & volumes
    try (Network network = Network.newNetwork()) {
      // Unique per run, so the volume names below cannot collide with another run's.
      String runId = UUID.randomUUID().toString().substring(0, 8);
      Node node1 = new Node(1, network, runId);
      Node node2 = new Node(2, network, runId);
      Node node3 = new Node(3, network, runId);
      List<Node> nodes = List.of(node1, node2, node3);
      try {
        // A volume is global to the Docker daemon, so data a killed run left behind would make the
        // upgrade start from the wrong index.
        nodes.forEach(Node::removeVolume);

        // Starting solr-node1 with embedded ZooKeeper
        node1.create(beginImage, null, pullPolicyFor(beginImage));
        node1.start();
        node1.assertStarted(10_000);

        // start next 2 in parallel
        // Starting solr-node2 connected to first node's ZooKeeper
        node2.create(beginImage, node1.zkAddress(), pullPolicyFor(beginImage));
        // Started solr-node3 connected to first node's ZooKeeper
        node3.create(beginImage, node1.zkAddress(), pullPolicyFor(beginImage));
        Startables.deepStart(node2.container, node3.container).join();

        node2.assertStarted(30_000);
        node3.assertStarted(30_000);

        // Creating a Collection
        assertExec(
            node1.execAsSolr(
                "solr", "create", "-c", "test-collection", "-n", "techproducts", "--shards", "3"));

        // Checking collection health
        awaitHealthy(node1);

        // Add some sample data
        assertExec(
            node1.exec(
                "curl",
                "-fsS",
                "-H",
                "Content-Type: application/json",
                "--data-binary",
                "[{\"id\":\"rolling-upgrade-smoke\"}]",
                node1.httpAddress() + "/solr/test-collection/update?commit=true"));

        // Begin rolling upgrade - upgrade node 3 first (reverse order: 3, 2, 1)
        node3.upgradeTo(endImage, node1.zkAddress(), pullPolicyFor(endImage));
        node3.assertStarted(30_000);

        // Upgrade node 2 second
        node2.upgradeTo(endImage, node1.zkAddress(), pullPolicyFor(endImage));
        node2.assertStarted(30_000);

        // Upgrading node 1 (ZK node)
        node1.upgradeTo(endImage, null, pullPolicyFor(endImage));
        node1.assertStarted(30_000);

        // Final collection health check
        awaitHealthy(node1);

        // Checking the indexed document survived the upgrade
        ExecResult select =
            node1.exec(
                "curl",
                "-fsS",
                node1.httpAddress()
                    + "/solr/test-collection/select?q=id%3Arolling-upgrade-smoke&wt=json");
        assertExec(select);
        assertEquals("1", firstGroup(select.getStdout(), "\"numFound\"\\s*:\\s*(\\d+)"));

        // checking cluster has exactly 3 live nodes
        ExecResult clusterStatus =
            node1.exec(
                "curl", "-s", node1.httpAddress() + "/solr/admin/collections?action=CLUSTERSTATUS");
        assertExec(clusterStatus);
        assertEquals(3, liveNodeCount(clusterStatus.getStdout()));

        failed = false;
      } finally {
        teardown(nodes, failed);
      }
    }
  }

  /**
   * Works out which two images to upgrade between.
   *
   * <p>The previous major is derived from {@code solr.version}, which the Gradle task sets from
   * {@code project.version}. The version to upgrade <em>to</em> is the image id {@code dockerBuild}
   * wrote, passed in {@code solr.docker.imageId} — an id rather than a tag, so nothing has to be
   * tagged and an {@code apache/solr:<version>} already on the machine is never consulted.
   *
   * <p>Either end can still be overridden by environment variable, e.g. {@code export
   * SOLR_BEGIN_IMAGE=solr:9.11-slim}. Neither is checked for availability: a missing image should
   * fail loudly where a container needs it, rather than skipping.
   */
  private void resolveImages() throws IOException {
    int previousMajor = Integer.parseInt(requireProperty("solr.version").split("\\.", 2)[0]) - 1;
    beginImage = imageOverride("SOLR_BEGIN_IMAGE", "solr:" + previousMajor + "-slim");
    endImage = imageOverride("SOLR_END_IMAGE", requireProperty("solr.docker.imageId"));

    artifactDir = Files.createDirectories(Path.of(requireProperty("solr.docker.artifactDir")));
  }

  /**
   * A property the Gradle task is responsible for; absent means the test was run some other way.
   */
  private static String requireProperty(String name) {
    String value = System.getProperty(name);
    if (value == null || value.isBlank()) {
      throw new IllegalStateException(
          name
              + " is not set. Run this through './gradlew -p solr/docker dockerRollingUpgradeTest'.");
    }
    return value;
  }

  /**
   * The begin image is always re-pulled, so the test tracks the latest release of the previous
   * major. The end image keeps Testcontainers' default policy, which pulls only when the image is
   * not already local: the one the Gradle task just built therefore never leaves the machine, while
   * a hand-set {@code SOLR_END_IMAGE} naming a published image still resolves.
   */
  private ImagePullPolicy pullPolicyFor(String image) {
    return image.equals(beginImage) ? PullPolicy.alwaysPull() : PullPolicy.defaultPolicy();
  }

  private String imageOverride(String variable, String fallback) {
    String override = System.getenv(variable);
    return (override == null || override.isBlank()) ? fallback : override;
  }

  /** Waits for {@code solr healthcheck} inside the container to report the collection healthy. */
  private void awaitHealthy(Node node) {
    TimeOut timeout = new TimeOut(30, TimeUnit.SECONDS, TimeSource.NANO_TIME);
    try {
      timeout.waitFor(
          "test-collection to report healthy on " + node.name,
          () -> node.exec("solr", "healthcheck", "-c", "test-collection").getExitCode() == 0);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted waiting for " + node.name, e);
    } catch (TimeoutException e) {
      throw new AssertionError(e.getMessage(), e);
    }
  }

  /** Diagnostics, then artifacts, then removal. */
  private void teardown(List<Node> nodes, boolean failed) {
    for (Node node : nodes) {
      if (node.container == null || !node.container.isRunning()) {
        continue;
      }
      try {
        // Persist artifacts
        writeArtifact(node.name + ".log", node.container.getLogs());
        writeArtifact(node.name + ".ps.txt", node.exec("ps", "aux").getStdout());
        if (failed) {
          // Test failed - capturing Docker diagnostics
          log.error(
              "Container {} ({}, image {}) logs:\n{}",
              node.name,
              node.container.getContainerId(),
              node.container.getDockerImageName(),
              node.container.getLogs());
        }
      } catch (RuntimeException e) {
        // A container that died under us must not turn into a second, misleading failure.
        log.warn("Could not collect diagnostics for {}", node.name, e);
      }
    }
    if (artifactDir != null && log.isInfoEnabled()) {
      log.info("Docker artifacts saved to: {}", artifactDir);
    }
    for (Node node : nodes) {
      node.stop();
      node.removeVolume();
    }
  }

  private void writeArtifact(String name, String content) {
    try {
      Files.writeString(artifactDir.resolve(name), content, StandardCharsets.UTF_8);
    } catch (IOException e) {
      // Diagnostics only; losing one must not mask the failure the test is reporting.
      log.warn("Could not write Docker artifact {}", name, e);
    }
  }

  private static void assertExec(ExecResult result) {
    assertEquals(
        "exec failed:\nstdout: " + result.getStdout() + "\nstderr: " + result.getStderr(),
        0,
        result.getExitCode());
  }

  private static String firstGroup(String text, String regex) {
    Matcher matcher = Pattern.compile(regex).matcher(text);
    assertTrue("Output did not match " + regex + ": " + text, matcher.find());
    return matcher.group(1);
  }

  /** Port of {@code jq -r '.cluster.live_nodes | length'}; the array holds no nested brackets. */
  private static int liveNodeCount(String clusterStatusJson) {
    String entries = firstGroup(clusterStatusJson, "\"live_nodes\"\\s*:\\s*\\[([^]]*)]").strip();
    return entries.isEmpty() ? 0 : entries.split(",").length;
  }

  /**
   * One Solr node of the cluster: a container plus the name, port and volume that stay the same
   * when the container behind it is replaced by one on the new image.
   */
  private static final class Node {

    private final String name;
    private final String volume;
    private final int port;
    private final Network network;

    private GenericContainer<?> container;

    Node(int number, Network network, String runId) {
      this.name = "solr-node" + number;
      // Per run: a Docker volume is global to the daemon, so
      // two runs on one machine would otherwise wipe each other's data mid-upgrade.
      this.volume = "solr-data" + number + "-" + runId;
      this.port = 8982 + number;
      this.network = network;
    }

    /**
     * Where another node reaches this one's embedded ZooKeeper, as {@code <alias>:<port>}.
     *
     * <p>The host part is this container's network alias, resolved by Docker's DNS inside the
     * private network; the port is Solr's embedded-ZooKeeper convention of the Solr port plus 1000.
     * Derived from {@link #name} so it cannot drift from the alias the container is created with.
     */
    String zkAddress() {
      return name + ":" + (port + 1000);
    }

    /** Where this node is reached over HTTP from inside the network. */
    String httpAddress() {
      return "http://" + name + ":" + port;
    }

    /** Port of one {@code docker run}, up to but not including starting it. */
    @SuppressWarnings("resource")
    void create(String image, String zkHost, ImagePullPolicy pullPolicy) {
      List<String> command =
          new ArrayList<>(
              List.of(
                  "solr", "start", "-f", "-m", HEAP, "--host", name, "-p", String.valueOf(port)));
      if (zkHost != null) {
        command.add("-z");
        command.add(zkHost);
      }
      container =
          new GenericContainer<>(DockerImageName.parse(image))
              .withImagePullPolicy(pullPolicy)
              .withNetwork(network)
              .withNetworkAliases(name)
              .withEnv("SOLR_MODE", "solrcloud")
              .withCommand(command.toArray(new String[0]))
              .withExposedPorts(port)
              .withStartupTimeout(STARTUP_TIMEOUT)
              .waitingFor(Wait.forListeningPort())
              .withCreateContainerCmdModifier(
                  cmd -> cmd.getHostConfig().withMemory(MEMORY_LIMIT_BYTES));
      // -v solr-dataN:/var/solr. Docker creates the named volume on first use, and it outlives the
      // container, which is what makes the upgrade an upgrade rather than a fresh install.
      container.setBinds(List.of(Bind.parse(volume + ":/var/solr")));
    }

    void start() {
      container.start();
    }

    /** Port of {@code docker stop}, {@code docker rm} and a {@code docker run} on the new image. */
    void upgradeTo(String image, String zkHost, ImagePullPolicy pullPolicy) {
      stop();
      create(image, zkHost, pullPolicy);
      start();
    }

    /** Port of {@code docker exec <node> solr assert --started http://<node>:<port>}. */
    void assertStarted(int timeoutMillis) {
      assertExec(
          exec(
              "solr",
              "assert",
              "--started",
              "http://" + name + ":" + port,
              "--timeout",
              String.valueOf(timeoutMillis)));
    }

    ExecResult exec(String... command) {
      try {
        return container.execInContainer(command);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new AssertionError(e);
      }
    }

    /** Port of {@code docker exec --user=solr}. */
    ExecResult execAsSolr(String... command) {
      try {
        return container.execInContainer(
            ExecConfig.builder().user("solr").command(command).build());
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new AssertionError(e);
      }
    }

    /** Testcontainers' stop() also removes the container. */
    void stop() {
      if (container != null) {
        container.stop();
        container = null;
      }
    }

    void removeVolume() {
      try {
        DockerClientFactory.instance().client().removeVolumeCmd(volume).exec();
      } catch (RuntimeException e) {
        // Absent, or still in use by a container this test does not own; nothing to do either way.
        log.debug("Could not remove volume {}", volume, e);
      }
    }
  }
}
