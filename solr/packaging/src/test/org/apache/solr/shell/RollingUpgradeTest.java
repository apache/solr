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

package org.apache.solr.shell;

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
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.lucene.tests.util.LuceneTestCase.Nightly;
import org.apache.lucene.tests.util.QuickPatchThreadsFilter;
import org.apache.solr.SolrIgnoredThreadsFilter;
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
import org.testcontainers.images.RemoteDockerImage;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;

/**
 * Port of {@code test_rolling_upgrade.bats}.
 *
 * <p>Upgrades from the latest release of the previous major version to the image of the Solr
 * version under test, which must already exist locally ({@code ./gradlew dockerTag}); the test is
 * skipped otherwise. Either image can be overridden, e.g. {@code export
 * SOLR_BEGIN_IMAGE=solr:9.11-slim}.
 *
 * <p>Kept close to the original, including the order of the steps and the text each asserts on. The
 * original's {@code docker} command lines become Testcontainers calls, so nothing here is
 * shell-specific and the suite needs no {@code @EnabledOnShell}: it runs wherever Docker can run
 * the Solr images, and skips cleanly where it cannot. Unlike the other container-based Solr suites
 * this one cannot use a {@code @ClassRule}, because replacing a running node with one on the new
 * image mid-test <em>is</em> the thing under test.
 *
 * <p>8983-8985 and 9983 here are container ports reached over a private Docker network, not host
 * ports: this class binds none of the four ports it owns, and the one host port per node that
 * Testcontainers publishes for its wait strategy is chosen by Docker, so a class running in
 * parallel cannot collide with it.
 *
 * <p>Where it differs:
 *
 * <ul>
 *   <li>{@code docker run} becomes a {@link GenericContainer}, and {@code docker exec} {@link
 *       GenericContainer#execInContainer}, so an argument needs no shell quoting — which is what
 *       lets the inline JSON update body be written as-is;
 *   <li>containers get Testcontainers' generated names and carry {@code solr-node1..3} as network
 *       aliases instead, so a leftover container from an earlier run cannot make this one fail to
 *       create one; the volume names are the original's, and are removed before use as well as
 *       after, so stale data from a killed run cannot poison the upgrade;
 *   <li>{@code docker pull} / {@code docker image inspect} become {@link RemoteDockerImage} with
 *       the original's policies — always pull the begin image, prefer the local end image — and a
 *       failure becomes an assumption, which is the original's {@code skip};
 *   <li>the teardown's {@code docker logs} / {@code inspect} / {@code ps aux} artifacts are written
 *       from Java; {@code date -Iseconds} is gone with the {@code --since} it scoped, since every
 *       container here was created by this test;
 *   <li>{@code jq} becomes a regex over the JSON response, there being no need for a JSON library
 *       to read one number out of each of two responses;
 *   <li>three of the original's {@code assert_success} lines sat after a bare {@code docker exec}
 *       and so re-asserted the status of the {@code run curl} from much earlier in the test; here
 *       every step asserts its own exit status instead, those included;
 *   <li>{@code @Nightly}, which the original does not carry: it pulls the previous major's image
 *       from Docker Hub, and an ordinary build has no business reaching out to the network.
 * </ul>
 */
// ThreadLeakFilters are not additive, so SolrTestCase's own two are repeated here.
@ThreadLeakFilters(
    defaultFilters = true,
    filters = {
      SolrIgnoredThreadsFilter.class,
      QuickPatchThreadsFilter.class,
      RollingUpgradeTest.TestcontainersThreadsFilter.class
    })
public class RollingUpgradeTest extends SolrShellTestCase {

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

  /** The original's {@code -m 200m} heap in a {@code --memory=400m} container. */
  private static final long MEMORY_LIMIT_BYTES = 24L * 1024 * 1024; // TEMP

  private static final String HEAP = "200m";

  /** Equivalent of the original's {@code solr assert --started --timeout}, as a startup wait. */
  private static final Duration STARTUP_TIMEOUT = Duration.ofSeconds(120);

  private String beginImage;
  private String endImage;
  private Path artifactDir;

  @Test
  @Nightly
  public void dockerSolrCloudRollingUpgrade() throws IOException {
    requireDockerAndImages();

    boolean failed = true;
    // Networking & volumes
    try (Network network = Network.newNetwork()) {
      Node node1 = new Node(1, network);
      Node node2 = new Node(2, network);
      Node node3 = new Node(3, network);
      List<Node> nodes = List.of(node1, node2, node3);
      try {
        // A volume is global to the Docker daemon, so data a killed run left behind would make the
        // upgrade start from the wrong index. The original only removed them afterwards.
        nodes.forEach(Node::removeVolume);

        // Starting solr-node1 with embedded ZooKeeper
        node1.create(beginImage, null);
        node1.start();
        node1.assertStarted(10_000);

        // start next 2 in parallel
        // Starting solr-node2 connected to first node's ZooKeeper
        node2.create(beginImage, "solr-node1:9983");
        // Started solr-node3 connected to first node's ZooKeeper
        node3.create(beginImage, "solr-node1:9983");
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
                "http://localhost:8983/solr/test-collection/update?commit=true"));

        // Begin rolling upgrade - upgrade node 3 first (reverse order: 3, 2, 1)
        node3.upgradeTo(endImage, "solr-node1:9983");
        node3.assertStarted(30_000);

        // Upgrade node 2 second
        node2.upgradeTo(endImage, "solr-node1:9983");
        node2.assertStarted(30_000);

        // Upgrading node 1 (ZK node)
        node1.upgradeTo(endImage, null);
        node1.assertStarted(30_000);

        // Final collection health check
        awaitHealthy(node1);

        // Checking the indexed document survived the upgrade
        ExecResult select =
            node1.exec(
                "curl",
                "-fsS",
                "http://localhost:8983/solr/test-collection/select"
                    + "?q=id%3Arolling-upgrade-smoke&wt=json");
        assertExec(select);
        assertEquals("1", firstGroup(select.getStdout(), "\"numFound\"\\s*:\\s*(\\d+)"));

        // checking cluster has exactly 3 live nodes
        ExecResult clusterStatus =
            node1.exec(
                "curl", "-s", "http://solr-node1:8983/solr/admin/collections?action=CLUSTERSTATUS");
        assertExec(clusterStatus);
        assertEquals(3, liveNodeCount(clusterStatus.getStdout()));

        failed = false;
      } finally {
        teardown(nodes, failed);
      }
    }
  }

  /** The original's pre-checks: Docker itself, then each end of the upgrade. */
  private void requireDockerAndImages() throws IOException {
    assumeTrue("Docker is not available", DockerClientFactory.instance().isDockerAvailable());

    String solrVersion = harnessEnv.solrTip().getFileName().toString().replaceFirst("^solr-", "");
    int major = Integer.parseInt(solrVersion.substring(0, solrVersion.indexOf('.')));

    beginImage = imageOverride("SOLR_BEGIN_IMAGE", "solr:" + (major - 1) + "-slim");
    // The original always pulls this one, so that it tracks the latest release of that major.
    assumeTrue(
        "Docker image " + beginImage + " is not available",
        imageResolves(beginImage, PullPolicy.alwaysPull()));

    // Default matches the tag applied by the Gradle dockerTag task
    endImage = imageOverride("SOLR_END_IMAGE", "apache/solr:" + solrVersion);
    assumeTrue(
        "Docker image " + endImage + " is not available; build it with: ./gradlew dockerTag",
        imageResolves(endImage, PullPolicy.defaultPolicy()));

    // Persist artifacts under Gradle's test-output
    artifactDir = Files.createDirectories(harnessEnv.testOutputDir().resolve("docker"));
  }

  private String imageOverride(String variable, String fallback) {
    String override = env(variable);
    return (override == null || override.isBlank()) ? fallback : override;
  }

  /**
   * Port of {@code docker pull || skip} and {@code docker image inspect || docker pull || skip}.
   */
  private static boolean imageResolves(String image, ImagePullPolicy policy) {
    try {
      new RemoteDockerImage(DockerImageName.parse(image)).withImagePullPolicy(policy).get();
      return true;
    } catch (RuntimeException e) {
      log.warn("Docker image {} could not be resolved", image, e);
      return false;
    }
  }

  /** Port of {@code wait_for 30 1 docker exec solr-node1 solr healthcheck -c test-collection}. */
  private void awaitHealthy(Node node) {
    awaitOrFail(
        "test-collection to report healthy",
        Duration.ofSeconds(30),
        Duration.ofSeconds(1),
        () -> node.exec("solr", "healthcheck", "-c", "test-collection").getExitCode() == 0);
  }

  /** Port of the original's {@code teardown}: diagnostics, then artifacts, then removal. */
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

    Node(int number, Network network) {
      this.name = "solr-node" + number;
      this.volume = "solr-data" + number;
      this.port = 8982 + number;
      this.network = network;
    }

    /** Port of one {@code docker run}, up to but not including starting it. */
    @SuppressWarnings("resource")
    void create(String image, String zkHost) {
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
    void upgradeTo(String image, String zkHost) {
      stop();
      create(image, zkHost);
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

    /**
     * Testcontainers' stop() also removes the container, as the original's {@code docker rm} did.
     */
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
