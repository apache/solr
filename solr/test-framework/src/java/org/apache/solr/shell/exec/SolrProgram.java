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

package org.apache.solr.shell.exec;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.solr.cats.env.PortLayout;
import org.apache.solr.cats.exec.ProgramUnderTest;
import org.apache.solr.cats.exec.RunningProcess;
import org.apache.solr.cli.SolrProcessManager;

/**
 * Solr as the program the harness drives: how its nodes are discovered, stopped, and what they
 * leave behind.
 *
 * <p>Discovery is deliberately not done by reading pid files: {@code bin/solr} writes {@code
 * solr-<port>.pid} holding a PID while {@code bin\solr.cmd} writes {@code solr-<port>.port} holding
 * a port. Going through {@link SolrProcessManager}, which resolves either, means one code path for
 * both platforms and finds a node started in the <em>foreground</em>, which writes no file at all.
 *
 * <p>It is scoped by the command-line marker the harness starts its nodes with, so what comes back
 * is the harness' own nodes and never a developer's Solr or another suite's.
 */
public final class SolrProgram implements ProgramUnderTest {

  /**
   * The prefix every harness marker begins with. Discovery matches on a substring, so this finds
   * the harness' nodes from any run, where a full marker only finds one scope's.
   */
  public static final String MARKER_PREFIX = "solr-shell-test-";

  /**
   * Solr's block: the node port {@code P}, two more for the multi-node suites, and embedded
   * ZooKeeper on {@code P+1000} (see {@code SolrZkServer}).
   *
   * <p>A node on {@code P} also listens on {@code P-1000} for {@code solr stop} (see {@code
   * bin/solr}), so a class really occupies {@code P-1000..P-998}, {@code P..P+2} and {@code
   * P+1000..P+1002}. Those windows overlap between blocks, but the ports actually used do not:
   * every block shifts all three groups by the same stride, so a collision needs {@code (j-i) *
   * stride} to land on 1000 — which is why no more than 200 blocks are handed out per run.
   *
   * <p>The base range leaves room below for the {@code P-1000} stop ports, and stays under every
   * platform's ephemeral floor — Linux 32768, macOS and Windows 49152 — or the OS hands a block's
   * port to an outgoing connection and the pre-flight check fails.
   */
  public static final PortLayout LAYOUT =
      new PortLayout(4, new int[] {0, 1, 2, 1000}, 20_000, 30_000, 200, 0);

  @Override
  public PortLayout portLayout() {
    return LAYOUT;
  }

  /**
   * The running nodes whose command line contains {@code marker}.
   *
   * <p>Pass a full marker for one scope's nodes, or {@link #MARKER_PREFIX} for every harness node
   * on the host.
   */
  @Override
  public List<RunningProcess> running(String marker) {
    // SolrProcessManager reads the marker from a system property, via EnvUtils, when constructed.
    // Set and restored around the call so the harness JVM is left as it was.
    String previous = System.getProperty(SolrProcessManager.PROCESS_SCAN_MARKER);
    System.setProperty(SolrProcessManager.PROCESS_SCAN_MARKER, marker);
    try {
      SolrProcessManager processes = new SolrProcessManager();
      return processes.getAllRunning().stream()
          .map(node -> RunningProcess.of(node.pid(), node.port()))
          .toList();
    } finally {
      if (previous == null) {
        System.clearProperty(SolrProcessManager.PROCESS_SCAN_MARKER);
      } else {
        System.setProperty(SolrProcessManager.PROCESS_SCAN_MARKER, previous);
      }
    }
  }

  @Override
  public Optional<String> stopCommand(RunningProcess process) {
    return nodePort(process).map(port -> "solr stop -p " + port);
  }

  /**
   * Both spellings: {@code bin/solr} writes {@code solr-<port>.pid}, {@code bin\solr.cmd} writes
   * {@code solr-<port>.port}, and {@link SolrProcessManager} reads either. Only a graceful stop
   * removes its own file, so a stale one left by a force-kill is what {@code solr stop --all} and
   * {@code solr status} would later walk.
   */
  @Override
  public List<Path> staleStateFiles(Path stateDir, RunningProcess process) {
    return nodePort(process)
        .map(
            port ->
                List.of(
                    stateDir.resolve("solr-" + port + ".pid"),
                    stateDir.resolve("solr-" + port + ".port")))
        .orElseGet(List::of);
  }

  /**
   * A node is named by the port it serves on, which is the first and only one discovery reports.
   */
  private static Optional<Integer> nodePort(RunningProcess process) {
    return process.ports().stream().findFirst();
  }

  /**
   * One trait, {@code cli}: which command-line implementation {@code bin/solr} dispatches to,
   * either {@code picocli} or {@code commons-cli}.
   *
   * <p>The two mostly agree, but not on every word of their usage and error text, so a test
   * asserting on wording may hold for only one of them: {@link org.apache.solr.shell.EnabledOnCli}
   * skips such a test on the other, and {@code SolrCatsTestCase#forCli} picks the right wording for
   * whichever is running.
   *
   * <p>The default tracks {@code SolrCLI.main}, which reads {@code solr.picocli} and defaulted to
   * picocli in SOLR-18561. Keep the two in step: a run that pins neither takes whichever CLI {@code
   * bin/solr} dispatches to, and a mismatch here has the suites assert one implementation's wording
   * against the other's output.
   */
  @Override
  public Map<String, String> traits(Map<String, String> env) {
    boolean picocli = Boolean.parseBoolean(env.getOrDefault("SOLR_PICOCLI", "true"));
    return Map.of("cli", picocli ? "picocli" : "commons-cli");
  }
}
