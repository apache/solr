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
import org.apache.solr.cats.exec.ProgramUnderTest;
import org.apache.solr.cats.exec.RunningProcess;
import org.apache.solr.cli.process.SolrProcessManager;

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
   * The ports one test class gets: the node port {@code P} and two more for the multi-node suites,
   * the embedded ZooKeeper port, and a stop port for each of the three nodes — seven consecutive
   * ports, {@code at(0)} through {@code at(6)}, named by {@link
   * org.apache.solr.shell.env.SolrPorts}.
   *
   * <p>Four of the seven are passed to {@code bin/solr} today. The embedded ZooKeeper port is
   * handed over in a {@code zoo.cfg} the harness writes for the class, which {@code SolrZkServer}
   * reads before falling back to {@code P+1000}; once SOLR-7596 lands it can be passed as {@code
   * solr.zookeeper.embedded.port} instead, which is why {@code SOLR_ZOOKEEPER_EMBEDDED_PORT} is
   * already exported.
   *
   * <p>The three stop ports are allocated but not yet passed. {@code bin/solr} takes one {@code
   * STOP_PORT} per invocation, so a single environment variable would have every node of a class
   * share it and {@code stop --all} use it for all of them; a per-node stop port is SOLR-9714.
   * Until then the scripts derive {@code P-1000} themselves, and the allocation reserves the three
   * ports so that the switch is a one-line change rather than a re-think of the block.
   *
   * <p>That a derived port stays out of another class's block is arithmetic, not luck. Blocks are
   * seven consecutive ports from one run-wide base, so class {@code i} owns {@code base+7i} through
   * {@code base+7i+6}, and a port derived 1000 below or above one of them lands in class {@code
   * j}'s block only when {@code 7|i-j|} falls within 6 of 1000 — that is, when the two classes are
   * 142 or 143 blocks apart. A run has tens of classes, not 142, so a derived {@code P-1000} stop
   * port and the {@code P+1000} ZooKeeper port {@code RunExampleTool} still picks for itself are
   * both safe.
   */
  public static final int PORT_COUNT = 7;

  @Override
  public int portCount() {
    return PORT_COUNT;
  }

  @Override
  public int primaryPort() {
    return 0;
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
