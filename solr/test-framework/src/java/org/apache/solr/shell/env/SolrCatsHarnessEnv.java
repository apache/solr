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

package org.apache.solr.shell.env;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.solr.cats.env.CatsHarnessEnv;
import org.apache.solr.cats.env.PortBlock;
import org.apache.solr.cli.SolrProcessManager;
import org.apache.solr.shell.exec.SolrProgram;

/**
 * Solr as the installation under test: which distribution to drive, which {@code SOLR_HOME} to use,
 * and every other variable {@code bin/solr} reads.
 *
 * <p>The only place in the harness that knows those names. Each is looked up in the order {@link
 * org.apache.solr.cats.env.Lookup} describes — the environment variable, as the Gradle task
 * supplies it, then the system property {@code solr.cli.<lowercase.dotted.name>}, so an IDE run can
 * override it — and {@code SOLR_TIP} has no fallback, because without it there is nothing to test.
 */
public final class SolrCatsHarnessEnv extends CatsHarnessEnv {

  /** What a transcript echoes when it has to list the variables it knows. */
  private static final Set<String> ECHOED_PREFIXES = Set.of("SOLR", "ZK_", "TEST_");

  private final SolrPorts ports;

  private SolrCatsHarnessEnv(String scopeName, Path solrTip, Path outputDir) {
    super(new SolrProgram(), scopeName, solrTip, outputDir);
    this.ports = new SolrPorts(super.ports());
  }

  /**
   * Builds an environment from explicit directories instead of the process environment, with a
   * fresh port block.
   *
   * <p>This is how the harness' own tests exercise the library without an unpacked Solr
   * distribution: {@code solrTip} only has to contain a {@code bin} directory. Suites that actually
   * drive {@code bin/solr} use {@link #forClass(String)}.
   */
  public static SolrCatsHarnessEnv of(Path solrTip, Path outputDir) {
    return new SolrCatsHarnessEnv(null, solrTip, outputDir);
  }

  /**
   * Builds the environment for one test class: its own {@code SOLR_HOME}, {@code SOLR_PID_DIR},
   * logs and port block, all derived from the shared output directory.
   *
   * <p>Everything a class can write to or listen on is keyed by {@code className}: {@code
   * SOLR_PID_DIR} is what scopes {@code solr stop --all}, and the class setup wipes {@code
   * SOLR_HOME}, so sharing either across forks would have one class kill or delete another's
   * running node. Class names are unique within a run, so only the ports need a cross-fork claim.
   */
  public static SolrCatsHarnessEnv forClass(String className) {
    return new SolrCatsHarnessEnv(className, null, null);
  }

  @Override
  protected void assemble(
      Map<String, String> vars,
      Optional<Path> installDir,
      Path classDir,
      PortBlock block,
      String marker) {
    SolrPorts solrPorts = new SolrPorts(block);
    Path solrHome = createDir(classDir.resolve("solr-home"), "SOLR_HOME");

    vars.put("SOLR_TIP", installDir.orElseThrow().toString());
    vars.put("TEST_OUTPUT_DIR", outputDir().toString());
    vars.put("SOLR_HOME", solrHome.toString());
    // Keeping the PID dir inside SOLR_HOME is what scopes 'solr stop --all' to this class, and away
    // from a developer's own Solr instance.
    vars.put("SOLR_PID_DIR", solrHome.toString());
    vars.put("SOLR_LOGS_DIR", createDir(solrHome.resolve("logs"), "SOLR_LOGS_DIR").toString());
    vars.put("TEST_FAILURE_DIR", failureDir().toString());
    vars.put("SOLR_PORT", Integer.toString(solrPorts.solr()));
    vars.put("SOLR_PORT_LISTEN", Integer.toString(solrPorts.solr()));
    vars.put("SOLR2_PORT", Integer.toString(solrPorts.solr2()));
    vars.put("SOLR3_PORT", Integer.toString(solrPorts.solr3()));
    vars.put("ZK_PORT", Integer.toString(solrPorts.zk()));
    // BATS' common_setup does the same; the checks are noisy and irrelevant under test.
    vars.put("SOLR_ULIMIT_CHECKS", "false");

    // Sandbox node discovery to the nodes this scope starts; without it 'solr status' with no port
    // reports every Solr on the host. Two JVMs have to agree, hence two channels:
    //
    //   SOLR_PROCESS_SCAN_MARKER  the CLI's own JVM. EnvUtils turns any SOLR_* variable into the
    //                             matching solr.* property, so SolrProcessManager reads it without
    //                             the harness touching SOLR_TOOL_OPTS, which some tests set.
    //   SOLR_OPTS_INTERNAL        the node's command line, where the CLI looks for the marker. Both
    //                             scripts append it, it is documented as internal and no test sets
    //                             it, so a test overriding SOLR_OPTS cannot lose the marker.
    vars.put("SOLR_PROCESS_SCAN_MARKER", marker);
    vars.put("SOLR_OPTS_INTERNAL", "-D" + SolrProcessManager.PROCESS_SCAN_MARKER + "=" + marker);

    String picocli = lookup("SOLR_PICOCLI");
    if (picocli != null) {
      vars.put("SOLR_PICOCLI", picocli);
    }
  }

  @Override
  protected String lookupPrefix() {
    return "solr.cli.";
  }

  @Override
  protected String installDirVariable() {
    return "SOLR_TIP";
  }

  @Override
  protected boolean requiresInstallDir() {
    return true;
  }

  @Override
  protected String noInstallDirMessage() {
    return "SOLR_TIP is not set, so there is no Solr distribution to test. Run "
        + "'./gradlew -p solr/packaging dev' and set SOLR_TIP (or -Dsolr.cli.solr.tip=...) "
        + "to the unpacked distribution, or run the tests via the 'catsTests' Gradle task.";
  }

  @Override
  protected Path checkInstallDir(Path tip) {
    if (!Files.isDirectory(tip.resolve("bin"))) {
      throw new IllegalStateException(
          "SOLR_TIP=" + tip + " does not look like a Solr distribution (no bin/ directory)");
    }
    return tip;
  }

  @Override
  protected String markerPrefix() {
    return SolrProgram.MARKER_PREFIX;
  }

  @Override
  public Set<String> echoedPrefixes() {
    return ECHOED_PREFIXES;
  }

  @Override
  public SolrPorts ports() {
    return ports;
  }

  @Override
  public Path workDir() {
    return Path.of(vars().get("SOLR_HOME"));
  }

  @Override
  public Path stateDir() {
    return Path.of(vars().get("SOLR_PID_DIR"));
  }

  @Override
  public Path logDir() {
    return Path.of(vars().get("SOLR_LOGS_DIR"));
  }

  /** The distribution under test. */
  public Path solrTip() {
    return installDir().orElseThrow();
  }

  /** This class's {@code SOLR_HOME}, wiped before the class runs. */
  public Path solrHome() {
    return workDir();
  }

  /** Where {@code bin/solr} records the nodes it starts, and what scopes {@code stop --all}. */
  public Path pidDir() {
    return stateDir();
  }

  public Path logsDir() {
    return logDir();
  }

  public Path testOutputDir() {
    return outputDir();
  }
}
