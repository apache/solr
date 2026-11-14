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

import java.util.List;
import java.util.Map;
import org.apache.solr.SolrTestCase;
import org.apache.solr.cats.CatsDsl;
import org.apache.solr.cats.session.CatsSessionRule;
import org.apache.solr.cats.session.CliSession;
import org.apache.solr.shell.env.SolrCatsHarnessEnv;
import org.apache.solr.shell.env.SolrPorts;
import org.apache.solr.shell.exec.SolrProgram;
import org.apache.solr.shell.session.SolrCliHelpers;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

/**
 * Base class for tests that drive Solr's start scripts the way the BATS suites do, with {@code
 * run}, {@code status}, {@code output}, reusable helpers and automatic cleanup — but as one test
 * that runs against {@code bin/solr} under bash and {@code bin\solr.cmd} under cmd.exe.
 *
 * <p>The vocabulary a test writes in — {@code run}, {@code env}, {@code cd}, the file and HTTP
 * helpers — comes from {@link CatsDsl}, which is generic; this class adds Solr: the distribution to
 * drive, the {@code cli} trait, and the collection and ZooKeeper helpers.
 *
 * <p>A test is expected to pass under both shells; where {@code bin/solr} and {@code bin\solr.cmd}
 * genuinely differ — an option only one accepts, {@code solr.in.sh} versus {@code solr.in.cmd}
 * handling, a Windows-only path quirk — {@code @EnabledOnShell} restricts it to one of them.
 *
 * <p>Two scopes, mirroring BATS:
 *
 * <ul>
 *   <li>{@link #classCli} is the class-scoped session, BATS' {@code setup_file}. A subclass can
 *       start a long-lived Solr from its own {@code @BeforeClass}; this class's
 *       {@code @BeforeClass} has already run by then, and the node is stopped automatically after
 *       the class.
 *   <li>{@link #cli} is the per-test session, BATS' {@code setup}. Environment and cwd changes a
 *       test makes are discarded afterwards, and any Solr it started is stopped after it.
 * </ul>
 *
 * <p>On failure, {@code SOLR_HOME} and the command transcript are copied to {@code
 * $TEST_FAILURE_DIR/<Class>-<method>/} and the path is printed, which is BATS' {@code
 * save_home_on_failure}.
 */
public abstract class SolrCatsTestCase extends SolrTestCase implements CatsDsl {

  /** The program every session here drives: how a node is found, stopped and cleaned up after. */
  private static final SolrProgram PROGRAM = new SolrProgram();

  /** The harness: this class's environment and session, the leak checks, and the snapshots. */
  private static final CatsSessionRule<SolrCatsHarnessEnv> SESSION =
      new CatsSessionRule<SolrCatsHarnessEnv>(
          testClass -> SolrCatsHarnessEnv.forClass(testClass.getSimpleName()),
          PROGRAM,
          List.of(SolrCatsHarnessEnv::solrHome));

  /** The resolved environment: distribution, {@code SOLR_HOME}, ports, base environment. */
  protected static SolrCatsHarnessEnv harnessEnv;

  /** The class-scoped session. Solr started here lives for the whole class. */
  protected static CliSession classCli;

  /** The session for the running test. */
  protected CliSession cli;

  @BeforeClass
  public static void setUpShellHarness() {
    SESSION.openClass(
        getTestClass(),
        env -> CliGate.requireCli(env.traits(), getTestClass().getAnnotation(EnabledOnCli.class)));
    harnessEnv = SESSION.env();
    classCli = SESSION.classSession();
  }

  @AfterClass
  public static void tearDownShellHarness() {
    harnessEnv = null;
    classCli = null;
    SESSION.closeClass();
  }

  @Rule
  public final TestRule shellSessionRule =
      RuleChain.outerRule(new CliGate(() -> harnessEnv.traits()))
          .around(
              (base, description) -> SESSION.apply(base, description, session -> cli = session));

  @Override
  public CliSession cli() {
    return cli;
  }

  /** The ports this run owns, under the names the BATS suites used. */
  @Override
  public SolrPorts ports() {
    return harnessEnv.ports();
  }

  /** The wording for the command-line implementation this run targets. */
  // TODO: Remove this once the commons-cli cleanup happens.
  protected String forCli(String commonsCliText, String picocliText) {
    return wordingFor("cli", Map.of("commons-cli", commonsCliText, "picocli", picocliText));
  }

  /** The Solr half of the helpers, which the generic session does not hand back. */
  private SolrCliHelpers solrHelpers() {
    return new SolrCliHelpers(cli);
  }

  protected boolean collectionExists(String collection) {
    return solrHelpers().collectionExists(collection);
  }

  protected boolean configExists(String configName) {
    return solrHelpers().configExists(configName);
  }

  protected void deleteAllCollections() {
    solrHelpers().deleteAllCollections();
  }

  protected void shutdownAll() {
    solrHelpers().shutdownAll();
  }

  protected void dumpSolrLog() {
    solrHelpers().dumpSolrLog();
  }
}
