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

package org.apache.solr.cats;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.solr.cats.env.CatsHarnessEnv;
import org.apache.solr.cats.env.PortBlock;
import org.apache.solr.cats.exec.ProgramUnderTest;
import org.apache.solr.cats.session.CatsSessionRule;
import org.apache.solr.cats.session.CliSession;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.rules.TestRule;

/**
 * Base class for tests that drive a command-line program through the platform's own shell, for a
 * project with no test base class of its own to keep.
 *
 * <p>The program it drives declares nothing: no ports, no discovery, no stop command and no
 * variables of its own — the stateless-tool shape, which is enough for a tool that reads its
 * arguments, prints, writes files and exits. {@code CATS_INSTALL_DIR} names the installation to
 * test, and a tool already on {@code PATH} needs not even that.
 *
 * <p>A program that does have ports, state or instances to find keeps its own {@link
 * ProgramUnderTest} and {@link CatsHarnessEnv}, and a base class wiring the two in exactly the
 * shape below — which is the whole of this class, and the reason the vocabulary is {@link CatsDsl}
 * rather than inherited: a project whose tests already extend something can implement the interface
 * instead.
 *
 * <p>Two scopes:
 *
 * <ul>
 *   <li>{@link #classCli} is the class-scoped session. A subclass may start something long-lived
 *       from its own {@code @BeforeClass}; it is stopped automatically after the class.
 *   <li>{@link #cli} is the per-test session. Environment and cwd changes a test makes are
 *       discarded afterwards, and whatever it started is stopped after it.
 * </ul>
 *
 * <p>This class does not extend {@code org.junit.Assert}: assertions come from static imports.
 */
public abstract class CatsTestCase implements CatsDsl {

  /** A program that declares nothing, and so needs nothing implemented. */
  private static final ProgramUnderTest PROGRAM = new ProgramUnderTest() {};

  /** The harness: one environment and session per test class, one child session per test. */
  @ClassRule
  public static final CatsSessionRule<CatsHarnessEnv> catsClass =
      new CatsSessionRule<CatsHarnessEnv>(
          StatelessToolEnv::new, PROGRAM, List.of(CatsHarnessEnv::workDir));

  /** The class-scoped session: what a {@code @BeforeClass} here starts the program in. */
  protected static CliSession classCli;

  /** The session for the running test. */
  protected CliSession cli;

  @Rule
  public final TestRule catsSession =
      (base, description) -> catsClass.apply(base, description, session -> cli = session);

  @BeforeClass
  public static void openCatsClassSession() {
    classCli = catsClass.classSession();
  }

  @AfterClass
  public static void closeCatsClassSession() {
    classCli = null;
  }

  @Override
  public CliSession cli() {
    return cli;
  }

  /** The environment of a program that reads no variables of its own. */
  private static final class StatelessToolEnv extends CatsHarnessEnv {

    StatelessToolEnv(Class<?> testClass) {
      super(PROGRAM, testClass.getSimpleName(), null, null);
    }

    @Override
    protected void assemble(
        Map<String, String> vars,
        Optional<Path> installDir,
        Path classDir,
        PortBlock ports,
        String marker) {
      // Nothing to assemble: the tool is driven by its arguments alone.
    }
  }
}
