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

package org.apache.solr.cats.exec;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.solr.cats.env.PortLayout;

/**
 * Everything the harness needs to know about the program it is driving, and nothing more.
 *
 * <p>Every method has a default, so a stateless command-line tool implements none of them and still
 * gets shells, transcripts, variable resolution and the portability rails. A long-running server
 * opts in to the parts it needs: where its launcher lives, which ports it listens on, how to find
 * the processes a test started, and how to stop one.
 *
 * <p><b>Exactly one program per {@link org.apache.solr.cats.env.CatsHarnessEnv}.</b> Everything
 * here is asked by the lifecycle and never per command: {@link #launcherDir} when {@code PATH} is
 * assembled, {@link #portLayout} when the class claims its block, {@link #running}, {@link
 * #stopCommand} and {@link #staleStateFiles} in the leak checks and the teardown, {@link #traits}
 * once. A command is a line in a shell whose environment was fixed when the session was built, so
 * "which program does {@code run()} pick" is a question that cannot arise. A program that is both a
 * server and its own client CLI is still one program: one installation, one launcher directory, one
 * layout.
 *
 * <p>A fixture is not a program under test and needs none of this: a container the test starts, a
 * web root served to the program — nothing about them is discovered, stopped or port-allocated by
 * the harness.
 *
 * <p>Two programs genuinely under test are two {@link
 * org.apache.solr.cats.session.CatsSessionRule}s in one test class, which is where that shape is
 * described.
 */
public interface ProgramUnderTest {

  /**
   * The directory holding the program's launcher, prepended to {@code PATH} so one command string
   * resolves on every shell. An empty result means the program is already on {@code PATH}.
   *
   * @param installDir the installation being tested
   */
  default Optional<Path> launcherDir(Path installDir) {
    return Optional.of(installDir.resolve("bin"));
  }

  /** The block of ports one test class needs. */
  default PortLayout portLayout() {
    return PortLayout.NONE;
  }

  /**
   * The self-daemonising processes whose command line carries {@code marker}, which is how the
   * harness tells what a test started from everything else on the host.
   *
   * <p>Only for a program that outlives the shell that launched it. A direct child of that shell is
   * tracked by its handle instead, so a command-line tool and a converter that runs to completion
   * need no discovery at all.
   */
  default List<RunningProcess> running(String marker) {
    return List.of();
  }

  /**
   * The command line that stops {@code process} gracefully, run before the harness resorts to
   * killing its pid. An empty result means there is no graceful stop.
   */
  default Optional<String> stopCommand(RunningProcess process) {
    return Optional.empty();
  }

  /**
   * The files {@code process} would have removed itself, and that the harness must remove after a
   * force-kill so a later command does not walk them.
   *
   * @param stateDir where the program keeps them
   */
  default List<Path> staleStateFiles(Path stateDir, RunningProcess process) {
    return List.of();
  }

  /** The named traits of this run, as the program reads them off its environment. */
  default Map<String, String> traits(Map<String, String> env) {
    return Map.of();
  }
}
