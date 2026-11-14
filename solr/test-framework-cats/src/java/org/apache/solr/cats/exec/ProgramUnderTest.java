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
 * the instances a test started, and how to stop one.
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
   * The running instances whose command line carries {@code marker}, which is how the harness tells
   * what a test started from everything else on the host.
   */
  default List<RunningProgram> running(String marker) {
    return List.of();
  }

  /**
   * The command line that stops the instance on {@code port} gracefully, run before the harness
   * resorts to killing it. An empty result means there is no graceful stop.
   */
  default Optional<String> stopCommand(int port) {
    return Optional.empty();
  }

  /**
   * The files an instance on {@code port} would have removed itself, and that the harness must
   * remove after a force-kill so a later command does not walk them.
   *
   * @param stateDir where the program keeps them
   */
  default List<Path> staleStateFiles(Path stateDir, int port) {
    return List.of();
  }

  /** The named traits of this run, as the program reads them off its environment. */
  default Map<String, String> traits(Map<String, String> env) {
    return Map.of();
  }
}
