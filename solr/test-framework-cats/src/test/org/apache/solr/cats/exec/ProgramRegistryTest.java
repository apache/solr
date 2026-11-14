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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * The two kinds of process the registry tracks: a direct child of a shell the harness spawned,
 * which has no ports and is held by its handle, and a process discovery reported by marker.
 */
public class ProgramRegistryTest {

  private Path tmp;
  private Path tool;
  private final ProgramRegistry registry = new ProgramRegistry();

  @Before
  public void installStandInProgram() throws IOException {
    tmp = Files.createTempDirectory("cats-registry-test");
    boolean cmd = "cmd".equals(Shells.current().id());
    String fixture = cmd ? "mytool.cmd" : "mytool.sh";
    tool = tmp.resolve(cmd ? "mytool.cmd" : "mytool");
    try (InputStream in = ProgramRegistryTest.class.getResourceAsStream("/" + fixture)) {
      assertNotNull(fixture + " must be on the test classpath", in);
      Files.copy(in, tool);
    }
    if (!cmd) {
      Files.setPosixFilePermissions(tool, PosixFilePermissions.fromString("rwxr-xr-x"));
    }
  }

  @After
  public void removeTempDir() throws IOException {
    CatsFiles.rm(tmp);
  }

  @Test
  public void aChildWithNoPortsIsTrackedAndKilled() throws IOException, InterruptedException {
    Process child = startSleeper();
    try {
      registry.recordChild(child);
      assertTrue("the stand-in program should still be sleeping", child.isAlive());

      registry.killRecorded();

      assertTrue("the child should be gone", child.waitFor(30, TimeUnit.SECONDS));
      assertFalse(child.isAlive());
    } finally {
      child.descendants().forEach(ProcessHandle::destroyForcibly);
      child.destroyForcibly();
    }
  }

  @Test
  public void aForgottenChildIsLeftAlone() throws IOException {
    Process child = startSleeper();
    try {
      registry.recordChild(child);
      registry.forgetChild(child);

      registry.killRecorded();

      assertTrue(
          "a child the session already dealt with is none of the hook's business", child.isAlive());
    } finally {
      child.descendants().forEach(ProcessHandle::destroyForcibly);
      child.destroyForcibly();
    }
  }

  @Test
  public void aDiscoveredProcessWithNoPortsIsAccountedForOnce() {
    // A pid no process could have: nothing here starts or kills anything.
    RunningProcess discovered = RunningProcess.of(90_000_007L);
    assertTrue(registry.recordIfNew(discovered));
    assertFalse("a pid already recorded needs no second owner", registry.recordIfNew(discovered));

    registry.forget(discovered);
    assertTrue(registry.recordIfNew(discovered));
  }

  /** The stand-in program, sleeping long enough that only a kill ends it. */
  private Process startSleeper() throws IOException {
    Shell shell = Shells.current();
    String line = shell.quote(tool.toString()) + " sleep 300";
    List<String> argv = shell.processArgs(line, tmp, "sleeper");
    ProcessBuilder builder = new ProcessBuilder(argv);
    builder.directory(tmp.toFile());
    builder.redirectOutput(ProcessBuilder.Redirect.to(tmp.resolve("sleeper.out").toFile()));
    builder.redirectErrorStream(true);
    return builder.start();
  }
}
