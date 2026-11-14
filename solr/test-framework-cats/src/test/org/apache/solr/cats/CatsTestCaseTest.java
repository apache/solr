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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

/**
 * The zero-configuration shape: a program that overrides nothing — no installation, no ports, no
 * discovery, no variables — still gets a shell, a session per scope, a work directory and the whole
 * vocabulary.
 *
 * <p>Uses shell built-ins only, so there is nothing to install.
 */
public class CatsTestCaseTest extends CatsTestCase {

  @Test
  public void aProgramThatDeclaresNothingStillGetsAShell() {
    run("echo hello").assertExitCode(0).assertExact("hello");
  }

  @Test
  public void eachTestWritesInItsOwnDirectory() {
    write("note.txt", "from the test");
    assertTrue(exists("note.txt"));
    assertEquals("from the test", read("note.txt"));
    assertEquals(workDir(), path("note.txt").getParent());
  }

  @Test
  public void aProgramWithNoPortsGetsAnEmptyBlock() {
    assertEquals(0, ports().all().length);
  }

  @Test
  public void aRequestNamingNoPortSaysWhyItCannotBeSent() {
    // Not an NPE on a port that was never allocated: a program with no primary port has no answer
    // to "which port", and the message says so.
    IllegalStateException failure =
        assertThrows(IllegalStateException.class, () -> http("/status"));
    assertTrue(failure.getMessage(), failure.getMessage().contains("no primary port"));
  }

  @Test
  public void theClassSessionOutlivesTheTestsOwn() {
    assertNotNull(classCli);
    assertNotSame(classCli, cli);
  }
}
