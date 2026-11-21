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

package org.apache.solr.cats.env;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.Before;
import org.junit.Test;

/**
 * The allocator a test class gets its ports from: consecutive, disjoint from every other class's,
 * and tellable from a port an earlier run left behind.
 */
public class PortBlocksTest {

  private Path outputDir;

  @Before
  public void makeOutputDir() throws IOException {
    outputDir = Files.createTempDirectory("cats-port-blocks-test");
  }

  @Test
  public void aBlockIsAsManyConsecutivePortsAsTheProgramAskedFor() {
    PortBlock block = PortBlocks.claim(outputDir, 21_000, 3);

    assertEquals(21_000, block.base());
    assertArrayEquals(new int[] {21_000, 21_001, 21_002}, block.all());
    assertEquals(21_002, block.at(2));
    assertFalse(block.isEmpty());
    assertTrue(block.contains(21_001));
    assertFalse(block.contains(21_003));
  }

  @Test
  public void twoClaimsGetDisjointBlocksOneAfterTheOther() {
    PortBlock first = PortBlocks.claim(outputDir, 21_000, 7);
    PortBlock second = PortBlocks.claim(outputDir, 21_000, 7);

    assertEquals(21_000, first.base());
    assertEquals(21_007, second.base());
    for (int port : second.all()) {
      assertFalse("blocks share " + port, first.contains(port));
    }
  }

  @Test
  public void blocksStayDisjointWhenTwoProgramsAskForDifferentCounts() {
    // Both programs of a run claim from the one counter, so the next block starts where the
    // previous one ended whatever its size — there is no stride to agree on.
    PortBlock two = PortBlocks.claim(outputDir, 21_000, 2);
    PortBlock five = PortBlocks.claim(outputDir, 21_000, 5);
    PortBlock one = PortBlocks.claim(outputDir, 21_000, 1);

    assertArrayEquals(new int[] {21_000, 21_001}, two.all());
    assertArrayEquals(new int[] {21_002, 21_003, 21_004, 21_005, 21_006}, five.all());
    assertArrayEquals(new int[] {21_007}, one.all());
  }

  @Test
  public void aProgramThatListensOnNothingGetsAnEmptyBlockAndNoCounterFile() {
    PortBlock block = PortBlocks.claim(outputDir, 21_000, 0);

    assertTrue(block.isEmpty());
    assertEquals(0, block.all().length);
    assertFalse(block.contains(0));
    assertFalse(
        "a program with no ports must not create the counter",
        Files.exists(outputDir.resolve(".port-blocks")));
  }

  @Test
  public void onlyPortsTheRunHasHandedOutBelongToIt() {
    assertFalse(
        "nothing belongs to a run that has claimed nothing",
        PortBlocks.belongsToThisRun(outputDir, 21_000));

    PortBlocks.claim(outputDir, 21_000, 3);

    assertTrue(PortBlocks.belongsToThisRun(outputDir, 21_000));
    assertTrue(PortBlocks.belongsToThisRun(outputDir, 21_002));
    // Not handed out yet, so a process there is not this run's.
    assertFalse(PortBlocks.belongsToThisRun(outputDir, 21_003));
    // Below the base: an earlier run's, or a port derived from one of ours.
    assertFalse(PortBlocks.belongsToThisRun(outputDir, 20_999));

    PortBlocks.claim(outputDir, 21_000, 3);

    assertTrue("the range grows with each claim", PortBlocks.belongsToThisRun(outputDir, 21_003));
  }

  @Test
  public void anUnseededRunStartsSomewhereInTheHarnessOwnRange() {
    PortBlock block = PortBlocks.claim(outputDir, -1, 7);

    assertTrue(block.base() + " is below the harness' range", block.base() >= PortBlocks.MIN_PORT);
    assertTrue(block.base() + " is too high to leave room", block.base() < PortBlocks.MAX_BASE);
    for (int port : block.all()) {
      assertTrue(port + " is at or above the ephemeral floor", port < PortBlocks.MAX_PORT);
    }
  }
}
