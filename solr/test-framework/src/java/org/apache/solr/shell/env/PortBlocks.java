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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Hands each test class its own block of ports, so classes can run concurrently in separate forked
 * JVMs.
 *
 * <p>Blocks are claimed from a counter file in the shared output directory under an exclusive file
 * lock, which is what makes this safe across forks — every fork of a Gradle {@code Test} task sees
 * the same {@code TEST_OUTPUT_DIR}. The alternative, asking the OS for a free port and then probing
 * its neighbours, cannot be made safe: {@code ServerSocket(0)} only reserves the one port it
 * returns, so another fork can take {@code P+1} between our check and Solr's bind.
 *
 * <p>The block layout is dictated by Solr, not chosen. A node on port {@code P} also listens on
 * {@code P-1000} for {@code solr stop} (see {@code bin/solr}) and, in cloud mode, runs embedded
 * ZooKeeper on {@code P+1000} (see {@code SolrZkServer}). A class gets three node ports, so it
 * really occupies {@code P-1000..P-998}, {@code P..P+2} and {@code P+1000..P+1002}.
 *
 * <p>Those windows overlap between blocks, but the ports actually used do not: every block shifts
 * all three groups by the same {@link #STRIDE}, so a collision needs {@code (j-i) * STRIDE} to land
 * on 1000, which is why no more than {@link #MAX_BLOCKS} blocks may be handed out per run.
 */
public final class PortBlocks {

  /** Distance between consecutive blocks. Three node ports plus one spare. */
  static final int STRIDE = 4;

  /**
   * Blocks per run. At {@link #STRIDE} 4 a collision would first need block distance 250, where one
   * block's node ports would land on another's embedded ZooKeeper ports.
   */
  static final int MAX_BLOCKS = 200;

  /** Lowest base port a run may start from, leaving room for the {@code P-1000} stop ports. */
  private static final int MIN_BASE = 20_000;

  /** Highest base, leaving room for {@code MAX_BLOCKS} strides plus the {@code +1000} ZK window. */
  private static final int MAX_BASE = 50_000;

  private static final String COUNTER_FILE = ".port-blocks";
  private static final String BASE_KEY = "base";
  private static final String NEXT_KEY = "next";

  private PortBlocks() {}

  /**
   * Claims the next block in {@code outputDir}, creating the counter on first use.
   *
   * <p>The run's base port is recorded in the counter file by whichever JVM gets there first, so
   * every fork agrees on it without the Gradle task having to pass it in. {@code seedBase}, when
   * positive, is that first writer's starting point; otherwise it is random.
   */
  public static Ports claim(Path outputDir, int seedBase) {
    Path counter = outputDir.resolve(COUNTER_FILE);
    try {
      Files.createDirectories(outputDir);
      try (FileChannel channel =
          FileChannel.open(
              counter,
              StandardOpenOption.CREATE,
              StandardOpenOption.READ,
              StandardOpenOption.WRITE)) {
        // Held for the whole read-modify-write; this is the cross-fork mutual exclusion.
        try (FileLock lock = channel.lock()) {
          assert lock.isValid();
          return claimLocked(channel, counter, seedBase);
        }
      }
    } catch (IOException e) {
      throw new UncheckedIOException("Could not claim a port block from " + counter, e);
    }
  }

  /**
   * Whether {@code port} lies anywhere in the span this run can hand out.
   *
   * <p>Used to tell a node of this run from one an earlier run left behind: the counter file is
   * created fresh per run with a random base, so a port outside the span cannot belong to a block
   * claimed now. The span covers the {@code P-1000} stop ports and the {@code P+1000} ZooKeeper
   * ports as well as the node ports themselves.
   *
   * <p>False when no block has been claimed yet, since there is then no span to be inside.
   */
  public static boolean belongsToThisRun(Path outputDir, int port) {
    Path counter = outputDir.resolve(COUNTER_FILE);
    if (!Files.isRegularFile(counter)) {
      return false;
    }
    try (FileChannel channel = FileChannel.open(counter, StandardOpenOption.READ)) {
      int base = Integer.parseInt(read(channel).getProperty(BASE_KEY, "0"));
      if (base <= 0) {
        return false;
      }
      return port >= base - 1000 && port <= base + MAX_BLOCKS * STRIDE + 1002;
    } catch (IOException | NumberFormatException e) {
      return false;
    }
  }

  private static Ports claimLocked(FileChannel channel, Path counter, int seedBase)
      throws IOException {
    Properties state = read(channel);

    int base = Integer.parseInt(state.getProperty(BASE_KEY, "0"));
    if (base <= 0) {
      base = seedBase > 0 ? seedBase : ThreadLocalRandom.current().nextInt(MIN_BASE, MAX_BASE);
      state.setProperty(BASE_KEY, Integer.toString(base));
    }

    int index = Integer.parseInt(state.getProperty(NEXT_KEY, "0"));
    if (index >= MAX_BLOCKS) {
      throw new IllegalStateException(
          "Ran out of port blocks: "
              + MAX_BLOCKS
              + " already claimed from "
              + counter
              + ". Blocks are claimed per test class per run; raise PortBlocks.MAX_BLOCKS and "
              + "re-check the stride arithmetic if there really are this many classes.");
    }
    state.setProperty(NEXT_KEY, Integer.toString(index + 1));
    write(channel, state);

    // Mirrors Solr: node ports are consecutive from the base, embedded ZooKeeper sits 1000 above
    // the first of them.
    return Ports.fromBase(base + index * STRIDE);
  }

  private static Properties read(FileChannel channel) throws IOException {
    Properties state = new Properties();
    long size = channel.size();
    if (size == 0) {
      return state;
    }
    ByteBuffer buffer = ByteBuffer.allocate((int) size);
    channel.position(0);
    while (buffer.hasRemaining() && channel.read(buffer) > 0) {
      // keep reading
    }
    state.load(
        new java.io.StringReader(
            new String(buffer.array(), 0, buffer.position(), StandardCharsets.UTF_8)));
    return state;
  }

  private static void write(FileChannel channel, Properties state) throws IOException {
    String text =
        String.format(
            Locale.ROOT,
            "# Port blocks claimed by the Solr shell test harness, one per test class.%n%s=%s%n%s=%s%n",
            BASE_KEY,
            state.getProperty(BASE_KEY),
            NEXT_KEY,
            state.getProperty(NEXT_KEY));
    channel.truncate(0);
    channel.position(0);
    ByteBuffer buffer = ByteBuffer.wrap(text.getBytes(StandardCharsets.UTF_8));
    while (buffer.hasRemaining()) {
      channel.write(buffer);
    }
    channel.force(true);
  }
}
