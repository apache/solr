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

import java.io.IOException;
import java.io.StringReader;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Hands each test class its own run of consecutive ports, as many as the program under test asked
 * for, so classes can run concurrently in separate forked JVMs.
 *
 * <p>Blocks are claimed from a counter file in the shared output directory under an exclusive file
 * lock, which is what makes this safe across forks — every fork of a Gradle {@code Test} task sees
 * the same output directory. The alternative, asking the OS for a free port and then probing its
 * neighbours, cannot be made safe: {@code ServerSocket(0)} only reserves the one port it returns,
 * so another fork can take {@code P+1} between our check and the program's bind.
 *
 * <p>The counter holds two numbers: where the run started, and the next port it has not handed out.
 * A claim takes the next {@code count} ports and moves that mark along, so blocks stay disjoint
 * whatever the programs of one run ask for — two programs with different port counts included.
 *
 * <p>Nothing here knows what a port is <em>for</em>. Offsets, strides, and a port a program derives
 * from one it was given are the project's business: it asks for as many ports as it needs, and
 * names them itself.
 */
public final class PortBlocks {

  /**
   * The lowest port the harness hands out. High enough to clear the registered services a developer
   * machine runs, and to leave room below for a program that derives a lower port from one of ours.
   */
  public static final int MIN_PORT = 20_000;

  /**
   * One past the highest port the harness hands out. Below every platform's ephemeral floor — the
   * lowest is Linux' 32768 — or the OS hands a claimed port to an outgoing connection and the
   * pre-flight check fails. The gap above also leaves room for a program that derives a higher port
   * from one of ours.
   */
  public static final int MAX_PORT = 31_000;

  /**
   * One past the highest port a run may start from, so that every run has most of the range left to
   * hand out. A run's base is random below this, which is what tells a port claimed now from one an
   * earlier run left behind.
   */
  public static final int MAX_BASE = 26_000;

  private static final String COUNTER_FILE = ".port-blocks";
  private static final String BASE_KEY = "base";
  private static final String NEXT_KEY = "next";

  private PortBlocks() {}

  /**
   * Claims the next {@code count} consecutive ports in {@code outputDir}, creating the counter on
   * first use.
   *
   * <p>The run's base port is recorded in the counter file by whichever JVM gets there first, so
   * every fork agrees on it without the Gradle task having to pass it in. {@code seedBase}, when
   * positive, is that first writer's starting point; otherwise it is random below {@link
   * #MAX_BASE}.
   *
   * @param count how many ports the program needs; zero or less gets an empty block, and no counter
   *     file is created
   */
  public static PortBlock claim(Path outputDir, int seedBase, int count) {
    if (count <= 0) {
      return new Block(0, 0);
    }
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
          return new Block(claimLocked(channel, counter, seedBase, count), count);
        }
      }
    } catch (IOException e) {
      throw new UncheckedIOException("Could not claim a port block from " + counter, e);
    }
  }

  /**
   * Whether {@code port} is within the range this run has handed out so far — from its base up to,
   * but not including, the first port it has not claimed.
   *
   * <p>Used to tell an instance of this run from one an earlier run left behind: the counter file
   * is created fresh per run with a random base, so a port outside the range this one has reached
   * cannot belong to a block claimed now.
   *
   * <p>False when no block has been claimed yet, since there is then no range to measure against.
   */
  public static boolean belongsToThisRun(Path outputDir, int port) {
    Path counter = outputDir.resolve(COUNTER_FILE);
    if (!Files.isRegularFile(counter)) {
      return false;
    }
    try (FileChannel channel = FileChannel.open(counter, StandardOpenOption.READ)) {
      Properties state = read(channel);
      int base = Integer.parseInt(state.getProperty(BASE_KEY, "0"));
      int next = Integer.parseInt(state.getProperty(NEXT_KEY, "0"));
      return base > 0 && port >= base && port < next;
    } catch (IOException | NumberFormatException e) {
      return false;
    }
  }

  private static int claimLocked(FileChannel channel, Path counter, int seedBase, int count)
      throws IOException {
    Properties state = read(channel);

    int base = Integer.parseInt(state.getProperty(BASE_KEY, "0"));
    if (base <= 0) {
      base = seedBase > 0 ? seedBase : ThreadLocalRandom.current().nextInt(MIN_PORT, MAX_BASE);
      state.setProperty(BASE_KEY, Integer.toString(base));
      state.setProperty(NEXT_KEY, Integer.toString(base));
    }

    int claimed = Integer.parseInt(state.getProperty(NEXT_KEY, Integer.toString(base)));
    if (claimed + count > MAX_PORT) {
      throw new IllegalStateException(
          "Ran out of ports: the run based at "
              + base
              + " has reached "
              + claimed
              + " in "
              + counter
              + ", and nothing at or above "
              + MAX_PORT
              + " is handed out. Ports are claimed per test class per run; lower CATS_PORT_BASE, "
              + "or ask whether there really are this many classes.");
    }
    state.setProperty(NEXT_KEY, Integer.toString(claimed + count));
    write(channel, state);

    return claimed;
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
        new StringReader(new String(buffer.array(), 0, buffer.position(), StandardCharsets.UTF_8)));
    return state;
  }

  private static void write(FileChannel channel, Properties state) throws IOException {
    String text =
        String.format(
            Locale.ROOT,
            "# Ports claimed by the CATS harness, in consecutive runs, one per test class.%n"
                + "%s=%s%n%s=%s%n",
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

  /** {@code count} consecutive ports from {@code base}. */
  private record Block(int base, int count) implements PortBlock {

    @Override
    public int at(int index) {
      if (index < 0 || index >= count) {
        throw new IndexOutOfBoundsException(
            "The program under test asked for " + count + " ports, so there is no port " + index);
      }
      return base + index;
    }

    @Override
    public int[] all() {
      int[] ports = new int[count];
      for (int i = 0; i < count; i++) {
        ports[i] = base + i;
      }
      return ports;
    }

    @Override
    public boolean contains(int port) {
      return count > 0 && port >= base && port < base + count;
    }

    @Override
    public boolean isEmpty() {
      return count == 0;
    }

    @Override
    public String toString() {
      return Arrays.toString(all());
    }
  }
}
