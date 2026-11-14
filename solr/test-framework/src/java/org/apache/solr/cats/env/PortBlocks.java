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
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Hands each test class its own block of ports, in the shape the program under test asked for, so
 * classes can run concurrently in separate forked JVMs.
 *
 * <p>Blocks are claimed from a counter file in the shared output directory under an exclusive file
 * lock, which is what makes this safe across forks — every fork of a Gradle {@code Test} task sees
 * the same output directory. The alternative, asking the OS for a free port and then probing its
 * neighbours, cannot be made safe: {@code ServerSocket(0)} only reserves the one port it returns,
 * so another fork can take {@code P+1} between our check and the program's bind.
 *
 * <p>What a block looks like, how far apart two blocks sit and how many of them a run may hand out
 * are all {@link PortLayout}, which the program declares. A program that listens on nothing gets an
 * empty block and no counter file.
 */
public final class PortBlocks {

  private static final String COUNTER_FILE = ".port-blocks";
  private static final String BASE_KEY = "base";
  private static final String NEXT_KEY = "next";

  private PortBlocks() {}

  /**
   * Claims the next block in {@code outputDir}, creating the counter on first use.
   *
   * <p>The run's base port is recorded in the counter file by whichever JVM gets there first, so
   * every fork agrees on it without the Gradle task having to pass it in. {@code seedBase}, when
   * positive, is that first writer's starting point; otherwise it is random within the layout's
   * base range.
   */
  public static PortBlock claim(Path outputDir, int seedBase, PortLayout layout) {
    if (layout.isEmpty()) {
      return layout.blockAt(0);
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
          return layout.blockAt(claimLocked(channel, counter, seedBase, layout));
        }
      }
    } catch (IOException e) {
      throw new UncheckedIOException("Could not claim a port block from " + counter, e);
    }
  }

  /**
   * Whether {@code port} is one this run can hand out, per {@link PortLayout#spans}.
   *
   * <p>Used to tell an instance of this run from one an earlier run left behind: the counter file
   * is created fresh per run with a random base, so a port the layout cannot derive from that base
   * cannot belong to a block claimed now.
   *
   * <p>False when no block has been claimed yet, since there is then no base to measure from.
   */
  public static boolean belongsToThisRun(Path outputDir, int port, PortLayout layout) {
    Path counter = outputDir.resolve(COUNTER_FILE);
    if (!Files.isRegularFile(counter)) {
      return false;
    }
    try (FileChannel channel = FileChannel.open(counter, StandardOpenOption.READ)) {
      int base = Integer.parseInt(read(channel).getProperty(BASE_KEY, "0"));
      if (base <= 0) {
        return false;
      }
      return layout.spans(base, port);
    } catch (IOException | NumberFormatException e) {
      return false;
    }
  }

  private static int claimLocked(FileChannel channel, Path counter, int seedBase, PortLayout layout)
      throws IOException {
    Properties state = read(channel);

    int base = Integer.parseInt(state.getProperty(BASE_KEY, "0"));
    if (base <= 0) {
      base =
          seedBase > 0
              ? seedBase
              : ThreadLocalRandom.current().nextInt(layout.minBase(), layout.maxBase());
      state.setProperty(BASE_KEY, Integer.toString(base));
    }

    int index = Integer.parseInt(state.getProperty(NEXT_KEY, "0"));
    if (index >= layout.maxBlocks()) {
      throw new IllegalStateException(
          "Ran out of port blocks: "
              + layout.maxBlocks()
              + " already claimed from "
              + counter
              + ". Blocks are claimed per test class per run; raise the layout's maxBlocks and "
              + "re-check the stride arithmetic if there really are this many classes.");
    }
    state.setProperty(NEXT_KEY, Integer.toString(index + 1));
    write(channel, state);

    return base + index * layout.stride();
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
            "# Port blocks claimed by the CATS harness, one per test class.%n%s=%s%n%s=%s%n",
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
