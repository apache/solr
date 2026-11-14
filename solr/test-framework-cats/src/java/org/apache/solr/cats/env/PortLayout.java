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

import java.util.Arrays;

/**
 * The shape of the port block the program under test needs, which the harness hands out one of per
 * test class.
 *
 * <p>Ports belong to the program rather than to the harness: a server may need several, a text
 * utility none. {@link #NONE} is the latter, and {@link PortBlocks} hands it an empty block without
 * claiming anything.
 *
 * @param stride distance between the bases of two consecutive blocks, so that the ports of two
 *     blocks never coincide
 * @param offsets the block's ports, relative to its base
 * @param minBase lowest base a run may start from
 * @param maxBase highest base a run may start from
 * @param maxBlocks blocks a run may hand out before the stride arithmetic stops keeping them
 *     disjoint
 * @param primary index into {@code offsets} of the port a request goes to when none is named, or -1
 *     when the program has no such port
 */
public record PortLayout(
    int stride, int[] offsets, int minBase, int maxBase, int maxBlocks, int primary) {

  /** A program that listens on nothing. */
  public static final PortLayout NONE = new PortLayout(0, new int[0], 0, 0, 0, -1);

  public PortLayout {
    if (offsets.length > 0 && stride < 1) {
      throw new IllegalArgumentException("A layout with ports needs a positive stride");
    }
    offsets = offsets.clone();
  }

  @Override
  public int[] offsets() {
    return offsets.clone();
  }

  /** Whether the program listens on nothing. */
  public boolean isEmpty() {
    return offsets.length == 0;
  }

  /** The block based at {@code base}. */
  public PortBlock blockAt(int base) {
    int[] ports = new int[offsets.length];
    for (int i = 0; i < ports.length; i++) {
      ports[i] = base + offsets[i];
    }
    return new Block(base, ports);
  }

  /**
   * Whether {@code port} is one that a run starting from {@code base} could have handed out — that
   * is, {@code base + i * stride + offset} for some block index and some offset.
   *
   * <p>How a port of this run is told from one an earlier run left behind: the base is random per
   * run, so a port this arithmetic does not produce cannot belong to a block claimed now.
   */
  public boolean spans(int base, int port) {
    for (int offset : offsets) {
      int distance = port - base - offset;
      if (distance >= 0 && distance % stride == 0 && distance / stride < maxBlocks) {
        return true;
      }
    }
    return false;
  }

  private record Block(int base, int[] ports) implements PortBlock {

    @Override
    public int at(int index) {
      return ports[index];
    }

    @Override
    public int[] all() {
      return ports.clone();
    }

    @Override
    public boolean contains(int port) {
      return Arrays.stream(ports).anyMatch(p -> p == port);
    }

    @Override
    public String toString() {
      return Arrays.toString(ports);
    }
  }
}
