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

import org.apache.solr.cats.env.PortBlock;

/**
 * Solr's names for the ports a shell test class owns: three node ports, the embedded ZooKeeper
 * port, and a stop port per node. The first four mirror the {@code SOLR_PORT}, {@code SOLR2_PORT},
 * {@code SOLR3_PORT} and {@code ZK_PORT} variables the BATS suites used.
 *
 * <p>A view over the seven consecutive ports the harness claimed, which is where all of Solr's port
 * knowledge lives — the generic harness only counts them. {@link
 * org.apache.solr.shell.exec.SolrProgram#PORT_COUNT} says how many, and why the stop ports are
 * allocated before anything passes them.
 */
public record SolrPorts(PortBlock block) implements PortBlock {

  public int solr() {
    return block.at(0);
  }

  public int solr2() {
    return block.at(1);
  }

  public int solr3() {
    return block.at(2);
  }

  public int zk() {
    return block.at(3);
  }

  /** The stop port of a node on {@link #solr()}. */
  public int stop() {
    return block.at(4);
  }

  /** The stop port of a node on {@link #solr2()}. */
  public int stop2() {
    return block.at(5);
  }

  /** The stop port of a node on {@link #solr3()}. */
  public int stop3() {
    return block.at(6);
  }

  @Override
  public int base() {
    return block.base();
  }

  @Override
  public int at(int index) {
    return block.at(index);
  }

  @Override
  public int[] all() {
    return block.all();
  }

  /**
   * Whether a port belongs to this block.
   *
   * <p>Used to tell the harness' own nodes apart from everyone else's where the record of them is
   * shared: {@code bin/solr.cmd} keeps it in {@code %SOLR_TIP%\bin} for every class and every
   * concurrent suite alike.
   */
  @Override
  public boolean contains(int port) {
    return block.contains(port);
  }

  @Override
  public boolean isEmpty() {
    return block.isEmpty();
  }

  @Override
  public String toString() {
    return block.toString();
  }
}
