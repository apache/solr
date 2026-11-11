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

/**
 * The block of ports a shell test run owns, mirroring the {@code SOLR_PORT}, {@code SOLR2_PORT},
 * {@code SOLR3_PORT} and {@code ZK_PORT} variables used by the BATS suites.
 */
public record Ports(int solr, int solr2, int solr3, int zk) {

  /**
   * Derives the conventional block from a base port: {@code P}, {@code P+1}, {@code P+2}, {@code
   * P+1000}.
   */
  public static Ports fromBase(int base) {
    return new Ports(base, base + 1, base + 2, base + 1000);
  }

  public int[] all() {
    return new int[] {solr, solr2, solr3, zk};
  }

  /**
   * Whether a port belongs to this block.
   *
   * <p>Used to tell the harness' own nodes apart from everyone else's where the record of them is
   * shared: {@code bin/solr.cmd} keeps it in {@code %SOLR_TIP%\bin} for every class and every
   * concurrent suite alike.
   */
  public boolean contains(int port) {
    return port == solr || port == solr2 || port == solr3 || port == zk;
  }
}
