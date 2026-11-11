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

package org.apache.solr.shell.exec;

import java.util.List;
import org.apache.solr.cli.SolrProcessManager;
import org.apache.solr.cli.SolrProcessManager.SolrProcess;

/**
 * Which Solr nodes the harness has running, asked of the same code {@code bin/solr status} uses.
 *
 * <p>Deliberately not done by reading pid files: {@code bin/solr} writes {@code solr-<port>.pid}
 * holding a PID while {@code bin\solr.cmd} writes {@code solr-<port>.port} holding a port. Going
 * through {@link SolrProcessManager}, which resolves either, means one code path for both platforms
 * and finds a node started in the <em>foreground</em>, which writes no file at all.
 *
 * <p>Scoped by the command-line marker the harness starts its nodes with, so what comes back is the
 * harness' own nodes and never a developer's Solr or another suite's.
 */
public final class SolrNodes {

  /**
   * The prefix every harness marker begins with. Discovery matches on a substring, so this finds
   * the harness' nodes from any run, where a full marker only finds one scope's.
   */
  public static final String MARKER_PREFIX = "solr-shell-test-";

  private SolrNodes() {}

  /**
   * The running nodes whose command line contains {@code marker}.
   *
   * <p>Pass a full marker for one scope's nodes, or {@link #MARKER_PREFIX} for every harness node
   * on the host.
   */
  public static List<SolrProcess> running(String marker) {
    // SolrProcessManager reads the marker from a system property, via EnvUtils, when constructed.
    // Set and restored around the call so the harness JVM is left as it was.
    String previous = System.getProperty(SolrProcessManager.PROCESS_SCAN_MARKER);
    System.setProperty(SolrProcessManager.PROCESS_SCAN_MARKER, marker);
    try {
      return List.copyOf(new SolrProcessManager().getAllRunning());
    } finally {
      if (previous == null) {
        System.clearProperty(SolrProcessManager.PROCESS_SCAN_MARKER);
      } else {
        System.setProperty(SolrProcessManager.PROCESS_SCAN_MARKER, previous);
      }
    }
  }
}
