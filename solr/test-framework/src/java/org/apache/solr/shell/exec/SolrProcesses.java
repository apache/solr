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
import java.util.Optional;

/**
 * Finds Solr nodes running anywhere on this machine, the same way {@code bin/solr status} does.
 *
 * <p>The harness needs this to tell a developer that their own Solr is in the way of a test that
 * wants an idle machine. It deliberately mirrors {@code SolrProcessManager}: a Java process whose
 * command line carries {@code -Dsolr.port.listen=}. Nothing here kills anything — a developer's
 * Solr is theirs.
 */
public final class SolrProcesses {

  private static final String PORT_ARG = "-Dsolr.port.listen=";

  private SolrProcesses() {}

  /**
   * The ports of every Solr node visible on this machine, including nodes no test started.
   *
   * <p>Best effort. A process owned by another user, or one whose command line the OS will not
   * disclose, is invisible here; on Windows {@code ProcessHandle} often reports no arguments at
   * all, which is why {@code SolrProcessManager} reaches for WMI. Use it to produce a helpful
   * message, never as proof that the machine is idle.
   */
  public static List<Integer> listeningPorts() {
    return ProcessHandle.allProcesses()
        .filter(p -> p.info().command().orElse("").contains("java"))
        .map(SolrProcesses::listenPort)
        .filter(Optional::isPresent)
        .map(Optional::get)
        .distinct()
        .sorted()
        .toList();
  }

  private static Optional<Integer> listenPort(ProcessHandle process) {
    for (String arg : process.info().arguments().orElse(new String[0])) {
      if (arg.startsWith(PORT_ARG)) {
        return parsePort(arg.substring(PORT_ARG.length()));
      }
    }
    // arguments() is often empty while commandLine() is not, so fall back to scanning the line, as
    // SolrProcessManager does.
    String commandLine = process.info().commandLine().orElse("");
    int at = commandLine.indexOf(PORT_ARG);
    if (at < 0) {
      return Optional.empty();
    }
    String tail = commandLine.substring(at + PORT_ARG.length());
    int end = tail.indexOf(' ');
    return parsePort(end < 0 ? tail : tail.substring(0, end));
  }

  private static Optional<Integer> parsePort(String value) {
    try {
      return Optional.of(Integer.valueOf(value.trim()));
    } catch (NumberFormatException e) {
      return Optional.empty();
    }
  }
}
