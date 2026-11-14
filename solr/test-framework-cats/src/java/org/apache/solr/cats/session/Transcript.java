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

package org.apache.solr.cats.session;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Locale;
import java.util.Map;
import org.apache.solr.cats.exec.CommandHistory;
import org.apache.solr.cats.exec.CommandResult;

/**
 * The record of what a scope has run, written to {@code transcript.log} as it goes and kept in
 * memory so a failure can quote it.
 *
 * <p>Formatted to read like a shell session, because on a failure this is the whole diagnosis.
 *
 * <pre>
 *   $ solr start
 *   exit 0 in 3.1s
 *   | Started Solr server on port 46894 (pid=1234). Happy searching!
 *
 *   $ solr zk ls / -z localhost:47894
 *   exit 0 in 1.2s
 *   | aliases.json
 * </pre>
 */
public final class Transcript implements CommandHistory {

  /**
   * Output lines kept per command in the in-memory history, so one noisy command cannot bury it.
   */
  private static final int MAX_OUTPUT_LINES = 40;

  private final Path file;
  private final StringBuilder buffer = new StringBuilder();

  public Transcript(Path file) {
    this.file = file;
  }

  /** Appends one command, preceded by the environment changes relative to the base environment. */
  public synchronized void record(CommandResult result, Map<String, String> envDiff) {
    StringBuilder entry = new StringBuilder();
    if (!envDiff.isEmpty()) {
      envDiff.forEach(
          (k, v) -> entry.append("  export ").append(k).append('=').append(v).append('\n'));
    }
    entry.append("$ ").append(result.command()).append('\n');
    entry
        .append("exit ")
        .append(result.timedOut() ? "TIMED OUT" : result.status())
        .append(" in ")
        .append(String.format(Locale.ROOT, "%.1fs", result.duration().toMillis() / 1000d))
        .append('\n');
    appendOutput(entry, result.output());
    entry.append('\n');
    append(entry.toString());
  }

  /**
   * Appends one HTTP request as the {@code curl} that reproduces it, so a transcript reads as the
   * whole session a test ran rather than only its {@code bin/solr} half.
   */
  public synchronized void recordHttp(String curlCommand, int status, String body) {
    StringBuilder entry = new StringBuilder();
    entry.append("$ ").append(curlCommand).append('\n');
    entry.append(status < 0 ? "no response" : "HTTP " + status).append('\n');
    appendOutput(entry, body);
    entry.append('\n');
    append(entry.toString());
  }

  /** Appends a free-form note, for something the harness itself did. */
  public synchronized void note(String message) {
    append("# " + message + "\n\n");
  }

  private static void appendOutput(StringBuilder entry, String output) {
    if (output.isEmpty()) {
      return;
    }
    String[] lines = output.split("\n", -1);
    int shown = Math.min(lines.length, MAX_OUTPUT_LINES);
    for (int i = 0; i < shown; i++) {
      entry.append("| ").append(lines[i]).append('\n');
    }
    if (lines.length > shown) {
      entry.append("| ... [").append(lines.length - shown).append(" more lines]\n");
    }
  }

  private void append(String text) {
    buffer.append(text);
    try {
      Files.createDirectories(file.getParent());
      Files.writeString(
          file, text, StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    } catch (IOException e) {
      // The in-memory copy is what failures report with; losing the file is not worth failing a
      // test over.
      buffer
          .append("# could not write transcript to ")
          .append(file)
          .append(": ")
          .append(e)
          .append('\n');
    }
  }

  public Path file() {
    return file;
  }

  @Override
  public synchronized String text() {
    return buffer.toString();
  }
}
