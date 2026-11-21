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
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Locale;
import java.util.Map;
import org.apache.solr.cats.exec.CommandHistory;
import org.apache.solr.cats.exec.CommandResult;

/**
 * The record of what a scope has run, written to {@code transcript.log} as it goes and kept in
 * memory so a failure can quote it.
 *
 * <p>Formatted to read like a shell session, because on a failure this is the whole diagnosis: a
 * {@code $} line per command and per environment change, the command's output verbatim, and what
 * the harness itself has to say as {@code # CATS:} comments, so the record pastes back into a
 * shell.
 *
 * <pre>
 * # CATS transcript: MytoolTest.startsAndLists
 * # started: 2026-11-14T12:05:22Z  shell: bash
 *
 * $ export MYTOOL_PORT=46894
 * $ mytool start
 * # CATS: exit 0 in 3.1s
 * Started mytool on port 46894 (pid=1234).
 *
 * $ mytool list --port 46894
 * # CATS: exit 0 in 1.2s
 * nothing yet
 *
 * # CATS: PASSED in 4.6s
 * </pre>
 */
public final class Transcript implements CommandHistory {

  /**
   * Output lines kept per command in the in-memory history, so one noisy command cannot bury it.
   */
  private static final int MAX_OUTPUT_LINES = 40;

  private static final String CATS = "# CATS: ";

  /** UTC, because a test JVM's default zone may be anything the test runner randomised it to. */
  private static final DateTimeFormatter TIMESTAMP = DateTimeFormatter.ISO_INSTANT;

  private final Path file;
  private final StringBuilder buffer = new StringBuilder();
  private final Instant started = Instant.now();
  private boolean echo;

  public Transcript(Path file) {
    this.file = file;
  }

  /** Opens the transcript with what a reader needs to place it. Written once, before anything. */
  public synchronized void header(String scope, String shellId) {
    append(
        "# CATS transcript: "
            + scope
            + "\n# started: "
            + TIMESTAMP.format(started.truncatedTo(ChronoUnit.SECONDS))
            + "  shell: "
            + shellId
            + "\n");
  }

  /**
   * Closes the transcript with the scope's outcome and how long it ran since the header. {@code
   * detail} is a one-line reason, or null.
   */
  public synchronized void outcome(String verdict, String detail) {
    String line = CATS + verdict + " in " + seconds(Duration.between(started, Instant.now()));
    append("\n" + line + (detail == null || detail.isBlank() ? "" : ": " + detail) + "\n");
  }

  /** While on, every entry is also printed to standard output as it is written. */
  public synchronized void echo(boolean on) {
    this.echo = on;
  }

  /**
   * Appends one command: the environment changes since the previous command, the command, what it
   * read on standard input if anything, its exit status and its output.
   */
  public synchronized void record(CommandResult result, Map<String, String> envDiff, String stdin) {
    StringBuilder entry = new StringBuilder("\n");
    appendEnv(entry, envDiff);
    entry.append("$ ").append(result.command()).append('\n');
    if (stdin != null) {
      appendComment(entry, "on standard input:\n" + stdin);
    }
    appendExit(entry, result);
    appendOutput(entry, result.output());
    append(entry.toString());
  }

  /**
   * Appends a command started in the background, which the {@code &} marks as still running. What
   * it did is appended by {@link #recordBackgroundEnd} once it ends.
   */
  public synchronized void recordBackgroundStart(
      String line, long pid, Map<String, String> envDiff) {
    StringBuilder entry = new StringBuilder("\n");
    appendEnv(entry, envDiff);
    entry.append("$ ").append(line).append(" &\n");
    entry.append(CATS).append("started as pid ").append(pid).append('\n');
    append(entry.toString());
  }

  /** Appends what a background command did. Its line is repeated: other commands ran in between. */
  public synchronized void recordBackgroundEnd(CommandResult result) {
    StringBuilder entry = new StringBuilder("\n");
    entry.append("$ ").append(result.command()).append(" &\n");
    appendExit(entry, result);
    appendOutput(entry, result.output());
    append(entry.toString());
  }

  /**
   * Appends one HTTP request as the {@code curl} that reproduces it, so a transcript reads as the
   * whole session a test ran rather than only its command-line half.
   */
  public synchronized void recordHttp(String curlCommand, int status, String body) {
    StringBuilder entry = new StringBuilder("\n");
    entry.append("$ ").append(curlCommand).append('\n');
    entry.append(CATS).append(status < 0 ? "no response" : "HTTP " + status).append('\n');
    appendOutput(entry, body);
    append(entry.toString());
  }

  /** Appends a free-form note, for something the harness itself did. */
  public synchronized void note(String message) {
    StringBuilder entry = new StringBuilder("\n");
    appendComment(entry, message);
    append(entry.toString());
  }

  private static void appendEnv(StringBuilder entry, Map<String, String> envDiff) {
    envDiff.forEach(
        (k, v) -> entry.append("$ export ").append(k).append('=').append(v).append('\n'));
  }

  private static void appendExit(StringBuilder entry, CommandResult result) {
    entry
        .append(CATS)
        .append("exit ")
        .append(result.timedOut() ? "TIMED OUT" : result.status())
        .append(" in ")
        .append(seconds(result.duration()))
        .append('\n');
  }

  /** The first line as a {@code # CATS:} comment, the rest as plain {@code #} comments. */
  private static void appendComment(StringBuilder entry, String message) {
    String[] lines = message.stripTrailing().split("\n", -1);
    for (int i = 0; i < lines.length; i++) {
      entry.append(i == 0 ? CATS : "# ").append(lines[i]).append('\n');
    }
  }

  private static void appendOutput(StringBuilder entry, String output) {
    if (output.isEmpty()) {
      return;
    }
    String[] lines = output.split("\n", -1);
    int shown = Math.min(lines.length, MAX_OUTPUT_LINES);
    for (int i = 0; i < shown; i++) {
      entry.append(lines[i]).append('\n');
    }
    if (lines.length > shown) {
      entry.append(CATS).append("... [").append(lines.length - shown).append(" more lines]\n");
    }
  }

  private static String seconds(Duration duration) {
    return String.format(Locale.ROOT, "%.1fs", duration.toMillis() / 1000d);
  }

  private void append(String text) {
    buffer.append(text);
    if (echo) {
      System.out.print(text);
    }
    try {
      Files.createDirectories(file.getParent());
      Files.writeString(
          file, text, StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    } catch (IOException e) {
      // The in-memory copy is what failures report with; losing the file is not worth failing a
      // test over.
      buffer
          .append(CATS)
          .append("could not write transcript to ")
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
