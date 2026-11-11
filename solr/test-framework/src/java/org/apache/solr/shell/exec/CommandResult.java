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

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.regex.Pattern;

/**
 * The outcome of one shell command, with BATS-style fluent assertions.
 *
 * <p>Every assertion failure carries the command, shell, cwd, status, duration and the captured
 * output, so a CI log alone is enough to diagnose it.
 *
 * @param command the line as handed to the shell
 * @param output stdout and stderr merged, as BATS' {@code run} produces, or their concatenation
 *     when the command was run with the streams kept apart
 * @param stdout stdout alone; equal to {@code output} when the streams were merged
 * @param stderr stderr alone; empty when the streams were merged
 * @param timedOut whether the command was killed for exceeding its timeout
 * @param picocli whether this run targets the experimental picocli CLI
 */
public record CommandResult(
    String command,
    Shell shell,
    Path cwd,
    int status,
    String output,
    String stdout,
    String stderr,
    Duration duration,
    boolean timedOut,
    boolean picocli) {

  /** How much of the output an assertion message shows. */
  private static final int MESSAGE_OUTPUT_LIMIT = 20_000;

  public boolean succeeded() {
    return status == 0;
  }

  public boolean failed() {
    return status != 0;
  }

  /** The merged output split on {@code \n}; empty output yields an empty list. */
  public List<String> lines() {
    return output.isEmpty() ? List.of() : List.of(output.split("\n", -1));
  }

  public CommandResult assertSuccess() {
    if (failed()) {
      throw fail("expected exit status 0");
    }
    return this;
  }

  public CommandResult assertFailure() {
    if (succeeded()) {
      throw fail("expected a non-zero exit status");
    }
    return this;
  }

  public CommandResult assertStatus(int expected) {
    if (status != expected) {
      throw fail("expected exit status " + expected);
    }
    return this;
  }

  /** Port of {@code assert_output --partial}. */
  public CommandResult assertOutputContains(String substring) {
    if (!output.contains(substring)) {
      throw fail("expected output to contain: " + substring);
    }
    return this;
  }

  /** Port of {@code refute_output --partial}. */
  public CommandResult assertOutputNotContains(String substring) {
    if (output.contains(substring)) {
      throw fail("expected output NOT to contain: " + substring);
    }
    return this;
  }

  /** Port of {@code assert_output --regexp}. The pattern may match anywhere in the output. */
  public CommandResult assertOutputMatches(String regex) {
    if (!Pattern.compile(regex, Pattern.DOTALL).matcher(output).find()) {
      throw fail("expected output to match regex: " + regex);
    }
    return this;
  }

  /** Port of {@code refute_output --regexp}. */
  public CommandResult assertOutputNotMatches(String regex) {
    if (Pattern.compile(regex, Pattern.DOTALL).matcher(output).find()) {
      throw fail("expected output NOT to match regex: " + regex);
    }
    return this;
  }

  /** Port of {@code assert_output} with no flag: the whole output must equal {@code expected}. */
  public CommandResult assertOutputEquals(String expected) {
    if (!output.equals(expected)) {
      throw fail("expected output to equal: " + expected);
    }
    return this;
  }

  /** Port of {@code assert_line --index <i>}. */
  public CommandResult assertLine(int index, String expected) {
    List<String> lines = lines();
    if (index < 0 || index >= lines.size()) {
      throw fail("expected at least " + (index + 1) + " output line(s)");
    }
    if (!lines.get(index).equals(expected)) {
      throw fail("expected line " + index + " to equal: " + expected);
    }
    return this;
  }

  /**
   * Port of {@code assert_output_for_cli}: asserts the commons-cli wording, or the picocli wording
   * when the run targets the picocli CLI.
   */
  // TODO: Remove this once the commons-cli cleanup happens.
  public CommandResult assertOutputForCli(String commonsCliText, String picocliText) {
    return assertOutputContains(picocli ? picocliText : commonsCliText);
  }

  private AssertionError fail(String what) {
    return new AssertionError(what + "\n\n" + transcriptEntry());
  }

  /** A single human-readable block describing this command and its outcome. */
  public String transcriptEntry() {
    StringBuilder sb = new StringBuilder();
    sb.append("command : ").append(command).append('\n');
    sb.append("shell   : ").append(shell).append('\n');
    sb.append("cwd     : ").append(cwd).append('\n');
    sb.append("status  : ").append(status);
    if (timedOut) {
      sb.append("  (TIMED OUT)");
    }
    sb.append('\n');
    sb.append("duration: ").append(duration.toMillis()).append(" ms\n");
    sb.append("output  :\n").append(truncate(output)).append('\n');
    if (!stderr.isEmpty() && !stderr.equals(output)) {
      sb.append("stderr  :\n").append(truncate(stderr)).append('\n');
    }
    return sb.toString();
  }

  private static String truncate(String s) {
    if (s.length() <= MESSAGE_OUTPUT_LIMIT) {
      return s;
    }
    return s.substring(0, MESSAGE_OUTPUT_LIMIT)
        + "\n... ["
        + (s.length() - MESSAGE_OUTPUT_LIMIT)
        + " more characters truncated]";
  }
}
