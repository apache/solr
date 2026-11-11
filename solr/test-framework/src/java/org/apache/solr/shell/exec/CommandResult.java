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
import java.util.Map;
import java.util.regex.Matcher;
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
 * @param vars an unmodifiable view of the session environment this ran with. Assertions resolve
 *     {@code ${NAME}} in their expected text against it, so an assertion reads exactly like the
 *     BATS one it was ported from.
 * @param history everything the scope has run, which a failure quotes instead of a stack trace
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
    Map<String, String> vars,
    CommandHistory history) {

  /** Whether this run targets the experimental picocli CLI. */
  public boolean picocli() {
    return Boolean.parseBoolean(vars.getOrDefault("SOLR_PICOCLI", "false"));
  }

  /** Resolves {@code ${NAME}} and friends in an assertion's expected text. */
  private String resolve(String expected) {
    return Variables.resolve(expected, Variables.ANY, vars, "an assertion");
  }

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

  /** Port of {@code | wc -l}. */
  public int lineCount() {
    return lines().size();
  }

  /** Port of {@code | grep <regex>}: the lines the pattern matches anywhere in. */
  public List<String> linesMatching(String regex) {
    Pattern pattern = Pattern.compile(resolve(regex));
    return lines().stream().filter(line -> pattern.matcher(line).find()).toList();
  }

  /**
   * Port of a {@code sed} capture: the first match of {@code regex}, narrowed to capturing group
   * one when the pattern has one.
   *
   * @throws AssertionError if the pattern does not match, since a test that goes on to use the
   *     empty string would fail later and less clearly
   */
  public String firstMatch(String regex) {
    Matcher matcher = Pattern.compile(resolve(regex), Pattern.DOTALL).matcher(output);
    if (!matcher.find()) {
      throw fail("output did not match: " + resolve(regex));
    }
    return matcher.groupCount() >= 1 ? matcher.group(1) : matcher.group();
  }

  /** Port of {@code | head -n <count>}. Fewer lines than asked for is not an error. */
  public List<String> firstLines(int count) {
    List<String> lines = lines();
    return lines.subList(0, Math.min(count, lines.size()));
  }

  /** Port of {@code | tail -n <count>}. Fewer lines than asked for is not an error. */
  public List<String> lastLines(int count) {
    List<String> lines = lines();
    return lines.subList(Math.max(0, lines.size() - count), lines.size());
  }

  /** Port of {@code | awk '{print $n}'}: the whitespace-separated fields of one output line. */
  public List<String> fields(int lineIndex) {
    List<String> lines = lines();
    if (lineIndex < 0 || lineIndex >= lines.size()) {
      throw fail("output has " + lines.size() + " line(s), wanted line " + lineIndex);
    }
    String line = lines.get(lineIndex).strip();
    return line.isEmpty() ? List.of() : List.of(line.split("\\s+"));
  }

  /** Port of {@code | grep -c}: how many times {@code substring} occurs in the output. */
  public int countOccurrences(String substring) {
    String needle = resolve(substring);
    if (needle.isEmpty()) {
      throw new IllegalArgumentException("countOccurrences needs a non-empty substring");
    }
    int count = 0;
    for (int from = output.indexOf(needle); from >= 0; from = output.indexOf(needle, from + 1)) {
      count++;
    }
    return count;
  }

  public CommandResult assertSuccess() {
    if (failed()) {
      throw fail("command exited " + status + ", expected 0");
    }
    return this;
  }

  public CommandResult assertFailure() {
    if (succeeded()) {
      throw fail("command exited 0, expected a non-zero status");
    }
    return this;
  }

  public CommandResult assertStatus(int expected) {
    if (status != expected) {
      throw fail("command exited " + status + ", expected " + expected);
    }
    return this;
  }

  /** Port of {@code assert_output --partial}. */
  public CommandResult assertContains(String substring) {
    String expected = resolve(substring);
    if (!output.contains(expected)) {
      throw fail("output did not contain: " + expected);
    }
    return this;
  }

  /** Port of {@code refute_output --partial}. */
  public CommandResult assertNotContains(String substring) {
    String expected = resolve(substring);
    if (output.contains(expected)) {
      throw fail("output contained what it should not: " + expected);
    }
    return this;
  }

  /** Port of {@code assert_output --regexp}. The pattern may match anywhere in the output. */
  public CommandResult assertMatches(String regex) {
    if (!Pattern.compile(resolve(regex), Pattern.DOTALL).matcher(output).find()) {
      throw fail("output did not match: " + resolve(regex));
    }
    return this;
  }

  /** Port of {@code refute_output --regexp}. */
  public CommandResult assertNotMatches(String regex) {
    if (Pattern.compile(resolve(regex), Pattern.DOTALL).matcher(output).find()) {
      throw fail("output matched what it should not: " + resolve(regex));
    }
    return this;
  }

  /**
   * Port of {@code assert_output} with no flag: the whole output must be exactly {@code expected}.
   *
   * <p>Named {@code assertExact} rather than {@code assertEquals} so it cannot be mistaken, in a
   * diff, for the static {@code assertEquals(expected, actual)} every test class inherits from
   * JUnit.
   */
  public CommandResult assertExact(String expectedText) {
    String expected = resolve(expectedText);
    if (!output.equals(expected)) {
      throw fail("output was not exactly: " + expected);
    }
    return this;
  }

  /** Port of {@code assert_line --index <i>}. */
  public CommandResult assertLine(int index, String expected) {
    List<String> lines = lines();
    if (index < 0 || index >= lines.size()) {
      throw fail("output has " + lines.size() + " line(s), wanted line " + index);
    }
    if (!lines.get(index).equals(resolve(expected))) {
      throw fail("line " + index + " was not: " + resolve(expected));
    }
    return this;
  }

  /** Port of {@code [ $(... | wc -l) -eq n ]}. */
  public CommandResult assertLineCount(int expected) {
    if (lineCount() != expected) {
      throw fail("output had " + lineCount() + " line(s), expected " + expected);
    }
    return this;
  }

  /** Port of {@code [ $(... | grep -c x) -eq n ]}. */
  public CommandResult assertContainsTimes(String substring, int expected) {
    int actual = countOccurrences(substring);
    if (actual != expected) {
      throw fail(
          "output contained "
              + resolve(substring)
              + " "
              + actual
              + " time(s), expected "
              + expected);
    }
    return this;
  }

  /**
   * Port of {@code assert_output_for_cli}: asserts the commons-cli wording, or the picocli wording
   * when the run targets the picocli CLI.
   */
  // TODO: Remove this once the commons-cli cleanup happens.
  public CommandResult assertContainsForCli(String commonsCliText, String picocliText) {
    return assertContains(picocli() ? picocliText : commonsCliText);
  }

  /**
   * The failure a test sees: everything the scope ran, then one line saying how this command
   * differed from what the test expected.
   *
   * <p>A {@link ShellAssertionError}, so no Java stack trace is printed over it. The frames would
   * all be harness internals, while the commands and their output are the diagnosis.
   */
  private AssertionError fail(String reason) {
    String ran = history.text();
    StringBuilder report = new StringBuilder("\n");
    if (ran.isBlank()) {
      // No session behind this result, so report the one command we do have.
      report.append("$ ").append(command).append('\n').append(truncate(output)).append("\n\n");
    } else {
      report.append(ran);
    }
    report.append("FAILED: ").append(reason).append('\n');
    return new ShellAssertionError(report.toString());
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
