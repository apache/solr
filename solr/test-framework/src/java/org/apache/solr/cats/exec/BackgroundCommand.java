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

package org.apache.solr.cats.exec;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * A command still running: the live process, what it has printed so far, and the three things a
 * test does with it — wait for a line, wait for it to exit, or stop it.
 *
 * <p>For the shapes {@code run} cannot drive: a program started in the foreground that only exits
 * when it is stopped, and a long-running tool a test watches as it works. The harness holds the
 * process handle, so nothing is detached and no shell-specific {@code nohup} or {@code start /b} is
 * involved — it behaves the same under cmd.exe.
 *
 * <p>Output is captured to a file, as every command's is, so {@link #output()} never blocks and a
 * command that prints more than the harness reads cannot stall.
 */
public final class BackgroundCommand {

  /** How long a process gets to die after being asked to. */
  private static final Duration KILL_TIMEOUT = Duration.ofSeconds(10);

  /** How often {@link #awaitOutput} re-reads the captured output. */
  private static final Duration POLL_INTERVAL = Duration.ofMillis(100);

  private final ShellExecutor.Spec spec;
  private final Process process;
  private final long startNanos;
  private final Path stdoutFile;
  private final Path stderrFile;
  private final Consumer<CommandResult> onExit;

  /** The outcome, once the command has ended; null while it is still running. */
  private CommandResult result;

  BackgroundCommand(
      ShellExecutor.Spec spec,
      Process process,
      long startNanos,
      Path stdoutFile,
      Path stderrFile,
      Consumer<CommandResult> onExit) {
    this.spec = spec;
    this.process = process;
    this.startNanos = startNanos;
    this.stdoutFile = stdoutFile;
    this.stderrFile = stderrFile;
    this.onExit = onExit;
  }

  /** The command line, as it was handed to the shell. */
  public String command() {
    return spec.line();
  }

  /** The pid of the shell process the command runs in. */
  public long pid() {
    return process.pid();
  }

  /** The process handle, for a caller that needs to ask the operating system something. */
  public Process process() {
    return process;
  }

  /** Whether the command has ended: it exited, was waited for, or was killed. */
  public synchronized boolean hasEnded() {
    return result != null;
  }

  /**
   * Everything captured so far, stdout and stderr merged exactly as {@link ShellExecutor#execute}
   * merges them. Reads the capture file, so it never blocks and may be called as often as a poll
   * needs.
   */
  public String output() {
    try {
      String stdout = ShellExecutor.read(stdoutFile);
      return spec.separateStreams() ? stdout + ShellExecutor.read(stderrFile) : stdout;
    } catch (IOException e) {
      throw new UncheckedIOException("Could not read the output of: " + spec.line(), e);
    }
  }

  /**
   * Waits for {@code substring} to appear in the output — the readiness line a program prints when
   * it is up — and fails with everything it has printed so far if it does not.
   *
   * <p>{@code ${NAME}} in the expected text resolves against the session environment, as it does in
   * every other assertion.
   */
  public BackgroundCommand awaitOutput(String substring, Duration timeout) {
    String expected =
        Variables.resolve(
            substring, Variables.ANY, spec.env(), "an assertion", spec.echoedPrefixes());
    long deadline = System.nanoTime() + timeout.toNanos();
    while (true) {
      String output = output();
      if (output.contains(expected)) {
        return this;
      }
      if (System.nanoTime() >= deadline) {
        throw new ShellAssertionError(
            "\n"
                + spec.history().text()
                + "$ "
                + spec.line()
                + " &\n"
                + output
                + "\n\nFAILED: timed out after "
                + timeout.toSeconds()
                + "s waiting for \""
                + expected
                + "\" in the output above\n");
      }
      sleep();
    }
  }

  /**
   * Waits for the command to exit and returns its result, with the same duration semantics as a
   * foreground run: spawn to exit.
   *
   * <p>A command that overruns {@code timeout} is killed and the test fails, exactly as it would
   * have in the foreground.
   */
  public CommandResult waitFor(Duration timeout) {
    CommandResult exited = awaitExit(timeout);
    if (exited.timedOut()) {
      throw exited.fail(
          "the background command did not finish within "
              + timeout.toSeconds()
              + "s "
              + "and was killed");
    }
    return exited;
  }

  /**
   * Stops the command, if it has not ended already. Idempotent, so a test may kill what it waited
   * for and a teardown may kill what the test already stopped.
   */
  public synchronized void kill() {
    if (result != null) {
      return;
    }
    if (process.isAlive()) {
      ProcessKill.killTree(process.toHandle(), KILL_TIMEOUT);
    }
    finish(false);
  }

  /** Waits for the exit, reporting a timeout in the result rather than throwing. */
  synchronized CommandResult awaitExit(Duration timeout) {
    if (result != null) {
      return result;
    }
    boolean exited;
    try {
      exited = process.waitFor(timeout.toMillis(), TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted while running: " + spec.line(), e);
    }
    if (!exited) {
      ProcessKill.killTree(process.toHandle(), KILL_TIMEOUT);
    }
    return finish(!exited);
  }

  /** Builds the result, hands it to the session once, and remembers it. */
  private CommandResult finish(boolean timedOut) {
    Duration duration = Duration.ofNanos(System.nanoTime() - startNanos);
    String stdout;
    String stderr;
    try {
      stdout = ShellExecutor.read(stdoutFile);
      stderr = spec.separateStreams() ? ShellExecutor.read(stderrFile) : "";
    } catch (IOException e) {
      throw new UncheckedIOException("Could not read the output of: " + spec.line(), e);
    }
    result =
        new CommandResult(
            spec.line(),
            spec.shell(),
            spec.cwd(),
            timedOut || process.isAlive() ? -1 : process.exitValue(),
            spec.separateStreams() ? stdout + stderr : stdout,
            stdout,
            stderr,
            duration,
            timedOut,
            // A view, not a copy: assertions resolve against it immediately after the command.
            Collections.unmodifiableMap(spec.env()),
            spec.echoedPrefixes(),
            spec.history());
    onExit.accept(result);
    return result;
  }

  private static void sleep() {
    try {
      TimeUnit.MILLISECONDS.sleep(POLL_INTERVAL.toMillis());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted while waiting for output", e);
    }
  }

  @Override
  public String toString() {
    return "BackgroundCommand[pid " + process.pid() + ": " + spec.line() + "]";
  }
}
