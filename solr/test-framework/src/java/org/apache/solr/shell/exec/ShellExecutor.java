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

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.ProcessBuilder.Redirect;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.apache.lucene.util.SuppressForbidden;

/**
 * Runs one command line in a fresh shell process and collects the result.
 *
 * <p>stdout and stderr always go to <em>files</em>, never pipes: a detached Solr JVM that inherits
 * a file handle cannot block the harness, while one that inherits a pipe can. stdin is the null
 * device, so a command that prompts for input fails fast instead of hanging.
 */
public final class ShellExecutor {

  /** How much output is captured before truncation, in bytes. */
  private static final long OUTPUT_LIMIT_BYTES = 10L * 1024 * 1024;

  private ShellExecutor() {}

  /**
   * What to run and how.
   *
   * @param line the command line, handed to the shell as-is
   * @param workDir where temp scripts and the captured output files are written
   * @param tag a name unique within {@code workDir}, used for those files
   * @param separateStreams keep stdout and stderr apart instead of merging them like BATS' {@code
   *     run} does
   */
  public record Spec(
      String line,
      Shell shell,
      Path cwd,
      Map<String, String> env,
      Path workDir,
      String tag,
      Duration timeout,
      boolean separateStreams,
      boolean picocli) {}

  /**
   * Runs the command, killing it if it overruns {@link Spec#timeout()}.
   *
   * @param processRegistrar notified of the live process, so the caller can force-kill it later if
   *     cleanup finds it still alive
   */
  public static CommandResult execute(Spec spec, Consumer<Process> processRegistrar) {
    try {
      Files.createDirectories(spec.workDir());
      Path stdoutFile = spec.workDir().resolve(spec.tag() + ".out");
      Path stderrFile = spec.workDir().resolve(spec.tag() + ".err");

      List<String> argv = spec.shell().processArgs(spec.line(), spec.workDir(), spec.tag());

      ProcessBuilder pb = new ProcessBuilder(argv);
      pb.directory(spec.cwd().toFile());
      pb.environment().clear();
      pb.environment().putAll(spec.env());
      pb.redirectInput(Redirect.from(nullDevice()));
      pb.redirectOutput(Redirect.to(stdoutFile.toFile()));
      if (spec.separateStreams()) {
        pb.redirectError(Redirect.to(stderrFile.toFile()));
      } else {
        pb.redirectErrorStream(true);
      }

      long startNanos = System.nanoTime();
      Process process = pb.start();
      processRegistrar.accept(process);

      boolean exited = process.waitFor(spec.timeout().toMillis(), TimeUnit.MILLISECONDS);
      boolean timedOut = !exited;
      if (timedOut) {
        ProcessKill.killTree(process.toHandle(), Duration.ofSeconds(10));
      }
      Duration duration = Duration.ofNanos(System.nanoTime() - startNanos);
      int status = timedOut ? -1 : process.exitValue();

      String stdout = read(stdoutFile);
      String stderr = spec.separateStreams() ? read(stderrFile) : "";
      String output = spec.separateStreams() ? stdout + stderr : stdout;

      return new CommandResult(
          spec.line(),
          spec.shell(),
          spec.cwd(),
          status,
          output,
          stdout,
          stderr,
          duration,
          timedOut,
          spec.picocli());
    } catch (IOException e) {
      throw new UncheckedIOException("Could not run: " + spec.line(), e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted while running: " + spec.line(), e);
    }
  }

  /** Decodes as UTF-8, normalises CRLF to LF and strips one trailing newline, as BATS does. */
  private static String read(Path file) throws IOException {
    if (!Files.exists(file)) {
      return "";
    }
    long size = Files.size(file);
    String text;
    if (size <= OUTPUT_LIMIT_BYTES) {
      text = Files.readString(file, StandardCharsets.UTF_8);
    } else {
      byte[] head = new byte[(int) OUTPUT_LIMIT_BYTES];
      try (var in = Files.newInputStream(file)) {
        int read = in.readNBytes(head, 0, head.length);
        text =
            new String(head, 0, read, StandardCharsets.UTF_8)
                + "\n... ["
                + (size - read)
                + " more bytes truncated]";
      }
    }
    text = text.replace("\r\n", "\n");
    if (text.endsWith("\n")) {
      text = text.substring(0, text.length() - 1);
    }
    return text;
  }

  @SuppressForbidden(reason = "ProcessBuilder.Redirect only accepts java.io.File")
  private static File nullDevice() {
    boolean windows =
        System.getProperty("os.name", "").toLowerCase(Locale.ROOT).startsWith("windows");
    return new File(windows ? "NUL" : "/dev/null");
  }
}
