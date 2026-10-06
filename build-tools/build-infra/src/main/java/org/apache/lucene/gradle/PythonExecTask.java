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

package org.apache.lucene.gradle;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import javax.inject.Inject;
import org.gradle.api.DefaultTask;
import org.gradle.api.GradleException;
import org.gradle.api.file.ConfigurableFileCollection;
import org.gradle.api.file.DirectoryProperty;
import org.gradle.api.file.RegularFileProperty;
import org.gradle.api.provider.ListProperty;
import org.gradle.api.provider.Property;
import org.gradle.api.tasks.CacheableTask;
import org.gradle.api.tasks.InputFile;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.Internal;
import org.gradle.api.tasks.Optional;
import org.gradle.api.tasks.OutputDirectory;
import org.gradle.api.tasks.OutputFile;
import org.gradle.api.tasks.PathSensitive;
import org.gradle.api.tasks.PathSensitivity;
import org.gradle.api.tasks.TaskAction;
import org.gradle.process.ExecOperations;
import org.gradle.process.ExecResult;

/**
 * Runs a Python script, either with a system Python interpreter or, when none is available, with
 * GraalPy resolved as a regular Gradle dependency and launched on the JVM.
 *
 * <p>Interpreter selection is configured centrally in {@code gradle/python.gradle}; tasks only
 * describe <em>what</em> to run. The interpreter is deliberately not a task input, so build cache
 * entries are shared between machines using CPython and machines using GraalPy.
 */
@CacheableTask
public abstract class PythonExecTask extends DefaultTask {
  private static final String GRAALPY_MAIN_CLASS = "com.oracle.graal.python.shell.GraalPythonMain";

  private static final int FAILURE_OUTPUT_LINES = 20;

  @Inject
  protected abstract ExecOperations getExecOperations();

  /** The script to execute. Only its content matters, not its location. */
  @InputFile
  @PathSensitive(PathSensitivity.NONE)
  public abstract RegularFileProperty getScript();

  /** Files and directories the script reads. */
  @InputFiles
  @PathSensitive(PathSensitivity.RELATIVE)
  public abstract ConfigurableFileCollection getScriptInputs();

  /**
   * Arguments passed after the script name. Not an input: these are absolute paths derived from
   * {@link #getScriptInputs()} and the task's outputs, which are tracked in their own right.
   */
  @Internal
  public abstract ListProperty<String> getScriptArgs();

  /** Directory the script writes into. Created before the script runs. */
  @Optional
  @OutputDirectory
  public abstract DirectoryProperty getOutputDir();

  /** If set, stdout and stderr are both captured into this file. */
  @Optional
  @OutputFile
  public abstract RegularFileProperty getLogFile();

  /** System Python interpreter. When absent or blank, the GraalPy fallback is used. */
  @Internal
  public abstract Property<String> getExecutable();

  /** GraalPy runtime jars. Empty unless the fallback is in use. */
  @Internal
  public abstract ConfigurableFileCollection getGraalPyClasspath();

  /** Where Truffle unpacks the Python standard library. */
  @Internal
  public abstract DirectoryProperty getGraalPyResourceCache();

  /** Message prefix used when the script exits with a non-zero status. */
  @Internal
  public abstract Property<String> getFailureMessage();

  @TaskAction
  public void runScript() throws IOException {
    boolean graalPy = getExecutable().getOrElse("").isBlank();

    List<String> argv = new ArrayList<>();
    if (graalPy) {
      // The launcher defaults to the native POSIX backend, which needs Truffle NFI jars that
      // python-language does not depend on. The pure-Java backend is what embedders get anyway.
      argv.add("--python.PosixModuleBackend=java");
    }
    // Never write __pycache__ next to the script; the scripts live in the source tree.
    argv.add("-B");
    argv.add(getScript().get().getAsFile().getAbsolutePath());
    argv.addAll(getScriptArgs().get());

    if (getOutputDir().isPresent()) {
      Files.createDirectories(getOutputDir().get().getAsFile().toPath());
    }

    File log = getLogFile().isPresent() ? getLogFile().get().getAsFile() : null;
    if (log != null && log.getParentFile() != null) {
      Files.createDirectories(log.getParentFile().toPath());
    }

    ByteArrayOutputStream captured = new ByteArrayOutputStream();
    ExecResult result;
    try (OutputStream logStream =
        log != null
            ? new BufferedOutputStream(Files.newOutputStream(log.toPath()))
            : OutputStream.nullOutputStream()) {

      // Both streams share one sink. Gradle pumps stdout and stderr on separate threads and
      // closes each independently, so the log stream has to survive being closed twice; both
      // sinks are synchronized internally, so concurrent writes are safe.
      OutputStream sink = log != null ? nonClosing(logStream) : captured;

      if (graalPy) {
        if (getGraalPyClasspath().isEmpty()) {
          throw new GradleException(
              "No Python interpreter available for "
                  + getPath()
                  + ". Install python3, pass -Ppython3.exe=<path>, or allow the GraalPy"
                  + " fallback with -Psolr.python.mode=auto.");
        }
        getLogger().info("Running {} with the GraalPy fallback", getPath());
        result =
            getExecOperations()
                .javaexec(
                    spec -> {
                      spec.setClasspath(getGraalPyClasspath());
                      spec.getMainClass().set(GRAALPY_MAIN_CLASS);
                      spec.setJvmArgs(graalPyJvmArgs());
                      spec.setArgs(argv);
                      spec.setStandardOutput(sink);
                      spec.setErrorOutput(sink);
                      spec.setIgnoreExitValue(true);
                    });
      } else {
        String executable = getExecutable().get().strip();
        getLogger().info("Running {} with interpreter '{}'", getPath(), executable);
        result =
            getExecOperations()
                .exec(
                    spec -> {
                      spec.setExecutable(executable);
                      spec.setArgs(argv);
                      spec.setStandardOutput(sink);
                      spec.setErrorOutput(sink);
                      spec.setIgnoreExitValue(true);
                    });
      }
    }

    if (result.getExitValue() != 0) {
      StringBuilder message =
          new StringBuilder(getFailureMessage().getOrElse("Python script failed"));
      message.append(" (").append(graalPy ? "GraalPy" : getExecutable().get().strip()).append(")");
      if (captured.size() > 0) {
        message.append(":\n").append(firstLines(captured.toString(StandardCharsets.UTF_8)));
      } else if (log != null) {
        message.append(". First lines of ").append(log).append(":\n");
        message.append(firstLines(Files.readString(log.toPath(), StandardCharsets.UTF_8)));
      }
      throw new GradleException(message.toString());
    }
  }

  private List<String> graalPyJvmArgs() {
    List<String> args =
        new ArrayList<>(
            List.of(
                "-Dfile.encoding=UTF-8",
                "-Dstdout.encoding=UTF-8",
                "-Dstderr.encoding=UTF-8",
                // On a stock JDK Truffle always uses the fallback (interpreter) runtime.
                "-Dpolyglot.engine.WarnInterpreterOnly=false",
                // Interpreted Python frames are much larger than CPython's, and both scripts
                // recurse.
                "-Xss32m"));
    if (getGraalPyResourceCache().isPresent()) {
      args.add(
          "-Dpolyglot.engine.userResourceCache="
              + getGraalPyResourceCache().get().getAsFile().getAbsolutePath());
    }
    return args;
  }

  private static String firstLines(String text) {
    List<String> lines = Arrays.asList(text.split("\\R", -1));
    return String.join("\n", lines.subList(0, Math.min(FAILURE_OUTPUT_LINES, lines.size())));
  }

  /** Wraps a stream so Gradle's output pumps cannot close it from under each other. */
  private static OutputStream nonClosing(OutputStream delegate) {
    return new FilterOutputStream(delegate) {
      @Override
      public void write(byte[] b, int off, int len) throws IOException {
        out.write(b, off, len);
      }

      @Override
      public void close() {
        // No-op; the caller owns the stream.
      }
    };
  }
}
