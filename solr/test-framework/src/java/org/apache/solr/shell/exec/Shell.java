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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;

/** The shells a command line can be handed to. One process per command; no interactive session. */
public enum Shell {

  /** POSIX shell. Invoked with a single {@code -c} argument, no profile and no rc files. */
  BASH {
    @Override
    public List<String> processArgs(String line, Path scriptDir, String scriptName) {
      return List.of("bash", "--noprofile", "--norc", "-c", line);
    }

    @Override
    public String quote(String arg) {
      return "'" + arg.replace("'", "'\\''") + "'";
    }
  },

  /**
   * Windows {@code cmd.exe}.
   *
   * <p>The line goes into a temp {@code .cmd} script rather than onto the {@code cmd /c} command
   * line, because {@code ProcessBuilder}'s quoting rules for {@code cmd /c} and {@code .cmd}
   * arguments are special and fragile. {@code call} is required: without it a batch file invoking
   * {@code solr.cmd} never gets control back. {@code /d} disables AutoRun registry hooks.
   */
  CMD {
    @Override
    public List<String> processArgs(String line, Path scriptDir, String scriptName)
        throws IOException {
      // 'call' is required for a single command, because without it a batch file invoking
      // solr.cmd never gets control back. It takes one command though, so a multi-line body —
      // which is the whole point of script(bash, cmd) — is written verbatim instead, and is
      // responsible for its own 'call' where it invokes another batch file.
      String command = line.contains("\n") ? line : "call " + line;
      String body =
          String.join("\r\n", "@echo off", "chcp 65001 >nul", command, "exit /b %ERRORLEVEL%")
              + "\r\n";
      Path script = scriptDir.resolve(scriptName + ".cmd");
      Files.writeString(
          script, body.replace("\n", "\r\n").replace("\r\r\n", "\r\n"), StandardCharsets.UTF_8);
      return List.of("cmd.exe", "/d", "/c", script.toString());
    }

    /**
     * Double quotes protect {@code & | < > ^ ( )} from cmd's parser, and {@code !} is literal
     * because these scripts never enable delayed expansion. {@code %} is the exception: it is
     * expanded even inside double quotes, and in a batch file a literal one is written {@code %%}.
     *
     * <p>An embedded double quote is refused rather than mangled. cmd has no escape for one that
     * survives both its own quote counting and the target program's parser, and guessing would
     * produce a command that runs but means something else.
     */
    @Override
    public String quote(String arg) {
      if (arg.indexOf('"') >= 0) {
        throw new UnsupportedOperationException(
            "Cannot quote an argument containing a double quote for cmd.exe. "
                + "Use script(bashBody, cmdBody) for this one: "
                + arg);
      }
      String escaped = arg.replace("%", "%%");
      return NEEDS_CMD_QUOTING.matcher(arg).find() ? '"' + escaped + '"' : escaped;
    }
  };

  /** Characters that make cmd.exe need an argument wrapped in double quotes. */
  private static final Pattern NEEDS_CMD_QUOTING = Pattern.compile("[\\s&|<>^()]");

  /** System property that overrides the shell choice. */
  public static final String SHELL_SYSPROP = "solr.cli.shell";

  /** Environment variable that overrides the shell choice. */
  public static final String SHELL_ENV = "SOLR_CLI_SHELL";

  /**
   * Builds the argv for running {@code line}, writing any helper script into {@code scriptDir}.
   *
   * @param scriptName a per-command name, unique within {@code scriptDir}
   */
  public abstract List<String> processArgs(String line, Path scriptDir, String scriptName)
      throws IOException;

  /** Quotes a single argument so this shell passes it through verbatim. */
  public abstract String quote(String arg);

  /** Joins an argv into one command line for this shell. */
  public String joinArgs(List<String> argv) {
    StringBuilder sb = new StringBuilder();
    for (String arg : argv) {
      if (sb.length() > 0) {
        sb.append(' ');
      }
      sb.append(quote(arg));
    }
    return sb.toString();
  }

  /**
   * {@code CMD} on Windows and {@code BASH} elsewhere, overridable with the {@value #SHELL_SYSPROP}
   * system property or the {@value #SHELL_ENV} environment variable.
   */
  public static Shell defaultShell() {
    String override = System.getProperty(SHELL_SYSPROP, System.getenv(SHELL_ENV));
    if (override != null && !override.isBlank()) {
      return valueOf(override.trim().toUpperCase(Locale.ROOT));
    }
    return System.getProperty("os.name", "").toLowerCase(Locale.ROOT).startsWith("windows")
        ? CMD
        : BASH;
  }
}
