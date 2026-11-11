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
   * Windows {@code cmd.exe}. Not yet exercised by CI; the Windows side of the harness is a
   * placeholder until {@code bin/solr.cmd} coverage lands.
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
      Path script = scriptDir.resolve(scriptName + ".cmd");
      String body =
          String.join(
                  "\r\n", "@echo off", "chcp 65001 >nul", "call " + line, "exit /b %ERRORLEVEL%")
              + "\r\n";
      Files.writeString(script, body, StandardCharsets.UTF_8);
      return List.of("cmd.exe", "/d", "/c", script.toString());
    }

    @Override
    public String quote(String arg) {
      // TODO: cmd.exe quoting needs the full ^-escaping treatment for & ^ % ! | < >.
      return '"' + arg.replace("\"", "\"\"") + '"';
    }
  };

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
