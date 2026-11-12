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
   * arguments are special and fragile. {@code call} is prepended to a line that invokes a command —
   * without it a batch file invoking {@code solr.cmd} never gets control back — while a multi-line
   * body, or one that opens with cmd's own syntax, is written verbatim and is responsible for its
   * own. {@code /d} disables AutoRun registry hooks.
   */
  CMD {
    @Override
    public List<String> processArgs(String line, Path scriptDir, String scriptName)
        throws IOException {
      String command = takesCallPrefix(line) ? "call " + line : line;
      String body =
          String.join("\r\n", "@echo off", "chcp 65001 >nul", command, "exit /b %ERRORLEVEL%")
              + "\r\n";
      Path script = scriptDir.resolve(scriptName + ".cmd");
      Files.writeString(
          script, body.replace("\n", "\r\n").replace("\r\r\n", "\r\n"), StandardCharsets.UTF_8);
      return List.of("cmd.exe", "/d", "/c", script.toString());
    }

    /**
     * Double quotes protect {@code & | < > ( )} from cmd's parser, and {@code !} is literal because
     * these scripts never enable delayed expansion. {@code %} is expanded even inside double
     * quotes, so a literal one is written {@code %%} as any batch file must.
     *
     * <p>Two characters are refused outright, cmd offering no escape that is both correct and
     * predictable: a double quote, which has no escape surviving cmd's quote counting and the
     * target program's parser at once, and a caret, which the {@code call} used for a single
     * command doubles as it re-parses its line, so {@code "a^b"} arrives as {@code "a^^b"}. Pass
     * either with {@code script(bashBody, cmdBody)}.
     */
    @Override
    public String quote(String arg) {
      if (arg.indexOf('"') >= 0) {
        throw new UnsupportedOperationException(
            "cmd.exe has no escape for a double quote that survives both its own quote counting "
                + "and the target program's parser. Use script(bashBody, cmdBody) for: "
                + arg);
      }
      if (arg.indexOf('^') >= 0) {
        throw new UnsupportedOperationException(
            "cmd.exe doubles a caret when 'call' re-parses the line, so it cannot be passed as an "
                + "argument. Use script(bashBody, cmdBody) for: "
                + arg);
      }
      String escaped = arg.replace("%", "%%");
      // An empty argument matches nothing in NEEDS_CMD_QUOTING, and bare emptiness is not an
      // argument at all to cmd.exe - it would vanish, where bash keeps it.
      if (arg.isEmpty()) {
        return "\"\"";
      }
      return NEEDS_CMD_QUOTING.matcher(arg).find() ? '"' + escaped + '"' : escaped;
    }
  };

  /** Characters that make cmd.exe need an argument wrapped in double quotes. */
  private static final Pattern NEEDS_CMD_QUOTING = Pattern.compile("[\\s&|<>()]");

  /**
   * Lines cmd.exe parses as batch syntax rather than as a command to run. {@code call} only ever
   * prefixes a command name: {@code call (echo a&echo b)} sends cmd looking for a program named
   * {@code (echo} and loses the rest of the group, and {@code call if}/{@code call for} fail
   * outright. Chaining and redirection operators later in the line are fine, so they are not
   * listed; cmd runs each side of those in its own context anyway.
   */
  private static final Pattern CMD_SYNTAX_LINE =
      Pattern.compile(
          "^\\s*(?:[(@]|(?:if|else|for|rem|goto|setlocal|endlocal)\\b)", Pattern.CASE_INSENSITIVE);

  /** Whether {@code call} may be prepended to a whole cmd line; see {@link #CMD_SYNTAX_LINE}. */
  private static boolean takesCallPrefix(String line) {
    return !line.contains("\n") && !CMD_SYNTAX_LINE.matcher(line).find();
  }

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
