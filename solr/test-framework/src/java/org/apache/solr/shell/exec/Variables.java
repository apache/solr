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

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Variable references in the text a test writes, resolved by the harness rather than by a shell.
 *
 * <p>{@code ${NAME}}, {@code $NAME} and {@code {{NAME}}} all name an environment variable, and all
 * mean the same thing wherever a test writes them — in a command line and in the expected text of
 * an assertion. That is what lets a BATS line port over unchanged, assertion included.
 *
 * <p>Resolving in Java rather than letting the shell do it is what makes one line work on both
 * shells: bash expands {@code $NAME} and cmd.exe does not, while cmd.exe wants {@code %NAME%},
 * which bash leaves alone. It also means an unknown name fails the test instead of quietly becoming
 * the empty string.
 */
public final class Variables {

  /**
   * Every spelling allowed in a cross-shell command line, and in an assertion's expected text.
   *
   * <p>{@code $'..'}, {@code $1} and a bare {@code $} do not match and are left for the shell. It
   * is sound to read {@code $NAME} as an environment variable because a harness command line has no
   * shell state of its own — one process per command, so no locals and no positional parameters.
   */
  public static final Pattern ANY =
      Pattern.compile(
          "\\{\\{([A-Za-z_][A-Za-z0-9_]*)}}"
              + "|\\$\\{([A-Za-z_][A-Za-z0-9_]*)}"
              + "|\\$([A-Za-z_][A-Za-z0-9_]*)");

  /**
   * {@code {{NAME}}} alone — the only form resolved inside a shell-specific script, where {@code $}
   * belongs to the shell.
   *
   * <p>Double braces are inert in both bash, which only expands braces containing a comma or {@code
   * ..}, and cmd.exe.
   */
  public static final Pattern BRACES_ONLY = Pattern.compile("\\{\\{([A-Za-z_][A-Za-z0-9_]*)}}");

  /**
   * The cmd.exe spelling. Never resolved, because {@code %} is also {@link String#format}'s, but
   * worth rejecting in a command line so that one written this way cannot pass on Windows and fail
   * on bash.
   */
  public static final Pattern CMD_FORM = Pattern.compile("%([A-Za-z_][A-Za-z0-9_]*)%");

  private Variables() {}

  /**
   * Replaces each reference {@code pattern} matches with the variable it names.
   *
   * @param vars the environment to resolve against
   * @param context what is being resolved, for the failure message
   * @throws AssertionError if a name is not defined — a typo should be findable, not silent
   */
  public static String resolve(
      String text, Pattern pattern, Map<String, String> vars, String context) {
    Matcher m = pattern.matcher(text);
    StringBuilder resolved = new StringBuilder();
    while (m.find()) {
      String key = null;
      for (int group = 1; group <= m.groupCount() && key == null; group++) {
        key = m.group(group);
      }
      String value = vars.get(key);
      if (value == null) {
        throw new AssertionError(
            "Unknown variable "
                + m.group()
                + " in "
                + context
                + ": "
                + text
                + "\nDefined: "
                + definedNames(vars));
      }
      m.appendReplacement(resolved, Matcher.quoteReplacement(value));
    }
    m.appendTail(resolved);
    return resolved.toString();
  }

  /** The harness' own variables, which is what a typo is almost always a near-miss of. */
  private static String definedNames(Map<String, String> vars) {
    return vars.keySet().stream()
        .filter(k -> k.startsWith("SOLR") || k.startsWith("ZK_") || k.startsWith("TEST_"))
        .sorted()
        .toList()
        .toString();
  }
}
