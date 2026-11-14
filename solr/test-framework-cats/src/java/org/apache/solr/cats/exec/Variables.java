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

import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Variable references in the text a test writes, resolved by the harness rather than by a shell.
 *
 * <p>{@code ${NAME}}, {@code $NAME} and {@code {{NAME}}} all name an environment variable, and mean
 * the same wherever a test writes them — in a command line and in an assertion's expected text
 * alike — which is what lets a BATS line port over unchanged. See {@code CliSession#run(String)}
 * for why the harness resolves them rather than the shell.
 */
public final class Variables {

  /**
   * Every spelling allowed in a cross-shell command line, and in an assertion's expected text.
   *
   * <p>{@code $'..'}, {@code $1} and a bare {@code $} do not match and are left for the shell.
   */
  public static final Pattern ANY =
      Pattern.compile(
          "\\{\\{([A-Za-z_][A-Za-z0-9_]*)}}"
              + "|\\$\\{([A-Za-z_][A-Za-z0-9_]*)}"
              + "|\\$([A-Za-z_][A-Za-z0-9_]*)");

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
   * @param echoedPrefixes which names the failure message lists; empty means all of them
   * @throws AssertionError if a name is not defined — a typo should be findable, not silent
   */
  public static String resolve(
      String text,
      Pattern pattern,
      Map<String, String> vars,
      String context,
      Set<String> echoedPrefixes) {
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
                + definedNames(vars, echoedPrefixes));
      }
      m.appendReplacement(resolved, Matcher.quoteReplacement(value));
    }
    m.appendTail(resolved);
    return resolved.toString();
  }

  /**
   * The program's own variables, which is what a typo is almost always a near-miss of. Which
   * prefixes those are is the environment's to say; the whole inherited environment would tell a
   * reader nothing.
   */
  private static String definedNames(Map<String, String> vars, Set<String> echoedPrefixes) {
    return vars.keySet().stream()
        .filter(k -> echoedPrefixes.isEmpty() || echoedPrefixes.stream().anyMatch(k::startsWith))
        .sorted()
        .toList()
        .toString();
  }
}
