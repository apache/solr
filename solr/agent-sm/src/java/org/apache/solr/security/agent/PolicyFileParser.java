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
package org.apache.solr.security.agent;

import java.io.IOException;
import java.io.Reader;
import java.io.StreamTokenizer;
import java.util.ArrayList;
import java.util.List;

/**
 * Tokenizer-based parser for JDK-style {@code .policy} files.
 *
 * <p>Uses {@link StreamTokenizer} which natively handles {@code //} line comments, {@code /* *\/}
 * block comments, and quoted strings — no regex involved.
 *
 * <p>This file was derived from the OpenSearch project and modified. See {@code NOTICE.txt} for
 * attribution.
 */
class PolicyFileParser {

  private PolicyFileParser() {}

  record GrantEntry(String codeBase, List<PermEntry> permissions) {}

  record PermEntry(String permission, String name, String action) {}

  static class ParsingException extends Exception {
    ParsingException(String message) {
      super(message);
    }

    ParsingException(int line, String expected, String found) {
      super("line " + line + ": expected [" + expected + "], found [" + found + "]");
    }
  }

  static List<GrantEntry> read(Reader policy) throws ParsingException, IOException {
    return read(policy, true);
  }

  /**
   * @param strictTopLevel if {@code true}, any token that is not the start of a {@code grant} block
   *     causes a {@link ParsingException}; if {@code false}, such tokens are silently skipped
   *     (lenient mode, kept for internal use only)
   */
  static List<GrantEntry> read(Reader policy, boolean strictTopLevel)
      throws ParsingException, IOException {
    List<GrantEntry> entries = new ArrayList<>();
    PolicyTokenStream ts = new PolicyTokenStream(policy);
    while (!ts.isEOF()) {
      if (peek(ts, "grant")) {
        entries.add(parseGrantEntry(ts));
      } else if (strictTopLevel) {
        PolicyToken tok = ts.peek();
        throw new ParsingException(tok.line(), "grant", tok.text());
      } else {
        // skip unexpected top-level token (lenient mode)
        ts.consume();
      }
    }
    return entries;
  }

  private static GrantEntry parseGrantEntry(PolicyTokenStream ts)
      throws ParsingException, IOException {
    String codeBase = null;
    List<PermEntry> perms = new ArrayList<>();

    poll(ts, "grant");

    while (!peek(ts, "{")) {
      if (pollOnMatch(ts, "codeBase")) {
        String raw = poll(ts, ts.peek().text());
        codeBase = expand(ts, raw);
      } else {
        // Skip unknown grant modifiers
        ts.consume();
      }
    }

    poll(ts, "{");

    while (!peek(ts, "}")) {
      if (ts.isEOF()) break;
      if (peek(ts, "permission")) {
        perms.add(parsePermEntry(ts));
        pollOnMatch(ts, ";");
      } else {
        ts.consume(); // skip unexpected token inside grant block
      }
    }

    poll(ts, "}");
    pollOnMatch(ts, ";");

    return new GrantEntry(codeBase, List.copyOf(perms));
  }

  private static PermEntry parsePermEntry(PolicyTokenStream ts)
      throws ParsingException, IOException {
    poll(ts, "permission");
    String permClass = poll(ts, ts.peek().text());

    String name = null;
    if (isQuoted(ts.peek())) {
      name = expand(ts, poll(ts, ts.peek().text()));
    }

    String action = null;
    pollOnMatch(ts, ",");
    if (isQuoted(ts.peek())) {
      action = expand(ts, poll(ts, ts.peek().text()));
    }

    return new PermEntry(permClass, name, action);
  }

  private static String expand(PolicyTokenStream ts, String raw) throws ParsingException {
    // Capture the line number before attempting expansion so we preserve it if expansion fails.
    int lineNum = -1;
    try {
      lineNum = ts.line();
    } catch (IOException ignored) {
      // best-effort — -1 signals "line unknown"
    }
    try {
      return PolicyPropertyExpander.expand(raw);
    } catch (PolicyPropertyExpander.ExpandException e) {
      ParsingException pe =
          lineNum >= 0
              ? new ParsingException(lineNum, e.getMessage(), raw)
              : new ParsingException(e.getMessage());
      pe.initCause(e);
      throw pe;
    }
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private static boolean peek(PolicyTokenStream ts, String expected) throws IOException {
    return expected.equalsIgnoreCase(ts.peek().text());
  }

  private static boolean pollOnMatch(PolicyTokenStream ts, String expected)
      throws ParsingException, IOException {
    if (peek(ts, expected)) {
      poll(ts, expected);
      return true;
    }
    return false;
  }

  private static String poll(PolicyTokenStream ts, String expected)
      throws ParsingException, IOException {
    PolicyToken token = ts.consume();
    boolean isKeywordOrSymbol =
        expected.equalsIgnoreCase("grant")
            || expected.equalsIgnoreCase("codeBase")
            || expected.equalsIgnoreCase("permission")
            || expected.equals("{")
            || expected.equals("}")
            || expected.equals(";")
            || expected.equals(",");
    if (isKeywordOrSymbol && !expected.equalsIgnoreCase(token.text())) {
      throw new ParsingException(token.line(), expected, token.text());
    }
    return token.text();
  }

  private static boolean isQuoted(PolicyToken token) {
    return token.type() == '"' || token.type() == '\'';
  }
}
