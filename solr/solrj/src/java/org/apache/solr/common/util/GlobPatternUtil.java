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
package org.apache.solr.common.util;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;

/** Provides methods for matching glob patterns against input strings. */
public class GlobPatternUtil {

  /**
   * Matches an input string against a provided glob patterns. This uses the implementation from
   * Apache Commons IO FilenameUtils. We are just redoing the implementation here instead of
   * bringing in commons-io as a dependency.
   *
   * @see <a
   *     href="https://commons.apache.org/proper/commons-io/apidocs/org/apache/commons/io/FilenameUtils.html#wildcardMatch(java.lang.String,java.lang.String)">This
   *     uses code from Apache Commons IO</a>
   * @param pattern the glob pattern to match against
   * @param input the input string to match against a glob pattern
   * @return true if the input string matches the glob pattern, false otherwise
   */
  public static boolean matches(String pattern, String input) {
    if (input == null && pattern == null) {
      return true;
    }
    if (input == null || pattern == null) {
      return false;
    }
    final String[] wcs = splitOnTokens(pattern);
    boolean anyChars = false;
    int textIdx = 0;
    int wcsIdx = 0;
    final Deque<int[]> backtrack = new ArrayDeque<>(wcs.length);

    // loop around a backtrack stack, to handle complex * matching
    do {
      if (!backtrack.isEmpty()) {
        final int[] array = backtrack.pop();
        wcsIdx = array[0];
        textIdx = array[1];
        anyChars = true;
      }

      // loop whilst tokens and text left to process
      while (wcsIdx < wcs.length) {

        if (wcs[wcsIdx].equals("?")) {
          // ? so move to next text char
          textIdx++;
          if (textIdx > input.length()) {
            break;
          }
          anyChars = false;

        } else if (wcs[wcsIdx].equals("*")) {
          // set any chars status
          anyChars = true;
          if (wcsIdx == wcs.length - 1) {
            textIdx = input.length();
          }

        } else {
          // matching text token
          if (anyChars) {
            // any chars then try to locate text token
            textIdx = checkIndexOf(input, textIdx, wcs[wcsIdx]);
            if (textIdx == -1) {
              // token not found
              break;
            }
            final int repeat = checkIndexOf(input, textIdx + 1, wcs[wcsIdx]);
            if (repeat >= 0) {
              backtrack.push(new int[] {wcsIdx, repeat});
            }
          } else if (!input.regionMatches(false, textIdx, wcs[wcsIdx], 0, wcs[wcsIdx].length())) {
            // matching from current position
            // couldn't match token
            break;
          }

          // matched text token, move text index to end of matched token
          textIdx += wcs[wcsIdx].length();
          anyChars = false;
        }

        wcsIdx++;
      }

      // full match
      if (wcsIdx == wcs.length && textIdx == input.length()) {
        return true;
      }

    } while (!backtrack.isEmpty());

    return false;
  }

  /**
   * Splits a string into a number of tokens. The text is split by '?' and '*'. Where multiple '*'
   * occur consecutively they are collapsed into a single '*'.
   *
   * @see <a
   *     href="https://commons.apache.org/proper/commons-io/apidocs/org/apache/commons/io/FilenameUtils.html">This
   *     uses code from Apache Commons IO</a>
   * @param text the text to split
   * @return the array of tokens, never null
   */
  private static String[] splitOnTokens(final String text) {
    // used by wildcardMatch
    // package level so a unit test may run on this

    if (text.indexOf('?') == -1 && text.indexOf('*') == -1) {
      return new String[] {text};
    }

    final char[] array = text.toCharArray();
    final ArrayList<String> list = new ArrayList<>();
    final StringBuilder buffer = new StringBuilder();
    char prevChar = 0;
    for (final char ch : array) {
      if (ch == '?' || ch == '*') {
        if (buffer.length() != 0) {
          list.add(buffer.toString());
          buffer.setLength(0);
        }
        if (ch == '?') {
          list.add("?");
        } else if (prevChar != '*') { // ch == '*' here; check if previous char was '*'
          list.add("*");
        }
      } else {
        buffer.append(ch);
      }
      prevChar = ch;
    }
    if (buffer.length() != 0) {
      list.add(buffer.toString());
    }

    return list.toArray(new String[] {});
  }

  /**
   * Checks if one string contains another starting at a specific index using the case-sensitivity
   * rule.
   *
   * <p>This method mimics parts of {@link String#indexOf(String, int)} but takes case-sensitivity
   * into account.
   *
   * @see <a
   *     href="https://commons.apache.org/proper/commons-io/apidocs/org/apache/commons/io/FilenameUtils.html">This
   *     uses code from Apache Commons IO</a>
   * @param str the string to check, not null
   * @param strStartIndex the index to start at in str
   * @param search the start to search for, not null
   * @return the first index of the search String, -1 if no match or {@code null} string input
   * @throws NullPointerException if either string is null
   * @since 2.0
   */
  private static int checkIndexOf(final String str, final int strStartIndex, final String search) {
    final int endIndex = str.length() - search.length();
    if (endIndex >= strStartIndex) {
      for (int i = strStartIndex; i <= endIndex; i++) {
        if (str.regionMatches(false, i, search, 0, search.length())) {
          return i;
        }
      }
    }
    return -1;
  }
}
