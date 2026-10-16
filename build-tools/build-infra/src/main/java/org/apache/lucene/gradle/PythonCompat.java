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

import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * String and regular expression routines with the exact semantics that the changelog converter is
 * specified to follow.
 */
final class PythonCompat {

  private static final String RE_SPECIAL_CHARS = "()[]{}?*+-|^$\\.&~# \t\n\r\u000B\f";

  private PythonCompat() {}

  /**
   * The whitespace definition that this class's trimming uses: Java whitespace, Java space
   * characters, and U+0085.
   */
  static boolean isSpace(char c) {
    return Character.isWhitespace(c) || Character.isSpaceChar(c) || c == '\u0085';
  }

  static String strip(String text) {
    int begin = 0;
    int end = text.length();
    while (begin < end && isSpace(text.charAt(begin))) {
      begin++;
    }
    while (end > begin && isSpace(text.charAt(end - 1))) {
      end--;
    }
    return text.substring(begin, end);
  }

  /**
   * Prefixes a backslash before every character that is special in a regular expression, so the
   * text matches literally.
   */
  static String escapeRegex(String text) {
    StringBuilder escaped = new StringBuilder();
    for (char c : text.toCharArray()) {
      if (RE_SPECIAL_CHARS.indexOf(c) >= 0) {
        escaped.append('\\');
      }
      escaped.append(c);
    }
    return escaped.toString();
  }

  /**
   * Replaces every match of the pattern in the text with the string the replacement function
   * computes for that match.
   */
  static String replaceAll(Pattern pattern, String text, Function<Matcher, String> replacement) {
    Matcher matcher = pattern.matcher(text);
    StringBuilder result = new StringBuilder();
    int last = 0;
    while (matcher.find()) {
      result.append(text, last, matcher.start());
      result.append(replacement.apply(matcher));
      last = matcher.end();
    }
    result.append(text.substring(last));
    return result.toString();
  }
}
