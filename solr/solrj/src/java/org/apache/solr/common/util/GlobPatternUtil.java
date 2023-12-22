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

import java.nio.file.FileSystems;
import java.nio.file.Paths;

/** Provides methods for matching glob patterns against input strings. */
public class GlobPatternUtil {

  /**
   * Matches an input string against a provided glob patterns. This uses Java NIO FileSystems
   * PathMatcher to match glob patterns in the same way to how glob patterns are matches for file
   * paths, rather than implementing our own glob pattern matching.
   *
   * @param pattern the glob pattern to match against
   * @param input the input string to match against a glob pattern
   * @return true if the input string matches the glob pattern, false otherwise
   */
  public static boolean matches(String pattern, String input) {
    return FileSystems.getDefault().getPathMatcher("glob:" + pattern).matches(Paths.get(input));
  }
}
