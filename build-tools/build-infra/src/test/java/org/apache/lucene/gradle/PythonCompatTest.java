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

import static org.junit.Assert.assertEquals;

import org.junit.Test;

/** Checks the Python-semantics helpers against the Python results they reproduce. */
public class PythonCompatTest {
  @Test
  public void stripRemovesNonBreakingAndLineSeparatorSpaces() {
    assertEquals("x", PythonCompat.strip("  x  "));
  }

  @Test
  public void stripKeepsZeroWidthSpace() {
    assertEquals("​x​", PythonCompat.strip("​x​"));
  }

  @Test
  public void escapeRegexEscapesPythonSpecialCharacters() {
    assertEquals("v9\\.0\\-rc1\\ \\(a\\)", PythonCompat.escapeRegex("v9.0-rc1 (a)"));
  }
}
