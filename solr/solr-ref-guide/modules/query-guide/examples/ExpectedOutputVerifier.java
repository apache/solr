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
package org.apache.solr.client.ref_guide_examples;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayDeque;
import java.util.Queue;

public class ExpectedOutputVerifier {
  private static Queue<String> expectedLines = new ArrayDeque<>();
  
  public static void expectLine(String expectedLine) {
    expectedLines.add(expectedLine);
  }

  public static void print(String actualOutput) {
    final String nextExpectedLine = expectedLines.poll();
    assertNotNull("No more output expected, but was asked to print: " + actualOutput, nextExpectedLine);

    final String unexpectedOutputMessage = "Expected line containing " + nextExpectedLine + ", but printed line was: "
        + actualOutput;
    assertTrue(unexpectedOutputMessage, actualOutput.contains(nextExpectedLine));
  }

  public static void ensureNoLeftoverOutputExpectations() {
    if (expectedLines.isEmpty()) return;

    final StringBuilder builder = new StringBuilder();
    builder.append("Leftover output was expected but not printed:");
    for (String expectedLine : expectedLines) {
      builder.append("\n\t").append(expectedLine);
    }
    fail(builder.toString());
  }
  
  public static void clear() {
    expectedLines.clear();
  }
}
