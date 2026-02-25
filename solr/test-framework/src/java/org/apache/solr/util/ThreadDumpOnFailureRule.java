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
package org.apache.solr.util;

import org.apache.solr.core.Diagnostics;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

/**
 * JUnit rule that captures and logs a thread dump when a test fails with a timeout. This is useful
 * for debugging flaky tests with timeout issues, as it shows what threads were doing at the time of
 * failure.
 *
 * <p>The thread dump is only captured if the failure throwable or its message contains "Timeout"
 * (case-insensitive).
 *
 * <p>Usage:
 *
 * <pre>{@code
 * @Rule
 * public TestRule threadDumpRule = new ThreadDumpOnFailureRule();
 * }</pre>
 */
public class ThreadDumpOnFailureRule extends TestWatcher {

  @Override
  protected void failed(Throwable e, Description description) {
    if (isTimeoutRelated(e)) {
      Diagnostics.logThreadDumps(
          "============ THREAD DUMP ON TIMEOUT FAILURE: "
              + description.getDisplayName()
              + " ============\nFailure: "
              + e.getClass().getSimpleName()
              + ": "
              + e.getMessage());
    }
  }

  private boolean isTimeoutRelated(Throwable e) {
    if (e == null) {
      return false;
    }
    if (e.toString().contains("Timeout")) {
      return true;
    }
    // Check the cause recursively
    return isTimeoutRelated(e.getCause());
  }
}
