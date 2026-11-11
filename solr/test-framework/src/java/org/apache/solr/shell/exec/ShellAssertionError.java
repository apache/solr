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

/**
 * A shell test failure, reported as the commands that ran rather than as a Java stack trace.
 *
 * <p>Carries no stack trace: the frames would all be harness internals, and would bury the
 * transcript. Only for a command's <em>outcome</em> — a wrong exit status, output that did not
 * match, a timeout. A malformed test, such as an unknown {@code ${NAME}} or a cmd-only spelling in
 * a cross-shell line, still throws an ordinary {@link AssertionError} with its frames, because
 * there the useful information is which line of the test is wrong.
 */
public final class ShellAssertionError extends AssertionError {

  private static final long serialVersionUID = 1L;

  public ShellAssertionError(String message) {
    super(message);
  }

  /** No frames: the message is the diagnosis. */
  @Override
  public synchronized Throwable fillInStackTrace() {
    return this;
  }
}
