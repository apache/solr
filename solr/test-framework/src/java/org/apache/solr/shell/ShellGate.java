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

package org.apache.solr.shell;

import static org.junit.Assume.assumeTrue;

import java.util.List;
import org.apache.solr.shell.exec.Shell;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

/**
 * Honours {@link EnabledOnShell} on a test method, skipping it on any other shell.
 *
 * <p>{@link SolrShellTestCase} applies this itself. A test that drives the harness without needing
 * a Solr distribution — and so does not extend it — adds the rule:
 *
 * <pre>
 *   &#64;Rule public final ShellGate shellGate = new ShellGate();
 * </pre>
 */
public final class ShellGate implements TestRule {

  @Override
  public Statement apply(Statement base, Description description) {
    return new Statement() {
      @Override
      public void evaluate() throws Throwable {
        requireShell(description.getAnnotation(EnabledOnShell.class));
        base.evaluate();
      }
    };
  }

  /** Skips as an assumption failure when {@code restriction} does not name the current shell. */
  public static void requireShell(EnabledOnShell restriction) {
    if (restriction == null) {
      return;
    }
    Shell current = Shell.defaultShell();
    List<Shell> allowed = List.of(restriction.value());
    assumeTrue("Only runs on " + allowed + "; this run uses " + current, allowed.contains(current));
  }
}
