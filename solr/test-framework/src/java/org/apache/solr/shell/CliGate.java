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

import java.util.Map;
import java.util.function.Supplier;
import org.apache.solr.cats.Traits;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

/**
 * Honours {@link EnabledOnCli} on a test method, skipping it on any other command-line
 * implementation. The Solr counterpart of {@link org.apache.solr.cats.ShellGate}, over the {@code
 * cli} trait that {@link org.apache.solr.shell.exec.SolrProgram} reports.
 *
 * <p>{@link SolrShellTestCase} applies this itself; a test that drives the harness without
 * extending it adds the rule, handing it the traits of its own run.
 */
public final class CliGate implements TestRule {

  private final Supplier<Map<String, String>> traits;

  public CliGate(Supplier<Map<String, String>> traits) {
    this.traits = traits;
  }

  @Override
  public Statement apply(Statement base, Description description) {
    return new Statement() {
      @Override
      public void evaluate() throws Throwable {
        requireCli(traits.get(), description.getAnnotation(EnabledOnCli.class));
        base.evaluate();
      }
    };
  }

  /** Skips as an assumption failure when {@code restriction} does not name this run's CLI. */
  public static void requireCli(Map<String, String> traits, EnabledOnCli restriction) {
    if (restriction != null) {
      Traits.assume(traits, "cli", restriction.value());
    }
  }
}
