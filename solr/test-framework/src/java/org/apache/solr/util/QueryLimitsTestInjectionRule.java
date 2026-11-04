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

import java.lang.invoke.MethodHandles;
import java.util.function.BooleanSupplier;
import org.apache.solr.search.QueryLimit;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryLimitsTestInjectionRule implements TestRule {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static BooleanSupplier enableSupplier;

  public QueryLimitsTestInjectionRule(BooleanSupplier enableSupplier) {
    QueryLimitsTestInjectionRule.enableSupplier = enableSupplier;
  }

  @Override
  public Statement apply(final Statement base, final Description description) {
    if (!enableSupplier.getAsBoolean()) {
      return base;
    }
    return new Statement() {
      @Override
      public void evaluate() throws Throwable {
        if (!enableSupplier.getAsBoolean()) {
          base.evaluate();
          return;
        }
        log.info("###Test is configured to use QueryLimits");
        try {
          assert TestInjection.queryTimeout == null : "Disabled too late, or was init'ed elsewhere";
          TestInjection.queryTimeout =
              new QueryLimit() {
                @Override
                public Object currentValue() {
                  return "No-Op injected QueryLimit";
                }

                @Override
                public boolean shouldExit() {
                  return false;
                }
              };

          base.evaluate();
        } finally {
          // always reset the queryTimeout
          TestInjection.queryTimeout = null;
        }
      }
    };
  }

  /** Disables for the whole test suite (class), not just for this individual test. */
  public static void disable() {
    QueryLimitsTestInjectionRule.enableSupplier = Boolean.FALSE::booleanValue;
  }
}
