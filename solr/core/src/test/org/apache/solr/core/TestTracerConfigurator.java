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
package org.apache.solr.core;

import io.opentracing.util.GlobalTracer;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.util.ExecutorUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestTracerConfigurator extends SolrTestCaseJ4 {

  @BeforeClass
  public static void setUpProperties() throws Exception {
    System.setProperty("otel.service.name", "something");
    System.setProperty("solr.otelDefaultConfigurator", "configuratorClassDoesNotExistTest");
  }

  @AfterClass
  public static void clearProperties() throws Exception {
    System.clearProperty("solr.otelDefaultConfigurator");
    System.clearProperty("otel.service.name");
  }

  @Test
  public void configuratorClassDoesNotExistTest() {
    // to be safe because this test tests tracing.
    resetGlobalTracer();
    ExecutorUtil.resetThreadLocalProviders();
    assertTrue(TracerConfigurator.shouldAutoConfigOTEL());
    SolrResourceLoader loader = new SolrResourceLoader(TEST_PATH().resolve("collection1"));
    TracerConfigurator.loadTracer(loader, null);

    assertTrue(
        "Expecting noop otel after failure to auto-init",
        GlobalTracer.get().toString().contains("GlobalTracer"));
  }

  @Test
  public void otelDisabledByProperty() {
    System.setProperty("otel.sdk.disabled", "true");
    try {
      assertFalse(TracerConfigurator.shouldAutoConfigOTEL());
    } finally {
      System.clearProperty("otel.sdk.disabled");
    }
  }
}
