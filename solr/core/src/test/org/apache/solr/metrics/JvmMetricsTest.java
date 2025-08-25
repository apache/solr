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
package org.apache.solr.metrics;

import io.opentelemetry.exporter.prometheus.PrometheusMetricReader;
import io.prometheus.metrics.model.snapshots.MetricSnapshots;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.solr.SolrJettyTestBase;
import org.apache.solr.core.NodeConfig;
import org.apache.solr.core.SolrXmlConfig;
import org.junit.BeforeClass;
import org.junit.Test;

public class JvmMetricsTest extends SolrJettyTestBase {
  static final String[] STRING_OS_METRICS = {"arch", "name", "version"};
  static final String[] NUMERIC_OS_METRICS = {"availableProcessors", "systemLoadAverage"};

  static final String[] BUFFER_METRICS = {
    "direct.Count",
    "direct.MemoryUsed",
    "direct.TotalCapacity",
    "mapped.Count",
    "mapped.MemoryUsed",
    "mapped.TotalCapacity"
  };

  @BeforeClass
  public static void beforeTest() throws Exception {
    System.setProperty("solr.metrics.jvm.enabled", "true");
    createAndStartJetty(legacyExampleCollection1SolrHome());
  }

  @Test
  public void testHiddenSysProps() throws Exception {
    Path home = TEST_PATH();

    // default config
    String solrXml = Files.readString(home.resolve("solr.xml"), StandardCharsets.UTF_8);
    NodeConfig config = SolrXmlConfig.fromString(home, solrXml);
    NodeConfig.NodeConfigBuilder.DEFAULT_HIDDEN_SYS_PROPS.forEach(
        s -> assertTrue(s, config.isSysPropHidden(s)));

    // custom config
    solrXml = Files.readString(home.resolve("solr-hiddensysprops.xml"), StandardCharsets.UTF_8);
    NodeConfig config2 = SolrXmlConfig.fromString(home, solrXml);
    Arrays.asList("foo", "bar", "baz").forEach(s -> assertTrue(s, config2.isSysPropHidden(s)));
  }

  @Test
  public void testSetupJvmMetrics() {
    PrometheusMetricReader reader =
        getJetty().getCoreContainer().getMetricManager().getPrometheusMetricReader("solr.jvm");
    MetricSnapshots snapshots = reader.collect();
    assertTrue("Should have metric snapshots", snapshots.size() > 0);

    Set<String> metricNames =
        snapshots.stream()
            .map(metric -> metric.getMetadata().getPrometheusName())
            .collect(Collectors.toSet());

    assertTrue(
        "Should have JVM memory metrics",
        metricNames.stream().anyMatch(name -> name.startsWith("jvm_memory")));

    assertTrue(
        "Should have JVM thread metrics",
        metricNames.stream().anyMatch(name -> name.startsWith("jvm_thread")));

    assertTrue(
        "Should have JVM class metrics",
        metricNames.stream().anyMatch(name -> name.startsWith("jvm_class")));

    assertTrue(
        "Should have JVM CPU metrics",
        metricNames.stream().anyMatch(name -> name.startsWith("jvm_cpu")));

    assertTrue(
        "Should have JVM GC metrics",
        metricNames.stream().anyMatch(name -> name.startsWith("jvm_gc")));

    assertTrue(
        "Should have JVM buffer metrics",
        metricNames.stream().anyMatch(name -> name.startsWith("jvm_buffer")));
  }
}
