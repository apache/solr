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
package org.apache.solr.metrics.otel;

import io.prometheus.metrics.model.snapshots.Labels;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import org.apache.solr.SolrTestCaseJ4;
import org.junit.Test;

/**
 * Test class for FilterablePrometheusMetricReader which focuses on requiredLabelsFilter method
 * which filters metric data points based on label requirements.
 */
public class FilterablePrometheusMetricReaderTest extends SolrTestCaseJ4 {

  Labels actualLabels = Labels.of("key1", "value1", "key2", "value2", "key3", "value3");

  @Test
  public void testFilterMatchingLabel() {
    SortedMap<String, Set<String>> requiredLabels = new TreeMap<>(Map.of("key1", Set.of("value1")));
    assertTrue(FilterablePrometheusMetricReader.requiredLabelsFilter(actualLabels, requiredLabels));
  }

  @Test
  public void testFilterOneMatchingLabelValue() {
    SortedMap<String, Set<String>> requiredLabels =
        new TreeMap<>(Map.of("key1", Set.of("value1", "value123", "value456", "value789")));
    assertTrue(FilterablePrometheusMetricReader.requiredLabelsFilter(actualLabels, requiredLabels));
  }

  @Test
  public void testFilterNoMatchingLabel() {
    SortedMap<String, Set<String>> requiredLabels =
        new TreeMap<>(Map.of("dummyKey", Set.of("dummyValue")));
    assertFalse(
        FilterablePrometheusMetricReader.requiredLabelsFilter(actualLabels, requiredLabels));
  }

  @Test
  public void testFilterAllMultipleMatchingLabels() {
    SortedMap<String, Set<String>> requiredLabels =
        new TreeMap<>(Map.of("key1", Set.of("value1"), "key2", Set.of("value2")));
    assertTrue(FilterablePrometheusMetricReader.requiredLabelsFilter(actualLabels, requiredLabels));
  }

  @Test
  public void testFilterMultipleWithOneLabelValueNotMatching() {
    SortedMap<String, Set<String>> requiredLabels =
        new TreeMap<>(Map.of("key1", Set.of("value1"), "key2", Set.of("value999")));
    assertFalse(
        FilterablePrometheusMetricReader.requiredLabelsFilter(actualLabels, requiredLabels));
  }

  @Test
  public void testFilterMultipleWithOneLabelMissing() {
    SortedMap<String, Set<String>> requiredLabels =
        new TreeMap<>(Map.of("key1", Set.of("value1"), "key999", Set.of("value2")));
    assertFalse(
        FilterablePrometheusMetricReader.requiredLabelsFilter(actualLabels, requiredLabels));
  }

  @Test
  public void testFilterLabelsWithMixedValues() {
    // Scenario with multiple label values some matching and some not
    SortedMap<String, Set<String>> requiredLabels =
        new TreeMap<>(
            Map.of("key1", Set.of("value1", "value123"), "key2", Set.of("value456", "value2")));
    assertTrue(FilterablePrometheusMetricReader.requiredLabelsFilter(actualLabels, requiredLabels));
  }

  @Test
  public void testFilterEmptyLabelValues() {
    // Edge case where label has empty set of allowed values
    SortedMap<String, Set<String>> requiredLabels = new TreeMap<>(Map.of("key1", Set.of()));
    assertFalse(
        FilterablePrometheusMetricReader.requiredLabelsFilter(actualLabels, requiredLabels));
  }
}
