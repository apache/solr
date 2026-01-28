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
package org.apache.solr.metrics.otel.instruments;

import static org.apache.solr.metrics.SolrCoreMetricManager.COLLECTION_ATTR;
import static org.apache.solr.metrics.SolrCoreMetricManager.CORE_ATTR;
import static org.apache.solr.metrics.SolrCoreMetricManager.SHARD_ATTR;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.LongHistogram;
import io.opentelemetry.api.metrics.LongUpDownCounter;
import java.util.Set;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.metrics.SolrMetricsContext;
import org.apache.solr.metrics.otel.OtelUnit;

/**
 * Factory for creating metrics instruments that can write to either single or dual registries (core
 * and node).
 */
// TODO consider making this a base abstraction with a simple impl and another "Dual" one.
public class AttributedInstrumentFactory {

  // These attributes are on a core but don't want to aggregate them.
  private static final Set<AttributeKey<?>> FILTER_ATTRS_SET =
      Set.of(COLLECTION_ATTR, CORE_ATTR, SHARD_ATTR);

  private final SolrMetricsContext primaryMetricsContext;
  private final Attributes primaryAttributes;
  private final boolean aggregateToNodeRegistry;
  private final boolean primaryIsNodeRegistry;
  private final SolrMetricsContext nodeMetricsContext;
  private final Attributes nodeAttributes;

  public AttributedInstrumentFactory(
      SolrMetricsContext primaryMetricsContext,
      Attributes primaryAttributes,
      boolean aggregateToNodeRegistry) {
    this.primaryMetricsContext = primaryMetricsContext;
    this.primaryAttributes = primaryAttributes;
    this.aggregateToNodeRegistry = aggregateToNodeRegistry;

    // Primary could be a node
    this.primaryIsNodeRegistry =
        primaryMetricsContext.getRegistryName().equals(SolrMetricManager.NODE_REGISTRY);

    // Only create node registry if we want dual registry mode AND primary is not already a node
    // registry
    if (aggregateToNodeRegistry && !primaryIsNodeRegistry) {
      this.nodeMetricsContext =
          new SolrMetricsContext(
              primaryMetricsContext.getMetricManager(), SolrMetricManager.NODE_REGISTRY);
      this.nodeAttributes = createNodeAttributes(primaryAttributes);
    } else {
      this.nodeMetricsContext = null;
      this.nodeAttributes = null;
    }
  }

  public AttributedLongCounter attributedLongCounter(
      String metricName, String description, Attributes additionalAttributes) {
    Attributes finalPrimaryAttrs = appendAttributes(primaryAttributes, additionalAttributes);

    if (aggregateToNodeRegistry && nodeMetricsContext != null) {
      Attributes finalNodeAttrs = appendAttributes(nodeAttributes, additionalAttributes);

      LongCounter primaryCounter = primaryMetricsContext.longCounter(metricName, description);
      LongCounter nodeCounter = nodeMetricsContext.longCounter(metricName, description);
      return new DualRegistryAttributedLongCounter(
          primaryCounter, finalPrimaryAttrs, nodeCounter, finalNodeAttrs);
    } else {
      String finalMetricName = primaryIsNodeRegistry ? toNodeMetricName(metricName) : metricName;
      LongCounter counter = primaryMetricsContext.longCounter(finalMetricName, description);
      return new AttributedLongCounter(counter, finalPrimaryAttrs);
    }
  }

  public AttributedLongUpDownCounter attributedLongUpDownCounter(
      String metricName, String description, Attributes additionalAttributes) {
    Attributes finalPrimaryAttrs = appendAttributes(primaryAttributes, additionalAttributes);

    if (aggregateToNodeRegistry && nodeMetricsContext != null) {
      Attributes finalNodeAttrs = appendAttributes(nodeAttributes, additionalAttributes);

      LongUpDownCounter primaryCounter =
          primaryMetricsContext.longUpDownCounter(metricName, description);
      LongUpDownCounter nodeCounter = nodeMetricsContext.longUpDownCounter(metricName, description);
      return new DualRegistryAttributedLongUpDownCounter(
          primaryCounter, finalPrimaryAttrs, nodeCounter, finalNodeAttrs);
    } else {
      String finalMetricName = primaryIsNodeRegistry ? toNodeMetricName(metricName) : metricName;
      LongUpDownCounter counter =
          primaryMetricsContext.longUpDownCounter(finalMetricName, description);
      return new AttributedLongUpDownCounter(counter, finalPrimaryAttrs);
    }
  }

  public AttributedLongTimer attributedLongTimer(
      String metricName, String description, OtelUnit unit, Attributes additionalAttributes) {
    Attributes finalPrimaryAttrs = appendAttributes(primaryAttributes, additionalAttributes);

    if (aggregateToNodeRegistry && nodeMetricsContext != null) {
      Attributes finalNodeAttrs = appendAttributes(nodeAttributes, additionalAttributes);
      LongHistogram primaryHistogram =
          primaryMetricsContext.longHistogram(metricName, description, unit);
      LongHistogram nodeHistogram = nodeMetricsContext.longHistogram(metricName, description, unit);
      return new DualRegistryAttributedLongTimer(
          primaryHistogram, finalPrimaryAttrs, nodeHistogram, finalNodeAttrs);
    } else {
      String finalMetricName = primaryIsNodeRegistry ? toNodeMetricName(metricName) : metricName;
      LongHistogram histogram =
          primaryMetricsContext.longHistogram(finalMetricName, description, unit);
      return new AttributedLongTimer(histogram, finalPrimaryAttrs);
    }
  }

  /** Replace core metric name prefix to node prefix */
  private String toNodeMetricName(String coreMetricName) {
    return coreMetricName.replace("solr_core", "solr_node");
  }

  /** Filter out core attributes and keep all others for node-level metrics */
  @SuppressWarnings("unchecked")
  private Attributes createNodeAttributes(Attributes coreAttributes) {
    var builder = Attributes.builder();

    coreAttributes.forEach(
        (key, value) -> {
          if (!FILTER_ATTRS_SET.contains(key)) {
            builder.put((AttributeKey<Object>) key, value);
          }
        });

    return builder.build();
  }

  private Attributes appendAttributes(Attributes base, Attributes additional) {
    return base.toBuilder().putAll(additional).build();
  }
}
