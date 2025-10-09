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

import static org.apache.solr.metrics.SolrMetricProducer.CATEGORY_ATTR;
import static org.apache.solr.metrics.SolrMetricProducer.HANDLER_ATTR;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.LongHistogram;
import io.opentelemetry.api.metrics.LongUpDownCounter;
import org.apache.solr.core.SolrInfoBean;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.metrics.SolrMetricsContext;
import org.apache.solr.metrics.otel.OtelUnit;

/**
 * Factory for creating metrics instruments that can write to either single or dual registries (core
 * and node).
 */
public class AttributedInstrumentFactory {

  private final SolrMetricsContext primaryMetricsContext;
  private final Attributes primaryAttributes;
  private final boolean aggregateToNodeRegistry;
  private final boolean primaryIsNodeRegistry;
  private SolrMetricsContext nodeMetricsContext = null;
  private Attributes nodeAttributes = null;

  public AttributedInstrumentFactory(
      SolrMetricsContext primaryMetricsContext,
      Attributes primaryAttributes,
      boolean aggregateToNodeRegistry) {
    this.primaryMetricsContext = primaryMetricsContext;
    this.primaryAttributes = primaryAttributes;
    this.aggregateToNodeRegistry = aggregateToNodeRegistry;

    // Primary could be a node
    this.primaryIsNodeRegistry =
        primaryMetricsContext
            .getRegistryName()
            .equals(SolrMetricManager.getRegistryName(SolrInfoBean.Group.node));

    // Only create node registry if we want dual registry mode AND primary is not already a node
    // registry
    if (aggregateToNodeRegistry && !primaryIsNodeRegistry) {
      this.nodeMetricsContext =
          new SolrMetricsContext(
              primaryMetricsContext.getMetricManager(),
              SolrMetricManager.getRegistryName(SolrInfoBean.Group.node),
              null);
      this.nodeAttributes = createNodeAttributes(primaryAttributes);
    }
  }

  public AttributedLongCounter attributedLongCounter(
      MetricNameProvider metricNameProvider, String description, Attributes additionalAttributes) {
    Attributes finalPrimaryAttrs = appendAttributes(primaryAttributes, additionalAttributes);

    if (aggregateToNodeRegistry && nodeMetricsContext != null) {
      Attributes finalNodeAttrs = appendAttributes(nodeAttributes, additionalAttributes);

      LongCounter primaryCounter =
          primaryMetricsContext.longCounter(metricNameProvider.getPrimaryMetricName(), description);
      LongCounter nodeCounter =
          nodeMetricsContext.longCounter(metricNameProvider.getNodeMetricName(), description);
      return new DualRegistryAttributedLongCounter(
          primaryCounter, finalPrimaryAttrs, nodeCounter, finalNodeAttrs);
    } else {
      String metricName =
          primaryIsNodeRegistry
              ? metricNameProvider.getNodeMetricName()
              : metricNameProvider.getPrimaryMetricName();

      LongCounter counter = primaryMetricsContext.longCounter(metricName, description);
      return new AttributedLongCounter(counter, finalPrimaryAttrs);
    }
  }

  public AttributedLongUpDownCounter attributedLongUpDownCounter(
      MetricNameProvider metricNameProvider, String description, Attributes additionalAttributes) {
    Attributes finalPrimaryAttrs = appendAttributes(primaryAttributes, additionalAttributes);

    if (aggregateToNodeRegistry && nodeMetricsContext != null) {
      Attributes finalNodeAttrs = appendAttributes(nodeAttributes, additionalAttributes);

      LongUpDownCounter primaryCounter =
          primaryMetricsContext.longUpDownCounter(
              metricNameProvider.getPrimaryMetricName(), description);
      LongUpDownCounter nodeCounter =
          nodeMetricsContext.longUpDownCounter(metricNameProvider.getNodeMetricName(), description);

      return new DualRegistryAttributedLongUpDownCounter(
          primaryCounter, finalPrimaryAttrs, nodeCounter, finalNodeAttrs);
    } else {
      String metricName =
          primaryIsNodeRegistry
              ? metricNameProvider.getNodeMetricName()
              : metricNameProvider.getPrimaryMetricName();

      LongUpDownCounter counter = primaryMetricsContext.longUpDownCounter(metricName, description);
      return new AttributedLongUpDownCounter(counter, finalPrimaryAttrs);
    }
  }

  public AttributedLongTimer attributedLongTimer(
      MetricNameProvider metricNameProvider,
      String description,
      OtelUnit unit,
      Attributes additionalAttributes) {
    Attributes finalPrimaryAttrs = appendAttributes(primaryAttributes, additionalAttributes);

    if (aggregateToNodeRegistry) {
      Attributes finalNodeAttrs = appendAttributes(nodeAttributes, additionalAttributes);
      LongHistogram coreHistogram =
          primaryMetricsContext.longHistogram(
              metricNameProvider.getPrimaryMetricName(), description, unit);
      LongHistogram nodeHistogram =
          nodeMetricsContext.longHistogram(
              metricNameProvider.getNodeMetricName(), description, unit);
      return new DualRegistryAttributedLongTimer(
          coreHistogram, finalPrimaryAttrs, nodeHistogram, finalNodeAttrs);
    } else {
      String metricName =
          primaryIsNodeRegistry
              ? metricNameProvider.getNodeMetricName()
              : metricNameProvider.getPrimaryMetricName();

      LongHistogram histogram = primaryMetricsContext.longHistogram(metricName, description, unit);
      return new AttributedLongTimer(histogram, finalPrimaryAttrs);
    }
  }

  // Filter out core attributes and keep only category and handler if they exist
  private Attributes createNodeAttributes(Attributes coreAttributes) {
    var builder = Attributes.builder();

    if (coreAttributes.get(CATEGORY_ATTR) != null)
      builder.put(CATEGORY_ATTR, coreAttributes.get(CATEGORY_ATTR));
    if (coreAttributes.get(HANDLER_ATTR) != null)
      builder.put(HANDLER_ATTR, coreAttributes.get(HANDLER_ATTR));

    return builder.build();
  }

  private Attributes appendAttributes(Attributes base, Attributes additional) {
    return base.toBuilder().putAll(additional).build();
  }

  public interface MetricNameProvider {
    String getPrimaryMetricName();

    String getNodeMetricName();
  }

  public static MetricNameProvider standardNameProvider(
      String corePrefix, String nodePrefix, String metricSuffix) {
    return new MetricNameProvider() {
      @Override
      public String getPrimaryMetricName() {
        return corePrefix + metricSuffix;
      }

      @Override
      public String getNodeMetricName() {
        return nodePrefix + metricSuffix;
      }
    };
  }
}
