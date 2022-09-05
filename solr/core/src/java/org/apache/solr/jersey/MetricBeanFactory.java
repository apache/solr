/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.solr.jersey;

import org.apache.solr.core.PluginBag;
import org.glassfish.hk2.api.Factory;

/**
 * Factory to inject JerseyMetricsLookupRegistry instances into Jersey resources and filters.
 *
 * <p>Currently, Jersey resources that have a corresponding v1 API produce the same metrics as their
 * v1 equivalent and rely on the v1 requestHandler instance to do so. Solr facilitates this by
 * building a map of the Jersey resource to requestHandler mapping (a {@link
 * org.apache.solr.core.PluginBag.JerseyMetricsLookupRegistry}), and injecting it into the pre- and
 * post- Jersey filters that handle metrics.
 *
 * <p>This isn't ideal, as requestHandler's don't really "fit" conceptually here. But it's
 * unavoidable while we want our v2 APIs to exactly match the metrics produced by v1 calls.
 *
 * @see RequestMetricHandling.PreRequestMetricsFilter
 * @see RequestMetricHandling.PostRequestMetricsFilter
 */
public class MetricBeanFactory implements Factory<PluginBag.JerseyMetricsLookupRegistry> {

  private final PluginBag.JerseyMetricsLookupRegistry metricsLookupRegistry;

  public MetricBeanFactory(PluginBag.JerseyMetricsLookupRegistry metricsLookupRegistry) {
    this.metricsLookupRegistry = metricsLookupRegistry;
  }

  @Override
  public PluginBag.JerseyMetricsLookupRegistry provide() {
    return metricsLookupRegistry;
  }

  @Override
  public void dispose(PluginBag.JerseyMetricsLookupRegistry instance) {
    /* No-op */
  }
}
