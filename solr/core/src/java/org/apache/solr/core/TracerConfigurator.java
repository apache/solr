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

import io.opentelemetry.api.trace.Tracer;
import org.apache.solr.util.plugin.NamedListInitializedPlugin;
import org.apache.solr.util.tracing.SimplePropagator;
import org.apache.solr.util.tracing.TraceUtils;

/** Produces a {@link Tracer} from configuration. */
public abstract class TracerConfigurator implements NamedListInitializedPlugin {

  public static final boolean TRACE_ID_GEN_ENABLED =
      Boolean.parseBoolean(System.getProperty("solr.alwaysOnTraceId", "true"));

  public static Tracer loadTracer(SolrResourceLoader loader, PluginInfo info) {
    if (info != null && info.isEnabled()) {
      TracerConfigurator configurator =
          loader.newInstance(info.className, TracerConfigurator.class);
      configurator.init(info.initArgs);
      return configurator.getTracer();

    } else if (TRACE_ID_GEN_ENABLED) {
      return SimplePropagator.load();
    } else {
      return TraceUtils.noop();
    }
  }

  protected abstract Tracer getTracer();
}
