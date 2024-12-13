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

import java.util.Collections;

/** */
public class MetricsConfig {

  private final PluginInfo[] metricReporters;
  private final PluginInfo counterSupplier;
  private final PluginInfo meterSupplier;
  private final PluginInfo timerSupplier;
  private final PluginInfo histogramSupplier;
  private final Object nullNumber;
  private final Object notANumber;
  private final Object nullString;
  private final Object nullObject;
  private final boolean enabled;
  private final CacheConfig cacheConfig;

  private MetricsConfig(
      boolean enabled,
      PluginInfo[] metricReporters,
      PluginInfo counterSupplier,
      PluginInfo meterSupplier,
      PluginInfo timerSupplier,
      PluginInfo histogramSupplier,
      Object nullNumber,
      Object notANumber,
      Object nullString,
      Object nullObject,
      CacheConfig cacheConfig) {
    this.enabled = enabled;
    this.metricReporters = metricReporters;
    this.counterSupplier = counterSupplier;
    this.meterSupplier = meterSupplier;
    this.timerSupplier = timerSupplier;
    this.histogramSupplier = histogramSupplier;
    this.nullNumber = nullNumber;
    this.notANumber = notANumber;
    this.nullString = nullString;
    this.nullObject = nullObject;
    this.cacheConfig = cacheConfig;
  }

  public boolean isEnabled() {
    return enabled;
  }

  public CacheConfig getCacheConfig() {
    return cacheConfig;
  }

  private static final PluginInfo[] NO_OP_REPORTERS = new PluginInfo[0];

  public PluginInfo[] getMetricReporters() {
    if (enabled) {
      return metricReporters;
    } else {
      return NO_OP_REPORTERS;
    }
  }

  public Object getNullNumber() {
    return nullNumber;
  }

  public Object getNotANumber() {
    return notANumber;
  }

  public Object getNullString() {
    return nullString;
  }

  public Object getNullObject() {
    return nullObject;
  }

  /** Symbolic name to use as plugin class name for no-op implementations. */
  public static final String NOOP_IMPL_CLASS = "__noop__";

  private static final PluginInfo NO_OP_PLUGIN =
      new PluginInfo("typeUnused", Collections.singletonMap("class", NOOP_IMPL_CLASS), null, null);

  public PluginInfo getCounterSupplier() {
    if (enabled) {
      return counterSupplier;
    } else {
      return NO_OP_PLUGIN;
    }
  }

  public PluginInfo getMeterSupplier() {
    if (enabled) {
      return meterSupplier;
    } else {
      return NO_OP_PLUGIN;
    }
  }

  public PluginInfo getTimerSupplier() {
    if (enabled) {
      return timerSupplier;
    } else {
      return NO_OP_PLUGIN;
    }
  }

  public PluginInfo getHistogramSupplier() {
    if (enabled) {
      return histogramSupplier;
    } else {
      return NO_OP_PLUGIN;
    }
  }

  public static class MetricsConfigBuilder {
    private PluginInfo[] metricReporterPlugins = new PluginInfo[0];
    private PluginInfo counterSupplier;
    private PluginInfo meterSupplier;
    private PluginInfo timerSupplier;
    private PluginInfo histogramSupplier;
    private Object nullNumber = null;
    private Object notANumber = null;
    private Object nullString = null;
    private Object nullObject = null;
    // default to metrics enabled
    private boolean enabled = true;
    private CacheConfig cacheConfig = null;

    public MetricsConfigBuilder() {}

    public MetricsConfigBuilder setEnabled(boolean enabled) {
      this.enabled = enabled;
      return this;
    }

    public MetricsConfigBuilder setCacheConfig(CacheConfig cacheConfig) {
      this.cacheConfig = cacheConfig;
      return this;
    }

    public MetricsConfigBuilder setMetricReporterPlugins(PluginInfo[] metricReporterPlugins) {
      this.metricReporterPlugins =
          metricReporterPlugins != null ? metricReporterPlugins : new PluginInfo[0];
      return this;
    }

    public MetricsConfigBuilder setCounterSupplier(PluginInfo info) {
      this.counterSupplier = info;
      return this;
    }

    public MetricsConfigBuilder setMeterSupplier(PluginInfo info) {
      this.meterSupplier = info;
      return this;
    }

    public MetricsConfigBuilder setTimerSupplier(PluginInfo info) {
      this.timerSupplier = info;
      return this;
    }

    public MetricsConfigBuilder setHistogramSupplier(PluginInfo info) {
      this.histogramSupplier = info;
      return this;
    }

    public MetricsConfigBuilder setNullNumber(Object nullNumber) {
      this.nullNumber = nullNumber;
      return this;
    }

    public MetricsConfigBuilder setNotANumber(Object notANumber) {
      this.notANumber = notANumber;
      return this;
    }

    public MetricsConfigBuilder setNullString(Object nullString) {
      this.nullString = nullString;
      return this;
    }

    public MetricsConfigBuilder setNullObject(Object nullObject) {
      this.nullObject = nullObject;
      return this;
    }

    public MetricsConfig build() {
      return new MetricsConfig(
          enabled,
          metricReporterPlugins,
          counterSupplier,
          meterSupplier,
          timerSupplier,
          histogramSupplier,
          nullNumber,
          notANumber,
          nullString,
          nullObject,
          cacheConfig);
    }
  }

  public static class CacheConfig {
    public Integer threadsIntervalSeconds; // intervals for which the threads metrics are cached

    public CacheConfig(Integer threadsIntervalSeconds) {
      this.threadsIntervalSeconds = threadsIntervalSeconds;
    }
  }
}
