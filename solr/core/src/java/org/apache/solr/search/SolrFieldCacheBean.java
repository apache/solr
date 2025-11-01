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
package org.apache.solr.search;

import io.opentelemetry.api.common.Attributes;
import org.apache.solr.common.util.EnvUtils;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.core.SolrInfoBean;
import org.apache.solr.metrics.SolrMetricsContext;
import org.apache.solr.metrics.otel.OtelUnit;
import org.apache.solr.uninverting.UninvertingReader;

/** A SolrInfoBean that provides introspection of the Solr FieldCache */
public class SolrFieldCacheBean implements SolrInfoBean {

  private boolean enableEntryList =
      EnvUtils.getPropertyAsBool("solr.metrics.fieldcache.entries.enabled", true);
  private boolean enableJmxEntryList =
      EnvUtils.getPropertyAsBool("solr.metrics.fieldcache.entries.jmx.enabled", true);
  private AutoCloseable toClose;

  private SolrMetricsContext solrMetricsContext;

  @Override
  public String getName() {
    return this.getClass().getName();
  }

  @Override
  public String getDescription() {
    return "Provides introspection of the Solr FieldCache ";
  }

  @Override
  public Category getCategory() {
    return Category.CACHE;
  }

  @Override
  public SolrMetricsContext getSolrMetricsContext() {
    return solrMetricsContext;
  }

  @Override
  public void initializeMetrics(SolrMetricsContext parentContext, Attributes attributes) {
    this.solrMetricsContext = parentContext;
    var solrCacheStats =
        solrMetricsContext.longGaugeMeasurement(
            "solr_core_field_cache_entries", "Number of field cache entries");
    var solrCacheSize =
        solrMetricsContext.longGaugeMeasurement(
            "solr_core_field_cache_size", "Size of field cache in bytes", OtelUnit.BYTES);
    this.toClose =
        solrMetricsContext.batchCallback(
            () -> {
              if (enableEntryList && enableJmxEntryList) {
                UninvertingReader.FieldCacheStats fieldCacheStats =
                    UninvertingReader.getUninvertedStats();
                String[] entries = fieldCacheStats.info;
                solrCacheStats.record(entries.length, attributes);
                solrCacheSize.record(fieldCacheStats.totalSize, attributes);
              } else {
                solrCacheStats.record(UninvertingReader.getUninvertedStatsSize(), attributes);
              }
            },
            solrCacheStats,
            solrCacheSize);
  }

  @Override
  public void close() {
    IOUtils.closeQuietly(toClose);
  }
}
