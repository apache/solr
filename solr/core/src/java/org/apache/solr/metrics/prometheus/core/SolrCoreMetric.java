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
package org.apache.solr.metrics.prometheus.core;

import static org.apache.solr.metrics.prometheus.core.PrometheusCoreFormatterInfo.CLOUD_CORE_PATTERN;

import com.codahale.metrics.Metric;
import java.util.regex.Matcher;
import org.apache.solr.common.SolrException;
import org.apache.solr.metrics.prometheus.SolrMetric;

/** Base class is a wrapper to export a solr.core {@link com.codahale.metrics.Metric} */
public abstract class SolrCoreMetric extends SolrMetric {
  public String coreName;

  public SolrCoreMetric(
      Metric dropwizardMetric, String coreName, String metricName, boolean cloudMode) {
    super(dropwizardMetric, metricName);
    this.coreName = coreName;
    labels.put("core", coreName);
    if (cloudMode) {
      appendCloudModeLabels();
    }
  }

  private void appendCloudModeLabels() {
    Matcher m = CLOUD_CORE_PATTERN.matcher(coreName);
    if (m.find()) {
      labels.put("collection", m.group(1));
      labels.put("shard", m.group(2));
      labels.put("replica", m.group(3));
    } else {
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR,
          "Core name does not match pattern for parsing " + coreName);
    }
  }
}
