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
import static org.apache.solr.metrics.prometheus.core.PrometheusCoreFormatterInfo.STANDALONE_CORE_PATTERN;

import com.codahale.metrics.Metric;
import java.util.regex.Matcher;
import org.apache.solr.common.SolrException;
import org.apache.solr.metrics.prometheus.SolrMetric;

/** Base class is a wrapper to export a solr.core {@link com.codahale.metrics.Metric} */
public abstract class SolrCoreMetric extends SolrMetric {
  public SolrCoreMetric(Metric dropwizardMetric, String metricName) {
    super(dropwizardMetric, metricName);
    Matcher cloudPattern = CLOUD_CORE_PATTERN.matcher(metricName);
    Matcher standalonePattern = STANDALONE_CORE_PATTERN.matcher(metricName);
    try {
      if (cloudPattern.find()) {
        labels.put("core", cloudPattern.group(1));
        labels.put("collection", cloudPattern.group(2));
        labels.put("shard", cloudPattern.group(3));
        labels.put("replica", cloudPattern.group(4));
      } else {
        standalonePattern.find();
        labels.put("core", standalonePattern.group(1));
      }
    } catch (IllegalStateException e) {
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR,
          "Core name does not match pattern for parsing " + metricName);
    }
  }
}
