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

import com.codahale.metrics.Metric;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.solr.common.SolrException;
import org.apache.solr.metrics.prometheus.SolrMetric;

/** Base class is a wrapper to export a solr.core {@link com.codahale.metrics.Metric} */
public abstract class SolrCoreMetric extends SolrMetric {

  public static Pattern CLOUD_CORE_PATTERN =
      Pattern.compile(
          "(?<core>^core_(?<collection>.*)_(?<shard>shard[0-9]+)_(?<replica>replica_.[0-9]+)).(.*)$");
  public static Pattern STANDALONE_CORE_PATTERN = Pattern.compile("^core_(?<core>.*?)\\.(.*)$");

  public String coreName;

  public SolrCoreMetric(Metric dropwizardMetric, String metricName) {
    // Remove Core Name prefix from metric as it is no longer needed for tag parsing from this point
    super(dropwizardMetric, metricName.substring(metricName.indexOf(".") + 1));
    Matcher cloudPattern = CLOUD_CORE_PATTERN.matcher(metricName);
    Matcher standalonePattern = STANDALONE_CORE_PATTERN.matcher(metricName);
    if (cloudPattern.find()) {
      coreName = cloudPattern.group("core");
      cloudPattern
          .namedGroups()
          .forEach((key, value) -> labels.put(key, cloudPattern.group(value)));
    } else if (standalonePattern.find()) {
      coreName = standalonePattern.group("core");
      standalonePattern
          .namedGroups()
          .forEach((key, value) -> labels.put(key, standalonePattern.group(value)));
    } else {
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR,
          "Core name does not match pattern for parsing in metric " + metricName);
    }
  }
}
