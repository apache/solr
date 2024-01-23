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

package org.apache.solr.client.solrj.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.solr.client.solrj.response.SimpleSolrResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.util.Utils;

/**
 * This class is responsible for fetching metrics and other attributes from a given node in Solr
 * cluster. This is a helper class that is used by {@link SolrClientNodeStateProvider}
 */
public class NodeValueFetcher {
  // well known tags
  public static final String NODE = "node";
  public static final String PORT = "port";
  public static final String HOST = "host";
  public static final String CORES = "cores";
  public static final String SYSPROP = "sysprop.";
  public static final Set<String> tags =
      Set.of(NODE, PORT, HOST, CORES, Tags.FREEDISK.tagName, Tags.HEAPUSAGE.tagName);
  public static final Pattern hostAndPortPattern = Pattern.compile("(?:https?://)?([^:]+):(\\d+)");
  public static final String METRICS_PREFIX = "metrics:";

  /** Various well known tags that can be fetched from a node */
  public enum Tags {
    FREEDISK(
        "freedisk", "solr.node", "CONTAINER.fs.usableSpace", "solr.node/CONTAINER.fs.usableSpace"),
    TOTALDISK(
        "totaldisk", "solr.node", "CONTAINER.fs.totalSpace", "solr.node/CONTAINER.fs.totalSpace"),
    CORES("cores", "solr.node", "CONTAINER.cores", null) {
      @Override
      public Object extractResult(Object root) {
        NamedList<?> node = (NamedList<?>) Utils.getObjectByPath(root, false, "solr.node");
        int count = 0;
        for (String leafCoreMetricName : new String[] {"lazy", "loaded", "unloaded"}) {
          Number n = (Number) node.get("CONTAINER.cores." + leafCoreMetricName);
          if (n != null) count += n.intValue();
        }
        return count;
      }
    },
    SYSLOADAVG("sysLoadAvg", "solr.jvm", "os.systemLoadAverage", "solr.jvm/os.systemLoadAverage"),
    HEAPUSAGE("heapUsage", "solr.jvm", "memory.heap.usage", "solr.jvm/memory.heap.usage");
    // the metrics group
    public final String group;
    // the metrics prefix
    public final String prefix;
    public final String tagName;
    // the json path in the response
    public final String path;

    Tags(String name, String group, String prefix, String path) {
      this.group = group;
      this.prefix = prefix;
      this.tagName = name;
      this.path = path;
    }

    public Object extractResult(Object root) {
      Object v = Utils.getObjectByPath(root, true, path);
      return v == null ? null : convertVal(v);
    }

    public Object convertVal(Object val) {
      if (val instanceof String) {
        return Double.valueOf((String) val);
      } else if (val instanceof Number) {
        Number num = (Number) val;
        return num.doubleValue();

      } else {
        throw new IllegalArgumentException("Unknown type : " + val);
      }
    }
  }

  private void getRemoteInfo(
      String solrNode, Set<String> requestedTags, SolrClientNodeStateProvider.RemoteCallCtx ctx) {
    if (!(ctx).isNodeAlive(solrNode)) return;
    Map<String, Set<Object>> metricsKeyVsTag = new HashMap<>();
    for (String tag : requestedTags) {
      if (tag.startsWith(SYSPROP)) {
        metricsKeyVsTag
            .computeIfAbsent(
                "solr.jvm:system.properties:" + tag.substring(SYSPROP.length()),
                k -> new HashSet<>())
            .add(tag);
      } else if (tag.startsWith(METRICS_PREFIX)) {
        metricsKeyVsTag
            .computeIfAbsent(tag.substring(METRICS_PREFIX.length()), k -> new HashSet<>())
            .add(tag);
      }
    }
    if (!metricsKeyVsTag.isEmpty()) {
      SolrClientNodeStateProvider.fetchReplicaMetrics(solrNode, ctx, metricsKeyVsTag);
    }

    Set<String> groups = new HashSet<>();
    List<String> prefixes = new ArrayList<>();
    for (Tags t : Tags.values()) {
      if (requestedTags.contains(t.tagName)) {
        groups.add(t.group);
        prefixes.add(t.prefix);
      }
    }
    if (groups.isEmpty() || prefixes.isEmpty()) return;

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("group", StrUtils.join(groups, ','));
    params.add("prefix", StrUtils.join(prefixes, ','));

    try {
      SimpleSolrResponse rsp = ctx.invokeWithRetry(solrNode, CommonParams.METRICS_PATH, params);
      NamedList<?> metrics = (NamedList<?>) rsp.getResponse().get("metrics");
      if (metrics != null) {
        for (Tags t : Tags.values()) {
          ctx.tags.put(t.tagName, t.extractResult(metrics));
        }
      }
    } catch (Exception e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Error getting remote info", e);
    }
  }

  public void getTags(
      String solrNode, Set<String> requestedTags, SolrClientNodeStateProvider.RemoteCallCtx ctx) {
    try {
      if (requestedTags.contains(NODE)) ctx.tags.put(NODE, solrNode);
      if (requestedTags.contains(HOST)) {
        Matcher hostAndPortMatcher = hostAndPortPattern.matcher(solrNode);
        if (hostAndPortMatcher.find()) ctx.tags.put(HOST, hostAndPortMatcher.group(1));
      }
      if (requestedTags.contains(PORT)) {
        Matcher hostAndPortMatcher = hostAndPortPattern.matcher(solrNode);
        if (hostAndPortMatcher.find()) ctx.tags.put(PORT, hostAndPortMatcher.group(2));
      }
      getRemoteInfo(solrNode, requestedTags, ctx);
    } catch (Exception e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }
  }
}
