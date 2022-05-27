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

package org.apache.solr.common.cloud;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.net.URLEncoder;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.solr.client.solrj.impl.CloudLegacySolrClient;
import org.apache.solr.common.NavigableObject;
import org.apache.solr.common.util.Utils;

/** Fetch lazily and cache a node's system properties */
public class NodesSysPropsCacher implements AutoCloseable {
  private volatile boolean isClosed = false;
  private final Map<String, Map<String, Object>> nodeVsTagsCache = new ConcurrentHashMap<>();
  private ZkStateReader zkStateReader;
  private final CloudLegacySolrClient solrClient;

  public NodesSysPropsCacher(CloudLegacySolrClient solrClient, ZkStateReader zkStateReader) {
    this.zkStateReader = zkStateReader;
    this.solrClient = solrClient;
    zkStateReader.registerLiveNodesListener(
        (oldNodes, newNodes) -> {
          for (String n : oldNodes) {
            if (!newNodes.contains(n)) {
              // this node has gone down, clear data
              nodeVsTagsCache.remove(n);
            }
          }
          return isClosed;
        });
  }

  public Map<String, Object> getSysProps(String nodeName, Collection<String> tags) {
    Map<String, Object> cached =
        nodeVsTagsCache.computeIfAbsent(nodeName, s -> new LinkedHashMap<>());
    Map<String, Object> result = new LinkedHashMap<>();
    for (String tag : tags) {
      if (!cached.containsKey(tag)) {
        // at least one property is missing. fetch properties from the node
        Map<String, Object> props = fetchProps(nodeName, tags);
        // make a copy
        cached = new LinkedHashMap<>(cached);
        // merge all properties
        cached.putAll(props);
        // update the cache with the new set of properties
        nodeVsTagsCache.put(nodeName, cached);
        return props;
      } else {
        result.put(tag, cached.get(tag));
      }
    }
    return result;
  }

  private Map<String, Object> fetchProps(String nodeName, Collection<String> tags) {
    StringBuilder sb = new StringBuilder(zkStateReader.getBaseUrlForNodeName(nodeName));
    sb.append("/admin/metrics?omitHeader=true&wt=javabin");
    LinkedHashMap<String, String> keys = new LinkedHashMap<>();
    for (String tag : tags) {
      String metricsKey = "solr.jvm:system.properties:" + tag;
      keys.put(tag, metricsKey);
      sb.append("&key=").append(URLEncoder.encode(metricsKey, UTF_8));
    }

    Map<String, Object> result = new LinkedHashMap<>();
    NavigableObject response =
        (NavigableObject)
            Utils.executeGET(solrClient.getHttpClient(), sb.toString(), Utils.JAVABINCONSUMER);
    NavigableObject metrics = (NavigableObject) response._get("metrics", Collections.emptyMap());
    keys.forEach((tag, key) -> result.put(tag, metrics._get(key, null)));
    return result;
  }

  @Override
  public void close() {
    isClosed = true;
  }
}
