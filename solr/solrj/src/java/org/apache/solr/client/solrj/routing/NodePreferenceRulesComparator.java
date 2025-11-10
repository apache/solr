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

package org.apache.solr.client.solrj.routing;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import org.apache.solr.common.cloud.NodesSysProps;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.params.SolrParams;

/**
 * This comparator makes sure that the given replicas are sorted according to the given list of
 * preferences. E.g. If all nodes prefer local cores then a bad/heavily-loaded node will receive
 * fewer requests from healthy nodes. This will help prevent a distributed deadlock or timeouts in
 * all the healthy nodes due to one bad node.
 *
 * <p>Optional final preferenceRule is *not* used for pairwise sorting, but instead defines how
 * "equivalent" replicas will be ordered (the base ordering). Defaults to "random"; may specify
 * "stable".
 */
public class NodePreferenceRulesComparator {

  private final NodesSysProps sysProps;
  private final String nodeName;
  private final List<PreferenceRule> sortRules;
  private final List<PreferenceRule> preferenceRules;
  private final String baseUrl;
  private final String hostName;
  private final ReplicaListTransformer baseReplicaListTransformer;

  public NodePreferenceRulesComparator(
      final List<PreferenceRule> preferenceRules,
      final SolrParams requestParams,
      final ReplicaListTransformerFactory defaultRltFactory,
      final ReplicaListTransformerFactory stableRltFactory) {
    this(
        preferenceRules,
        requestParams,
        null,
        null,
        null,
        null,
        defaultRltFactory,
        stableRltFactory);
  }

  public NodePreferenceRulesComparator(
      final List<PreferenceRule> preferenceRules,
      final SolrParams requestParams,
      final String nodeName,
      final String baseUrl,
      final String hostName,
      final NodesSysProps sysProps,
      final ReplicaListTransformerFactory defaultRltFactory,
      final ReplicaListTransformerFactory stableRltFactory) {
    this.sysProps = sysProps;
    this.preferenceRules = preferenceRules;
    this.nodeName = nodeName;
    this.baseUrl = baseUrl;
    this.hostName = hostName;
    final int maxIdx = preferenceRules.size() - 1;
    final PreferenceRule lastRule = preferenceRules.get(maxIdx);
    if (!ShardParams.SHARDS_PREFERENCE_REPLICA_BASE.equals(lastRule.name)) {
      this.sortRules = preferenceRules;
      this.baseReplicaListTransformer =
          defaultRltFactory.getInstance(
              null, requestParams, RequestReplicaListTransformerGenerator.RANDOM_RLTF);
    } else {
      if (maxIdx == 0) {
        this.sortRules = null;
      } else {
        this.sortRules = preferenceRules.subList(0, maxIdx);
      }
      String[] parts = lastRule.value.split(":", 2);
      switch (parts[0]) {
        case ShardParams.REPLICA_RANDOM:
          this.baseReplicaListTransformer =
              RequestReplicaListTransformerGenerator.RANDOM_RLTF.getInstance(
                  parts.length == 1 ? null : parts[1], requestParams, null);
          break;
        case ShardParams.REPLICA_STABLE:
          this.baseReplicaListTransformer =
              stableRltFactory.getInstance(
                  parts.length == 1 ? null : parts[1],
                  requestParams,
                  RequestReplicaListTransformerGenerator.RANDOM_RLTF);
          break;
        default:
          throw new IllegalArgumentException("Invalid base replica order spec");
      }
    }
  }

  private static final ReplicaListTransformer NOOP_RLT = NoOpReplicaListTransformer.INSTANCE;
  private static final ReplicaListTransformerFactory NOOP_RLTF =
      (String configSpec, SolrParams requestParams, ReplicaListTransformerFactory fallback) ->
          NOOP_RLT;

  /**
   * For compatibility with tests, which expect this constructor to have no effect on the *base*
   * order.
   */
  NodePreferenceRulesComparator(
      final List<PreferenceRule> preferenceRules, final SolrParams requestParams) {
    this(preferenceRules, requestParams, NOOP_RLTF, null);
  }

  /**
   * For compatibility with tests, which expect this constructor to have no effect on the *base*
   * order.
   */
  NodePreferenceRulesComparator(
      final List<PreferenceRule> preferenceRules,
      final SolrParams requestParams,
      final String nodeName,
      final String baseUrl,
      final String hostName) {
    this(preferenceRules, requestParams, nodeName, baseUrl, hostName, null, NOOP_RLTF, null);
  }

  public ReplicaListTransformer getBaseReplicaListTransformer() {
    return baseReplicaListTransformer;
  }

  @SuppressWarnings({"unchecked"})
  public <T> Comparator<T> getComparator(T example) {
    if (example instanceof Replica) {
      return (Comparator<T>) getReplicaComparator();
    } else if (example instanceof String) {
      return (Comparator<T>) getUrlComparator();
    } else {
      return null;
    }
  }

  public Comparator<Replica> getReplicaComparator() {
    Comparator<Replica> comparator = null;
    if (this.sortRules != null) {
      for (PreferenceRule preferenceRule : this.sortRules) {
        Comparator<Replica> nextComparator = getPreferenceReplicaComparator(preferenceRule);
        if (nextComparator != null) {
          if (comparator != null) {
            comparator = comparator.thenComparing(nextComparator);
          } else {
            comparator = nextComparator;
          }
        }
      }
    }
    return comparator;
  }

  public Comparator<String> getUrlComparator() {
    Comparator<String> comparator = null;
    if (this.sortRules != null) {
      for (PreferenceRule preferenceRule : this.sortRules) {
        Comparator<String> nextComparator = getPreferenceUrlComparator(preferenceRule);
        if (nextComparator != null) {
          if (comparator != null) {
            comparator = comparator.thenComparing(nextComparator);
          } else {
            comparator = nextComparator;
          }
        }
      }
    }
    return comparator;
  }

  private Comparator<Replica> getPreferenceReplicaComparator(PreferenceRule preferenceRule) {
    Comparator<Replica> comparator =
        switch (preferenceRule.name) {
          case ShardParams.SHARDS_PREFERENCE_REPLICA_TYPE:
            yield Comparator.comparing(
                r -> r.getType().toString().equalsIgnoreCase(preferenceRule.value));
          case ShardParams.SHARDS_PREFERENCE_REPLICA_LOCATION:
            yield switch (preferenceRule.value) {
              case ShardParams.REPLICA_LOCAL -> {
                if (baseUrl == null) {
                  // For SolrJ clients, which do not have a baseUrl, this preference won't be used
                  yield null;
                }
                yield Comparator.comparing(r -> r.getBaseUrl().equals(baseUrl));
              }
              case ShardParams.REPLICA_HOST -> {
                if (hostName == null) {
                  // For SolrJ clients, which do not have a hostName, this preference won't be used
                  yield null;
                }
                final String hostNameWithColon = hostName + ":";
                yield Comparator.comparing(r -> r.getNodeName().startsWith(hostNameWithColon));
              }
              default -> Comparator.comparing(r -> r.getCoreUrl().startsWith(preferenceRule.value));
            };
          case ShardParams.SHARDS_PREFERENCE_REPLICA_LEADER:
            final boolean preferredIsLeader = Boolean.parseBoolean(preferenceRule.value);
            yield Comparator.comparing(r -> r.isLeader() == preferredIsLeader);
          case ShardParams.SHARDS_PREFERENCE_NODE_WITH_SAME_SYSPROP:
            if (sysProps == null) {
              // For SolrJ clients, which do not have Solr sysProps, this preference won't be used
              yield null;
            }
            Collection<String> tags = Collections.singletonList(preferenceRule.value);
            Map<String, Object> currentNodeMetric = sysProps.getSysProps(nodeName, tags);
            yield Comparator.comparing(
                r -> currentNodeMetric.equals(sysProps.getSysProps(r.getNodeName(), tags)));
          case ShardParams.SHARDS_PREFERENCE_REPLICA_BASE:
            throw new IllegalArgumentException(
                "only one base replica order may be specified in "
                    + ShardParams.SHARDS_PREFERENCE
                    + ", and it must be specified last");
          default:
            throw new IllegalArgumentException(
                "Invalid " + ShardParams.SHARDS_PREFERENCE + " type: " + preferenceRule.name);
        };
    // Boolean comparators are 'false' first by default, so we need to reverse
    return comparator != null ? comparator.reversed() : null;
  }

  private Comparator<String> getPreferenceUrlComparator(PreferenceRule preferenceRule) {
    Comparator<String> comparator =
        switch (preferenceRule.name) {
            // These preferences are not supported for URLs
          case ShardParams.SHARDS_PREFERENCE_REPLICA_TYPE:
          case ShardParams.SHARDS_PREFERENCE_REPLICA_LEADER:
          case ShardParams.SHARDS_PREFERENCE_NODE_WITH_SAME_SYSPROP:
            yield null;
          case ShardParams.SHARDS_PREFERENCE_REPLICA_LOCATION:
            yield switch (preferenceRule.value) {
              case ShardParams.REPLICA_LOCAL -> {
                if (baseUrl == null) {
                  // For SolrJ clients, which do not have a baseUrl, this preference won't be used
                  yield null;
                }
                yield Comparator.comparing(url -> url.startsWith(baseUrl));
              }
              case ShardParams.REPLICA_HOST -> {
                if (hostName == null) {
                  // For SolrJ clients, which do not have a hostName, this preference won't be used
                  yield null;
                }
                String scheme = baseUrl.startsWith("https") ? "https" : "http";
                final String baseUrlHostPrefix = scheme + "://" + hostName + ":";
                yield Comparator.comparing(url -> url.startsWith(baseUrlHostPrefix));
              }
              default -> Comparator.comparing(url -> url.startsWith(preferenceRule.value));
            };
          case ShardParams.SHARDS_PREFERENCE_REPLICA_BASE:
            throw new IllegalArgumentException(
                "only one base replica order may be specified in "
                    + ShardParams.SHARDS_PREFERENCE
                    + ", and it must be specified last");
          default:
            throw new IllegalArgumentException(
                "Invalid " + ShardParams.SHARDS_PREFERENCE + " type: " + preferenceRule.name);
        };
    // Boolean comparators are 'false' first by default, so we need to reverse
    return comparator != null ? comparator.reversed() : null;
  }

  public List<PreferenceRule> getPreferenceRules() {
    return preferenceRules;
  }
}
