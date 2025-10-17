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

import java.lang.invoke.MethodHandles;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.stream.Collectors;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.NodesSysProps;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.params.SolrParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RequestReplicaListTransformerGenerator {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final Random r = new Random();

  private static final ReplicaListTransformer shufflingReplicaListTransformer =
      new ShufflingReplicaListTransformer(r);
  public static final ReplicaListTransformerFactory RANDOM_RLTF =
      (String configSpec, SolrParams requestParams, ReplicaListTransformerFactory fallback) ->
          shufflingReplicaListTransformer;
  private final ReplicaListTransformerFactory stableRltFactory;
  private final ReplicaListTransformerFactory defaultRltFactory;
  private final String defaultShardPreferences;
  private final String nodeName;
  private final String baseUrl;
  private final String hostName;
  private final NodesSysProps sysProps;

  public RequestReplicaListTransformerGenerator() {
    this(null);
  }

  public RequestReplicaListTransformerGenerator(ReplicaListTransformerFactory defaultRltFactory) {
    this(defaultRltFactory, null);
  }

  public RequestReplicaListTransformerGenerator(
      ReplicaListTransformerFactory defaultRltFactory,
      ReplicaListTransformerFactory stableRltFactory) {
    this(defaultRltFactory, stableRltFactory, null, null, null, null, null);
  }

  public RequestReplicaListTransformerGenerator(
      String defaultShardPreferences,
      String nodeName,
      String baseUrl,
      String hostName,
      NodesSysProps sysProps) {
    this(null, null, defaultShardPreferences, nodeName, baseUrl, hostName, sysProps);
  }

  public RequestReplicaListTransformerGenerator(
      ReplicaListTransformerFactory defaultRltFactory,
      ReplicaListTransformerFactory stableRltFactory,
      String defaultShardPreferences,
      String nodeName,
      String baseUrl,
      String hostName,
      NodesSysProps sysProps) {
    this.defaultRltFactory = Objects.requireNonNullElse(defaultRltFactory, RANDOM_RLTF);
    this.stableRltFactory =
        Objects.requireNonNullElseGet(stableRltFactory, AffinityReplicaListTransformerFactory::new);
    this.defaultShardPreferences = Objects.requireNonNullElse(defaultShardPreferences, "");
    this.nodeName = nodeName;
    this.baseUrl = baseUrl;
    this.hostName = hostName;
    this.sysProps = sysProps;
  }

  public ReplicaListTransformer getReplicaListTransformer(final SolrParams requestParams) {
    return getReplicaListTransformer(requestParams, null);
  }

  public ReplicaListTransformer getReplicaListTransformer(
      final SolrParams requestParams, String defaultShardPreferences) {
    return getReplicaListTransformer(
        requestParams, defaultShardPreferences, null, null, null, null);
  }

  public ReplicaListTransformer getReplicaListTransformer(
      final SolrParams requestParams,
      String defaultShardPreferences,
      String nodeName,
      String baseUrl,
      String hostName,
      NodesSysProps sysProps) {
    defaultShardPreferences =
        Objects.requireNonNullElse(defaultShardPreferences, this.defaultShardPreferences);
    final String shardsPreferenceSpec =
        requestParams.get(ShardParams.SHARDS_PREFERENCE, defaultShardPreferences);

    if (!shardsPreferenceSpec.isEmpty()) {
      List<PreferenceRule> preferenceRules = PreferenceRule.from(shardsPreferenceSpec);
      NodePreferenceRulesComparator replicaComp =
          new NodePreferenceRulesComparator(
              preferenceRules,
              requestParams,
              nodeName != null ? nodeName : this.nodeName, // could be still null
              baseUrl != null ? baseUrl : this.baseUrl, // could still be null
              hostName != null ? hostName : this.hostName, // could still be null
              sysProps != null ? sysProps : this.sysProps, // could still be null
              defaultRltFactory,
              stableRltFactory);
      ReplicaListTransformer baseReplicaListTransformer =
          replicaComp.getBaseReplicaListTransformer();
      if (replicaComp.getPreferenceRules() == null || replicaComp.getPreferenceRules().isEmpty()) {
        // only applying base transformation
        return baseReplicaListTransformer;
      } else {
        return new TopLevelReplicaListTransformer(replicaComp, baseReplicaListTransformer);
      }
    }

    return defaultRltFactory.getInstance(null, requestParams, RANDOM_RLTF);
  }

  /**
   * Private class responsible for applying pairwise sort based on inherent replica attributes, and
   * subsequently reordering any equivalent replica sets according to behavior specified by the
   * baseReplicaListTransformer.
   */
  private static final class TopLevelReplicaListTransformer implements ReplicaListTransformer {

    private final NodePreferenceRulesComparator replicaComp;
    private final ReplicaListTransformer baseReplicaListTransformer;

    public TopLevelReplicaListTransformer(
        NodePreferenceRulesComparator replicaComp,
        ReplicaListTransformer baseReplicaListTransformer) {
      this.replicaComp = replicaComp;
      this.baseReplicaListTransformer = baseReplicaListTransformer;
    }

    @Override
    public <T> void transform(List<T> choices) {
      if (choices.size() > 1) {
        if (log.isDebugEnabled()) {
          log.debug(
              "Applying the following sorting preferences to replicas: {}",
              replicaComp.getPreferenceRules().stream()
                  .map(PreferenceRule::toString)
                  .collect(Collectors.joining(",", "[", "]")));
        }

        Comparator<T> comparator;
        try {
          comparator = replicaComp.getComparator(choices.get(0));
        } catch (IllegalArgumentException iae) {
          throw new SolrException(ErrorCode.BAD_REQUEST, iae.getMessage());
        }
        if (comparator == null) {
          // A null comparator means that the choices cannot be sorted by the given rules.
          // Just sort by the base transformer and return.
          baseReplicaListTransformer.transform(choices);
          return;
        }
        // First, sort according to comparator rules.
        try {
          choices.sort(comparator);
        } catch (IllegalArgumentException iae) {
          throw new SolrException(ErrorCode.BAD_REQUEST, iae.getMessage());
        }

        // Next determine all boundaries between replicas ranked as "equivalent" by the comparator
        Iterator<T> iter = choices.iterator();
        T prev = iter.next();
        T current;
        int idx = 1;
        int boundaryCount = 0;
        int[] boundaries = new int[choices.size()];
        do {
          current = iter.next();
          if (comparator.compare(prev, current) != 0) {
            boundaries[boundaryCount++] = idx;
          }
          prev = current;
          idx++;
        } while (iter.hasNext());
        boundaries[boundaryCount++] = idx;

        // Finally, inspect boundaries to apply base transformation, where necessary (separate phase
        // to avoid ConcurrentModificationException)
        int startIdx = 0;
        int endIdx;
        for (int i = 0; i < boundaryCount; i++) {
          endIdx = boundaries[i];
          if (endIdx - startIdx > 1) {
            baseReplicaListTransformer.transform(choices.subList(startIdx, endIdx));
          }
          startIdx = endIdx;
        }

        if (log.isDebugEnabled()) {
          log.debug(
              "Applied sorting preferences to replica list: {}",
              choices.stream().map(T::toString).collect(Collectors.joining(",", "[", "]")));
        }
      }
    }
  }
}
