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

package org.apache.solr.handler.admin;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import org.apache.solr.client.api.model.GetSegmentDataResponse;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.io.SolrClientCache;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.RoutingRule;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.common.util.Utils;
import org.apache.solr.jersey.SolrJacksonMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Report low-level details of collection. */
public class ColStatus {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final ClusterState clusterState;
  private final ZkNodeProps props;
  private final SolrClientCache solrClientCache;

  public static final String CORE_INFO_PROP = SegmentsInfoRequestHandler.CORE_INFO_PARAM;
  public static final String FIELD_INFO_PROP = SegmentsInfoRequestHandler.FIELD_INFO_PARAM;
  public static final String SIZE_INFO_PROP = SegmentsInfoRequestHandler.SIZE_INFO_PARAM;
  public static final String RAW_SIZE_PROP = SegmentsInfoRequestHandler.RAW_SIZE_PARAM;
  public static final String RAW_SIZE_SUMMARY_PROP =
      SegmentsInfoRequestHandler.RAW_SIZE_SUMMARY_PARAM;
  public static final String RAW_SIZE_DETAILS_PROP =
      SegmentsInfoRequestHandler.RAW_SIZE_DETAILS_PARAM;
  public static final String RAW_SIZE_SAMPLING_PERCENT_PROP =
      SegmentsInfoRequestHandler.RAW_SIZE_SAMPLING_PERCENT_PARAM;
  public static final String SEGMENTS_PROP = "segments";

  public ColStatus(SolrClientCache solrClientCache, ClusterState clusterState, ZkNodeProps props) {
    this.props = props;
    this.solrClientCache = solrClientCache;
    this.clusterState = clusterState;
  }

  @SuppressWarnings({"unchecked"})
  public void getColStatus(NamedList<Object> results) {
    Collection<String> collections;
    String col = props.getStr(ZkStateReader.COLLECTION_PROP);
    if (col == null) {
      collections = clusterState.getCollectionNames();
    } else {
      collections = Collections.singleton(col);
    }
    boolean withFieldInfo = props.getBool(FIELD_INFO_PROP, false);
    boolean withCoreInfo = props.getBool(CORE_INFO_PROP, false);
    boolean withSizeInfo = props.getBool(SIZE_INFO_PROP, false);
    boolean withRawSizeInfo = props.getBool(RAW_SIZE_PROP, false);
    boolean withRawSizeSummary = props.getBool(RAW_SIZE_SUMMARY_PROP, false);
    boolean withRawSizeDetails = props.getBool(RAW_SIZE_DETAILS_PROP, false);
    // FieldInfo and SizeInfo imply segments=true, since they add to the data reported about each
    // segment
    boolean withSegments = props.getBool(SEGMENTS_PROP, false);
    withSegments |= withFieldInfo || withSizeInfo;

    Object samplingPercentVal = props.get(RAW_SIZE_SAMPLING_PERCENT_PROP);
    Float samplingPercent =
        samplingPercentVal != null ? Float.parseFloat(String.valueOf(samplingPercentVal)) : null;
    if (withRawSizeSummary || withRawSizeDetails) {
      withRawSizeInfo = true;
    }
    boolean getSegments = false;
    if (withFieldInfo
        || withSegments
        || withSizeInfo
        || withCoreInfo
        || withRawSizeInfo
        || withRawSizeDetails
        || withRawSizeSummary) {
      getSegments = true;
    }
    for (String collection : collections) {
      DocCollection coll = clusterState.getCollectionOrNull(collection);
      if (coll == null) {
        continue;
      }
      SimpleOrderedMap<Object> colMap = new SimpleOrderedMap<>();
      colMap.add("znodeVersion", coll.getZNodeVersion());
      colMap.add("creationTimeMillis", coll.getCreationTime().toEpochMilli());
      Map<String, Object> props = new TreeMap<>(coll.getProperties());
      props.remove("shards");
      colMap.add("properties", props);
      colMap.add("activeShards", coll.getActiveSlices().size());
      colMap.add("inactiveShards", coll.getSlices().size() - coll.getActiveSlices().size());
      results.add(collection, colMap);

      Set<String> nonCompliant = new TreeSet<>();

      SimpleOrderedMap<Object> shards = new SimpleOrderedMap<>();
      for (Slice s : coll.getSlices()) {
        SimpleOrderedMap<Object> sliceMap = new SimpleOrderedMap<>();
        shards.add(s.getName(), sliceMap);
        SimpleOrderedMap<Object> replicaMap = new SimpleOrderedMap<>();
        int totalReplicas = s.getReplicas().size();
        int activeReplicas = 0;
        int downReplicas = 0;
        int recoveringReplicas = 0;
        int recoveryFailedReplicas = 0;
        for (Replica r : s.getReplicas()) {
          // replica may still be marked as ACTIVE even though its node is no longer live
          if (!r.isActive(clusterState.getLiveNodes())) {
            downReplicas++;
            continue;
          }
          switch (r.getState()) {
            case ACTIVE:
              activeReplicas++;
              break;
            case DOWN:
              downReplicas++;
              break;
            case RECOVERING:
              recoveringReplicas++;
              break;
            case RECOVERY_FAILED:
              recoveryFailedReplicas++;
              break;
          }
        }
        replicaMap.add("total", totalReplicas);
        replicaMap.add("active", activeReplicas);
        replicaMap.add("down", downReplicas);
        replicaMap.add("recovering", recoveringReplicas);
        replicaMap.add("recovery_failed", recoveryFailedReplicas);
        sliceMap.add("state", s.getState().toString());
        if (s.getRange() != null) {
          sliceMap.add("range", s.getRange().toString());
        }
        Map<String, RoutingRule> rules = s.getRoutingRules();
        if (rules != null && !rules.isEmpty()) {
          sliceMap.add("routingRules", rules);
        }
        sliceMap.add("replicas", replicaMap);
        Replica leader = s.getLeader();
        if (leader == null) { // pick the first one
          leader = s.getReplicas().size() > 0 ? s.getReplicas().iterator().next() : null;
        }
        if (leader == null) {
          continue;
        }
        SimpleOrderedMap<Object> leaderMap = new SimpleOrderedMap<>();
        sliceMap.add("leader", leaderMap);
        leaderMap.add("coreNode", leader.getName());
        leaderMap.addAll(leader.getProperties());
        if (!leader.isActive(clusterState.getLiveNodes())) {
          continue;
        }
        String url = ZkCoreNodeProps.getCoreUrl(leader);
        if (url == null) {
          continue;
        }
        if (getSegments) {
          try (SolrClient client = solrClientCache.getHttpSolrClient(url)) {
            ModifiableSolrParams params = new ModifiableSolrParams();
            params.add(CommonParams.QT, "/admin/segments");
            params.add(FIELD_INFO_PROP, "true");
            params.add(CORE_INFO_PROP, String.valueOf(withCoreInfo));
            params.add(SIZE_INFO_PROP, String.valueOf(withSizeInfo));
            params.add(RAW_SIZE_PROP, String.valueOf(withRawSizeInfo));
            params.add(RAW_SIZE_SUMMARY_PROP, String.valueOf(withRawSizeSummary));
            params.add(RAW_SIZE_DETAILS_PROP, String.valueOf(withRawSizeDetails));
            if (samplingPercent != null) {
              params.add(RAW_SIZE_SAMPLING_PERCENT_PROP, String.valueOf(samplingPercent));
            }
            QueryRequest req = new QueryRequest(params);
            NamedList<Object> rsp = client.request(req);
            final var segmentResponse =
                SolrJacksonMapper.getObjectMapper().convertValue(rsp, GetSegmentDataResponse.class);
            segmentResponse.responseHeader = null;

            final var segs = segmentResponse.segments;
            if (segs != null) {
              for (Map.Entry<String, GetSegmentDataResponse.SingleSegmentData> entry :
                  segs.entrySet()) {
                final var fieldInfoByName = entry.getValue().fields;
                if (fieldInfoByName != null) {
                  for (Map.Entry<String, GetSegmentDataResponse.SegmentSingleFieldInfo> fEntry :
                      fieldInfoByName.entrySet()) {
                    if (fEntry.getValue().nonCompliant != null) {
                      nonCompliant.add(fEntry.getKey());
                    }
                  }
                }
                if (!withFieldInfo) {
                  entry.getValue().fields = null;
                }
              }
            }
            if (!withSegments) {
              segmentResponse.segments = null;
            }
            if (!withFieldInfo) {
              segmentResponse.fieldInfoLegend = null;
            }
            leaderMap.add("segInfos", Utils.reflectToMap(segmentResponse));
          } catch (SolrServerException | IOException e) {
            log.warn("Error getting details of replica segments from {}", url, e);
          }
        }
      }
      if (nonCompliant.isEmpty()) {
        nonCompliant.add("(NONE)");
      }
      colMap.add("schemaNonCompliant", nonCompliant);
      colMap.add("shards", shards);
    }
  }
}
