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

import static org.apache.solr.common.cloud.ZkStateReader.COLLECTION_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.REPLICA_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.SHARD_ID_PROP;
import static org.apache.solr.common.params.SizeParams.AVG_DOC_SIZE;
import static org.apache.solr.common.params.SizeParams.DELETED_DOCS;
import static org.apache.solr.common.params.SizeParams.DOC_CACHE_MAX;
import static org.apache.solr.common.params.SizeParams.ESTIMATION_RATIO;
import static org.apache.solr.common.params.SizeParams.FILTER_CACHE_MAX;
import static org.apache.solr.common.params.SizeParams.NUM_DOCS;
import static org.apache.solr.common.params.SizeParams.QUERY_RES_CACHE_MAX;
import static org.apache.solr.common.params.SizeParams.QUERY_RES_MAX_DOCS;
import static org.apache.solr.common.params.SizeParams.SIZE;

import java.io.IOException;
import java.text.ParseException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.solr.client.solrj.SolrRequest.METHOD;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SizeParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.util.NumberUtils;

/** This class is used to produce sizing estimates for a cluster. */
public class ClusterSizing {
  private final ZkStateReader zkStateReader;
  private final SolrParams params;
  private final String sizeUnit; // size unit requested by the user

  public ClusterSizing(ZkStateReader zkStateReader, SolrParams params) {
    this.zkStateReader = zkStateReader;
    this.params = params;
    this.sizeUnit = this.params.get(SizeParams.SIZE_UNIT);
  }

  public void populate(NamedList<Object> values) {
    ClusterState state = zkStateReader.getClusterState();
    Set<String> collections = new HashSet<>(state.getCollectionStates().keySet());
    List<String> collectionList = getParamAsList(COLLECTION_PROP);
    if (!collectionList.isEmpty()) {
      collections.retainAll(collectionList);
    }
    List<String> shardList = getParamAsList(SHARD_ID_PROP);
    List<String> replicaList = getParamAsList(REPLICA_PROP);
    SolrParams sizeParams = buildSizeParams();
    NamedList<Object> sizePerCollection = new NamedList<>();
    ConcurrentMap<String, Map<String, Long>> sizePerNode = new ConcurrentHashMap<>();

    for (String colName : collections) {
      DocCollection collection = state.getCollection(colName);

      NamedList<Object> sizePerShard = new NamedList<>();
      sizePerCollection.add(colName, sizePerShard);
      Map<String, Slice> sliceMap = collection.getSlicesMap();
      for (Entry<String, Slice> sliceEntry : sliceMap.entrySet()) {
        if (shardList.isEmpty() || shardList.contains(sliceEntry.getKey())) {
          Slice slice = sliceEntry.getValue();

          NamedList<Object> sizePerReplica = new NamedList<>();
          sizePerShard.add(slice.getName(), sizePerReplica);
          Map<String, Replica> replicaMap = slice.getReplicasMap();
          for (Entry<String, Replica> replicaEntry : replicaMap.entrySet()) {
            if (replicaList.isEmpty() || replicaList.contains(replicaEntry.getKey())) {
              Replica replica = replicaEntry.getValue();
              String url = replica.getCoreUrl();
              String node = replica.getNodeName();
              QueryRequest req = new QueryRequest(sizeParams);
              req.setMethod(METHOD.POST);
              HttpSolrClient.Builder httpBuilder = new HttpSolrClient.Builder(url);
              try (HttpSolrClient solrClient = httpBuilder.build()) {
                @SuppressWarnings("unchecked")
                NamedList<Object> size = (NamedList<Object>) solrClient.request(req).get(SIZE);
                sizePerReplica.add(replica.getName(), size);
                adjustNodeSize(sizePerNode, node, size);
              } catch (SolrServerException | IOException | ParseException e) {
                sizePerReplica.add(
                    replica.getName(), "Could not get size for replica! " + e.getMessage());
              }
            }
          }
        }
      }
    }
    NamedList<Object> cluster = new NamedList<>();
    cluster.add("nodes", nodesToNamedList(sizePerNode));
    cluster.add("collections", sizePerCollection);
    values.add("cluster", cluster);
  }

  private NamedList<Object> nodesToNamedList(ConcurrentMap<String, Map<String, Long>> sizePerNode) {
    NamedList<Object> nodes = new NamedList<>();
    for (Entry<String, Map<String, Long>> entry : sizePerNode.entrySet()) {
      NamedList<Object> details = new NamedList<>();
      for (Entry<String, Long> detailEntry : entry.getValue().entrySet()) {
        if (detailEntry.getKey().equals("estimated-num-docs")) {
          details.add(detailEntry.getKey(), detailEntry.getValue());
        } else {
          if (this.sizeUnit == null) {
            details.add(detailEntry.getKey(), NumberUtils.readableSize(detailEntry.getValue()));
          } else {
            details.add(
                detailEntry.getKey(),
                NumberUtils.normalizedSize(detailEntry.getValue(), this.sizeUnit));
          }
        }
      }
      nodes.add(entry.getKey(), details);
    }
    return nodes;
  }

  private void adjustNodeSize(
      ConcurrentMap<String, Map<String, Long>> sizePerNode, String node, NamedList<Object> size)
      throws ParseException {
    Map<String, Long> current =
        sizePerNode.putIfAbsent(node, new ConcurrentHashMap<String, Long>());
    if (current == null) current = sizePerNode.get(node);
    adjust(current, size, "total-disk-size");
    adjust(current, size, "total-lucene-RAM");
    adjust(current, size, "total-solr-RAM");
    adjustLong(current, size, "estimated-num-docs");
  }

  private void adjust(Map<String, Long> current, NamedList<Object> adjustments, String name)
      throws ParseException {
    Long currentSize = current.get(name);
    long adjusted = 0;
    if (this.sizeUnit == null) {
      String adjustment = (String) adjustments.get(name);
      adjusted =
          (currentSize == null ? 0 : currentSize)
              + (adjustment == null ? 0 : NumberUtils.sizeFromReadable(adjustment));
    } else {
      Double adjustment = (Double) adjustments.get(name);
      adjusted =
          (currentSize == null ? 0 : currentSize)
              + (adjustment == null
                  ? 0
                  : NumberUtils.sizeFromNormalized(adjustment, this.sizeUnit));
    }
    current.put(name, adjusted);
  }

  private void adjustLong(Map<String, Long> current, NamedList<Object> adjustments, String name)
      throws ParseException {
    Long currentSize = current.get(name);
    Long adjustment = (Long) adjustments.get(name);
    long adjusted = (currentSize == null ? 0 : currentSize) + (adjustment == null ? 0 : adjustment);
    current.put(name, adjusted);
  }

  private List<String> getParamAsList(String name) {
    String param = params.get(name);
    if (param != null) {
      return Arrays.asList(param.split(","));
    } else {
      return Collections.emptyList();
    }
  }

  private SolrParams buildSizeParams() {
    ModifiableSolrParams solrParams = new ModifiableSolrParams(params);
    solrParams.set(SIZE, true);
    solrParams.set(CommonParams.DISTRIB, false);
    return solrParams.toFilteredSolrParams(
        Arrays.asList(
            AVG_DOC_SIZE,
            NUM_DOCS,
            DELETED_DOCS,
            FILTER_CACHE_MAX,
            QUERY_RES_CACHE_MAX,
            DOC_CACHE_MAX,
            QUERY_RES_MAX_DOCS,
            ESTIMATION_RATIO,
            SIZE,
            SizeParams.SIZE_UNIT,
            CommonParams.DISTRIB));
  }
}
