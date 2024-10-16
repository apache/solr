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

// import static org.apache.solr.cloud.Overseer.QUEUE_OPERATION;
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
// import static org.apache.solr.handler.admin.CollectionsHandler.DEFAULT_COLLECTION_OP_TIMEOUT;

import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import jakarta.inject.Inject;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
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
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.solr.client.api.endpoint.ClusterSizingApi;
import org.apache.solr.client.api.model.ClusterSizingRequestBody;
import org.apache.solr.client.api.model.SolrJerseyResponse;
import org.apache.solr.client.solrj.SolrRequest.METHOD;
// import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
// import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
// import org.apache.solr.common.params.CollectionParams.CollectionAction;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SizeParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.admin.MetricsHandler.MetricType;
import org.apache.solr.handler.admin.api.AdminAPIBase;
import org.apache.solr.handler.api.V2ApiUtils;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.util.NumberUtils;
import org.apache.solr.util.stats.MetricUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** This class is used to produce sizing estimates for a cluster. */
public class ClusterSizing extends AdminAPIBase implements ClusterSizingApi {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final ZkStateReader zkStateReader;
  private final SolrParams params;
  private final String sizeUnit; // size unit requested by the user

  @Inject
  public ClusterSizing(
      final CoreContainer coreContainer,
      final SolrQueryRequest solrQueryRequest,
      final SolrQueryResponse solrQueryResponse) {
    super(coreContainer, solrQueryRequest, solrQueryResponse);
    this.zkStateReader = coreContainer.getZkController().getZkStateReader();
    this.params = solrQueryRequest.getParams();
    this.sizeUnit = this.params.get(SizeParams.SIZE_UNIT);
  }

  public void populate(NamedList<Object> values) {
    final ClusterState state = zkStateReader.getClusterState();
    final Set<String> collections = new HashSet<>(state.getCollectionStates().keySet());
    final List<String> collectionList = getParamAsList(COLLECTION_PROP);
    if (!collectionList.isEmpty()) {
      collections.retainAll(collectionList);
    }
    final List<String> shardList = getParamAsList(SHARD_ID_PROP);
    final List<String> replicaList = getParamAsList(REPLICA_PROP);
    final SolrParams sizeParams = buildSizeParams();
    final NamedList<Object> sizePerCollection = new NamedList<>();
    final ConcurrentMap<String, Map<String, Long>> sizePerNode = new ConcurrentHashMap<>();

    for (String colName : collections) {
      final DocCollection collection = state.getCollection(colName);

      final NamedList<Object> sizePerShard = new NamedList<>();
      sizePerCollection.add(colName, sizePerShard);
      final Map<String, Slice> sliceMap = collection.getSlicesMap();
      for (Entry<String, Slice> sliceEntry : sliceMap.entrySet()) {
        if (shardList.isEmpty() || shardList.contains(sliceEntry.getKey())) {
          final Slice slice = sliceEntry.getValue();

          final NamedList<Object> sizePerReplica = new NamedList<>();
          sizePerShard.add(slice.getName(), sizePerReplica);
          final Map<String, Replica> replicaMap = slice.getReplicasMap();
          for (Entry<String, Replica> replicaEntry : replicaMap.entrySet()) {
            if (replicaList.isEmpty() || replicaList.contains(replicaEntry.getKey())) {
              final Replica replica = replicaEntry.getValue();
              final String url = replica.getCoreUrl();
              final String node = replica.getNodeName();
              final QueryRequest req = new QueryRequest(sizeParams);
              req.setMethod(METHOD.POST);
              if (log.isDebugEnabled()) {
                log.debug("Request size from '{}' with params: {}", url, sizeParams);
              }
              final Http2SolrClient.Builder builder = new Http2SolrClient.Builder(url);
              try (Http2SolrClient solrClient = builder.build()) {
                if (coreContainer.getPkiAuthenticationSecurityBuilder() != null) {
                  coreContainer.getPkiAuthenticationSecurityBuilder().setup(solrClient);
                }
                @SuppressWarnings("unchecked")
                final NamedList<Object> size =
                    (NamedList<Object>) solrClient.request(req).get(SIZE);
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
    final NamedList<Object> cluster = new NamedList<>();
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

  @SuppressWarnings("deprecation")
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

  public SolrJerseyResponse getMetricsEstimate(final ClusterSizingRequestBody request)
      throws Exception {
    final SolrJerseyResponse response = instantiateJerseyResponse(SolrJerseyResponse.class);
    validateZooKeeperAwareCoreContainer(coreContainer);

    final NamedList<Object> map = new SimpleOrderedMap<>();
    try (final MetricsHandler metricsHandler = new MetricsHandler(coreContainer)) {
      // TODO: Which registries ?
      final Set<String> registries = metricsHandler.parseRegistries(null, null);

      // TODO: Which metric types ? filters ?
      final List<MetricType> metricTypes = List.of(MetricsHandler.MetricType.all);
      final List<MetricFilter> metricFilters =
          metricTypes.stream().map(MetricType::asMetricFilter).collect(Collectors.toList());
      final MetricFilter mustMatchFilter = MetricFilter.ALL;
      final Predicate<CharSequence> propertyFilter = MetricUtils.ALL_PROPERTIES;

      for (final String registryName : registries) {
        final MetricRegistry registry = metricsHandler.metricManager.registry(registryName);
        final SimpleOrderedMap<Object> result = new SimpleOrderedMap<>();
        MetricUtils.toMaps(
            registry,
            metricFilters,
            mustMatchFilter,
            propertyFilter,
            false,
            false,
            false,
            false,
            (k, v) -> result.add(k, v));
        if (result.size() > 0) {
          map.add(registryName, result);
        }
      }
    } catch (IOException e) {
      throw new ClusterSizingException("Error closing a resource: Metrics Handler", e);
    }

    // TODO : Calculate estimates with request params:  estimationRatio, numDocs, etc

    solrQueryResponse.getValues().add("cluster", map);

    V2ApiUtils.squashIntoSolrResponseWithHeader(solrQueryResponse, response);

    return response;
  }

  @SuppressWarnings("serial")
  public class ClusterSizingException extends Exception {
    public ClusterSizingException(final String message, final Throwable cause) {
      super(message, cause);
    }
  }
}
