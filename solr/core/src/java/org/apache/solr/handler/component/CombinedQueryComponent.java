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
package org.apache.solr.handler.component;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.CombinerParams;
import org.apache.solr.common.params.CursorMarkParams;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.core.SolrCore;
import org.apache.solr.response.BasicResultContext;
import org.apache.solr.response.ResultContext;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.DocListAndSet;
import org.apache.solr.search.QueryResult;
import org.apache.solr.search.SolrReturnFields;
import org.apache.solr.search.SortSpec;
import org.apache.solr.search.combine.QueryAndResponseCombiner;
import org.apache.solr.search.combine.ReciprocalRankFusion;
import org.apache.solr.util.SolrResponseUtil;
import org.apache.solr.util.plugin.SolrCoreAware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The CombinedQueryComponent class extends QueryComponent and provides support for executing
 * multiple queries and combining their results.
 */
public class CombinedQueryComponent extends QueryComponent implements SolrCoreAware {

  public static final String COMPONENT_NAME = "combined_query";
  protected NamedList<?> initParams;
  private Map<String, QueryAndResponseCombiner> combiners = new ConcurrentHashMap<>();
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @Override
  public void init(NamedList<?> args) {
    super.init(args);
    this.initParams = args;
  }

  @Override
  public void inform(SolrCore core) {
    if (initParams != null && initParams.size() > 0) {
      log.info("Initializing CombinedQueryComponent");
      NamedList<?> all = (NamedList<?>) initParams.get("combiners");
      for (int i = 0; i < all.size(); i++) {
        String name = all.getName(i);
        NamedList<?> combinerConfig = (NamedList<?>) all.getVal(i);
        String className = (String) combinerConfig.get("class");
        QueryAndResponseCombiner combiner =
            core.getResourceLoader().newInstance(className, QueryAndResponseCombiner.class);
        combiner.init(combinerConfig);
        combiners.computeIfAbsent(name, combinerName -> combiner);
      }
    }
    combiners.computeIfAbsent(
        CombinerParams.RECIPROCAL_RANK_FUSION,
        key -> {
          ReciprocalRankFusion reciprocalRankFusion = new ReciprocalRankFusion();
          reciprocalRankFusion.init(initParams);
          return reciprocalRankFusion;
        });
  }

  /**
   * Overrides the prepare method to handle combined queries.
   *
   * @param rb the ResponseBuilder to prepare
   * @throws IOException if an I/O error occurs during preparation
   */
  @Override
  public void prepare(ResponseBuilder rb) throws IOException {
    if (rb instanceof CombinedQueryResponseBuilder crb) {
      SolrParams params = crb.req.getParams();
      String[] queriesToCombineKeys = params.getParams(CombinerParams.COMBINER_QUERY);
      for (String queryKey : queriesToCombineKeys) {
        final var unparsedQuery = params.get(queryKey);
        ResponseBuilder rbNew = new ResponseBuilder(rb.req, new SolrQueryResponse(), rb.components);
        rbNew.setQueryString(unparsedQuery);
        super.prepare(rbNew);
        crb.responseBuilders.add(rbNew);
      }
    }
    super.prepare(rb);
  }

  /**
   * Overrides the process method to handle CombinedQueryResponseBuilder instances. This method
   * processes the responses from multiple shards, combines them using the specified
   * QueryAndResponseCombiner strategy, and sets the appropriate results and metadata in the
   * CombinedQueryResponseBuilder.
   *
   * @param rb the ResponseBuilder object to process
   * @throws IOException if an I/O error occurs during processing
   */
  @Override
  public void process(ResponseBuilder rb) throws IOException {
    if (rb instanceof CombinedQueryResponseBuilder crb) {
      boolean partialResults = false;
      boolean segmentTerminatedEarly = false;
      List<QueryResult> queryResults = new ArrayList<>();
      for (ResponseBuilder rbNow : crb.responseBuilders) {
        super.process(rbNow);
        DocListAndSet docListAndSet = rbNow.getResults();
        QueryResult queryResult = new QueryResult();
        queryResult.setDocListAndSet(docListAndSet);
        queryResults.add(queryResult);
        partialResults |= SolrQueryResponse.isPartialResults(rbNow.rsp.getResponseHeader());
        rbNow.setCursorMark(rbNow.getCursorMark());
        if (rbNow.rsp.getResponseHeader() != null) {
          segmentTerminatedEarly |=
              (boolean)
                  rbNow
                      .rsp
                      .getResponseHeader()
                      .getOrDefault(
                          SolrQueryResponse.RESPONSE_HEADER_SEGMENT_TERMINATED_EARLY_KEY, false);
        }
      }
      QueryAndResponseCombiner combinerStrategy =
          QueryAndResponseCombiner.getImplementation(rb.req.getParams(), combiners);
      QueryResult combinedQueryResult = combinerStrategy.combine(queryResults, rb.req.getParams());
      combinedQueryResult.setPartialResults(partialResults);
      combinedQueryResult.setSegmentTerminatedEarly(segmentTerminatedEarly);
      crb.setResult(combinedQueryResult);
      ResultContext ctx = new BasicResultContext(crb);
      crb.rsp.addResponse(ctx);
      crb.rsp
          .getToLog()
          .add(
              "hits",
              crb.getResults() == null || crb.getResults().docList == null
                  ? 0
                  : crb.getResults().docList.matches());
      if (!crb.req.getParams().getBool(ShardParams.IS_SHARD, false)) {
        if (null != crb.getNextCursorMark()) {
          crb.rsp.add(
              CursorMarkParams.CURSOR_MARK_NEXT, crb.getNextCursorMark().getSerializedTotem());
        }
      }

      if (crb.mergeFieldHandler != null) {
        crb.mergeFieldHandler.handleMergeFields(crb, crb.req.getSearcher());
      } else {
        doFieldSortValues(rb, crb.req.getSearcher());
      }
      doPrefetch(crb);
    } else {
      super.process(rb);
    }
  }

  @Override
  protected void mergeIds(ResponseBuilder rb, ShardRequest sreq) {
    List<MergeStrategy> mergeStrategies = rb.getMergeStrategies();
    if (mergeStrategies != null) {
      mergeStrategies.sort(MergeStrategy.MERGE_COMP);
      boolean idsMerged = false;
      for (MergeStrategy mergeStrategy : mergeStrategies) {
        mergeStrategy.merge(rb, sreq);
        if (mergeStrategy.mergesIds()) {
          idsMerged = true;
        }
      }

      if (idsMerged) {
        return; // ids were merged above so return.
      }
    }

    SortSpec ss = rb.getSortSpec();

    // If the shard request was also used to get fields (along with the scores), there is no reason
    // to copy over the score dependent fields, since those will already exist in the document with
    // the return fields
    Set<String> scoreDependentFields;
    if ((sreq.purpose & ShardRequest.PURPOSE_GET_FIELDS) == 0) {
      scoreDependentFields =
          rb.rsp.getReturnFields().getScoreDependentReturnFields().keySet().stream()
              .filter(field -> !field.equals(SolrReturnFields.SCORE))
              .collect(Collectors.toSet());
    } else {
      scoreDependentFields = Collections.emptySet();
    }

    IndexSchema schema = rb.req.getSchema();
    SchemaField uniqueKeyField = schema.getUniqueKeyField();

    // id to shard mapping, to eliminate any accidental dups
    HashMap<Object, String> uniqueDoc = new HashMap<>();

    NamedList<Object> shardInfo = null;
    if (rb.req.getParams().getBool(ShardParams.SHARDS_INFO, false)) {
      shardInfo = new SimpleOrderedMap<>();
      rb.rsp.getValues().add(ShardParams.SHARDS_INFO, shardInfo);
    }

    long numFound = 0;
    boolean hitCountIsExact = true;
    Float maxScore = null;
    boolean thereArePartialResults = false;
    Boolean segmentTerminatedEarly = null;
    boolean maxHitsTerminatedEarly = false;
    long approximateTotalHits = 0;
    int failedShardCount = 0;
    Map<String, List<ShardDoc>> shardDocMap = new HashMap<>();
    for (ShardResponse srsp : sreq.responses) {
      SolrDocumentList docs = null;
      NamedList<?> responseHeader = null;

      if (shardInfo != null) {
        SimpleOrderedMap<Object> nl = new SimpleOrderedMap<>();

        if (srsp.getException() != null) {
          Throwable t = srsp.getException();
          if (t instanceof SolrServerException && t.getCause() != null) {
            t = t.getCause();
          }
          nl.add("error", t.toString());
          if (!rb.req.getCore().getCoreContainer().hideStackTrace()) {
            StringWriter trace = new StringWriter();
            t.printStackTrace(new PrintWriter(trace));
            nl.add("trace", trace.toString());
          }
          if (!StrUtils.isNullOrEmpty(srsp.getShardAddress())) {
            nl.add("shardAddress", srsp.getShardAddress());
          }
        } else {
          responseHeader =
              (NamedList<?>)
                  SolrResponseUtil.getSubsectionFromShardResponse(
                      rb, srsp, "responseHeader", false);
          if (responseHeader == null) {
            continue;
          }
          final Object rhste =
              responseHeader.get(SolrQueryResponse.RESPONSE_HEADER_SEGMENT_TERMINATED_EARLY_KEY);
          if (rhste != null) {
            nl.add(SolrQueryResponse.RESPONSE_HEADER_SEGMENT_TERMINATED_EARLY_KEY, rhste);
          }
          final Object rhmhte =
              responseHeader.get(SolrQueryResponse.RESPONSE_HEADER_MAX_HITS_TERMINATED_EARLY_KEY);
          if (rhmhte != null) {
            nl.add(SolrQueryResponse.RESPONSE_HEADER_MAX_HITS_TERMINATED_EARLY_KEY, rhmhte);
          }
          final Object rhath =
              responseHeader.get(SolrQueryResponse.RESPONSE_HEADER_APPROXIMATE_TOTAL_HITS_KEY);
          if (rhath != null) {
            nl.add(SolrQueryResponse.RESPONSE_HEADER_APPROXIMATE_TOTAL_HITS_KEY, rhath);
          }
          docs =
              (SolrDocumentList)
                  SolrResponseUtil.getSubsectionFromShardResponse(rb, srsp, "response", false);
          if (docs == null) {
            continue;
          }
          nl.add("numFound", docs.getNumFound());
          nl.add("numFoundExact", docs.getNumFoundExact());
          nl.add("maxScore", docs.getMaxScore());
          nl.add("shardAddress", srsp.getShardAddress());
        }
        if (srsp.getSolrResponse() != null) {
          nl.add("time", srsp.getSolrResponse().getElapsedTime());
        }
        // This ought to be better, but at least this ensures no duplicate keys in JSON result
        String shard = srsp.getShard();
        if (StrUtils.isNullOrEmpty(shard)) {
          failedShardCount += 1;
          shard = "unknown_shard_" + failedShardCount;
        }
        shardInfo.add(shard, nl);
      }
      // now that we've added the shard info, let's only proceed if we have no error.
      if (srsp.getException() != null) {
        thereArePartialResults = true;
        continue;
      }

      if (docs == null) { // could have been initialized in the shards info block above
        docs =
            Objects.requireNonNull(
                (SolrDocumentList)
                    SolrResponseUtil.getSubsectionFromShardResponse(rb, srsp, "response", false));
      }

      if (responseHeader == null) { // could have been initialized in the shards info block above
        responseHeader =
            Objects.requireNonNull(
                (NamedList<?>)
                    SolrResponseUtil.getSubsectionFromShardResponse(
                        rb, srsp, "responseHeader", false));
      }

      final boolean thisResponseIsPartial;
      thisResponseIsPartial =
          Boolean.TRUE.equals(
              responseHeader.getBooleanArg(SolrQueryResponse.RESPONSE_HEADER_PARTIAL_RESULTS_KEY));
      thereArePartialResults |= thisResponseIsPartial;

      if (!Boolean.TRUE.equals(segmentTerminatedEarly)) {
        final Object ste =
            responseHeader.get(SolrQueryResponse.RESPONSE_HEADER_SEGMENT_TERMINATED_EARLY_KEY);
        if (Boolean.TRUE.equals(ste)) {
          segmentTerminatedEarly = Boolean.TRUE;
        } else if (Boolean.FALSE.equals(ste)) {
          segmentTerminatedEarly = Boolean.FALSE;
        }
      }

      if (!maxHitsTerminatedEarly) {
        if (Boolean.TRUE.equals(
            responseHeader.get(SolrQueryResponse.RESPONSE_HEADER_MAX_HITS_TERMINATED_EARLY_KEY))) {
          maxHitsTerminatedEarly = true;
        }
      }
      Object ath = responseHeader.get(SolrQueryResponse.RESPONSE_HEADER_APPROXIMATE_TOTAL_HITS_KEY);
      if (ath == null) {
        approximateTotalHits += numFound;
      } else {
        approximateTotalHits += ((Number) ath).longValue();
      }

      // calculate global maxScore and numDocsFound
      if (docs.getMaxScore() != null) {
        maxScore = maxScore == null ? docs.getMaxScore() : Math.max(maxScore, docs.getMaxScore());
      }
      numFound += docs.getNumFound();

      if (hitCountIsExact && Boolean.FALSE.equals(docs.getNumFoundExact())) {
        hitCountIsExact = false;
      }

      @SuppressWarnings("unchecked")
      NamedList<List<Object>> sortFieldValues =
          (NamedList<List<Object>>)
              SolrResponseUtil.getSubsectionFromShardResponse(rb, srsp, "sort_values", true);
      if (null == sortFieldValues) {
        sortFieldValues = new NamedList<>();
      }

      // if the SortSpec contains a field besides score or the Lucene docid, then the values will
      // need to be unmarshalled from sortFieldValues.
      boolean needsUnmarshalling = ss.includesNonScoreOrDocField();

      // if we need to unmarshal the sortFieldValues for sorting but we have none, which can happen
      // if partial results are being returned from the shard, then skip merging the results for the
      // shard. This avoids an exception below. if the shard returned partial results but we don't
      // need to unmarshal (a normal scoring query), then merge what we got.
      if (thisResponseIsPartial && sortFieldValues.size() == 0 && needsUnmarshalling) {
        continue;
      }

      // Checking needsUnmarshalling saves on iterating the SortFields in the SortSpec again.
      NamedList<List<Object>> unmarshalledSortFieldValues =
          needsUnmarshalling ? unmarshalSortValues(ss, sortFieldValues, schema) : new NamedList<>();

      // go through every doc in this response, construct a ShardDoc, and
      // put it in the priority queue so it can be ordered.
      for (int i = 0; i < docs.size(); i++) {
        SolrDocument doc = docs.get(i);
        Object id = doc.getFieldValue(uniqueKeyField.getName());
        ShardDoc shardDoc = new ShardDoc();
        shardDoc.id = id;
        shardDoc.shard = srsp.getShard();
        shardDoc.orderInShard = i;
        Object scoreObj = doc.getFieldValue(SolrReturnFields.SCORE);
        if (scoreObj != null) {
          if (scoreObj instanceof String) {
            shardDoc.score = Float.parseFloat((String) scoreObj);
          } else {
            shardDoc.score = ((Number) scoreObj).floatValue();
          }
        }
        if (!scoreDependentFields.isEmpty()) {
          shardDoc.scoreDependentFields = doc.getSubsetOfFields(scoreDependentFields);
        }

        shardDoc.sortFieldValues = unmarshalledSortFieldValues;
        shardDocMap.computeIfAbsent(srsp.getShard(), list -> new ArrayList<>()).add(shardDoc);
        String prevShard = uniqueDoc.put(id, srsp.getShard());
        if (prevShard != null) {
          // duplicate detected
          numFound--;
        }
      } // end for-each-doc-in-response
    } // end for-each-response

    SolrDocumentList responseDocs = new SolrDocumentList();
    if (maxScore != null) responseDocs.setMaxScore(maxScore);
    rb.rsp.addToLog("hits", numFound);

    responseDocs.setNumFound(numFound);
    responseDocs.setNumFoundExact(hitCountIsExact);
    responseDocs.setStart(ss.getOffset());

    // save these results in a private area so we can access them
    // again when retrieving stored fields.
    // TODO: use ResponseBuilder (w/ comments) or the request context?
    rb.resultIds = createShardResult(rb, shardDocMap, responseDocs);
    rb.setResponseDocs(responseDocs);

    populateNextCursorMarkFromMergedShards(rb);

    if (thereArePartialResults) {
      rb.rsp
          .getResponseHeader()
          .asShallowMap()
          .put(SolrQueryResponse.RESPONSE_HEADER_PARTIAL_RESULTS_KEY, Boolean.TRUE);
    }
    if (segmentTerminatedEarly != null) {
      final Object existingSegmentTerminatedEarly =
          rb.rsp
              .getResponseHeader()
              .get(SolrQueryResponse.RESPONSE_HEADER_SEGMENT_TERMINATED_EARLY_KEY);
      if (existingSegmentTerminatedEarly == null) {
        rb.rsp
            .getResponseHeader()
            .add(
                SolrQueryResponse.RESPONSE_HEADER_SEGMENT_TERMINATED_EARLY_KEY,
                segmentTerminatedEarly);
      } else if (!Boolean.TRUE.equals(existingSegmentTerminatedEarly)
          && Boolean.TRUE.equals(segmentTerminatedEarly)) {
        rb.rsp
            .getResponseHeader()
            .remove(SolrQueryResponse.RESPONSE_HEADER_SEGMENT_TERMINATED_EARLY_KEY);
        rb.rsp
            .getResponseHeader()
            .add(
                SolrQueryResponse.RESPONSE_HEADER_SEGMENT_TERMINATED_EARLY_KEY,
                segmentTerminatedEarly);
      }
    }
    if (maxHitsTerminatedEarly) {
      rb.rsp
          .getResponseHeader()
          .add(SolrQueryResponse.RESPONSE_HEADER_MAX_HITS_TERMINATED_EARLY_KEY, Boolean.TRUE);
      if (approximateTotalHits > 0) {
        rb.rsp
            .getResponseHeader()
            .add(
                SolrQueryResponse.RESPONSE_HEADER_APPROXIMATE_TOTAL_HITS_KEY, approximateTotalHits);
      }
    }
  }

  /**
   * Combines and sorts documents from multiple shards to create the final result set. This method
   * uses a combiner strategy to merge shard responses, then sorts the resulting documents using a
   * priority queue based on the request's sort specification. It handles pagination (offset and
   * count) and calculates the maximum score for the response.
   *
   * @param rb The ResponseBuilder containing the request and context, such as sort specifications.
   * @param shardDocMap A map from shard addresses to the list of documents returned by each shard.
   * @param responseDocs The final response document list, which will be populated with null
   *     placeholders and have its max score set.
   * @return A map from document IDs to the corresponding ShardDoc objects for the documents in the
   *     final sorted page of results.
   */
  protected Map<Object, ShardDoc> createShardResult(
      ResponseBuilder rb, Map<String, List<ShardDoc>> shardDocMap, SolrDocumentList responseDocs) {
    QueryAndResponseCombiner combinerStrategy =
        QueryAndResponseCombiner.getImplementation(rb.req.getParams(), combiners);
    List<ShardDoc> combinedShardDocs = combinerStrategy.combine(shardDocMap, rb.req.getParams());
    Map<String, ShardDoc> shardDocIdMap = new HashMap<>();
    shardDocMap.forEach(
        (shardKey, shardDocs) ->
            shardDocs.forEach(shardDoc -> shardDocIdMap.put(shardDoc.id.toString(), shardDoc)));
    Map<Object, ShardDoc> resultIds = new HashMap<>();
    float maxScore = 0.0f;
    Sort sort = rb.getSortSpec().getSort();
    SortField[] sortFields;
    if (sort != null) {
      sortFields = sort.getSort();
    } else {
      sortFields = new SortField[] {SortField.FIELD_SCORE};
    }
    final ShardFieldSortedHitQueue queue =
        new ShardFieldSortedHitQueue(
            sortFields,
            rb.getSortSpec().getOffset() + rb.getSortSpec().getCount(),
            rb.req.getSearcher());
    combinedShardDocs.forEach(queue::insertWithOverflow);
    int resultSize = queue.size() - rb.getSortSpec().getOffset();
    resultSize = Math.max(0, resultSize);
    for (int i = resultSize - 1; i >= 0; i--) {
      ShardDoc shardDoc = queue.pop();
      shardDoc.positionInResponse = i;
      maxScore = Math.max(maxScore, shardDoc.score);
      if (Float.isNaN(shardDocIdMap.get(shardDoc.id.toString()).score)) {
        shardDoc.score = Float.NaN;
      }
      resultIds.put(shardDoc.id.toString(), shardDoc);
    }
    responseDocs.setMaxScore(maxScore);
    for (int i = 0; i < resultSize; i++) responseDocs.add(null);
    return resultIds;
  }

  @Override
  public String getDescription() {
    return "Combined Query Component to support multiple query execution";
  }
}
