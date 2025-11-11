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

import static java.lang.Math.max;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CombinerParams;
import org.apache.solr.common.params.CursorMarkParams;
import org.apache.solr.common.params.GroupParams;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.component.combine.QueryAndResponseCombiner;
import org.apache.solr.handler.component.combine.ReciprocalRankFusion;
import org.apache.solr.response.BasicResultContext;
import org.apache.solr.response.ResultContext;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.DocListAndSet;
import org.apache.solr.search.QueryResult;
import org.apache.solr.search.SolrReturnFields;
import org.apache.solr.search.SortSpec;
import org.apache.solr.util.SolrResponseUtil;
import org.apache.solr.util.plugin.SolrCoreAware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The CombinedQueryComponent class extends QueryComponent and provides support for executing
 * multiple queries and combining their results.
 */
public class CombinedQueryComponent extends QueryComponent implements SolrCoreAware {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  public static final String COMPONENT_NAME = "combined_query";
  protected NamedList<?> initParams;
  private final Map<String, QueryAndResponseCombiner> combiners = new HashMap<>();
  private int maxCombinerQueries;
  private static final String RESPONSE_PER_QUERY_KEY = "response_per_query";

  @Override
  public void init(NamedList<?> args) {
    super.init(args);
    this.initParams = args;
    this.maxCombinerQueries = CombinerParams.DEFAULT_MAX_COMBINER_QUERIES;
  }

  @Override
  public void inform(SolrCore core) {
    for (Map.Entry<String, ?> initEntry : initParams) {
      if ("combiners".equals(initEntry.getKey())
          && initEntry.getValue() instanceof NamedList<?> all) {
        for (int i = 0; i < all.size(); i++) {
          String name = all.getName(i);
          NamedList<?> combinerConfig = (NamedList<?>) all.getVal(i);
          String className = (String) combinerConfig.get("class");
          QueryAndResponseCombiner combiner =
              core.getResourceLoader().newInstance(className, QueryAndResponseCombiner.class);
          combiner.init(combinerConfig);
          combiners.compute(
              name,
              (k, existingCombiner) -> {
                if (existingCombiner == null) {
                  return combiner;
                }
                throw new SolrException(
                    SolrException.ErrorCode.BAD_REQUEST,
                    "Found more than one combiner with same name");
              });
        }
      }
    }
    Object maxQueries = initParams.get("maxCombinerQueries");
    if (maxQueries != null) {
      this.maxCombinerQueries = Integer.parseInt(maxQueries.toString());
    }
    combiners.computeIfAbsent(
        CombinerParams.RECIPROCAL_RANK_FUSION,
        key -> {
          ReciprocalRankFusion reciprocalRankFusion = new ReciprocalRankFusion();
          reciprocalRankFusion.init(initParams);
          return reciprocalRankFusion;
        });
  }

  @Override
  protected boolean isForceDistributed() {
    return true;
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
      if (params.get(CursorMarkParams.CURSOR_MARK_PARAM) != null
          || params.getBool(GroupParams.GROUP, false)) {
        throw new SolrException(
            SolrException.ErrorCode.BAD_REQUEST, "Unsupported functionality for Combined Queries.");
      }
      String[] queriesToCombineKeys = params.getParams(CombinerParams.COMBINER_QUERY);
      if (queriesToCombineKeys.length > maxCombinerQueries) {
        throw new SolrException(
            SolrException.ErrorCode.BAD_REQUEST,
            "Too many queries to combine: limit is " + maxCombinerQueries);
      }
      for (String queryKey : queriesToCombineKeys) {
        final var unparsedQuery = params.get(queryKey);
        ResponseBuilder rbNew = new ResponseBuilder(rb.req, new SolrQueryResponse(), rb.components);
        rbNew.setQueryString(unparsedQuery);
        super.prepare(rbNew);
        crb.setFilters(rbNew.getFilters());
        crb.responseBuilders.add(rbNew);
      }
    }
    super.prepare(rb);
  }

  /**
   * Overrides the process method to handle CombinedQueryResponseBuilder instances. This method
   * processes the responses from multiple queries, combines them using the specified
   * QueryAndResponseCombiner strategy, and sets the appropriate results and metadata in the
   * CombinedQueryResponseBuilder.
   *
   * @param rb the ResponseBuilder object to process
   * @throws IOException if an I/O error occurs during processing
   */
  @Override
  @SuppressWarnings("unchecked")
  public void process(ResponseBuilder rb) throws IOException {
    if (rb instanceof CombinedQueryResponseBuilder crb) {
      boolean partialResults = false;
      boolean segmentTerminatedEarly = false;
      Boolean setMaxHitsTerminatedEarly = null;
      List<QueryResult> queryResults = new ArrayList<>();
      int rbIndex = 0;
      boolean shouldReturn = false;
      // TODO: to be parallelized
      for (ResponseBuilder thisRb : crb.responseBuilders) {
        // Just a placeholder for future implementation for Cursors
        thisRb.setCursorMark(crb.getCursorMark());
        super.process(thisRb);
        int purpose =
            thisRb
                .req
                .getParams()
                .getInt(ShardParams.SHARDS_PURPOSE, ShardRequest.PURPOSE_GET_TOP_IDS);
        if ((purpose & ShardRequest.PURPOSE_GET_TERM_STATS) != 0) {
          shouldReturn = true;
          continue;
        }
        DocListAndSet docListAndSet = thisRb.getResults();
        QueryResult queryResult = new QueryResult();
        queryResult.setDocListAndSet(docListAndSet);
        queryResults.add(queryResult);
        partialResults |= queryResult.isPartialResults();
        if (queryResult.getSegmentTerminatedEarly() != null) {
          segmentTerminatedEarly |= queryResult.getSegmentTerminatedEarly();
        }
        if (queryResult.getMaxHitsTerminatedEarly() != null) {
          if (setMaxHitsTerminatedEarly == null) {
            setMaxHitsTerminatedEarly = queryResult.getMaxHitsTerminatedEarly();
          }
          setMaxHitsTerminatedEarly |= queryResult.getMaxHitsTerminatedEarly();
        }
        doFieldSortValues(thisRb, crb.req.getSearcher());
        NamedList<Object[]> sortValues =
            (NamedList<Object[]>) thisRb.rsp.getValues().get("sort_values");
        crb.rsp.add(String.format(Locale.ROOT, "sort_values_%s", rbIndex), sortValues);
        ResultContext ctx = new BasicResultContext(thisRb);
        if (crb.rsp.getValues().get(RESPONSE_PER_QUERY_KEY) == null) {
          crb.rsp.add(RESPONSE_PER_QUERY_KEY, new ArrayList<>(List.of(ctx)));
        } else {
          ((List<ResultContext>) crb.rsp.getValues().get(RESPONSE_PER_QUERY_KEY)).add(ctx);
        }
        rbIndex++;
      }
      if (shouldReturn) {
        return;
      }
      prepareCombinedResponseBuilder(
          crb, queryResults, partialResults, segmentTerminatedEarly, setMaxHitsTerminatedEarly);
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

  private void prepareCombinedResponseBuilder(
      CombinedQueryResponseBuilder crb,
      List<QueryResult> queryResults,
      boolean partialResults,
      boolean segmentTerminatedEarly,
      Boolean setMaxHitsTerminatedEarly) {
    QueryResult combinedQueryResult = QueryAndResponseCombiner.simpleCombine(queryResults);
    combinedQueryResult.setPartialResults(partialResults);
    combinedQueryResult.setSegmentTerminatedEarly(segmentTerminatedEarly);
    combinedQueryResult.setMaxHitsTerminatedEarly(setMaxHitsTerminatedEarly);
    crb.setResult(combinedQueryResult);
    ResultContext ctx = new BasicResultContext(crb);
    crb.rsp.addResponse(ctx);
    crb.rsp.addToLog(
        "hits",
        crb.getResults() == null || crb.getResults().docList == null
            ? 0
            : crb.getResults().docList.matches());
    if (!crb.req.getParams().getBool(ShardParams.IS_SHARD, false)) {
      // for non-distributed request and future cursor improvement
      if (null != crb.getNextCursorMark()) {
        crb.rsp.add(
            CursorMarkParams.CURSOR_MARK_NEXT,
            crb.responseBuilders.getFirst().getNextCursorMark().getSerializedTotem());
      }
    }
  }

  @Override
  protected void mergeIds(ResponseBuilder rb, ShardRequest sreq) {
    SortSpec ss = rb.getSortSpec();
    Sort sort = ss.getSort();

    SortField[] sortFields;
    if (sort != null) sortFields = sort.getSort();
    else {
      sortFields = new SortField[] {SortField.FIELD_SCORE};
    }

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

    NamedList<Object> shardInfo = null;
    if (rb.req.getParams().getBool(ShardParams.SHARDS_INFO, false)) {
      shardInfo = new SimpleOrderedMap<>();
      rb.rsp.getValues().add(ShardParams.SHARDS_INFO, shardInfo);
    }

    long numFound = 0;
    boolean hitCountIsExact = true;
    boolean thereArePartialResults = false;
    Boolean segmentTerminatedEarly = null;
    boolean maxHitsTerminatedEarly = false;
    long approximateTotalHits = 0;
    Map<String, List<ShardDoc>> shardDocMap = new HashMap<>();
    String[] queriesToCombineKeys = rb.req.getParams().getParams(CombinerParams.COMBINER_QUERY);
    // TODO: to be parallelized outer loop
    for (int queryIndex = 0; queryIndex < queriesToCombineKeys.length; queryIndex++) {
      int failedShardCount = 0;
      long queryNumFound = 0;
      long queryApproximateTotalHits = 0;
      final ShardDocQueue queuePerQuery =
          newShardDocQueue(rb.req.getSearcher(), sortFields, ss.getOffset() + ss.getCount());
      for (ShardResponse srsp : sreq.responses) {
        SolrDocumentList docs = null;
        NamedList<?> responseHeader;

        if (SolrResponseUtil.getSubsectionFromShardResponse(rb, srsp, RESPONSE_PER_QUERY_KEY, false)
                instanceof List<?> docList
            && docList.get(queryIndex) instanceof SolrDocumentList solrDocumentList) {
          docs = Objects.requireNonNull(solrDocumentList);
          queryNumFound += docs.getNumFound();
          hitCountIsExact = hitCountIsExact && Boolean.FALSE.equals(docs.getNumFoundExact());
        }
        failedShardCount +=
            addShardInfo(
                shardInfo, failedShardCount, srsp, rb, queriesToCombineKeys[queryIndex], docs);
        if (srsp.getException() != null) {
          thereArePartialResults = true;
          continue;
        }

        responseHeader =
            Objects.requireNonNull(
                (NamedList<?>)
                    SolrResponseUtil.getSubsectionFromShardResponse(
                        rb, srsp, "responseHeader", false));

        final boolean thisResponseIsPartial;
        thisResponseIsPartial =
            Boolean.TRUE.equals(
                responseHeader.getBooleanArg(
                    SolrQueryResponse.RESPONSE_HEADER_PARTIAL_RESULTS_KEY));
        thereArePartialResults |= thisResponseIsPartial;

        if (!Boolean.TRUE.equals(segmentTerminatedEarly)) {
          final Object ste =
              responseHeader.get(SolrQueryResponse.RESPONSE_HEADER_SEGMENT_TERMINATED_EARLY_KEY);
          if (ste instanceof Boolean steFlag) {
            segmentTerminatedEarly = steFlag;
          }
        }

        if (!maxHitsTerminatedEarly
            && Boolean.TRUE.equals(
                responseHeader.get(
                    SolrQueryResponse.RESPONSE_HEADER_MAX_HITS_TERMINATED_EARLY_KEY))) {
          maxHitsTerminatedEarly = true;
        }

        Object ath =
            responseHeader.get(SolrQueryResponse.RESPONSE_HEADER_APPROXIMATE_TOTAL_HITS_KEY);
        if (ath == null) {
          queryApproximateTotalHits += queryNumFound;
        } else {
          queryApproximateTotalHits += ((Number) ath).longValue();
        }

        @SuppressWarnings("unchecked")
        NamedList<List<Object>> sortFieldValues =
            (NamedList<List<Object>>)
                SolrResponseUtil.getSubsectionFromShardResponse(
                    rb, srsp, String.format(Locale.ROOT, "sort_values_%s", queryIndex), true);
        if (null == sortFieldValues) {
          sortFieldValues = new NamedList<>();
        }

        boolean needsUnmarshalling = ss.includesNonScoreOrDocField();
        if (thisResponseIsPartial && sortFieldValues.size() == 0 && needsUnmarshalling) {
          continue;
        }
        NamedList<List<Object>> unmarshalledSortFieldValues =
            needsUnmarshalling
                ? unmarshalSortValues(ss, sortFieldValues, schema)
                : new NamedList<>();
        // go through every doc in this response, construct a ShardDoc, and
        // put it in the uniqueDoc to dedup
        for (int i = 0; i < docs.size(); i++) {
          SolrDocument doc = docs.get(i);
          Object id = doc.getFieldValue(uniqueKeyField.getName());
          ShardDoc shardDoc = new ShardDoc();
          shardDoc.id = id;
          shardDoc.orderInShard = i;
          shardDoc.shard = srsp.getShard();
          Object scoreObj = doc.getFieldValue(SolrReturnFields.SCORE);
          if (scoreObj != null) {
            if (scoreObj instanceof String scoreStr) {
              shardDoc.score = Float.parseFloat(scoreStr);
            } else {
              shardDoc.score = ((Number) scoreObj).floatValue();
            }
          }
          if (!scoreDependentFields.isEmpty()) {
            shardDoc.scoreDependentFields = doc.getSubsetOfFields(scoreDependentFields);
          }
          shardDoc.sortFieldValues = unmarshalledSortFieldValues;
          if (!queuePerQuery.push(shardDoc)) {
            numFound--;
          }
        } // end for-each-doc-in-response
      } // end for-each-response
      List<ShardDoc> shardDocsPerQuery = new ArrayList<>(queuePerQuery.resultIds(0).values());
      shardDocsPerQuery.sort(Comparator.comparingInt(a -> a.positionInResponse));
      shardDocMap.put(queriesToCombineKeys[queryIndex], shardDocsPerQuery);
      numFound = max(numFound, queryNumFound);
      approximateTotalHits = max(approximateTotalHits, queryApproximateTotalHits);
    } // for each query to combine

    rb.rsp.addToLog("hits", numFound);

    SolrDocumentList responseDocs = new SolrDocumentList();
    responseDocs.setNumFound(numFound);
    responseDocs.setNumFoundExact(hitCountIsExact);
    responseDocs.setStart(ss.getOffset());

    rb.resultIds = computeResultIdsWithCombiner(rb, shardDocMap, responseDocs, sortFields);
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
      } else if (!Boolean.TRUE.equals(existingSegmentTerminatedEarly) && segmentTerminatedEarly) {
        rb.rsp
            .getResponseHeader()
            .remove(SolrQueryResponse.RESPONSE_HEADER_SEGMENT_TERMINATED_EARLY_KEY);
        rb.rsp
            .getResponseHeader()
            .add(SolrQueryResponse.RESPONSE_HEADER_SEGMENT_TERMINATED_EARLY_KEY, true);
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
   * Populate shardInfo from mostly ShardResponse. Returns failedShardCount (may be increased from
   * the param).
   */
  private int addShardInfo(
      NamedList<Object> shardInfo,
      int failedShardCount,
      ShardResponse srsp,
      ResponseBuilder rb,
      String queryKey,
      SolrDocumentList docs) {
    if (shardInfo == null) {
      return failedShardCount;
    }
    SimpleOrderedMap<Object> nl = new SimpleOrderedMap<>();
    NamedList<?> responseHeader;
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
              SolrResponseUtil.getSubsectionFromShardResponse(rb, srsp, "responseHeader", false);
      if (responseHeader == null) {
        return failedShardCount;
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
      if (docs == null) {
        return failedShardCount;
      }
      nl.add("numFound", docs.getNumFound());
      nl.add("numFoundExact", docs.getNumFoundExact());
      nl.add("maxScore", docs.getMaxScore());
      nl.add("shardAddress", srsp.getShardAddress());
    }

    if (srsp.getSolrResponse() != null) {
      nl.add("time", srsp.getSolrResponse().getElapsedTime());
    }
    nl.add("queryKey", queryKey);

    // This ought to be better, but at least this ensures no duplicate keys in JSON result
    String shard = srsp.getShard() + "_" + queryKey;
    shardInfo.add(shard, nl);
    return failedShardCount;
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
   * @param sortFields An array of field for sorting to be applied.
   * @return A map from document IDs to the corresponding ShardDoc objects for the documents in the
   *     final sorted page of results.
   */
  protected Map<Object, ShardDoc> computeResultIdsWithCombiner(
      ResponseBuilder rb,
      Map<String, List<ShardDoc>> shardDocMap,
      SolrDocumentList responseDocs,
      SortField[] sortFields) {
    String algorithm =
        rb.req.getParams().get(CombinerParams.COMBINER_ALGORITHM, CombinerParams.DEFAULT_COMBINER);
    QueryAndResponseCombiner combinerStrategy =
        QueryAndResponseCombiner.getImplementation(algorithm, combiners);
    List<ShardDoc> combinedShardDocs = combinerStrategy.combine(shardDocMap, rb.req.getParams());

    // adding explanation for the ordered shard docs as debug info
    if (rb.isDebugResults()) {
      String[] queryKeys = rb.req.getParams().getParams(CombinerParams.COMBINER_QUERY);
      NamedList<Explanation> explanations =
          combinerStrategy.getExplanations(
              queryKeys, shardDocMap, combinedShardDocs, rb.req.getParams());
      rb.addDebugInfo("combinerExplanations", explanations);
    }
    Map<String, ShardDoc> shardDocIdMap = new HashMap<>();
    shardDocMap.forEach(
        (shardKey, shardDocs) ->
            shardDocs.forEach(shardDoc -> shardDocIdMap.put(shardDoc.id.toString(), shardDoc)));

    // creating a queue to sort basis on all the comparator and tie-break on docId
    Map<Object, ShardDoc> resultIds = new HashMap<>();
    float maxScore = 0.0f;
    final ShardFieldSortedHitQueue queue =
        new ShardFieldSortedHitQueue(
            sortFields,
            rb.getSortSpec().getOffset() + rb.getSortSpec().getCount(),
            rb.req.getSearcher()) {
          @Override
          protected boolean lessThan(ShardDoc docA, ShardDoc docB) {
            int c = 0;
            for (int i = 0; i < comparators.length && c == 0; i++) {
              c =
                  (fields[i].getReverse())
                      ? comparators[i].compare(docB, docA)
                      : comparators[i].compare(docA, docB);
            }

            if (c == 0) {
              c = docA.id.toString().compareTo(docB.id.toString());
            }
            return c < 0;
          }
        };
    combinedShardDocs.forEach(queue::insertWithOverflow);

    // get the resultSize as expected and fetch that shardDoc from the queue, putting it in a map
    int resultSize = max(0, queue.size() - rb.getSortSpec().getOffset());
    for (int i = resultSize - 1; i >= 0; i--) {
      ShardDoc shardDoc = queue.pop();
      shardDoc.positionInResponse = i;
      maxScore = max(maxScore, shardDoc.score);
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
