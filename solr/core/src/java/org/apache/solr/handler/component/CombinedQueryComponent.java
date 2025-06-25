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
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;
import org.apache.solr.common.params.CombinerParams;
import org.apache.solr.common.params.CursorMarkParams;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.response.BasicResultContext;
import org.apache.solr.response.ResultContext;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.search.DocListAndSet;
import org.apache.solr.search.QueryResult;
import org.apache.solr.search.combine.QueryAndResponseCombiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The CombinedQueryComponent class extends QueryComponent and provides support for executing
 * multiple queries and combining their results.
 */
public class CombinedQueryComponent extends QueryComponent {

  public static final String COMPONENT_NAME = "combined_query";
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

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
          QueryAndResponseCombiner.getImplementation(rb.req.getParams());
      QueryResult combinedQueryResult = combinerStrategy.combine(queryResults);
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
  public String getDescription() {
    return "Combined Query Component to support multiple query execution";
  }
}
