package org.apache.solr.handler.component;

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

import java.io.IOException;
import java.util.*;

public class CombinedQueryComponent extends QueryComponent {

    public static final String COMPONENT_NAME = "combined_query";
    private static final Logger LOGGER = LoggerFactory.getLogger(CombinedQueryComponent.class);
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
                    segmentTerminatedEarly |= (boolean) rbNow.rsp.getResponseHeader()
                            .getOrDefault(SolrQueryResponse.RESPONSE_HEADER_SEGMENT_TERMINATED_EARLY_KEY, false);
                }
            }
            QueryAndResponseCombiner combinerStrategy = QueryAndResponseCombiner.getImplementation(rb.req.getParams());
            QueryResult combinedQueryResult = combinerStrategy.combine(queryResults);
            combinedQueryResult.setPartialResults(partialResults);
            combinedQueryResult.setSegmentTerminatedEarly(segmentTerminatedEarly);
            crb.setResult(combinedQueryResult);
            ResultContext ctx = new BasicResultContext(crb);
            crb.rsp.addResponse(ctx);
            crb.rsp.getToLog().add("hits", crb.getResults() == null || crb.getResults().docList == null
                                    ? 0 : crb.getResults().docList.matches());
            if (!crb.req.getParams().getBool(ShardParams.IS_SHARD, false)) {
                if (null != crb.getNextCursorMark()) {
                    crb.rsp.add(CursorMarkParams.CURSOR_MARK_NEXT, crb.getNextCursorMark().getSerializedTotem());
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
