package org.apache.solr.handler.component;

import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

import java.util.ArrayList;
import java.util.List;

public class CombinedQueryResponseBuilder extends ResponseBuilder {

    public final List<ResponseBuilder> responseBuilders = new ArrayList<>();
    public CombinedQueryResponseBuilder(SolrQueryRequest req, SolrQueryResponse rsp, List<SearchComponent> components) {
        super(req, rsp, components);
    }
}
