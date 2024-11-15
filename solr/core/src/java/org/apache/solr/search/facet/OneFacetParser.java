package org.apache.solr.search.facet;

import org.apache.solr.request.SolrQueryRequest;

import java.util.Map;

public interface OneFacetParser {
    FacetRequest parseOneFacetReq(SolrQueryRequest req, Map<String, Object> params);
}
