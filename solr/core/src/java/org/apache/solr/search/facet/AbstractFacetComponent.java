package org.apache.solr.search.facet;

import org.apache.solr.common.params.SolrParams;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.handler.component.SearchComponent;
import org.apache.solr.request.SimpleFacets;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.DocSet;

import java.util.Map;

public abstract class AbstractFacetComponent extends SearchComponent {
    protected FacetParserFactory createFacetRequestFactory(SolrQueryRequest req, Map<String, Object> jsonFacet) {
      return new FacetParserFactory();
    }

    protected SimpleFacets newSimpleFacets(
            SolrQueryRequest req, DocSet docSet, SolrParams params, ResponseBuilder rb) {
        return new SimpleFacets(req, docSet, params, rb) {
            @Override
            public FacetRequest parseOneFacetReq(SolrQueryRequest req, Map<String, Object> jsonFacet) {
                OneFacetParser facetRequestFactory = createFacetRequestFactory(req, jsonFacet);
                return facetRequestFactory.parseOneFacetReq(req, jsonFacet);
            }
        };
    }

}
