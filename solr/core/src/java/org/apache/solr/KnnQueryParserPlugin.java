package org.apache.solr;

import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.ExtendedDismaxQParser;
import org.apache.solr.search.KnnVectorQueryParser;
import org.apache.solr.search.QParser;
import org.apache.solr.search.QParserPlugin;

public class KnnQueryParserPlugin extends QParserPlugin {
    public static final String NAME = "knn";

    @Override
    public QParser createParser(String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
        return new KnnVectorQueryParser(qstr, localParams, params, req);
    }
}
