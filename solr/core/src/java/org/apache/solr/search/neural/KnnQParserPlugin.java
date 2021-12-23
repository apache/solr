package org.apache.solr.search.neural;

import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.QParser;
import org.apache.solr.search.QParserPlugin;

/**
 * A neural query parser to run K-nearest neighbors search on Dense Vector fields.
 * See Wiki page  https://solr.apache.org/guide/neural-search.html
 */
public class KnnQParserPlugin extends QParserPlugin {
    public static final String NAME = "knn";

    @Override
    public QParser createParser(String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
        return new KnnQParser(qstr, localParams, params, req);
    }
}
