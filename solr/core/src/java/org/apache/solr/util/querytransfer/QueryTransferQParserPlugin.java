package org.apache.solr.util.querytransfer;

import org.apache.lucene.search.Query;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.QParser;
import org.apache.solr.search.QParserPlugin;
import org.apache.solr.search.SyntaxError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Base64;

public class QueryTransferQParserPlugin extends QParserPlugin {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    public static final String NAME = "qubyen64";

    @Override
    public QParser createParser(String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
        return new QParser(qstr,localParams, params,req) {
            @Override
            public Query parse() throws SyntaxError {
                final byte[] decode = Base64.getDecoder().decode(qstr);
                try {
                    return QueryTransfer.receive(decode);
                } catch (IOException e) {
                    log.error(e.getMessage(), e);
                    throw new SyntaxError(e);
                }
            }
        };
    }
}
