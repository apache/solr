package org.apache.solr.search.mlt;

import org.apache.lucene.queries.mlt.MoreLikeThis;
import org.apache.lucene.search.Query;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.QParser;
import org.apache.solr.search.QParserPlugin;
import org.apache.solr.search.QueryParsing;
import org.apache.solr.search.SyntaxError;

import java.io.IOException;
import java.io.StringReader;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class MLTContentQParserPlugin extends QParserPlugin {
    public static final String NAME = "mlt_content";

    @Override
    public QParser createParser(String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
        return new AbstractMLTQParser(qstr, localParams, params, req) {
            @Override
            public Query parse() throws SyntaxError {
                String content = localParams.get(QueryParsing.V);
                try {
                    return parseMLTQuery(this::getFieldsFromSchema, (mlt)->likeContent(mlt, content));
                } catch (IOException e) {
                    throw new SolrException(
                            SolrException.ErrorCode.BAD_REQUEST, "Error completing MLT request" + e.getMessage());
                }
            }
        };
    }

    protected Query likeContent(MoreLikeThis moreLikeThis, String content) throws IOException {
        final String[] fieldNames = moreLikeThis.getFieldNames();
        if (fieldNames.length == 1) {
            return moreLikeThis.like(fieldNames[0], new StringReader(content));
        } else {
            Collection<Object> streamValue = Collections.singleton(content);
            Map<String, Collection<Object>> multifieldDoc = new HashMap<>(fieldNames.length);
            for (String field : fieldNames) {
                multifieldDoc.put(field, streamValue);
            }
            return moreLikeThis.like(multifieldDoc);
        }
    }
}
