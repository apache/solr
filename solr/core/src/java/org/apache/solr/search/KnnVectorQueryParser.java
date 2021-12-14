package org.apache.solr.search;

import org.apache.lucene.search.Query;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.DenseVectorField;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.SchemaField;

public class KnnVectorQueryParser extends QParser {
    /**
     * Constructor for the QParser
     *
     * @param qstr        The part of the query string specific to this parser
     * @param localParams The set of parameters that are specific to this QParser.  See https://solr.apache.org/guide/local-parameters-in-queries.html
     * @param params      The rest of the {@link SolrParams}
     * @param req         The original {@link SolrQueryRequest}.
     */
    public KnnVectorQueryParser(String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
        super(qstr, localParams, params, req);
    }

    @Override
    public Query parse() throws SyntaxError {
        String field = localParams.get(QueryParsing.F);
        String vector = localParams.get(QueryParsing.V);
        Integer k = localParams.getInt("k");

        if (field == null) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "'f' not specified");
        }

        if (vector == null) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "vector missing");
        }

        if (k == null) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "k missing");
        }

        SchemaField schemaField = req.getCore().getLatestSchema().getField(field);
        FieldType fieldType = schemaField.getType();
        if (!(fieldType instanceof DenseVectorField)){
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "only DenseVectorField is accepted");
        }

        return ((DenseVectorField) fieldType).getKnnVectorQuery(schemaField, vector, k);
    }
}
