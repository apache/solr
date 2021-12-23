package org.apache.solr.search.neural;

import org.apache.lucene.search.Query;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.DenseVectorField;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.QParser;
import org.apache.solr.search.QueryParsing;

public class KnnQParser extends QParser {

    static final String TOP_K = "topK";// retrieve the top K results based on the distance similarity function 
    static final int DEFAULT_TOP_K = 10;

    /**
     * Constructor for the QParser
     *
     * @param qstr        The part of the query string specific to this parser
     * @param localParams The set of parameters that are specific to this QParser.  See https://solr.apache.org/guide/local-parameters-in-queries.html
     * @param params      The rest of the {@link SolrParams}
     * @param req         The original {@link SolrQueryRequest}.
     */
    public KnnQParser(String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
        super(qstr, localParams, params, req);
    }

    @Override
    public Query parse() {
        String denseVectorField = localParams.get(QueryParsing.F);
        String vectorToSearch = localParams.get(QueryParsing.V);
        int topK = localParams.getInt(TOP_K, DEFAULT_TOP_K);

        if (denseVectorField == null) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "the Dense Vector field 'f' is missing");
        }

        if (vectorToSearch == null || vectorToSearch.isEmpty()) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "the Dense Vector to search is missing");
        }

        SchemaField schemaField = req.getCore().getLatestSchema().getField(denseVectorField);
        FieldType fieldType = schemaField.getType();
        if (!(fieldType instanceof DenseVectorField)) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "only DenseVectorField is compatible with this Query Parser");
        }

        return ((DenseVectorField) fieldType).getKnnVectorQuery(schemaField, vectorToSearch, topK);
    }
}
