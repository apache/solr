package org.apache.solr.schema;

import org.apache.lucene.document.KnnVectorField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.KnnVectorQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SortField;
import org.apache.solr.common.SolrException;
import org.apache.solr.response.TextResponseWriter;
import org.apache.solr.search.QParser;
import org.apache.solr.uninverting.UninvertingReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.Optional.ofNullable;

/**
 * Provides a field type to support Lucene's {@link
 * org.apache.lucene.document.KnnVectorField}.
 * See {@link org.apache.lucene.search.KnnVectorQuery} for more details.
 * It supports a fixed cardinality dimension for the vector and a fixed similarity function. 
 * See subclasses for details.
 * 
 * <br>
 * Only {@code Indexed} and {@code Stored} attributes are supported.
 */
public class DenseVectorField extends FieldType {

    static final String KNN_VECTOR_DIMENSION = "vectorDimension";
    static final String KNN_SIMILARITY_FUNCTION = "similarityFunction";
    
    int dimension;
    VectorSimilarityFunction similarityFunction;

    @Override
    public void init(IndexSchema schema, Map<String, String> args){
        this.dimension = ofNullable(args.get(KNN_VECTOR_DIMENSION))
                .map(value -> parseDimension(value))
                .orElseThrow(() -> new SolrException(SolrException.ErrorCode.SERVER_ERROR, "the vector dimension is a mandatory parameter"));
        args.remove(KNN_VECTOR_DIMENSION);

        this.similarityFunction = ofNullable(args.get(KNN_SIMILARITY_FUNCTION))
                .map(value -> VectorSimilarityFunction.valueOf(value.toUpperCase(Locale.ROOT)))
                .orElseThrow(() -> new SolrException(SolrException.ErrorCode.SERVER_ERROR, "similarity function is mandatory parameter"));
        args.remove(KNN_SIMILARITY_FUNCTION);

        this.properties &= ~MULTIVALUED;
        this.properties &= ~UNINVERTIBLE;
        
        super.init(schema, args);
    }

    private int parseDimension(String value) {
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "the vector dimension must be an integer");
        }
    }

    @Override
    public void checkSchemaField(final SchemaField field) throws SolrException {
        super.checkSchemaField(field);
        if (field.multiValued()) {
            throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
                    getClass().getSimpleName() + " fields can not be multiValued: " + field.getName());
        }
    }
    
    public List<IndexableField> createFields(SchemaField field, Object value) {
        List<IndexableField> fields = new ArrayList<>();
        if (field.indexed()){
            fields.add(createField(field, value));
        }
        if (field.stored()){
            fields.add(getStoredField(field, value));
        }
        return fields;
    }

    private IndexableField getStoredField(SchemaField field, Object value) {
        return new StoredField(field.getName(), value.toString());
    }

    @Override
    public UninvertingReader.Type getUninversionType(SchemaField sf) {
        return null;
    }

    @Override
    public void write(TextResponseWriter writer, String name, IndexableField f) throws IOException {
        writer.writeVal(name, f.stringValue());
    }

    @Override
    public IndexableField createField(SchemaField field, Object value) {
        float [] vector;

        if (value instanceof String) {
            vector = parseVector((String) value);
        } else {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "impossible to parse vector value");
        }
        return new KnnVectorField(field.getName(), vector, similarityFunction);
    }

    @Override
    public Query getFieldQuery(QParser parser, SchemaField field, String externalVal) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
                "Field Queries are not supported for Dense Vector fields. Please use the {!knn} query parser to run K nearest neighbors search queries.");
    }

    @Override
    public Query getRangeQuery(QParser parser, SchemaField field, String part1, String part2, boolean minInclusive, boolean maxInclusive) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
                "Range Queries are not supported for Dense Vector fields. Please use the {!knn} query parser to run K nearest neighbors search queries.");
    }
    
    @Override
    public SortField getSortField(SchemaField field, boolean top) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Cannot sort on a Dense Vector field");
    }

    public Query getKnnVectorQuery(SchemaField field, String valueToSearch, int topK) {
        float[] vector = parseVector(valueToSearch);
        return new KnnVectorQuery(field.getName(), vector, topK);
    }

    private float[] parseVector(String value){
        float[] vector = new float[dimension];

        List<Float> parsed =  Arrays.stream(value.substring(1, value.length() - 1).split(","))
                .map(Float::parseFloat)
                .collect(Collectors.toList());

        if (parsed.size() != dimension){
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "wrong vector dimension. The vector value used in the query has size "
            + parsed.size() + "while it is expected a vector with size " + dimension);
        }

        for (int i = 0; i < parsed.size(); i++){
            vector[i] = parsed.get(i);
        }

        return vector;
    }
}
