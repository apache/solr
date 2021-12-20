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
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.Optional.ofNullable;

public class DenseVectorField extends FieldType {

    static final String KNN_VECTOR_DIMENSION = "vector_dimension";
    static final String KNN_SIMILARITY_FUNCTION = "similarity_function";
    private int dimension;
    private VectorSimilarityFunction similarityFunction;

    @Override
    public void init(IndexSchema schema, Map<String, String> args){
        this.dimension = ofNullable(args.get(KNN_VECTOR_DIMENSION))
                .map(Integer::parseInt)
                .orElseThrow(() -> new SolrException(SolrException.ErrorCode.BAD_REQUEST, "k is mandatory parameter"));
        args.remove(KNN_VECTOR_DIMENSION);

        this.similarityFunction = ofNullable(args.get(KNN_SIMILARITY_FUNCTION))
                .map(this::parseVectorSimilarityFunction)
                .orElseThrow(() -> new SolrException(SolrException.ErrorCode.BAD_REQUEST, "similarity function is mandatory parameter"));
        args.remove(KNN_SIMILARITY_FUNCTION);

        super.init(schema, args);
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
        return null;
    }

    @Override
    public SortField getSortField(SchemaField field, boolean top) {
        return null;
    }

    public Query getKnnVectorQuery(SchemaField field, String externalVal, int k) {
        float[] vector = parseVector(externalVal);
        return new KnnVectorQuery(field.getName(), vector, k);
    }

    private float[] parseVector(String value){
        float[] vector = new float[dimension];

        List<Float> floats =  Arrays.stream(value.substring(1, value.length() - 1).split(","))
                .map(Float::parseFloat)
                .collect(Collectors.toList());

        if (floats.size() != dimension){
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "wrong vector dimension. The vector value used in the query has size "
            + floats.size() + "while it is expected a vector with size " + dimension);
        }

        for (int i = 0; i < floats.size(); i++){
            vector[i] = floats.get(i);
        }

        return vector;
    }


    private VectorSimilarityFunction parseVectorSimilarityFunction(String value){
        if (value.equals("euclidean")){
            return VectorSimilarityFunction.EUCLIDEAN;
        } else if (value.equals("cosine")){
            return VectorSimilarityFunction.COSINE;
        } else if (value.equals("dot_product")){
            return VectorSimilarityFunction.DOT_PRODUCT;
        }

        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, String.format("{} is not a valid similarity function", value));
    }

}
