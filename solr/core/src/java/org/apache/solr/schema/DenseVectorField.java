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
    private int dimension;
    private VectorSimilarityFunction similarityFunction;

    @Override
    public void init(IndexSchema schema, Map<String, String> args){
        this.dimension = ofNullable(args.get(KNN_VECTOR_DIMENSION)).map(Integer::parseInt).orElseThrow();
        args.remove(KNN_VECTOR_DIMENSION);
        this.similarityFunction = VectorSimilarityFunction.EUCLIDEAN;
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

        org.apache.lucene.document.FieldType fieldType = createFieldType(field, dimension, similarityFunction);

        return new KnnVectorField(field.getName(), vector, fieldType);
    }


    public org.apache.lucene.document.FieldType createFieldType(SchemaField field, int dimension, VectorSimilarityFunction similarityFunction){

        org.apache.lucene.document.FieldType type = new org.apache.lucene.document.FieldType();
        type.setVectorDimensionsAndSimilarityFunction(dimension, similarityFunction);
        return type;
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

}
