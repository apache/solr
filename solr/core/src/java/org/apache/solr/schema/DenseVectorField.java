/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.schema;

import org.apache.lucene.document.KnnVectorField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.search.KnnVectorQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SortField;
import org.apache.solr.common.SolrException;
import org.apache.solr.response.TextResponseWriter;
import org.apache.solr.search.QParser;
import org.apache.solr.uninverting.UninvertingReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static java.util.Optional.ofNullable;

/**
 * Provides a field type to support Lucene's {@link
 * org.apache.lucene.document.KnnVectorField}.
 * See {@link org.apache.lucene.search.KnnVectorQuery} for more details.
 * It supports a fixed cardinality dimension for the vector and a fixed similarity function.
 * The default similarity is EUCLIDEAN_HNSW (L2).
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
    VectorSimilarityFunction DEFAULT_SIMILARITY = VectorSimilarityFunction.EUCLIDEAN;

    @Override
    public void init(IndexSchema schema, Map<String, String> args){
        this.dimension = ofNullable(args.get(KNN_VECTOR_DIMENSION))
                .map(value -> parseDimensionParameter(value))
                .orElseThrow(() -> new SolrException(SolrException.ErrorCode.SERVER_ERROR, "the vector dimension is a mandatory parameter"));
        args.remove(KNN_VECTOR_DIMENSION);

        this.similarityFunction = ofNullable(args.get(KNN_SIMILARITY_FUNCTION))
                .map(value -> VectorSimilarityFunction.valueOf(value.toUpperCase(Locale.ROOT)))
                .orElse(DEFAULT_SIMILARITY);
        args.remove(KNN_SIMILARITY_FUNCTION);

        this.properties &= ~MULTIVALUED;
        this.properties &= ~UNINVERTIBLE;
        
        super.init(schema, args);
    }

    private int parseDimensionParameter(String value) {
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
            fields.add(createStoredField(field, value));
        }
        return fields;
    }

    private IndexableField createStoredField(SchemaField field, Object value) {
        return new StoredField(field.getName(), value.toString());
    }

    @Override
    public UninvertingReader.Type getUninversionType(SchemaField sf) {
        return null;
    }

    @Override
    public ValueSource getValueSource(SchemaField field, QParser parser) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
                "Function queries are not supported for Dense Vector fields.");
    }

    @Override
    public void write(TextResponseWriter writer, String name, IndexableField f) throws IOException {
        writer.writeVal(name, f.stringValue());
    }

    @Override
    public IndexableField createField(SchemaField field, Object value) {
        float[] parsedVector;
        try {
            parsedVector = parseVector(value.toString());
        } catch (RuntimeException e) {
            throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Error while creating field '" + field + "' from value '" + value + "', expected format:'[f1, f2, f3...fn]' e.g. [1.0, 3.4, 5.6]", e);
        }
        if (parsedVector == null) return null;

        return new KnnVectorField(field.getName(), parsedVector, similarityFunction);
    }

    /**
     * Parses a String vector.
     *
     * @param value with format: [f1, f2, f3, f4...fn]
     * @return a float array
     */
    private float[] parseVector(String value) {
        if (!value.startsWith("[") || !value.endsWith("]")) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "incorrect vector format." +
                    " The expected format is:'[f1,f2..f3]' where each element f is a float");
        }
        
        String[] elements = value.substring(1, value.length() - 1).split(",");
        if (elements.length != dimension) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "incorrect vector dimension." +
                    " The vector value has size "
                    + elements.length + " while it is expected a vector with size " + dimension);
        }
        float[] vector = new float[dimension];
        for (int i = 0; i < dimension; i++) {
            try {
                vector[i] = Float.parseFloat(elements[i]);
            } catch (NumberFormatException e) {
                throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "incorrect vector element: '" + elements[i] +
                        "'. The expected format is:'[f1,f2..f3]' where each element f is a float");
            }
        }
        return vector;
    }

    public Query getKnnVectorQuery(SchemaField field, String valueToSearch, int topK) {
        float[] vectorToSearch = parseVector(valueToSearch);
        return new KnnVectorQuery(field.getName(), vectorToSearch, topK);
    }

    /**
     * Not Supported
     */
    @Override
    public Query getFieldQuery(QParser parser, SchemaField field, String externalVal) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
                "Field Queries are not supported for Dense Vector fields. Please use the {!knn} query parser to run K nearest neighbors search queries.");
    }

    /**
     * Not Supported
     */
    @Override
    public Query getRangeQuery(QParser parser, SchemaField field, String part1, String part2, boolean minInclusive, boolean maxInclusive) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
                "Range Queries are not supported for Dense Vector fields. Please use the {!knn} query parser to run K nearest neighbors search queries.");
    }

    /**
     * Not Supported
     */
    @Override
    public SortField getSortField(SchemaField field, boolean top) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Cannot sort on a Dense Vector field");
    }

}
