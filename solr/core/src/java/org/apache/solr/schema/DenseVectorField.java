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

import static java.util.Optional.ofNullable;
import static org.apache.lucene.codecs.lucene92.Lucene92HnswVectorsFormat.DEFAULT_BEAM_WIDTH;
import static org.apache.lucene.codecs.lucene92.Lucene92HnswVectorsFormat.DEFAULT_MAX_CONN;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.lucene.document.KnnVectorField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.search.KnnVectorQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.hnsw.HnswGraph;
import org.apache.solr.common.SolrException;
import org.apache.solr.search.QParser;
import org.apache.solr.uninverting.UninvertingReader;

/**
 * Provides a field type to support Lucene's {@link org.apache.lucene.document.KnnVectorField}. See
 * {@link org.apache.lucene.search.KnnVectorQuery} for more details. It supports a fixed cardinality
 * dimension for the vector and a fixed similarity function. The default similarity is
 * EUCLIDEAN_HNSW (L2). The default algorithm is HNSW. For Lucene 9.1 e.g. See {@link
 * org.apache.lucene.util.hnsw.HnswGraph} for more details about the implementation. <br>
 * Only {@code Indexed} and {@code Stored} attributes are supported.
 */
public class DenseVectorField extends FloatPointField {
  public static final String HNSW_ALGORITHM = "hnsw";

  static final String KNN_VECTOR_DIMENSION = "vectorDimension";
  static final String KNN_SIMILARITY_FUNCTION = "similarityFunction";

  static final String KNN_ALGORITHM = "knnAlgorithm";
  static final String HNSW_MAX_CONNECTIONS = "hnswMaxConnections";
  static final String HNSW_BEAM_WIDTH = "hnswBeamWidth";

  private int dimension;
  private VectorSimilarityFunction similarityFunction;
  private VectorSimilarityFunction DEFAULT_SIMILARITY = VectorSimilarityFunction.EUCLIDEAN;

  private String knnAlgorithm;
  /**
   * This parameter is coupled with the hnsw algorithm. Controls how many of the nearest neighbor
   * candidates are connected to the new node. See {@link HnswGraph} for more details.
   */
  private int hnswMaxConn;
  /**
   * This parameter is coupled with the hnsw algorithm. The number of candidate neighbors to track
   * while searching the graph for each newly inserted node. See {@link HnswGraph} for details.
   */
  private int hnswBeamWidth;

  public DenseVectorField() {
    super();
  }

  public DenseVectorField(int dimension) {
    super();
    this.dimension = dimension;
    this.similarityFunction = DEFAULT_SIMILARITY;
  }

  public DenseVectorField(int dimension, VectorSimilarityFunction similarityFunction) {
    super();
    this.dimension = dimension;
    this.similarityFunction = similarityFunction;
  }

  @Override
  public void init(IndexSchema schema, Map<String, String> args) {
    this.dimension =
        ofNullable(args.get(KNN_VECTOR_DIMENSION))
            .map(value -> Integer.parseInt(value))
            .orElseThrow(
                () ->
                    new SolrException(
                        SolrException.ErrorCode.SERVER_ERROR,
                        "the vector dimension is a mandatory parameter"));
    args.remove(KNN_VECTOR_DIMENSION);

    this.similarityFunction =
        ofNullable(args.get(KNN_SIMILARITY_FUNCTION))
            .map(value -> VectorSimilarityFunction.valueOf(value.toUpperCase(Locale.ROOT)))
            .orElse(DEFAULT_SIMILARITY);
    args.remove(KNN_SIMILARITY_FUNCTION);

    this.knnAlgorithm = args.get(KNN_ALGORITHM);

    args.remove(KNN_ALGORITHM);

    this.hnswMaxConn =
        ofNullable(args.get(HNSW_MAX_CONNECTIONS))
            .map(value -> Integer.parseInt(value))
            .orElse(DEFAULT_MAX_CONN);
    args.remove(HNSW_MAX_CONNECTIONS);

    this.hnswBeamWidth =
        ofNullable(args.get(HNSW_BEAM_WIDTH))
            .map(value -> Integer.parseInt(value))
            .orElse(DEFAULT_BEAM_WIDTH);
    args.remove(HNSW_BEAM_WIDTH);

    this.properties &= ~MULTIVALUED;
    this.properties &= ~UNINVERTIBLE;

    super.init(schema, args);
  }

  public int getDimension() {
    return dimension;
  }

  public VectorSimilarityFunction getSimilarityFunction() {
    return similarityFunction;
  }

  public String getKnnAlgorithm() {
    return knnAlgorithm;
  }

  public Integer getHnswMaxConn() {
    return hnswMaxConn;
  }

  public Integer getHnswBeamWidth() {
    return hnswBeamWidth;
  }

  @Override
  public void checkSchemaField(final SchemaField field) throws SolrException {
    super.checkSchemaField(field);
    if (field.multiValued()) {
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR,
          getClass().getSimpleName() + " fields can not be multiValued: " + field.getName());
    }

    if (field.hasDocValues()) {
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR,
          getClass().getSimpleName() + " fields can not have docValues: " + field.getName());
    }
  }

  public List<IndexableField> createFields(SchemaField field, Object value) {
    ArrayList<IndexableField> fields = new ArrayList<>();
    float[] parsedVector;
    try {
      parsedVector = parseVector(value);
    } catch (RuntimeException e) {
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR,
          "Error while creating field '"
              + field
              + "' from value '"
              + value
              + "', expected format:'[f1, f2, f3...fn]' e.g. [1.0, 3.4, 5.6]",
          e);
    }

    if (field.indexed()) {
      fields.add(createField(field, parsedVector));
    }
    if (field.stored()) {
      fields.ensureCapacity(parsedVector.length + 1);
      for (float vectorElement : parsedVector) {
        fields.add(getStoredField(field, vectorElement));
      }
    }
    return fields;
  }

  @Override
  public IndexableField createField(SchemaField field, Object parsedVector) {
    if (parsedVector == null) return null;
    float[] typedVector = (float[]) parsedVector;
    return new KnnVectorField(field.getName(), typedVector, similarityFunction);
  }

  /**
   * Index Time Parsing The inputValue is an ArrayList with a type that depends on the loader used:
   * - {@link org.apache.solr.handler.loader.XMLLoader}, {@link
   * org.apache.solr.handler.loader.CSVLoader} produces an ArrayList of String - {@link
   * org.apache.solr.handler.loader.JsonLoader} produces an ArrayList of Double - {@link
   * org.apache.solr.handler.loader.JavabinLoader} produces an ArrayList of Float
   *
   * @param inputValue - An {@link ArrayList} containing the elements of the vector
   * @return the vector parsed
   */
  float[] parseVector(Object inputValue) {
    if (!(inputValue instanceof List)) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST,
          "incorrect vector format."
              + " The expected format is an array :'[f1,f2..f3]' where each element f is a float");
    }
    List<?> inputVector = (List<?>) inputValue;
    if (inputVector.size() != dimension) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST,
          "incorrect vector dimension."
              + " The vector value has size "
              + inputVector.size()
              + " while it is expected a vector with size "
              + dimension);
    }

    float[] vector = new float[dimension];
    if (inputVector.get(0) instanceof CharSequence) {
      for (int i = 0; i < dimension; i++) {
        try {
          vector[i] = Float.parseFloat(inputVector.get(i).toString());
        } catch (NumberFormatException e) {
          throw new SolrException(
              SolrException.ErrorCode.BAD_REQUEST,
              "incorrect vector element: '"
                  + inputVector.get(i)
                  + "'. The expected format is:'[f1,f2..f3]' where each element f is a float");
        }
      }
    } else if (inputVector.get(0) instanceof Number) {
      for (int i = 0; i < dimension; i++) {
        vector[i] = ((Number) inputVector.get(i)).floatValue();
      }
    } else {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST,
          "incorrect vector format."
              + " The expected format is an array :'[f1,f2..f3]' where each element f is a float");
    }

    return vector;
  }

  @Override
  public UninvertingReader.Type getUninversionType(SchemaField sf) {
    return null;
  }

  @Override
  public ValueSource getValueSource(SchemaField field, QParser parser) {
    throw new SolrException(
        SolrException.ErrorCode.BAD_REQUEST,
        "Function queries are not supported for Dense Vector fields.");
  }

  public Query getKnnVectorQuery(
      String fieldName, float[] vectorToSearch, int topK, Query filterQuery) {
    return new KnnVectorQuery(fieldName, vectorToSearch, topK, filterQuery);
  }

  /**
   * Not Supported. Please use the {!knn} query parser to run K nearest neighbors search queries.
   */
  @Override
  public Query getFieldQuery(QParser parser, SchemaField field, String externalVal) {
    throw new SolrException(
        SolrException.ErrorCode.BAD_REQUEST,
        "Field Queries are not supported for Dense Vector fields. Please use the {!knn} query parser to run K nearest neighbors search queries.");
  }

  /** Not Supported */
  @Override
  public Query getRangeQuery(
      QParser parser,
      SchemaField field,
      String part1,
      String part2,
      boolean minInclusive,
      boolean maxInclusive) {
    throw new SolrException(
        SolrException.ErrorCode.BAD_REQUEST,
        "Range Queries are not supported for Dense Vector fields. Please use the {!knn} query parser to run K nearest neighbors search queries.");
  }

  /** Not Supported */
  @Override
  public SortField getSortField(SchemaField field, boolean top) {
    throw new SolrException(
        SolrException.ErrorCode.BAD_REQUEST, "Cannot sort on a Dense Vector field");
  }
}
