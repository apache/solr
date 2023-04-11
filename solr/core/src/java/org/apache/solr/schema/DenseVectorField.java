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
import static org.apache.lucene.codecs.lucene95.Lucene95HnswVectorsFormat.DEFAULT_BEAM_WIDTH;
import static org.apache.lucene.codecs.lucene95.Lucene95HnswVectorsFormat.DEFAULT_MAX_CONN;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.lucene.document.KnnByteVectorField;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.search.KnnByteVectorQuery;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.BytesRef;
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
  public static final String DEFAULT_KNN_ALGORITHM = HNSW_ALGORITHM;
  static final String KNN_VECTOR_DIMENSION = "vectorDimension";
  static final String KNN_ALGORITHM = "knnAlgorithm";
  static final String HNSW_MAX_CONNECTIONS = "hnswMaxConnections";
  static final String HNSW_BEAM_WIDTH = "hnswBeamWidth";
  static final String VECTOR_ENCODING = "vectorEncoding";
  static final VectorEncoding DEFAULT_VECTOR_ENCODING = VectorEncoding.FLOAT32;
  static final String KNN_SIMILARITY_FUNCTION = "similarityFunction";
  static final VectorSimilarityFunction DEFAULT_SIMILARITY = VectorSimilarityFunction.EUCLIDEAN;
  private int dimension;
  private VectorSimilarityFunction similarityFunction;
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

  /**
   * Encoding for vector value representation. The possible values are FLOAT32 or BYTE. The default
   * encoding is FLOAT32
   */
  private VectorEncoding vectorEncoding;

  public DenseVectorField() {
    super();
  }

  public DenseVectorField(int dimension) {
    this(dimension, DEFAULT_SIMILARITY, DEFAULT_VECTOR_ENCODING);
  }

  public DenseVectorField(int dimension, VectorEncoding vectorEncoding) {
    this(dimension, DEFAULT_SIMILARITY, vectorEncoding);
  }

  public DenseVectorField(
      int dimension, VectorSimilarityFunction similarityFunction, VectorEncoding vectorEncoding) {
    super();
    this.dimension = dimension;
    this.similarityFunction = similarityFunction;
    this.vectorEncoding = vectorEncoding;
  }

  @Override
  public void init(IndexSchema schema, Map<String, String> args) {
    this.dimension =
        ofNullable(args.get(KNN_VECTOR_DIMENSION))
            .map(Integer::parseInt)
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

    this.knnAlgorithm = args.getOrDefault(KNN_ALGORITHM, DEFAULT_KNN_ALGORITHM);
    args.remove(KNN_ALGORITHM);

    this.vectorEncoding =
        ofNullable(args.get(VECTOR_ENCODING))
            .map(value -> VectorEncoding.valueOf(value.toUpperCase(Locale.ROOT)))
            .orElse(DEFAULT_VECTOR_ENCODING);
    args.remove(VECTOR_ENCODING);

    this.hnswMaxConn =
        ofNullable(args.get(HNSW_MAX_CONNECTIONS)).map(Integer::parseInt).orElse(DEFAULT_MAX_CONN);
    args.remove(HNSW_MAX_CONNECTIONS);

    this.hnswBeamWidth =
        ofNullable(args.get(HNSW_BEAM_WIDTH)).map(Integer::parseInt).orElse(DEFAULT_BEAM_WIDTH);
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

  public VectorEncoding getVectorEncoding() {
    return vectorEncoding;
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

  @Override
  public List<IndexableField> createFields(SchemaField field, Object value) {
    try {
      ArrayList<IndexableField> fields = new ArrayList<>();
      VectorBuilder vectorBuilder = getVectorBuilder(value, VectorBuilder.BuilderPhase.INDEX);

      if (field.indexed()) {
        fields.add(createField(field, vectorBuilder));
      }
      if (field.stored()) {
        switch (vectorEncoding) {
          case FLOAT32:
            fields.ensureCapacity(vectorBuilder.getFloatVector().length + 1);
            for (float vectorElement : vectorBuilder.getFloatVector()) {
              fields.add(getStoredField(field, vectorElement));
            }
            break;
          case BYTE:
            fields.add(new StoredField(field.getName(), vectorBuilder.getByteVector()));
            break;
        }
      }
      return fields;
    } catch (RuntimeException e) {
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR,
          "Error while creating field '" + field + "' from value '" + value + "'",
          e);
    }
  }

  @Override
  public IndexableField createField(SchemaField field, Object vectorValue) {
    if (vectorValue == null) return null;
    VectorBuilder vectorBuilder = (VectorBuilder) vectorValue;
    switch (vectorEncoding) {
      case BYTE:
        return new KnnByteVectorField(
            field.getName(), vectorBuilder.getByteVector(), similarityFunction);
      case FLOAT32:
        return new KnnFloatVectorField(
            field.getName(), vectorBuilder.getFloatVector(), similarityFunction);
      default:
        throw new SolrException(
            SolrException.ErrorCode.SERVER_ERROR,
            "Unexpected state. Vector Encoding: " + vectorEncoding);
    }
  }

  @Override
  public Object toObject(IndexableField f) {
    if (vectorEncoding.equals(VectorEncoding.BYTE)) {
      BytesRef bytesRef = f.binaryValue();
      if (bytesRef != null) {
        List<Number> ret = new ArrayList<>(dimension);
        for (byte b : bytesRef.bytes) {
          ret.add((int) b);
        }
        return ret;
      } else {
        throw new AssertionError("Unexpected state. Field: '" + f + "'");
      }
    }

    return super.toObject(f);
  }

  /**
   * Index Time Parsing The inputValue is an ArrayList with a type that depends on the loader used:
   * - {@link org.apache.solr.handler.loader.XMLLoader}, {@link
   * org.apache.solr.handler.loader.CSVLoader} produces an ArrayList of String - {@link
   * org.apache.solr.handler.loader.JsonLoader} produces an ArrayList of Double - {@link
   * org.apache.solr.handler.loader.JavabinLoader} produces an ArrayList of Float
   */
  public VectorBuilder getVectorBuilder(Object inputValue, VectorBuilder.BuilderPhase phase) {
    switch (vectorEncoding) {
      case FLOAT32:
        return new VectorBuilder.Float32VectorBuilder(dimension, inputValue, phase);
      case BYTE:
        return new VectorBuilder.ByteVectorBuilder(dimension, inputValue, phase);
      default:
        throw new SolrException(
            SolrException.ErrorCode.SERVER_ERROR,
            "Unexpected state. Vector Encoding: " + vectorEncoding);
    }
  }

  abstract static class VectorBuilder {

    public static enum BuilderPhase {
      INDEX,
      QUERY
    }

    protected BuilderPhase builderPhase;

    protected int dimension;
    protected Object inputValue;

    public float[] getFloatVector() {
      throw new UnsupportedOperationException("Requested wrong vector type");
    }

    public byte[] getByteVector() {
      throw new UnsupportedOperationException("Requested wrong vector type");
    }

    protected void parseVector() {
      switch (builderPhase) {
        case INDEX:
          parseIndexVector();
          break;
        case QUERY:
          parseQueryVector();
          break;
      }
    }

    protected void parseIndexVector() {
      if (!(inputValue instanceof List)) {
        throw new SolrException(
            SolrException.ErrorCode.BAD_REQUEST, "incorrect vector format. " + errorMessage());
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

      if (inputVector.get(0) instanceof CharSequence) {
        for (int i = 0; i < dimension; i++) {
          try {
            addStringElement(inputVector.get(i).toString());
          } catch (NumberFormatException e) {
            throw new SolrException(
                SolrException.ErrorCode.BAD_REQUEST,
                "incorrect vector element: '" + inputVector.get(i) + "'. " + errorMessage());
          }
        }
      } else if (inputVector.get(0) instanceof Number) {
        for (int i = 0; i < dimension; i++) {
          addNumberElement((Number) inputVector.get(i));
        }
      } else {
        throw new SolrException(
            SolrException.ErrorCode.BAD_REQUEST, "incorrect vector format. " + errorMessage());
      }
    }

    protected void parseQueryVector() {

      String value = inputValue.toString();
      if (!value.startsWith("[") || !value.endsWith("]")) {
        throw new SolrException(
            SolrException.ErrorCode.BAD_REQUEST, "incorrect vector format. " + errorMessage());
      }

      String[] elements = value.substring(1, value.length() - 1).split(",");
      if (elements.length != dimension) {
        throw new SolrException(
            SolrException.ErrorCode.BAD_REQUEST,
            "incorrect vector dimension. "
                + "The vector value has size "
                + elements.length
                + " while it is expected a vector with size "
                + dimension);
      }

      for (int i = 0; i < dimension; i++) {
        try {
          addStringElement(elements[i].trim());
        } catch (NumberFormatException e) {
          throw new SolrException(
              SolrException.ErrorCode.BAD_REQUEST,
              "incorrect vector element: '" + elements[i] + "'. " + errorMessage());
        }
      }
    }

    protected abstract void addNumberElement(Number element);

    protected abstract void addStringElement(String element);

    protected abstract String errorMessage();

    static class ByteVectorBuilder extends VectorBuilder {
      private byte[] byteVector;
      private int curPosition;

      public ByteVectorBuilder(int dimension, Object inputValue, BuilderPhase builderPhase) {
        this.dimension = dimension;
        this.inputValue = inputValue;
        this.builderPhase = builderPhase;
        this.curPosition = 0;
      }

      @Override
      public byte[] getByteVector() {
        if (byteVector == null) {
          byteVector = new byte[dimension];
          parseVector();
        }
        return byteVector;
      }

      @Override
      protected void addNumberElement(Number element) {
        byteVector[curPosition++] = element.byteValue();
      }

      @Override
      protected void addStringElement(String element) {
        byteVector[curPosition++] = Byte.parseByte(element);
      }

      @Override
      protected String errorMessage() {
        return "The expected format is:'[b1,b2..b3]' where each element b is a byte (-128 to 127)";
      }
    }

    static class Float32VectorBuilder extends VectorBuilder {
      private float[] vector;
      private int curPosition;

      public Float32VectorBuilder(int dimension, Object inputValue, BuilderPhase builderPhase) {
        this.dimension = dimension;
        this.inputValue = inputValue;
        this.curPosition = 0;
        this.builderPhase = builderPhase;
      }

      @Override
      public float[] getFloatVector() {
        if (vector == null) {
          vector = new float[dimension];
          parseVector();
        }
        return vector;
      }

      @Override
      protected void addNumberElement(Number element) {
        vector[curPosition++] = element.floatValue();
      }

      @Override
      protected void addStringElement(String element) {
        vector[curPosition++] = Float.parseFloat(element);
      }

      @Override
      protected String errorMessage() {
        return "The expected format is:'[f1,f2..f3]' where each element f is a float";
      }
    }
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
      String fieldName, String vectorToSearch, int topK, Query filterQuery) {

    VectorBuilder vectorBuilder =
        getVectorBuilder(vectorToSearch, VectorBuilder.BuilderPhase.QUERY);

    switch (vectorEncoding) {
      case FLOAT32:
        return new KnnFloatVectorQuery(
            fieldName, vectorBuilder.getFloatVector(), topK, filterQuery);
      case BYTE:
        return new KnnByteVectorQuery(fieldName, vectorBuilder.getByteVector(), topK, filterQuery);
      default:
        throw new SolrException(
            SolrException.ErrorCode.SERVER_ERROR,
            "Unexpected state. Vector Encoding: " + vectorEncoding);
    }
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
