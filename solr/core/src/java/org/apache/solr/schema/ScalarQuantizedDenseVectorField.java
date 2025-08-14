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
import static org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat.DEFAULT_NUM_MERGE_WORKER;

import java.util.Map;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.lucene99.Lucene99HnswScalarQuantizedVectorsFormat;
import org.apache.lucene.codecs.lucene99.Lucene99ScalarQuantizedVectorsFormat;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.solr.common.SolrException;

public class ScalarQuantizedDenseVectorField extends DenseVectorField {
  public static final String BITS_PARAM = "bits"; //
  public static final String CONFIDENCE_INTERVAL_PARAM = "confidenceInterval";
  public static final String DYNAMIC_CONFIDENCE_INTERVAL_PARAM = "dynamicConfidenceInterval";
  public static final String COMPRESS_PARAM =
      "compress"; // can only be enabled when bits = 4 per Lucene codec spec

  static final int DEFAULT_BITS = 7; // use signed byte as default when unspecified
  static final Float DEFAULT_CONFIDENCE_INTERVAL = null; // use dimension scaled confidence interval

  /**
   * Number of bits to use for storage Must be 4 (half-byte) or 7 (signed-byte) per Lucene codec
   * spec
   */
  private int bits;

  /**
   * Confidence interval to use for scalar quantization Default is calculated as
   * `1-1/(vector_dimensions + 1)`
   */
  private Float confidenceInterval;

  /**
   * When enabled, in conjunction with 4 bit size, will pair values into single bytes for 50%
   * reduction in memory usage (comes at the cost of some decode speed penalty)
   */
  private boolean compress;

  public ScalarQuantizedDenseVectorField() {
    super();
  }

  public ScalarQuantizedDenseVectorField(
      int dimension,
      VectorSimilarityFunction similarityFunction,
      VectorEncoding vectorEncoding,
      int bits,
      Float confidenceInterval,
      boolean compress) {
    super(dimension, similarityFunction, vectorEncoding);
    this.bits = bits;
    this.confidenceInterval = confidenceInterval;
    this.compress = compress;
  }

  @Override
  public void init(IndexSchema schema, Map<String, String> args) {
    this.bits = ofNullable(args.remove(BITS_PARAM)).map(Integer::parseInt).orElse(DEFAULT_BITS);

    this.compress =
        ofNullable(args.remove(COMPRESS_PARAM)).map(Boolean::parseBoolean).orElse(false);

    this.confidenceInterval =
        ofNullable(args.remove(CONFIDENCE_INTERVAL_PARAM))
            .map(Float::parseFloat)
            .orElse(DEFAULT_CONFIDENCE_INTERVAL);

    if (ofNullable(args.remove(DYNAMIC_CONFIDENCE_INTERVAL_PARAM))
        .map(Boolean::parseBoolean)
        .orElse(false)) {
      this.confidenceInterval = Lucene99ScalarQuantizedVectorsFormat.DYNAMIC_CONFIDENCE_INTERVAL;
    }

    super.init(schema, args);
  }

  @Override
  public KnnVectorsFormat buildKnnVectorsFormat() {
    return new Lucene99HnswScalarQuantizedVectorsFormat(
        getHnswMaxConn(),
        getHnswBeamWidth(),
        DEFAULT_NUM_MERGE_WORKER,
        getBits(),
        useCompression(),
        getConfidenceInterval(),
        null);
  }

  @Override
  public void checkSchemaField(final SchemaField field) throws SolrException {
    super.checkSchemaField(field);

    try {
      // format does not expose any validation, however by constructing
      // we will run lucene level argument validation
      buildKnnVectorsFormat();
    } catch (IllegalArgumentException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e.getMessage(), e);
    }
  }

  public int getBits() {
    return bits;
  }

  public boolean useCompression() {
    return compress;
  }

  public Float getConfidenceInterval() {
    return confidenceInterval;
  }
}
