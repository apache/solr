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

import com.google.common.annotations.VisibleForTesting;
import java.util.Map;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.lucene104.Lucene104HnswScalarQuantizedVectorsFormat;
import org.apache.lucene.codecs.lucene104.Lucene104ScalarQuantizedVectorsFormat.ScalarEncoding;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.solr.common.SolrException;
import org.apache.solr.logging.DeprecationLog;

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
   *
   * @deprecated Since Solr 10.1. No longer used by the underlying Lucene codec.
   */
  @Deprecated(since = "10.1")
  private Float confidenceInterval;

  /**
   * When enabled, in conjunction with 4 bit size, will pair values into single bytes for 50%
   * reduction in memory usage (comes at the cost of some decode speed penalty)
   *
   * @deprecated Since Solr 10.1. No longer used by the underlying Lucene codec.
   */
  @Deprecated(since = "10.1")
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

    // These params ("compress", "confidenceInterval", "dynamicConfidenceInterval") are deprecated
    // since Solr 10.1. Lucene 10.4's scalar-quantized vector format no longer consumes them
    // directly. They are parsed for backward compatibility but are no-ops going forward.
    String compressStr = args.remove(COMPRESS_PARAM);
    if (compressStr != null) {
      this.compress = Boolean.parseBoolean(compressStr);
      DeprecationLog.log(
          COMPRESS_PARAM,
          "The '"
              + COMPRESS_PARAM
              + "' parameter for ScalarQuantizedDenseVectorField is deprecated since Solr 10.1"
              + " and will be ignored. Please remove it from your schema.");
    }

    String confidenceIntervalStr = args.remove(CONFIDENCE_INTERVAL_PARAM);
    if (confidenceIntervalStr != null) {
      this.confidenceInterval = Float.parseFloat(confidenceIntervalStr);
      DeprecationLog.log(
          CONFIDENCE_INTERVAL_PARAM,
          "The '"
              + CONFIDENCE_INTERVAL_PARAM
              + "' parameter for ScalarQuantizedDenseVectorField is deprecated since Solr 10.1"
              + " and will be ignored. Please remove it from your schema.");
    } else {
      this.confidenceInterval = DEFAULT_CONFIDENCE_INTERVAL;
    }

    String dynamicConfidenceIntervalStr = args.remove(DYNAMIC_CONFIDENCE_INTERVAL_PARAM);
    if (Boolean.parseBoolean(dynamicConfidenceIntervalStr)) {
      this.confidenceInterval = 0f;
      DeprecationLog.log(
          DYNAMIC_CONFIDENCE_INTERVAL_PARAM,
          "The '"
              + DYNAMIC_CONFIDENCE_INTERVAL_PARAM
              + "' parameter for ScalarQuantizedDenseVectorField is deprecated since Solr 10.1"
              + " and will be ignored. Please remove it from your schema.");
    }

    super.init(schema, args);
  }

  @Override
  public KnnVectorsFormat buildKnnVectorsFormat() {
    ScalarEncoding encoding = ScalarEncoding.fromNumBits(getBits());
    return new Lucene104HnswScalarQuantizedVectorsFormat(
        encoding, getHnswM(), getHnswEfConstruction());
  }

  @Override
  public void checkSchemaField(final SchemaField field) throws SolrException {
    super.checkSchemaField(field);

    try {
      // format does not expose any validation, however by constructing
      // we will run lucene level argument validation
      buildKnnVectorsFormat();
    } catch (IllegalArgumentException e) {
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR,
          getClass().getSimpleName() + " " + e.getMessage() + ": " + field.getName(),
          e);
    }
  }

  public int getBits() {
    return bits;
  }

  /**
   * @deprecated Since Solr 10.1. No longer used by the underlying Lucene codec.
   */
  @Deprecated(since = "10.1")
  @VisibleForTesting
  boolean useCompression() {
    return compress;
  }

  /**
   * @deprecated Since Solr 10.1. No longer used by the underlying Lucene codec.
   */
  @Deprecated(since = "10.1")
  @VisibleForTesting
  Float getConfidenceInterval() {
    return confidenceInterval;
  }
}
