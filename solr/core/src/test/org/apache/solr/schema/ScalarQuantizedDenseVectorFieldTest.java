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

import static org.hamcrest.core.Is.is;

import org.apache.lucene.codecs.lucene99.Lucene99ScalarQuantizedVectorsFormat;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.solr.core.AbstractBadConfigTestBase;
import org.apache.solr.schema.neural.ScalarQuantizedDenseVectorField;
import org.junit.Before;
import org.junit.Test;

public class ScalarQuantizedDenseVectorFieldTest extends AbstractBadConfigTestBase {
  @Before
  public void init() {}

  @Test
  public void fieldTypeDefinition_invalidBitSize_shouldThrowException() throws Exception {
    assertConfigs(
        "solrconfig-basic.xml",
        "bad-schema-densevector-quantized-bits.xml",
        "ScalarQuantizedDenseVectorField fields must have bit size of 4 (half-byte) or 7 (signed-byte) v_scalar_bits");
  }

  @Test
  public void fieldTypeDefinition_improperCompressionUse_shouldThrowException() throws Exception {
    assertConfigs(
        "solrconfig-basic.xml",
        "bad-schema-densevector-quantized-compress.xml",
        "ScalarQuantizedDenseVectorField fields must have bit size of 4 to enable compression v_scalar_compressed");
  }

  @Test
  public void fieldTypeDefinition_confidenceIntervalTooLow_shouldThrowException() throws Exception {
    assertConfigs(
        "solrconfig-basic.xml",
        "bad-schema-densevector-quantized-confidence-interval-low.xml",
        "ScalarQuantizedDenseVectorField fields must have non-dynamic confidence interval between 0.9 and 1.0 v_scalar_ci");
  }

  @Test
  public void fieldTypeDefinition_confidenceIntervalTooHigh_shouldThrowException()
      throws Exception {
    assertConfigs(
        "solrconfig-basic.xml",
        "bad-schema-densevector-quantized-confidence-interval-high.xml",
        "ScalarQuantizedDenseVectorField fields must have non-dynamic confidence interval between 0.9 and 1.0 v_scalar_ci");
  }

  @Test
  public void fieldDefinition_default_shouldLoadSchemaField() throws Exception {
    try {
      initCore("solrconfig_codec.xml", "schema-densevector-quantized.xml");

      IndexSchema schema = h.getCore().getLatestSchema();

      SchemaField defaultVectorField = schema.getField("v_scalar_default");
      assertNotNull(defaultVectorField);

      ScalarQuantizedDenseVectorField defaultVectorType =
          (ScalarQuantizedDenseVectorField) defaultVectorField.getType();
      assertThat(defaultVectorType.getSimilarityFunction(), is(VectorSimilarityFunction.COSINE));
      assertThat(defaultVectorType.getDimension(), is(4));
      assertThat(defaultVectorType.getKnnAlgorithm(), is("hnsw"));
      assertThat(defaultVectorType.getBits(), is(ScalarQuantizedDenseVectorField.DEFAULT_BITS));
      assertThat(
          defaultVectorType.getConfidenceInterval(),
          is(ScalarQuantizedDenseVectorField.DEFAULT_CONFIDENCE_INTERVAL));
      assertThat(defaultVectorType.useCompression(), is(false));
    } finally {
      deleteCore();
    }
  }

  @Test
  public void fieldDefinition_halfByteSize_shouldLoadSchemaField() throws Exception {
    try {
      initCore("solrconfig_codec.xml", "schema-densevector-quantized.xml");

      IndexSchema schema = h.getCore().getLatestSchema();

      SchemaField vectorField = schema.getField("v_scalar_half_byte");
      assertNotNull(vectorField);

      ScalarQuantizedDenseVectorField vectorType =
          (ScalarQuantizedDenseVectorField) vectorField.getType();
      assertThat(vectorType.getBits(), is(4));
    } finally {
      deleteCore();
    }
  }

  @Test
  public void fieldDefinition_compressed_shouldLoadSchemaField() throws Exception {
    try {
      initCore("solrconfig_codec.xml", "schema-densevector-quantized.xml");

      IndexSchema schema = h.getCore().getLatestSchema();

      SchemaField vectorField = schema.getField("v_scalar_compressed");
      assertNotNull(vectorField);

      ScalarQuantizedDenseVectorField vectorType =
          (ScalarQuantizedDenseVectorField) vectorField.getType();
      assertThat(vectorType.getBits(), is(4));
      assertThat(vectorType.useCompression(), is(true));
    } finally {
      deleteCore();
    }
  }

  @Test
  public void fieldDefinition_customConfidenceInterval_shouldLoadSchemaField() throws Exception {
    try {
      initCore("solrconfig_codec.xml", "schema-densevector-quantized.xml");

      IndexSchema schema = h.getCore().getLatestSchema();

      SchemaField vectorField = schema.getField("v_scalar_confidence");
      assertNotNull(vectorField);

      ScalarQuantizedDenseVectorField vectorType =
          (ScalarQuantizedDenseVectorField) vectorField.getType();
      assertThat(vectorType.getConfidenceInterval(), is(0.91F));
    } finally {
      deleteCore();
    }
  }

  @Test
  public void fieldDefinition_dynamicConfidenceInterval_shouldLoadSchemaField() throws Exception {
    try {
      initCore("solrconfig_codec.xml", "schema-densevector-quantized.xml");

      IndexSchema schema = h.getCore().getLatestSchema();

      SchemaField vectorField = schema.getField("v_scalar_dynamic");
      assertNotNull(vectorField);

      ScalarQuantizedDenseVectorField vectorType =
          (ScalarQuantizedDenseVectorField) vectorField.getType();
      assertThat(
          vectorType.getConfidenceInterval(),
          is(Lucene99ScalarQuantizedVectorsFormat.DYNAMIC_CONFIDENCE_INTERVAL));
    } finally {
      deleteCore();
    }
  }
}
