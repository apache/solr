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

import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.codecs.lucene104.Lucene104ScalarQuantizedVectorsFormat;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.core.AbstractBadConfigTestBase;
import org.junit.Test;

public class ScalarQuantizedDenseVectorFieldTest extends AbstractBadConfigTestBase {
  @Test
  public void fieldTypeDefinition_invalidBitSize_shouldThrowException() throws Exception {
    assertConfigs(
        "solrconfig-basic.xml",
        "bad-schema-densevector-quantized-bits.xml",
        "ScalarQuantizedDenseVectorField No encoding for 6 bits: v_scalar_bits");
  }

  @Test
  public void fieldTypeDefinition_flatAlgorithm_byteEncoding_shouldThrowException()
      throws Exception {
    assertConfigs(
        "solrconfig-basic.xml",
        "bad-schema-densevector-flat-scalarQuantized-byte.xml",
        "vectorEncoding 'BYTE' is not supported");
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
      assertThat(vectorType.getConfidenceInterval(), is(0f));
    } finally {
      deleteCore();
    }
  }

  @Test
  public void fieldDefinition_flatAlgorithm_shouldLoadSchemaField() throws Exception {
    try {
      initCore("solrconfig_codec.xml", "schema-densevector-flat-scalarQuantized.xml");
      IndexSchema schema = h.getCore().getLatestSchema();

      SchemaField vector = schema.getField("vector_sq_flat");
      assertNotNull(vector);

      ScalarQuantizedDenseVectorField type = (ScalarQuantizedDenseVectorField) vector.getType();
      assertThat(type.getKnnAlgorithm(), is("flat"));
      assertThat(type.getDimension(), is(4));
      assertThat(type.getSimilarityFunction(), is(VectorSimilarityFunction.COSINE));
      assertThat(type.getBits(), is(ScalarQuantizedDenseVectorField.DEFAULT_BITS));

      assertTrue(vector.indexed());
      assertTrue(vector.stored());
    } finally {
      deleteCore();
    }
  }

  @Test
  public void flatAlgorithm_buildKnnVectorsFormat_shouldReturnScalarQuantizedFormat()
      throws Exception {
    try {
      initCore("solrconfig_codec.xml", "schema-densevector-flat-scalarQuantized.xml");
      IndexSchema schema = h.getCore().getLatestSchema();

      SchemaField vector = schema.getField("vector_sq_flat");
      ScalarQuantizedDenseVectorField type = (ScalarQuantizedDenseVectorField) vector.getType();

      assertThat(
          type.buildKnnVectorsFormat() instanceof Lucene104ScalarQuantizedVectorsFormat, is(true));
    } finally {
      deleteCore();
    }
  }

  @Test
  public void flatAlgorithm_vectorSimilarityFunction_shouldReturnResults() throws Exception {
    try {
      initCore("solrconfig_codec.xml", "schema-densevector-flat-scalarQuantized.xml");

      addDoc("0", 1.0f, 2.0f, 3.0f, 4.0f);
      addDoc("1", 2.0f, 3.0f, 4.0f, 5.0f);
      addDoc("2", 100.0f, 200.0f, 50.0f, 25.0f);

      assertU(commit());

      assertJQ(
          req(
              "q", "{!func}vectorSimilarity(vector_sq_flat,[1.0, 2.0, 3.0, 4.0])",
              "fl", "id,score"),
          "/response/numFound==3",
          "/response/docs/[0]/id=='0'");

      assertJQ(
          req(
              "q", "{!func}vectorSimilarity(vector_sq_flat,[1.0, 2.0, 3.0, 4.0])",
              "fq", "id:(0 2)",
              "fl", "id,score"),
          "/response/numFound==2",
          "/response/docs/[0]/id=='0'");
    } finally {
      deleteCore();
    }
  }

  @Test
  public void flatAlgorithm_knnQuery_shouldReturnResults() throws Exception {
    try {
      initCore("solrconfig_codec.xml", "schema-densevector-flat-scalarQuantized.xml");

      addDoc("0", 1.0f, 2.0f, 3.0f, 4.0f);
      addDoc("1", 2.0f, 3.0f, 4.0f, 5.0f);
      addDoc("2", 100.0f, 200.0f, 50.0f, 25.0f);

      assertU(commit());

      assertJQ(
          req(
              "q", "{!knn f=vector_sq_flat topK=2}[1.0, 2.0, 3.0, 4.0]",
              "fl", "id,score"),
          "/response/numFound==2",
          "/response/docs/[0]/id=='0'",
          "/response/docs/[1]/id=='1'");
    } finally {
      deleteCore();
    }
  }

  @Test
  public void flatAlgorithm_knnQuery_preFilter_shouldReturnFilteredResults() throws Exception {
    try {
      initCore("solrconfig_codec.xml", "schema-densevector-flat-scalarQuantized.xml");

      addDoc("0", 1.0f, 2.0f, 3.0f, 4.0f);
      addDoc("1", 2.0f, 3.0f, 4.0f, 5.0f);
      addDoc("2", 100.0f, 200.0f, 50.0f, 25.0f);

      assertU(commit());

      assertJQ(
          req(
              "q", "{!knn f=vector_sq_flat topK=2 preFilter='id:(1 2)'}[1.0, 2.0, 3.0, 4.0]",
              "fl", "id,score"),
          "/response/numFound==2",
          "/response/docs/[0]/id=='1'",
          "/response/docs/[1]/id=='2'");
    } finally {
      deleteCore();
    }
  }

  @Test
  public void flatAlgorithm_knnQuery_hnswParamsIgnored_shouldReturnResults() throws Exception {
    try {
      initCore("solrconfig_codec.xml", "schema-densevector-flat-scalarQuantized.xml");

      addDoc("0", 1.0f, 2.0f, 3.0f, 4.0f);
      addDoc("1", 2.0f, 3.0f, 4.0f, 5.0f);

      assertU(commit());

      assertJQ(
          req(
              "q",
              "{!knn f=vector_sq_flat topK=1 efSearchScaleFactor=2.0"
                  + " earlyTermination=true saturationThreshold=0.95 patience=3"
                  + " filteredSearchThreshold=60}[1.0, 2.0, 3.0, 4.0]",
              "fl",
              "id,score"),
          "/response/numFound==1",
          "/response/docs/[0]/id=='0'");
    } finally {
      deleteCore();
    }
  }

  @Test
  public void flatAlgorithm_vectorSimilarityQParser_shouldReturnResults() throws Exception {
    try {
      initCore("solrconfig_codec.xml", "schema-densevector-flat-scalarQuantized.xml");

      addDoc("0", 1.0f, 2.0f, 3.0f, 4.0f);
      addDoc("1", 2.0f, 3.0f, 4.0f, 5.0f);
      addDoc("2", 100.0f, 200.0f, 50.0f, 25.0f);

      assertU(commit());

      assertJQ(
          req(
              "q", "{!vectorSimilarity f=vector_sq_flat minReturn=0.0}[1.0, 2.0, 3.0, 4.0]",
              "fl", "id,score"),
          "/response/numFound==3",
          "/response/docs/[0]/id=='0'");
    } finally {
      deleteCore();
    }
  }

  private void addDoc(String id, float... v) {
    SolrInputDocument doc = new SolrInputDocument();
    doc.addField("id", id);
    List<Float> vector = new ArrayList<>(v.length);
    for (float value : v) {
      vector.add(value);
    }
    doc.addField("vector_sq_flat", vector);
    assertU(adoc(doc));
  }
}
