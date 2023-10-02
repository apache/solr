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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.core.AbstractBadConfigTestBase;
import org.apache.solr.util.vector.DenseVectorParser;
import org.hamcrest.MatcherAssert;
import org.junit.Before;
import org.junit.Test;

public class DenseVectorFieldTest extends AbstractBadConfigTestBase {

  private DenseVectorField toTestFloatEncoding;
  private DenseVectorField toTestByteEncoding;

  @Before
  public void init() {
    toTestFloatEncoding = new DenseVectorField(3, VectorEncoding.FLOAT32);
    toTestByteEncoding = new DenseVectorField(3, VectorEncoding.BYTE);
  }

  @Test
  public void fieldTypeDefinition_badVectorDimension_shouldThrowException() throws Exception {
    assertConfigs(
        "solrconfig-basic.xml",
        "bad-schema-densevector-dimension.xml",
        "For input string: \"4.6\"");
  }

  @Test
  public void fieldTypeDefinition_nullVectorDimension_shouldThrowException() throws Exception {
    assertConfigs(
        "solrconfig-basic.xml",
        "bad-schema-densevector-dimension-null.xml",
        "the vector dimension is a mandatory parameter");
  }

  @Test
  public void fieldTypeDefinition_badSimilarityDistance_shouldThrowException() throws Exception {
    assertConfigs(
        "solrconfig-basic.xml",
        "bad-schema-densevector-similarity.xml",
        "No enum constant org.apache.lucene.index.VectorSimilarityFunction.NOT_EXISTENT");
  }

  @Test
  public void fieldDefinition_docValues_shouldThrowException() throws Exception {
    assertConfigs(
        "solrconfig-basic.xml",
        "bad-schema-densevector-docvalues.xml",
        "DenseVectorField fields can not have docValues: vector");
  }

  @Test
  public void fieldDefinition_multiValued_shouldThrowException() throws Exception {
    assertConfigs(
        "solrconfig-basic.xml",
        "bad-schema-densevector-multivalued.xml",
        "DenseVectorField fields can not be multiValued: vector");
  }

  @Test
  public void fieldTypeDefinition_nullSimilarityDistance_shouldUseDefaultSimilarityEuclidean()
      throws Exception {
    try {
      initCore("solrconfig-basic.xml", "schema-densevector-similarity-null.xml");
      IndexSchema schema = h.getCore().getLatestSchema();

      SchemaField vector = schema.getField("vector");
      assertNotNull(vector);

      DenseVectorField type = (DenseVectorField) vector.getType();
      MatcherAssert.assertThat(
          type.getSimilarityFunction(), is(VectorSimilarityFunction.EUCLIDEAN));
      MatcherAssert.assertThat(type.getDimension(), is(4));

      assertTrue(vector.indexed());
      assertTrue(vector.stored());
    } finally {
      deleteCore();
    }
  }

  @Test
  public void fieldDefinition_correctConfiguration_shouldLoadSchemaField() throws Exception {
    try {
      initCore("solrconfig-basic.xml", "schema-densevector.xml");
      IndexSchema schema = h.getCore().getLatestSchema();

      SchemaField vector = schema.getField("vector");
      assertNotNull(vector);

      DenseVectorField type = (DenseVectorField) vector.getType();
      MatcherAssert.assertThat(type.getSimilarityFunction(), is(VectorSimilarityFunction.COSINE));
      MatcherAssert.assertThat(type.getDimension(), is(4));

      assertTrue(vector.indexed());
      assertTrue(vector.stored());
    } finally {
      deleteCore();
    }
  }

  @Test
  public void fieldDefinition_advancedCodecHyperParameter_shouldLoadSchemaField() throws Exception {
    try {
      initCore("solrconfig_codec.xml", "schema-densevector-codec-hyperparameter.xml");
      IndexSchema schema = h.getCore().getLatestSchema();

      SchemaField vector = schema.getField("vector");
      assertNotNull(vector);

      DenseVectorField type1 = (DenseVectorField) vector.getType();
      MatcherAssert.assertThat(type1.getSimilarityFunction(), is(VectorSimilarityFunction.COSINE));
      MatcherAssert.assertThat(type1.getDimension(), is(4));
      MatcherAssert.assertThat(type1.getKnnAlgorithm(), is("hnsw"));
      MatcherAssert.assertThat(type1.getHnswMaxConn(), is(10));
      MatcherAssert.assertThat(type1.getHnswBeamWidth(), is(40));

      SchemaField vector2 = schema.getField("vector2");
      assertNotNull(vector2);

      DenseVectorField type2 = (DenseVectorField) vector2.getType();
      MatcherAssert.assertThat(type2.getSimilarityFunction(), is(VectorSimilarityFunction.COSINE));
      MatcherAssert.assertThat(type2.getDimension(), is(4));
      MatcherAssert.assertThat(type2.getKnnAlgorithm(), is("hnsw"));
      MatcherAssert.assertThat(type2.getHnswMaxConn(), is(6));
      MatcherAssert.assertThat(type2.getHnswBeamWidth(), is(60));

      SchemaField vector3 = schema.getField("vector3");
      assertNotNull(vector3);

      DenseVectorField type3 = (DenseVectorField) vector3.getType();
      MatcherAssert.assertThat(type3.getSimilarityFunction(), is(VectorSimilarityFunction.COSINE));
      MatcherAssert.assertThat(type3.getDimension(), is(5));

      MatcherAssert.assertThat(type3.getKnnAlgorithm(), is("hnsw"));
      MatcherAssert.assertThat(type3.getHnswMaxConn(), is(8));
      MatcherAssert.assertThat(type3.getHnswBeamWidth(), is(46));

      SchemaField vectorDefault = schema.getField("vector_default");
      assertNotNull(vectorDefault);

      DenseVectorField typeDefault = (DenseVectorField) vectorDefault.getType();
      MatcherAssert.assertThat(
          typeDefault.getSimilarityFunction(), is(VectorSimilarityFunction.COSINE));
      MatcherAssert.assertThat(typeDefault.getKnnAlgorithm(), is("hnsw"));
      MatcherAssert.assertThat(typeDefault.getDimension(), is(4));
      MatcherAssert.assertThat(typeDefault.getHnswMaxConn(), is(16));
      MatcherAssert.assertThat(typeDefault.getHnswBeamWidth(), is(100));
    } finally {
      deleteCore();
    }
  }

  @Test
  public void parseVector_NotAList_shouldThrowException() {
    RuntimeException thrown =
        assertThrows(
            "Single string value should throw an exception",
            SolrException.class,
            () -> {
              toTestFloatEncoding
                  .getVectorBuilder("string", DenseVectorParser.BuilderPhase.INDEX)
                  .getFloatVector();
            });
    MatcherAssert.assertThat(
        thrown.getMessage(),
        is(
            "incorrect vector format. The expected format is:'[f1,f2..f3]' where each element f is a float"));

    thrown =
        assertThrows(
            "Single string value should throw an exception",
            SolrException.class,
            () -> {
              toTestByteEncoding
                  .getVectorBuilder("string", DenseVectorParser.BuilderPhase.INDEX)
                  .getByteVector();
            });
    MatcherAssert.assertThat(
        thrown.getMessage(),
        is(
            "incorrect vector format. The expected format is:'[b1,b2..b3]' where each element b is a byte (-128 to 127)"));

    thrown =
        assertThrows(
            "Single float value should throw an exception",
            SolrException.class,
            () -> {
              toTestFloatEncoding
                  .getVectorBuilder(1.5f, DenseVectorParser.BuilderPhase.INDEX)
                  .getFloatVector();
            });
    MatcherAssert.assertThat(
        thrown.getMessage(),
        is(
            "incorrect vector format. The expected format is:'[f1,f2..f3]' where each element f is a float"));

    thrown =
        assertThrows(
            "Single string value should throw an exception",
            SolrException.class,
            () -> {
              toTestByteEncoding
                  .getVectorBuilder("1", DenseVectorParser.BuilderPhase.INDEX)
                  .getByteVector();
            });
    MatcherAssert.assertThat(
        thrown.getMessage(),
        is(
            "incorrect vector format. The expected format is:'[b1,b2..b3]' where each element b is a byte (-128 to 127)"));
  }

  @Test
  public void parseVector_notNumericList_shouldThrowException() {
    RuntimeException thrown =
        assertThrows(
            "Incorrect elements should throw an exception",
            SolrException.class,
            () -> {
              toTestFloatEncoding
                  .getVectorBuilder(
                      Arrays.asList(
                          new DenseVectorField(3),
                          new DenseVectorField(4),
                          new DenseVectorField(5)),
                      DenseVectorParser.BuilderPhase.INDEX)
                  .getFloatVector();
            });
    MatcherAssert.assertThat(
        thrown.getMessage(),
        is(
            "incorrect vector format. The expected format is:'[f1,f2..f3]' where each element f is a float"));

    thrown =
        assertThrows(
            "Incorrect elements should throw an exception",
            SolrException.class,
            () -> {
              toTestByteEncoding
                  .getVectorBuilder(
                      Arrays.asList(
                          new DenseVectorField(3),
                          new DenseVectorField(4),
                          new DenseVectorField(5)),
                      DenseVectorParser.BuilderPhase.INDEX)
                  .getByteVector();
            });
    MatcherAssert.assertThat(
        thrown.getMessage(),
        is(
            "incorrect vector format. The expected format is:'[b1,b2..b3]' where each element b is a byte (-128 to 127)"));
  }

  @Test
  public void parseVector_incorrectVectorDimension_shouldThrowException() {
    toTestFloatEncoding = new DenseVectorField(3);

    RuntimeException thrown =
        assertThrows(
            "Incorrect vector dimension should throw an exception",
            SolrException.class,
            () -> {
              toTestFloatEncoding
                  .getVectorBuilder(Arrays.asList(1.0f, 1.5f), DenseVectorParser.BuilderPhase.INDEX)
                  .getFloatVector();
            });
    MatcherAssert.assertThat(
        thrown.getMessage(),
        is(
            "incorrect vector dimension. The vector value has size 2 while it is expected a vector with size 3"));

    thrown =
        assertThrows(
            "Incorrect vector dimension should throw an exception",
            SolrException.class,
            () -> {
              toTestByteEncoding
                  .getVectorBuilder(Arrays.asList(2, 1), DenseVectorParser.BuilderPhase.INDEX)
                  .getByteVector();
            });
    MatcherAssert.assertThat(
        thrown.getMessage(),
        is(
            "incorrect vector dimension. The vector value has size 2 while it is expected a vector with size 3"));
  }

  @Test
  public void parseVector_incorrectElement_shouldThrowException() {
    RuntimeException thrown =
        assertThrows(
            "Incorrect elements should throw an exception",
            SolrException.class,
            () -> {
              toTestFloatEncoding
                  .getVectorBuilder(
                      Arrays.asList("1.0f", "string", "string2"),
                      DenseVectorParser.BuilderPhase.INDEX)
                  .getFloatVector();
            });
    MatcherAssert.assertThat(
        thrown.getMessage(),
        is(
            "incorrect vector element: 'string'. The expected format is:'[f1,f2..f3]' where each element f is a float"));

    thrown =
        assertThrows(
            "Incorrect elements should throw an exception",
            SolrException.class,
            () -> {
              toTestByteEncoding
                  .getVectorBuilder(
                      Arrays.asList("1", "string", "string2"), DenseVectorParser.BuilderPhase.INDEX)
                  .getByteVector();
            });
    MatcherAssert.assertThat(
        thrown.getMessage(),
        is(
            "incorrect vector element: 'string'. The expected format is:'[b1,b2..b3]' where each element b is a byte (-128 to 127)"));
  }

  /**
   * The inputValue is an ArrayList with a type that depends on the loader used: - {@link
   * org.apache.solr.handler.loader.XMLLoader}, {@link org.apache.solr.handler.loader.CSVLoader}
   * produces an ArrayList of String
   */
  @Test
  public void parseVector_StringArrayList_shouldParseFloatArray() {
    float[] expected = new float[] {1.1f, 2.2f, 3.3f};
    MatcherAssert.assertThat(
        toTestFloatEncoding
            .getVectorBuilder(
                Arrays.asList("1.1", "2.2", "3.3"), DenseVectorParser.BuilderPhase.INDEX)
            .getFloatVector(),
        is(expected));
  }

  @Test
  public void parseVector_StringArrayList_shouldParseByteArray() {
    byte[] expected = new byte[] {1, 2, 3};
    MatcherAssert.assertThat(
        toTestByteEncoding
            .getVectorBuilder(Arrays.asList("1", "2", "3"), DenseVectorParser.BuilderPhase.INDEX)
            .getByteVector(),
        is(expected));
  }

  /**
   * The inputValue is an ArrayList with a type that depends on the loader used: - {@link
   * org.apache.solr.handler.loader.JsonLoader} produces an ArrayList of Double
   */
  @Test
  public void parseVector_DoubleArrayList_shouldParseFloatArray() {
    float[] expected = new float[] {1.7f, 5.4f, 6.6f};
    MatcherAssert.assertThat(
        toTestFloatEncoding
            .getVectorBuilder(Arrays.asList(1.7d, 5.4d, 6.6d), DenseVectorParser.BuilderPhase.INDEX)
            .getFloatVector(),
        is(expected));
  }

  /**
   * The inputValue is an ArrayList with a type that depends on the loader used: - {@link
   * org.apache.solr.handler.loader.JavabinLoader} produces an ArrayList of Float
   */
  @Test
  public void parseVector_FloatArrayList_shouldParseFloatArray() {
    toTestFloatEncoding = new DenseVectorField(3);
    float[] expected = new float[] {5.5f, 7.7f, 9.8f};

    MatcherAssert.assertThat(
        toTestFloatEncoding
            .getVectorBuilder(Arrays.asList(5.5f, 7.7f, 9.8f), DenseVectorParser.BuilderPhase.INDEX)
            .getFloatVector(),
        is(expected));
  }

  @Test
  public void parseVector_IntArrayList_shouldParseByteArray() {
    byte[] expected = new byte[] {5, 7, 9};
    MatcherAssert.assertThat(
        toTestByteEncoding
            .getVectorBuilder(Arrays.asList(5, 7, 9), DenseVectorParser.BuilderPhase.INDEX)
            .getByteVector(),
        is(expected));
  }

  @Test
  public void indexing_notAVectorValue_shouldThrowException() throws Exception {
    try {
      initCore("solrconfig-basic.xml", "schema-densevector.xml");

      assertFailedU(adoc("id", "0", "vector", "5.4"));
      assertFailedU(adoc("id", "0", "vector", "string"));
    } finally {
      deleteCore();
    }
  }

  @Test
  public void indexing_inconsistentVectorDimension_shouldThrowException() throws Exception {
    try {
      // vectorDimension = 4
      initCore("solrconfig-basic.xml", "schema-densevector.xml");

      SolrInputDocument toFailDoc1 = new SolrInputDocument();
      toFailDoc1.addField("id", "0");
      toFailDoc1.addField("vector", Arrays.asList(1, 2, 3));

      SolrInputDocument toFailDoc2 = new SolrInputDocument();
      toFailDoc2.addField("id", "0");
      toFailDoc2.addField("vector", Arrays.asList(1, 2, 3, 4, 5));

      assertFailedU(adoc(toFailDoc1));
      assertFailedU(adoc(toFailDoc2));
    } finally {
      deleteCore();
    }
  }

  @Test
  public void indexing_correctDocument_shouldBeIndexed() throws Exception {
    try {
      initCore("solrconfig-basic.xml", "schema-densevector.xml");

      SolrInputDocument correctDoc = new SolrInputDocument();
      correctDoc.addField("id", "0");
      correctDoc.addField("vector", Arrays.asList(1, 2, 3, 4));

      assertU(adoc(correctDoc));
    } finally {
      deleteCore();
    }
  }

  @Test
  public void indexing_highDimensionalityVectorDocument_shouldBeIndexed() throws Exception {
    try {
      initCore("solrconfig_codec.xml", "schema-densevector-high-dimensionality.xml");

      List<Float> highDimensionalityVector = new ArrayList<>();
      for (float i = 0; i < 2048f; i++) {
        highDimensionalityVector.add(i);
      }
      SolrInputDocument correctDoc = new SolrInputDocument();
      correctDoc.addField("id", "0");
      correctDoc.addField("vector", highDimensionalityVector);

      assertU(adoc(correctDoc));
    } finally {
      deleteCore();
    }
  }

  @Test
  public void query_vectorFloatEncoded_storedField_shouldBeReturnedInResults() throws Exception {
    try {
      initCore("solrconfig-basic.xml", "schema-densevector.xml");

      SolrInputDocument doc1 = new SolrInputDocument();
      doc1.addField("id", "0");
      doc1.addField("vector", Arrays.asList(1.1f, 2.1f, 3.1f, 4.1f));
      assertU(adoc(doc1));
      assertU(commit());

      assertJQ(
          req("q", "id:0", "fl", "vector"), "/response/docs/[0]=={'vector':[1.1,2.1,3.1,4.1]}");

    } finally {
      deleteCore();
    }
  }

  @Test
  public void query_vectorByteEncoded_storedField_shouldBeReturnedInResults() throws Exception {
    try {
      initCore("solrconfig-basic.xml", "schema-densevector.xml");

      SolrInputDocument doc1 = new SolrInputDocument();
      doc1.addField("id", "0");
      doc1.addField("vector_byte_encoding", Arrays.asList(1, 2, 3, 4));

      assertU(adoc(doc1));
      assertU(commit());

      assertJQ(
          req("q", "id:0", "fl", "vector_byte_encoding"),
          "/response/docs/[0]=={'vector_byte_encoding':[1,2,3,4]}");

    } finally {
      deleteCore();
    }
  }

  /** Not Supported */
  @Test
  public void query_rangeSearch_shouldThrowException() throws Exception {
    try {
      initCore("solrconfig-basic.xml", "schema-densevector.xml");

      assertQEx(
          "Running Range queries on a dense vector field should raise an Exception",
          "Cannot parse 'vector:[[1.0 2.0] TO [1.5 2.5]]'",
          req("q", "vector:[[1.0 2.0] TO [1.5 2.5]]", "fl", "vector"),
          SolrException.ErrorCode.BAD_REQUEST);

      assertQEx(
          "Running Range queries on a dense vector field should raise an Exception",
          "Range Queries are not supported for Dense Vector fields."
              + " Please use the {!knn} query parser to run K nearest neighbors search queries.",
          req("q", "vector:[1 TO 5]", "fl", "vector"),
          SolrException.ErrorCode.BAD_REQUEST);
    } finally {
      deleteCore();
    }
  }

  /** Not Supported */
  @Test
  public void query_existenceSearch_shouldThrowException() throws Exception {
    try {
      initCore("solrconfig-basic.xml", "schema-densevector.xml");

      assertQEx(
          "Running Existence queries on a dense vector field should raise an Exception",
          "Range Queries are not supported for Dense Vector fields."
              + " Please use the {!knn} query parser to run K nearest neighbors search queries.",
          req("q", "vector:[* TO *]", "fl", "vector"),
          SolrException.ErrorCode.BAD_REQUEST);
    } finally {
      deleteCore();
    }
  }

  /** Not Supported */
  @Test
  public void query_fieldQuery_shouldThrowException() throws Exception {
    try {
      initCore("solrconfig-basic.xml", "schema-densevector.xml");

      assertQEx(
          "Running Field queries on a dense vector field should raise an Exception",
          "Cannot parse 'vector:[1.0, 2.0, 3.0, 4.0]",
          req("q", "vector:[1.0, 2.0, 3.0, 4.0]", "fl", "vector"),
          SolrException.ErrorCode.BAD_REQUEST);

      assertQEx(
          "Running Field queries on a dense vector field should raise an Exception",
          "Field Queries are not supported for Dense Vector fields."
              + " Please use the {!knn} query parser to run K nearest neighbors search queries.",
          req("q", "vector:\"[1.0, 2.0, 3.0, 4.0]\"", "fl", "vector"),
          SolrException.ErrorCode.BAD_REQUEST);

      assertQEx(
          "Running Field queries on a dense vector field should raise an Exception",
          "Field Queries are not supported for Dense Vector fields."
              + " Please use the {!knn} query parser to run K nearest neighbors search queries.",
          req("q", "vector:2.0", "fl", "vector"),
          SolrException.ErrorCode.BAD_REQUEST);
    } finally {
      deleteCore();
    }
  }

  /** Not Supported */
  @Test
  public void query_sortByVectorField_shouldThrowException() throws Exception {
    try {
      initCore("solrconfig-basic.xml", "schema-densevector.xml");

      assertQEx(
          "Sort over vectors should raise an Exception",
          "Cannot sort on a Dense Vector field",
          req("q", "*:*", "sort", "vector desc"),
          SolrException.ErrorCode.BAD_REQUEST);
    } finally {
      deleteCore();
    }
  }

  @Test
  public void denseVectorField_shouldBePresentAfterAtomicUpdate() throws Exception {
    assumeTrue(
        "update log must be enabled for atomic update",
        Boolean.getBoolean(System.getProperty("enable.update.log")));
    try {
      initCore("solrconfig.xml", "schema-densevector.xml");
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField("id", "0");
      doc.addField("vector", Arrays.asList(1.1, 2.2, 3.3, 4.4));
      doc.addField("vector_byte_encoding", Arrays.asList(5, 6, 7, 8));
      doc.addField("string_field", "test");

      assertU(adoc(doc));
      assertU(commit());

      assertJQ(
          req("q", "id:0", "fl", "*"),
          "/response/docs/[0]/vector==[1.1,2.2,3.3,4.4]",
          "/response/docs/[0]/vector_byte_encoding==[5,6,7,8]",
          "/response/docs/[0]/string_field==test");

      SolrInputDocument updateDoc = new SolrInputDocument();
      updateDoc.addField("id", "0");
      updateDoc.addField("string_field", Map.of("set", "other test"));
      assertU(adoc(updateDoc));
      assertU(commit());

      assertJQ(
          req("q", "id:0", "fl", "*"),
          "/response/docs/[0]/vector==[1.1,2.2,3.3,4.4]",
          "/response/docs/[0]/vector_byte_encoding==[5,6,7,8]",
          "/response/docs/[0]/string_field=='other test'");

    } finally {
      deleteCore();
    }
  }

  @Test
  public void denseVectorFieldOnAtomicUpdate_shouldBeUpdatedCorrectly() throws Exception {
    assumeTrue(
        "update log must be enabled for atomic update",
        Boolean.getBoolean(System.getProperty("enable.update.log")));
    try {
      initCore("solrconfig.xml", "schema-densevector.xml");
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField("id", "0");
      doc.addField("vector", Arrays.asList(1.1, 2.2, 3.3, 4.4));
      doc.addField("vector_byte_encoding", Arrays.asList(5, 6, 7, 8));
      doc.addField("string_field", "test");

      assertU(adoc(doc));
      assertU(commit());

      assertJQ(
          req("q", "id:0", "fl", "*"),
          "/response/docs/[0]/vector==[1.1,2.2,3.3,4.4]",
          "/response/docs/[0]/vector_byte_encoding==[5,6,7,8]",
          "/response/docs/[0]/string_field==test");

      SolrInputDocument updateDoc = new SolrInputDocument();
      updateDoc.addField("id", "0");
      updateDoc.addField("vector", Map.of("set", Arrays.asList(9.2, 2.2, 3.3, 5.2)));
      updateDoc.addField("vector_byte_encoding", Map.of("set", Arrays.asList(8, 3, 1, 3)));
      assertU(adoc(updateDoc));
      assertU(commit());

      assertJQ(
          req("q", "id:0", "fl", "*"),
          "/response/docs/[0]/vector==[9.2,2.2,3.3,5.2]",
          "/response/docs/[0]/vector_byte_encoding==[8,3,1,3]",
          "/response/docs/[0]/string_field=='test'");

    } finally {
      deleteCore();
    }
  }

  @Test
  public void denseVectorByteEncoding_shouldRaiseExceptionWithValuesOutsideBoundaries()
      throws Exception {
    try {
      initCore("solrconfig.xml", "schema-densevector.xml");
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField("id", "0");
      doc.addField("vector_byte_encoding", Arrays.asList(128, 6, 7, 8));

      RuntimeException thrown =
          assertThrows(
              "Incorrect elements should throw an exception",
              SolrException.class,
              () -> {
                assertU(adoc(doc));
              });

      MatcherAssert.assertThat(
          thrown.getCause().getMessage(),
          is(
              "Error while creating field 'vector_byte_encoding{type=knn_vector_byte_encoding,properties=indexed,stored}' from value '[128, 6, 7, 8]'"));

      MatcherAssert.assertThat(
          thrown.getCause().getCause().getMessage(),
          is(
              "incorrect vector element: '128'. The expected format is:'[b1,b2..b3]' where each element b is a byte (-128 to 127)"));

      SolrInputDocument doc1 = new SolrInputDocument();
      doc1.addField("id", "1");
      doc1.addField("vector_byte_encoding", Arrays.asList(1, -129, 7, 8));

      thrown =
          assertThrows(
              "Incorrect elements should throw an exception",
              SolrException.class,
              () -> {
                assertU(adoc(doc1));
              });

      MatcherAssert.assertThat(
          thrown.getCause().getMessage(),
          is(
              "Error while creating field 'vector_byte_encoding{type=knn_vector_byte_encoding,properties=indexed,stored}' from value '[1, -129, 7, 8]'"));
      MatcherAssert.assertThat(
          thrown.getCause().getCause().getMessage(),
          is(
              "incorrect vector element: '-129'. The expected format is:'[b1,b2..b3]' where each element b is a byte (-128 to 127)"));
    } finally {
      deleteCore();
    }
  }

  @Test
  public void denseVectorByteEncoding_shouldRaiseExceptionWithFloatValues() throws Exception {
    try {
      initCore("solrconfig.xml", "schema-densevector.xml");
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField("id", "0");
      doc.addField("vector_byte_encoding", Arrays.asList(14.3, 6.2, 7.2, 8.1));

      RuntimeException thrown =
          assertThrows(
              "Incorrect elements should throw an exception",
              SolrException.class,
              () -> {
                assertU(adoc(doc));
              });

      MatcherAssert.assertThat(
          thrown.getCause().getMessage(),
          is(
              "Error while creating field 'vector_byte_encoding{type=knn_vector_byte_encoding,properties=indexed,stored}' from value '[14.3, 6.2, 7.2, 8.1]'"));

      MatcherAssert.assertThat(
          thrown.getCause().getCause().getMessage(),
          is(
              "incorrect vector element: '14.3'. The expected format is:'[b1,b2..b3]' where each element b is a byte (-128 to 127)"));
    } finally {
      deleteCore();
    }
  }
}
