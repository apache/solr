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
package org.apache.solr.search.function;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.SolrParams;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestDenseVectorFunctionQuery extends SolrTestCaseJ4 {
  String IDField = "id";
  String vectorField = "vector";
  String vectorField2 = "vector2";
  String byteVectorField = "vector_byte_encoding";

  @Before
  public void prepareIndex() throws Exception {
    /* vectorDimension="4" similarityFunction="cosine" */
    initCore("solrconfig-basic.xml", "schema-densevector.xml");

    List<SolrInputDocument> docsToIndex = this.prepareDocs();
    for (SolrInputDocument doc : docsToIndex) {
      assertU(adoc(doc));
    }

    assertU(commit());
  }

  @After
  public void cleanUp() {
    clearIndex();
    deleteCore();
  }

  private List<SolrInputDocument> prepareDocs() {
    int docsCount = 6;
    List<SolrInputDocument> docs = new ArrayList<>(docsCount);
    for (int i = 1; i < docsCount + 1; i++) {
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField(IDField, i);
      docs.add(doc);
    }

    docs.get(0).addField(vectorField, Arrays.asList(1f, 2f, 3f, 4f));
    docs.get(1).addField(vectorField, Arrays.asList(1.5f, 2.5f, 3.5f, 4.5f));
    docs.get(2).addField(vectorField, Arrays.asList(7.5f, 15.5f, 17.5f, 22.5f));

    docs.get(0).addField(vectorField2, Arrays.asList(5f, 4f, 1f, 2f));
    docs.get(1).addField(vectorField2, Arrays.asList(2f, 2f, 1f, 4f));
    docs.get(3).addField(vectorField, Arrays.asList(1.4f, 2.4f, 3.4f, 4.4f));

    docs.get(0).addField(byteVectorField, Arrays.asList(1, 2, 3, 4));
    docs.get(1).addField(byteVectorField, Arrays.asList(4, 2, 3, 1));

    return docs;
  }

  @Test
  public void floatConstantVectors_shouldReturnFloatSimilarity() {
    assertQ(
        req(
            CommonParams.Q,
            "{!func} vectorSimilarity(FLOAT32, COSINE, [1,2,3], [4,5,6])",
            "fq",
            "id:(1 2 3)",
            "fl",
            "id, score"),
        "//result[@numFound='" + 3 + "']",
        "//result/doc[1]/float[@name='score'][.='0.9873159']",
        "//result/doc[2]/float[@name='score'][.='0.9873159']",
        "//result/doc[3]/float[@name='score'][.='0.9873159']");
  }

  @Test
  public void byteConstantVectors_shouldReturnFloatSimilarity() {
    assertQ(
        req(
            CommonParams.Q,
            "{!func} vectorSimilarity(BYTE, COSINE, [1,2,3], [4,5,6])",
            "fq",
            "id:(1 2 3)",
            "fl",
            "id, score"),
        "//result[@numFound='" + 3 + "']",
        "//result/doc[1]/float[@name='score'][.='0.9873159']",
        "//result/doc[2]/float[@name='score'][.='0.9873159']",
        "//result/doc[3]/float[@name='score'][.='0.9873159']");
  }

  @Test
  public void floatFieldVectors_shouldReturnFloatSimilarity() {
    assertQ(
        req(
            CommonParams.Q,
            "{!func} vectorSimilarity(FLOAT32, DOT_PRODUCT, vector, vector2)",
            "fq",
            "id:(1 2)",
            "fl",
            "id, score"),
        "//result[@numFound='" + 2 + "']",
        "//result/doc[1]/float[@name='score'][.='15.25']",
        "//result/doc[2]/float[@name='score'][.='12.5']");
  }

  @Test
  public void byteFieldVectors_shouldReturnFloatSimilarity() {
    assertQ(
        req(
            CommonParams.Q,
            "{!func} vectorSimilarity(BYTE, EUCLIDEAN, vector_byte_encoding, vector_byte_encoding)",
            "fq",
            "id:(1 2)",
            "fl",
            "id, score"),
        "//result[@numFound='" + 2 + "']",
        "//result/doc[1]/float[@name='score'][.='1.0']",
        "//result/doc[2]/float[@name='score'][.='1.0']");
  }

  @Test
  public void resultOfVectorFunction_canBeUsedAsFloatFunctionInput() {

    assertQ(
        req(
            CommonParams.Q,
            "{!func} sub(1.5, vectorSimilarity(FLOAT32, EUCLIDEAN, [1,5,4,3], vector))",
            "fq",
            "id:(1 2)",
            "fl",
            "id, score"),
        "//result[@numFound='" + 2 + "']",
        "//result/doc[1]/float[@name='score'][.='1.4166666']",
        "//result/doc[2]/float[@name='score'][.='1.4']");
  }

  @Test
  public void byteFieldVectors_missingFieldValue_shouldReturnSimilarityZero() {
    assertQ(
        req(
            CommonParams.Q,
            "{!func} vectorSimilarity(BYTE, EUCLIDEAN,  [1,5,4,3], vector_byte_encoding)",
            "fq",
            "id:3",
            "fl",
            "id, score"),
        "//result[@numFound='" + 1 + "']",
        "//result/doc[1]/float[@name='score'][.='0.0']");
  }

  @Test
  public void floatFieldVectors_missingFieldValue_shouldReturnSimilarityZero() {

    // document 3 does not contain value for vector2
    assertQ(
        req(
            CommonParams.Q,
            "{!func} vectorSimilarity(FLOAT32, DOT_PRODUCT, [1,5,4,3], vector2)",
            "fq",
            "id:(3)",
            "fl",
            "id, score"),
        "//result[@numFound='" + 1 + "']",
        "//result/doc[1]/float[@name='score'][.='0.0']");
  }

  @Test
  public void vectorQueryInRerankQParser_ShouldRescoreOnlyFirstKResults() {
    assertQ(
        req(
            CommonParams.Q,
            "id:(1 2 3 4)",
            "rq",
            "{!rerank reRankQuery=$rqq reRankDocs=2 reRankWeight=1}",
            "rqq",
            "{!func} vectorSimilarity(FLOAT32, EUCLIDEAN, [1,5,4,3], vector)",
            "fl",
            "id, score"),
        "//result[@numFound='" + 4 + "']",
        "//result/doc[1]/float[@name='score'][.='0.8002023']",
        "//result/doc[2]/float[@name='score'][.='0.7835356']",
        "//result/doc[3]/float[@name='score'][.='0.7002023']",
        "//result/doc[4]/float[@name='score'][.='0.7002023']");
  }

  @Test
  public void testReportsErrorInvalidNumberOfArgs() {
    assertQEx(
        "vectorSimilarity test number of arguments failed!",
        "Invalid number of arguments. Please provide either two or four arguments.",
        req(CommonParams.Q, "{!func} vectorSimilarity()", "fq", "id:(1 2 3)", "fl", "id, score"),
        SolrException.ErrorCode.BAD_REQUEST);
    assertQEx(
        "vectorSimilarity test number of arguments failed!",
        "Invalid number of arguments. Please provide either two or four arguments.",
        req(
            CommonParams.Q,
            "{!func} vectorSimilarity(vector)",
            "fq",
            "id:(1 2 3)",
            "fl",
            "id, score"),
        SolrException.ErrorCode.BAD_REQUEST);
    assertQEx(
        "vectorSimilarity test number of arguments failed!",
        "Invalid number of arguments. Please provide either two or four arguments.",
        req(
            CommonParams.Q,
            "{!func} vectorSimilarity(vector,)",
            "fq",
            "id:(1 2 3)",
            "fl",
            "id, score"),
        SolrException.ErrorCode.BAD_REQUEST);
  }

  @Test
  public void testReportsErrorInvalidArgs() {
    assertQEx(
        "vectorSimilarity 2arg: first arg non-vector field",
        "undefined field: \"bogus\"",
        req(CommonParams.Q, "{!func} vectorSimilarity(bogus, vector_byte_encoding)"),
        SolrException.ErrorCode.BAD_REQUEST);
    assertQEx(
        "vectorSimilarity 2arg: second arg non-vector field",
        "undefined field: \"bogus\"",
        req(CommonParams.Q, "{!func} vectorSimilarity(vector_byte_encoding, bogus)"),
        SolrException.ErrorCode.BAD_REQUEST);
    assertQEx(
        "vectorSimilarity 3+ args: 1st arg not valid encoding",
        "Invalid argument: BOGUS is not a valid VectorEncoding. Expected one of [",
        req(
            CommonParams.Q,
            "{!func} vectorSimilarity(BOGUS, DOT_PRODUCT, vector_byte_encoding, vector_byte_encoding)"),
        SolrException.ErrorCode.BAD_REQUEST);
    assertQEx(
        "vectorSimilarity 3+ args: 2nd arg not valid encoding",
        "Invalid argument: BOGUS is not a valid VectorSimilarityFunction. Expected one of [",
        req(
            CommonParams.Q,
            "{!func} vectorSimilarity(BYTE, BOGUS, vector_byte_encoding, vector_byte_encoding)"),
        SolrException.ErrorCode.BAD_REQUEST);
    assertQEx(
        "vectorSimilarity 3 args: first two are valid for 2 arg syntax",
        "SyntaxError: Expected ')'",
        req(CommonParams.Q, "{!func} vectorSimilarity(vector_byte_encoding,[1,2,3,3],BOGUS)"),
        SolrException.ErrorCode.BAD_REQUEST);
    assertQEx(
        "vectorSimilarity 3 args: first two are valid for 4 arg syntax, w/valid 3rd arg field",
        "SyntaxError: Expected identifier",
        req(CommonParams.Q, "{!func} vectorSimilarity(BYTE, DOT_PRODUCT, vector_byte_encoding)"),
        SolrException.ErrorCode.BAD_REQUEST);
    assertQEx(
        "vectorSimilarity 3 args: first two are valid for 4 arg syntax, w/valid 3rd arg const vector",
        "SyntaxError: Expected identifier",
        req(CommonParams.Q, "{!func} vectorSimilarity(BYTE, DOT_PRODUCT, [1,2,3,3])"),
        SolrException.ErrorCode.BAD_REQUEST);
    assertQEx(
        "vectorSimilarity 5 args: valid 4 arg syntax with extra cruft",
        "SyntaxError: Expected ')'",
        req(
            CommonParams.Q,
            "{!func} vectorSimilarity(BYTE, DOT_PRODUCT, vector_byte_encoding, vector_byte_encoding, BOGUS)"),
        SolrException.ErrorCode.BAD_REQUEST);
  }

  @Test
  public void test2ArgsByteFieldAndConstVector() throws Exception {
    for (SolrParams main :
        Arrays.asList(
            params(CommonParams.Q, "{!func} vectorSimilarity(vector_byte_encoding, [1,2,3,3])"),
            params(
                CommonParams.Q,
                "{!func} vectorSimilarity(vector_byte_encoding, $vec)",
                "vec",
                "[1,2,3,3]"))) {
      assertQ(
          req(main, "fq", "id:(1 2)", "fl", "id, score", "rows", "1"),
          "//result[@numFound='" + 2 + "']",
          "//result/doc[1]/str[@name='id'][.=1]");
    }
    for (SolrParams main :
        Arrays.asList(
            params(CommonParams.Q, "{!func} vectorSimilarity(vector_byte_encoding, [3,3,2,1])"),
            params(
                CommonParams.Q,
                "{!func} vectorSimilarity(vector_byte_encoding, $vec)",
                "vec",
                "[3,3,2,1]"))) {

      assertQ(
          req(main, "fq", "id:(1 2)", "fl", "id, score", "rows", "1"),
          "//result[@numFound='" + 2 + "']",
          "//result/doc[1]/str[@name='id'][.=2]");
    }
  }

  @Test
  public void test2ArgsFloatFieldAndConstVector() throws Exception {
    for (SolrParams main :
        Arrays.asList(
            params(CommonParams.Q, "{!func} vectorSimilarity(vector, [1,2,3,3])"),
            params(CommonParams.Q, "{!func} vectorSimilarity(vector, $vec)", "vec", "[1,2,3,3]"))) {
      assertQ(
          req(main, "fq", "id:(1 2 3)", "fl", "id, score"),
          "//result[@numFound='" + 3 + "']",
          "//result/doc[1]/str[@name='id'][.=2]",
          "//result/doc[2]/str[@name='id'][.=3]",
          "//result/doc[3]/str[@name='id'][.=1]");
    }
  }

  @Test
  public void test2ArgsFloatVectorField() throws Exception {
    assertQ(
        req(
            CommonParams.Q,
            "{!func} vectorSimilarity(vector, vector2)",
            "fq",
            "id:(1 2 3 4)",
            "fl",
            "id, score"),
        "//result[@numFound='" + 4 + "']",
        "//result/doc[1]/str[@name='id'][.=2]",
        "//result/doc[2]/str[@name='id'][.=1]");
  }

  @Test
  public void test2ArgsIfEitherFieldMissingValueDocScoreZero() {
    assertQ(
        req(
            CommonParams.Q,
            "{!func} vectorSimilarity(vector, vector2)",
            "fq",
            "id:(3)",
            "fl",
            "id, score"),
        "//result[@numFound='" + 1 + "']",
        "//result/doc[1]/float[@name='score'][.=0.0]");
    assertQ(
        req(
            CommonParams.Q,
            "{!func} vectorSimilarity(vector, vector2)",
            "fq",
            "id:(4)",
            "fl",
            "id, score"),
        "//result[@numFound='" + 1 + "']",
        "//result/doc[1]/float[@name='score'][.=0.0]");
  }
}
