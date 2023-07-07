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
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.CommonParams;
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
}
