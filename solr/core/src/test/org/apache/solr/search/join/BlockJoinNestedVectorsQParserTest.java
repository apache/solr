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
package org.apache.solr.search.join;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.util.RandomNoReverseMergePolicyFactory;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TestRule;

public class BlockJoinNestedVectorsQParserTest extends SolrTestCaseJ4 {
  private static final List<Float> FLOAT_QUERY_VECTOR = Arrays.asList(1.0f, 1.0f, 1.0f, 1.0f);
  private static final List<Integer> BYTE_QUERY_VECTOR = Arrays.asList(1, 1, 1, 1);
  private static final List<Float> BINARY_QUANTIZED_QUERY_VECTOR =
      Arrays.asList(1.0f, 2.0f, 3.0f, 4.0f);

  @ClassRule
  public static final TestRule noReverseMerge = RandomNoReverseMergePolicyFactory.createRule();

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml", "schema15.xml");
    prepareIndex();
  }

  public static void prepareIndex() throws Exception {
    List<SolrInputDocument> docsToIndex = prepareDocs();
    for (SolrInputDocument doc : docsToIndex) {
      assertU(adoc(doc));
    }
    assertU(commit());
  }

  /**
   * The documents in the index are 10 parents, with some parent level metadata and 30 nested
   * documents (with vectors and children level metadata) Each parent document has 3 nested
   * documents with vectors.
   *
   * <p>This allows to run knn queries both at parent/children level and using various pre-filters
   * both for parent metadata and children.
   *
   * @return a list of documents to index
   */
  private static List<SolrInputDocument> prepareDocs() {
    int totalParentDocuments = 10;
    int totalNestedVectors = 30;
    int perParentChildren = totalNestedVectors / totalParentDocuments;

    final String[] klm = new String[] {"k", "l", "m"};
    final String[] abcdef = new String[] {"a", "b", "c", "d", "e", "f"};

    List<SolrInputDocument> docs = new ArrayList<>(totalParentDocuments);
    for (int i = 1; i < totalParentDocuments + 1; i++) {
      SolrInputDocument doc = new SolrInputDocument();
      doc.setField("id", i);
      doc.setField("parent_b", true);

      doc.setField("parent_s", abcdef[i % abcdef.length]);
      List<SolrInputDocument> children = new ArrayList<>(perParentChildren);

      // nested vector documents have a distance from the query vector inversely proportional to
      // their id
      for (int j = 0; j < perParentChildren; j++) {
        SolrInputDocument child = new SolrInputDocument();
        child.setField("id", i + "" + j);
        child.setField("child_s", klm[i % klm.length]);
        child.setField("vector", outDistanceFloat(FLOAT_QUERY_VECTOR, totalNestedVectors));
        child.setField("vector_byte", outDistanceByte(BYTE_QUERY_VECTOR, totalNestedVectors));
        totalNestedVectors--; // the higher the id of the nested document, lower the distance with
        // the query vector
        children.add(child);
      }
      doc.setField("vectors", children);
      docs.add(doc);
    }

    docs.addAll(prepareBinaryQuantizedDocs());
    return docs;
  }

  /**
   * 5 parents with 2 children each, the only documents with a binary quantized vector. Against
   * {@link #BINARY_QUANTIZED_QUERY_VECTOR}, binary quantization ranks bq_child_3 above the exact
   * match bq_child_1, so these documents can show re-ranking changing the result.
   */
  private static List<SolrInputDocument> prepareBinaryQuantizedDocs() {
    List<List<Float>> vectors =
        List.of(
            Arrays.asList(1f, 2f, 3f, 4f),
            Arrays.asList(1.5f, 2.5f, 3.5f, 4.5f),
            Arrays.asList(7.5f, 15.5f, 17.5f, 22.5f),
            Arrays.asList(1.4f, 2.4f, 3.4f, 4.4f),
            Arrays.asList(30f, 22f, 35f, 20f),
            Arrays.asList(40f, 1f, 1f, 200f),
            Arrays.asList(5f, 10f, 20f, 40f),
            Arrays.asList(120f, 60f, 30f, 15f),
            Arrays.asList(200f, 50f, 100f, 25f),
            Arrays.asList(1.8f, 2.5f, 3.7f, 4.9f));
    int[][] childVectorsPerParent = {{1, 8}, {4, 9}, {2, 5}, {10, 6}, {3, 7}};

    List<SolrInputDocument> docs = new ArrayList<>(childVectorsPerParent.length);
    for (int i = 0; i < childVectorsPerParent.length; i++) {
      SolrInputDocument parent = new SolrInputDocument();
      parent.setField("id", "bq_parent_" + (i + 1));
      parent.setField("bq_parent_s", "true");
      List<SolrInputDocument> children = new ArrayList<>(2);
      for (int vectorId : childVectorsPerParent[i]) {
        SolrInputDocument child = new SolrInputDocument();
        child.setField("id", "bq_child_" + vectorId);
        child.setField("vector_binary_quantized", vectors.get(vectorId - 1));
        children.add(child);
      }
      parent.setField("vectors", children);
      docs.add(parent);
    }
    return docs;
  }

  /**
   * Generate a resulting float vector with a distance from the original vector that is proportional
   * to the value in input (higher the value, higher the distance from the original vector)
   *
   * @param vector a numerical vector
   * @param value a numerical value to be added to the first element of the vector
   * @return a numerical vector that has a distance from the input vector, proportional to the value
   */
  private static List<Float> outDistanceFloat(List<Float> vector, int value) {
    List<Float> result = new ArrayList<>(vector.size());
    for (int i = 0; i < vector.size(); i++) {
      if (i == 0) {
        result.add(vector.get(i) + value);
      } else {
        result.add(vector.get(i));
      }
    }
    return result;
  }

  /**
   * Generate a resulting byte vector with a distance from the original vector that is proportional
   * to the value in input (higher the value, higher the distance from the original vector)
   *
   * @param vector a numerical vector
   * @param value a numerical value to be added to the first element of the vector
   * @return a numerical vector that has a distance from the input vector, proportional to the value
   */
  private static List<Integer> outDistanceByte(List<Integer> vector, int value) {
    List<Integer> result = new ArrayList<>(vector.size());
    for (int i = 0; i < vector.size(); i++) {
      if (i == 0) {
        result.add(vector.get(i) + value);
      } else {
        result.add(vector.get(i));
      }
    }
    return result;
  }

  @Test
  public void parentRetrieval_knnChildrenDiversifyingWithNoAllParents_shouldThrowException() {
    assertQEx(
        "When running a diversifying children KNN query, 'childrenOf' parameter is required",
        req(
            "q",
            "{!parent which=$allParents score=max v=$children.q}",
            "fl",
            "id,score",
            "children.q",
            "{!knn f=vector topK=3 parents.preFilter=$someParents}" + FLOAT_QUERY_VECTOR,
            "allParents",
            "parent_s:[* TO *]",
            "someParents",
            "parent_s:(a c)"),
        400);
  }

  @Test
  public void childrenRetrievalFloat_filteringByParentMetadata_shouldReturnKnnChildren() {
    assertQ(
        req(
            "fq", "{!child of=$allParents filters=$parent.fq}",
            "q", "{!knn f=vector topK=5}" + FLOAT_QUERY_VECTOR,
            "fl", "id",
            "parent.fq", "parent_s:(a c)",
            "allParents", "parent_s:[* TO *]"),
        "//*[@numFound='5']",
        "//result/doc[1]/str[@name='id'][.='82']",
        "//result/doc[2]/str[@name='id'][.='81']",
        "//result/doc[3]/str[@name='id'][.='80']",
        "//result/doc[4]/str[@name='id'][.='62']",
        "//result/doc[5]/str[@name='id'][.='61']");
  }

  @Test
  public void childrenRetrievalByte_filteringByParentMetadata_shouldReturnKnnChildren() {
    assertQ(
        req(
            "fq", "{!child of=$allParents filters=$parent.fq}",
            "q", "{!knn f=vector_byte topK=5}" + BYTE_QUERY_VECTOR,
            "fl", "id",
            "parent.fq", "parent_s:(a c)",
            "allParents", "parent_s:[* TO *]"),
        "//*[@numFound='5']",
        "//result/doc[1]/str[@name='id'][.='82']",
        "//result/doc[2]/str[@name='id'][.='81']",
        "//result/doc[3]/str[@name='id'][.='80']",
        "//result/doc[4]/str[@name='id'][.='62']",
        "//result/doc[5]/str[@name='id'][.='61']");
  }

  @Test
  public void parentRetrievalFloat_knnChildren_shouldReturnKnnParents() {
    assertQ(
        req(
            "q", "{!parent which=$allParents score=max v=$children.q}",
            "fl", "id,score",
            "children.q", "{!knn f=vector topK=3 childrenOf=$allParents}" + FLOAT_QUERY_VECTOR,
            "allParents", "parent_s:[* TO *]"),
        "//*[@numFound='3']",
        "//result/doc[1]/str[@name='id'][.='10']",
        "//result/doc[2]/str[@name='id'][.='9']",
        "//result/doc[3]/str[@name='id'][.='8']");
  }

  @Test
  public void parentRetrievalFloat_knnChildrenWithNoDiversifying_shouldReturnOneParent() {
    assertQ(
        req(
            "q", "{!parent which=$allParents score=max v=$children.q}",
            "fl", "id,score",
            "children.q", "{!knn f=vector topK=3}" + FLOAT_QUERY_VECTOR,
            "allParents", "parent_s:[* TO *]"),
        "//*[@numFound='1']",
        "//result/doc[1]/str[@name='id'][.='10']");
  }

  @Test
  public void childrenRetrievalBinaryQuantized_knnChildrenWithRerankOversample_shouldRerank() {
    assertQ(
        req(
            "q",
                "{!knn f=vector_binary_quantized topK=3 childrenOf=$allParents}"
                    + BINARY_QUANTIZED_QUERY_VECTOR,
            "fl", "id",
            "allParents", "bq_parent_s:true"),
        "//*[@numFound='3']",
        "//result/doc[1]/str[@name='id'][.='bq_child_3']",
        "//result/doc[2]/str[@name='id'][.='bq_child_1']",
        "//result/doc[3]/str[@name='id'][.='bq_child_4']");

    assertQ(
        req(
            "q",
                "{!knn f=vector_binary_quantized topK=3 rerankOversample=3 childrenOf=$allParents}"
                    + BINARY_QUANTIZED_QUERY_VECTOR,
            "fl", "id",
            "allParents", "bq_parent_s:true"),
        "//*[@numFound='3']",
        "//result/doc[1]/str[@name='id'][.='bq_child_1']",
        "//result/doc[2]/str[@name='id'][.='bq_child_4']",
        "//result/doc[3]/str[@name='id'][.='bq_child_2']");
  }

  @Test
  public void parentRetrievalFloat_knnChildrenWithParentFilter_shouldReturnKnnParents() {
    assertQ(
        req(
            "q", "{!parent which=$allParents score=max v=$children.q}",
            "fl", "id,score",
            "children.q",
                "{!knn f=vector topK=3 parents.preFilter=$someParents childrenOf=$allParents}"
                    + FLOAT_QUERY_VECTOR,
            "allParents", "parent_s:[* TO *]",
            "someParents", "parent_s:(a c)"),
        "//*[@numFound='3']",
        "//result/doc[1]/str[@name='id'][.='8']",
        "//result/doc[2]/str[@name='id'][.='6']",
        "//result/doc[3]/str[@name='id'][.='2']");
  }

  @Test
  public void parentRetrievalFloat_knnChildrenWithMultipleParentFilters_shouldReturnKnnParents() {
    assertQ(
        req(
            "q", "{!parent which=$allParents score=max v=$children.q}",
            "fl", "id,score",
            "children.q",
                "{!knn f=vector topK=3 parents.preFilter=$parentFilter1 parents.preFilter=$parentFilter2 childrenOf=$allParents}"
                    + FLOAT_QUERY_VECTOR,
            "allParents", "parent_s:[* TO *]",
            "parentFilter1", "parent_s:(a c)",
            "parentFilter2", "parent_s:(c e)"),
        "//*[@numFound='2']",
        "//result/doc[1]/str[@name='id'][.='8']",
        "//result/doc[2]/str[@name='id'][.='2']");
  }

  @Test
  public void
      parentRetrievalFloat_knnChildrenWithParentFilterAndChildrenFilter_shouldReturnKnnParents() {
    assertQ(
        req(
            "q", "{!parent which=$allParents score=max v=$children.q}",
            "fl", "id,score",
            "children.q",
                "{!knn f=vector topK=3 preFilter=child_s:m parents.preFilter=$someParents childrenOf=$allParents}"
                    + FLOAT_QUERY_VECTOR,
            "allParents", "parent_s:[* TO *]",
            "someParents", "parent_s:(a c)"),
        "//*[@numFound='2']",
        "//result/doc[1]/str[@name='id'][.='8']",
        "//result/doc[2]/str[@name='id'][.='2']");
  }

  @Test
  public void parentRetrievalByte_knnChildren_shouldReturnKnnParents() {
    assertQ(
        req(
            "q", "{!parent which=$allParents score=max v=$children.q}",
            "fl", "id,score",
            "children.q", "{!knn f=vector_byte topK=3 childrenOf=$allParents}" + BYTE_QUERY_VECTOR,
            "allParents", "parent_s:[* TO *]"),
        "//*[@numFound='3']",
        "//result/doc[1]/str[@name='id'][.='10']",
        "//result/doc[2]/str[@name='id'][.='9']",
        "//result/doc[3]/str[@name='id'][.='8']");
  }

  @Test
  public void parentRetrievalByte_knnChildrenWithParentFilter_shouldReturnKnnParents() {
    assertQ(
        req(
            "q", "{!parent which=$allParents score=max v=$children.q}",
            "fl", "id,score",
            "children.q",
                "{!knn f=vector_byte topK=3 parents.preFilter=$someParents childrenOf=$allParents}"
                    + BYTE_QUERY_VECTOR,
            "allParents", "parent_s:[* TO *]",
            "someParents", "parent_s:(a c)"),
        "//*[@numFound='3']",
        "//result/doc[1]/str[@name='id'][.='8']",
        "//result/doc[2]/str[@name='id'][.='6']",
        "//result/doc[3]/str[@name='id'][.='2']");
  }

  @Test
  public void parentRetrievalByte_knnChildrenWithMultipleParentFilters_shouldReturnKnnParents() {
    assertQ(
        req(
            "q", "{!parent which=$allParents score=max v=$children.q}",
            "fl", "id,score",
            "children.q",
                "{!knn f=vector_byte topK=3 parents.preFilter=$parentFilter1 parents.preFilter=$parentFilter2 childrenOf=$allParents}"
                    + BYTE_QUERY_VECTOR,
            "allParents", "parent_s:[* TO *]",
            "parentFilter1", "parent_s:(a c)",
            "parentFilter2", "parent_s:(c e)"),
        "//*[@numFound='2']",
        "//result/doc[1]/str[@name='id'][.='8']",
        "//result/doc[2]/str[@name='id'][.='2']");
  }

  @Test
  public void
      parentRetrievalByte_knnChildrenWithParentFilterAndChildrenFilter_shouldReturnKnnParents() {
    assertQ(
        req(
            "q", "{!parent which=$allParents score=max v=$children.q}",
            "fl", "id,score",
            "children.q",
                "{!knn f=vector_byte topK=3 preFilter=child_s:m parents.preFilter=$someParents childrenOf=$allParents}"
                    + BYTE_QUERY_VECTOR,
            "allParents", "parent_s:[* TO *]",
            "someParents", "parent_s:(a c)"),
        "//*[@numFound='2']",
        "//result/doc[1]/str[@name='id'][.='8']",
        "//result/doc[2]/str[@name='id'][.='2']");
  }

  @Test
  public void
      parentRetrievalByte_knnChildrenWithMultipleParentFiltersAndChildrenFilter_shouldReturnKnnParents() {
    assertQ(
        req(
            "q", "{!parent which=$allParents score=max v=$children.q}",
            "fl", "id,score",
            "children.q",
                "{!knn f=vector_byte topK=3 preFilter=child_s:m parents.preFilter=$parentFilter1 parents.preFilter=$parentFilter2 childrenOf=$allParents}"
                    + BYTE_QUERY_VECTOR,
            "allParents", "parent_s:[* TO *]",
            "parentFilter1", "parent_s:(a c)",
            "parentFilter2", "parent_s:(c e)"),
        "//*[@numFound='2']",
        "//result/doc[1]/str[@name='id'][.='8']",
        "//result/doc[2]/str[@name='id'][.='2']");
  }

  @Test
  public void
      parentRetrievalFloat_topKWithChildTransformerWithFilter_shouldUseOriginalChildTransformerFilter() {
    assertQ(
        req(
            "q", "{!parent which=$allParents score=max v=$children.q}",
            "fl", "id,score,vectors,vector,[child limit=2 fl=vector]",
            "children.q",
                "{!knn f=vector topK=3 parents.preFilter=$someParents childrenOf=$allParents}"
                    + FLOAT_QUERY_VECTOR,
            "allParents", "parent_s:[* TO *]",
            "someParents", "parent_s:(a c)"),
        "//result[@numFound='3']",
        "//result/doc[1]/str[@name='id'][.='8']",
        "//result/doc[1]/arr[@name='vectors'][1]/doc[1]/arr[@name='vector']/float[1][.='10.0']",
        "//result/doc[1]/arr[@name='vectors'][1]/doc[1]/arr[@name='vector']/float[2][.='1.0']",
        "//result/doc[1]/arr[@name='vectors'][1]/doc[1]/arr[@name='vector']/float[3][.='1.0']",
        "//result/doc[1]/arr[@name='vectors'][1]/doc[1]/arr[@name='vector']/float[4][.='1.0']",
        "//result/doc[1]/arr[@name='vectors'][1]/doc[2]/arr[@name='vector']/float[1][.='9.0']",
        "//result/doc[1]/arr[@name='vectors'][1]/doc[2]/arr[@name='vector']/float[2][.='1.0']",
        "//result/doc[1]/arr[@name='vectors'][1]/doc[2]/arr[@name='vector']/float[3][.='1.0']",
        "//result/doc[1]/arr[@name='vectors'][1]/doc[2]/arr[@name='vector']/float[4][.='1.0']",
        "//result/doc[2]/str[@name='id'][.='6']",
        "//result/doc[2]/arr[@name='vectors'][1]/doc[1]/arr[@name='vector']/float[1][.='16.0']",
        "//result/doc[2]/arr[@name='vectors'][1]/doc[1]/arr[@name='vector']/float[2][.='1.0']",
        "//result/doc[2]/arr[@name='vectors'][1]/doc[1]/arr[@name='vector']/float[3][.='1.0']",
        "//result/doc[2]/arr[@name='vectors'][1]/doc[1]/arr[@name='vector']/float[4][.='1.0']",
        "//result/doc[2]/arr[@name='vectors'][1]/doc[2]/arr[@name='vector']/float[1][.='15.0']",
        "//result/doc[2]/arr[@name='vectors'][1]/doc[2]/arr[@name='vector']/float[2][.='1.0']",
        "//result/doc[2]/arr[@name='vectors'][1]/doc[2]/arr[@name='vector']/float[3][.='1.0']",
        "//result/doc[2]/arr[@name='vectors'][1]/doc[2]/arr[@name='vector']/float[4][.='1.0']",
        "//result/doc[3]/str[@name='id'][.='2']",
        "//result/doc[3]/arr[@name='vectors'][1]/doc[1]/arr[@name='vector']/float[1][.='28.0']",
        "//result/doc[3]/arr[@name='vectors'][1]/doc[1]/arr[@name='vector']/float[2][.='1.0']",
        "//result/doc[3]/arr[@name='vectors'][1]/doc[1]/arr[@name='vector']/float[3][.='1.0']",
        "//result/doc[3]/arr[@name='vectors'][1]/doc[1]/arr[@name='vector']/float[4][.='1.0']",
        "//result/doc[3]/arr[@name='vectors'][1]/doc[2]/arr[@name='vector']/float[1][.='27.0']",
        "//result/doc[3]/arr[@name='vectors'][1]/doc[2]/arr[@name='vector']/float[2][.='1.0']",
        "//result/doc[3]/arr[@name='vectors'][1]/doc[2]/arr[@name='vector']/float[3][.='1.0']",
        "//result/doc[3]/arr[@name='vectors'][1]/doc[2]/arr[@name='vector']/float[4][.='1.0']");
  }

  @Test
  public void parentRetrievalFloat_topKWithChildTransformerWithFilter_shouldReturnBestChild() {
    assertQ(
        req(
            "q", "{!parent which=$allParents score=max v=$children.q}",
            "fl", "id,score,vectors,vector,[child fl=vector childFilter=$children.q]",
            "children.q",
                "{!knn f=vector topK=3 parents.preFilter=$someParents childrenOf=$allParents}"
                    + FLOAT_QUERY_VECTOR,
            "allParents", "parent_s:[* TO *]",
            "someParents", "parent_s:(b c)"),
        "//result[@numFound='3']",
        "//result/doc[1]/str[@name='id'][.='8']",
        "//result/doc[1]/arr[@name='vectors'][1]/doc[1]/arr[@name='vector']/float[1][.='8.0']",
        "//result/doc[1]/arr[@name='vectors'][1]/doc[1]/arr[@name='vector']/float[2][.='1.0']",
        "//result/doc[1]/arr[@name='vectors'][1]/doc[1]/arr[@name='vector']/float[3][.='1.0']",
        "//result/doc[1]/arr[@name='vectors'][1]/doc[1]/arr[@name='vector']/float[4][.='1.0']",
        "//result/doc[2]/str[@name='id'][.='7']",
        "//result/doc[2]/arr[@name='vectors'][1]/doc[1]/arr[@name='vector']/float[1][.='11.0']",
        "//result/doc[2]/arr[@name='vectors'][1]/doc[1]/arr[@name='vector']/float[2][.='1.0']",
        "//result/doc[2]/arr[@name='vectors'][1]/doc[1]/arr[@name='vector']/float[3][.='1.0']",
        "//result/doc[2]/arr[@name='vectors'][1]/doc[1]/arr[@name='vector']/float[4][.='1.0']",
        "//result/doc[3]/str[@name='id'][.='2']",
        "//result/doc[3]/arr[@name='vectors'][1]/doc[1]/arr[@name='vector']/float[1][.='26.0']",
        "//result/doc[3]/arr[@name='vectors'][1]/doc[1]/arr[@name='vector']/float[2][.='1.0']",
        "//result/doc[3]/arr[@name='vectors'][1]/doc[1]/arr[@name='vector']/float[3][.='1.0']",
        "//result/doc[3]/arr[@name='vectors'][1]/doc[1]/arr[@name='vector']/float[4][.='1.0']");
  }

  @Test
  public void parentRetrievalByte_topKWithChildTransformer_shouldReturnAllChildren() {
    assertQ(
        req(
            "q", "{!parent which=$allParents score=max v=$children.q}",
            "fl", "id,score,vectors,vector_byte,[child limit=2 fl=vector_byte]",
            "children.q",
                "{!knn f=vector_byte topK=3 parents.preFilter=$someParents childrenOf=$allParents}"
                    + BYTE_QUERY_VECTOR,
            "allParents", "parent_s:[* TO *]",
            "someParents", "parent_s:(b c)"),
        "//result[@numFound='3']",
        "//result/doc[1]/str[@name='id'][.='8']",
        "//result/doc[1]/arr[@name='vectors'][1]/doc[1]/arr[@name='vector_byte']/int[1][.='10']",
        "//result/doc[1]/arr[@name='vectors'][1]/doc[1]/arr[@name='vector_byte']/int[2][.='1']",
        "//result/doc[1]/arr[@name='vectors'][1]/doc[1]/arr[@name='vector_byte']/int[3][.='1']",
        "//result/doc[1]/arr[@name='vectors'][1]/doc[1]/arr[@name='vector_byte']/int[4][.='1']",
        "//result/doc[1]/arr[@name='vectors'][1]/doc[2]/arr[@name='vector_byte']/int[1][.='9']",
        "//result/doc[1]/arr[@name='vectors'][1]/doc[2]/arr[@name='vector_byte']/int[2][.='1']",
        "//result/doc[1]/arr[@name='vectors'][1]/doc[2]/arr[@name='vector_byte']/int[3][.='1']",
        "//result/doc[1]/arr[@name='vectors'][1]/doc[2]/arr[@name='vector_byte']/int[4][.='1']",
        "//result/doc[2]/str[@name='id'][.='7']",
        "//result/doc[2]/arr[@name='vectors'][1]/doc[1]/arr[@name='vector_byte']/int[1][.='13']",
        "//result/doc[2]/arr[@name='vectors'][1]/doc[1]/arr[@name='vector_byte']/int[2][.='1']",
        "//result/doc[2]/arr[@name='vectors'][1]/doc[1]/arr[@name='vector_byte']/int[3][.='1']",
        "//result/doc[2]/arr[@name='vectors'][1]/doc[1]/arr[@name='vector_byte']/int[4][.='1']",
        "//result/doc[2]/arr[@name='vectors'][1]/doc[2]/arr[@name='vector_byte']/int[1][.='12']",
        "//result/doc[2]/arr[@name='vectors'][1]/doc[2]/arr[@name='vector_byte']/int[2][.='1']",
        "//result/doc[2]/arr[@name='vectors'][1]/doc[2]/arr[@name='vector_byte']/int[3][.='1']",
        "//result/doc[2]/arr[@name='vectors'][1]/doc[2]/arr[@name='vector_byte']/int[4][.='1']",
        "//result/doc[3]/str[@name='id'][.='2']",
        "//result/doc[3]/arr[@name='vectors'][1]/doc[1]/arr[@name='vector_byte']/int[1][.='28']",
        "//result/doc[3]/arr[@name='vectors'][1]/doc[1]/arr[@name='vector_byte']/int[2][.='1']",
        "//result/doc[3]/arr[@name='vectors'][1]/doc[1]/arr[@name='vector_byte']/int[3][.='1']",
        "//result/doc[3]/arr[@name='vectors'][1]/doc[1]/arr[@name='vector_byte']/int[4][.='1']",
        "//result/doc[3]/arr[@name='vectors'][1]/doc[2]/arr[@name='vector_byte']/int[1][.='27']",
        "//result/doc[3]/arr[@name='vectors'][1]/doc[2]/arr[@name='vector_byte']/int[2][.='1']",
        "//result/doc[3]/arr[@name='vectors'][1]/doc[2]/arr[@name='vector_byte']/int[3][.='1']",
        "//result/doc[3]/arr[@name='vectors'][1]/doc[2]/arr[@name='vector_byte']/int[4][.='1']");
  }

  @Test
  public void parentRetrievalByte_topKWithChildTransformerWithFilter_shouldReturnBestChild() {
    assertQ(
        req(
            "q", "{!parent which=$allParents score=max v=$children.q}",
            "fl", "id,score,vectors,vector_byte,[child fl=vector_byte childFilter=$children.q]",
            "children.q",
                "{!knn f=vector_byte topK=3 parents.preFilter=$someParents childrenOf=$allParents}"
                    + BYTE_QUERY_VECTOR,
            "allParents", "parent_s:[* TO *]",
            "someParents", "parent_s:(b c)"),
        "//result[@numFound='3']",
        "//result/doc[1]/str[@name='id'][.='8']",
        "//result/doc[1]/arr[@name='vectors'][1]/doc[1]/arr[@name='vector_byte']/int[1][.='8']",
        "//result/doc[1]/arr[@name='vectors'][1]/doc[1]/arr[@name='vector_byte']/int[2][.='1']",
        "//result/doc[1]/arr[@name='vectors'][1]/doc[1]/arr[@name='vector_byte']/int[3][.='1']",
        "//result/doc[1]/arr[@name='vectors'][1]/doc[1]/arr[@name='vector_byte']/int[4][.='1']",
        "//result/doc[2]/str[@name='id'][.='7']",
        "//result/doc[2]/arr[@name='vectors'][1]/doc[1]/arr[@name='vector_byte']/int[1][.='11']",
        "//result/doc[2]/arr[@name='vectors'][1]/doc[1]/arr[@name='vector_byte']/int[2][.='1']",
        "//result/doc[2]/arr[@name='vectors'][1]/doc[1]/arr[@name='vector_byte']/int[3][.='1']",
        "//result/doc[2]/arr[@name='vectors'][1]/doc[1]/arr[@name='vector_byte']/int[4][.='1']",
        "//result/doc[3]/str[@name='id'][.='2']",
        "//result/doc[3]/arr[@name='vectors'][1]/doc[1]/arr[@name='vector_byte']/int[1][.='26']",
        "//result/doc[3]/arr[@name='vectors'][1]/doc[1]/arr[@name='vector_byte']/int[2][.='1']",
        "//result/doc[3]/arr[@name='vectors'][1]/doc[1]/arr[@name='vector_byte']/int[3][.='1']",
        "//result/doc[3]/arr[@name='vectors'][1]/doc[1]/arr[@name='vector_byte']/int[4][.='1']");
  }
}
