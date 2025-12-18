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
import java.util.List;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.util.RandomNoReverseMergePolicyFactory;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TestRule;

public class BlockJoinMultiValuedVectorsTest extends BlockJoinNestedVectorsParentQParserTest {

  protected static String VECTOR_FIELD = "vector_multivalued";
  protected static String VECTOR_BYTE_FIELD = "vector_byte_multivalued";

  @ClassRule
  public static final TestRule noReverseMerge = RandomNoReverseMergePolicyFactory.createRule();

  @BeforeClass
  public static void beforeClass() throws Exception {
    /* vectorDimension="4" similarityFunction="cosine" */
    initCore("solrconfig_codec.xml", "schema-densevector.xml");
    prepareIndex();
  }

  protected static void prepareIndex() throws Exception {
    List<SolrInputDocument> docsToIndex = prepareDocs();
    for (SolrInputDocument doc : docsToIndex) {
      updateJ(jsonAdd(doc), null);
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
  protected static List<SolrInputDocument> prepareDocs() {
    int totalParentDocuments = 10;
    int totalNestedVectors = 30;
    int perParentChildren = totalNestedVectors / totalParentDocuments;

    final String[] abcdef = new String[] {"a", "b", "c", "d", "e", "f"};

    List<SolrInputDocument> docs = new ArrayList<>(totalParentDocuments);
    for (int i = 1; i < totalParentDocuments + 1; i++) {
      SolrInputDocument doc = new SolrInputDocument();
      doc.setField("id", i);
      doc.setField("parent_b", true);
      doc.setField("parent_s", abcdef[i % abcdef.length]);
      List<List<Float>> floatVectors = new ArrayList<>(perParentChildren);
      List<List<Integer>> byteVectors = new ArrayList<>(perParentChildren);
      // nested vector documents have a distance from the query vector inversely proportional to
      // their id
      for (int j = 0; j < perParentChildren; j++) {
        floatVectors.add(outDistanceFloat(FLOAT_QUERY_VECTOR, totalNestedVectors));
        byteVectors.add(outDistanceByte(BYTE_QUERY_VECTOR, totalNestedVectors));
        totalNestedVectors--; // the higher the id of the nested document, lower the distance with
      }
      doc.setField(VECTOR_FIELD, floatVectors);
      doc.setField(VECTOR_BYTE_FIELD, byteVectors);

      docs.add(doc);
    }

    return docs;
  }

  @Test
  public void parentRetrieval_knnChildrenDiversifyingWithNoAllParents_shouldThrowException() {
    super.parentRetrieval_knnChildrenDiversifyingWithNoAllParents_shouldThrowException(
        VECTOR_FIELD);
  }

  @Test
  public void childrenRetrievalFloat_filteringByParentMetadata_shouldReturnKnnChildren() {
    assertQ(
        req(
            "fq", "{!child of=$allParents filters=$parent.fq}",
            "q", "{!knn f=" + VECTOR_FIELD + " topK=5}" + FLOAT_QUERY_VECTOR,
            "fl", "id",
            "parent.fq", "parent_s:(a c)",
            "allParents", "parent_s:[* TO *]"),
        "//*[@numFound='5']",
        "//result/doc[1]/str[@name='id'][.='8/vector_multivalued#2']",
        "//result/doc[2]/str[@name='id'][.='8/vector_multivalued#1']",
        "//result/doc[3]/str[@name='id'][.='8/vector_multivalued#0']",
        "//result/doc[4]/str[@name='id'][.='6/vector_multivalued#2']",
        "//result/doc[5]/str[@name='id'][.='6/vector_multivalued#1']");
  }

  @Test
  public void childrenRetrievalByte_filteringByParentMetadata_shouldReturnKnnChildren() {
    assertQ(
        req(
            "fq", "{!child of=$allParents filters=$parent.fq}",
            "q", "{!knn f=" + VECTOR_BYTE_FIELD + " topK=5}" + BYTE_QUERY_VECTOR,
            "fl", "id",
            "parent.fq", "parent_s:(a c)",
            "allParents", "parent_s:[* TO *]"),
        "//*[@numFound='5']",
        "//result/doc[1]/str[@name='id'][.='8/vector_byte_multivalued#2']",
        "//result/doc[2]/str[@name='id'][.='8/vector_byte_multivalued#1']",
        "//result/doc[3]/str[@name='id'][.='8/vector_byte_multivalued#0']",
        "//result/doc[4]/str[@name='id'][.='6/vector_byte_multivalued#2']",
        "//result/doc[5]/str[@name='id'][.='6/vector_byte_multivalued#1']");
  }

  @Test
  public void parentRetrievalFloat_knnChildren_shouldReturnKnnParents() {
    super.parentRetrieval_knnChildren_shouldReturnKnnParents(VECTOR_FIELD);
  }

  @Test
  public void parentRetrievalFloat_knnChildrenWithNoDiversifying_shouldReturnOneParent() {
    super.parentRetrievalFloat_knnChildrenWithNoDiversifying_shouldReturnOneParent(VECTOR_FIELD);
  }

  @Test
  public void parentRetrievalFloat_knnChildrenWithParentFilter_shouldReturnKnnParents() {
    super.parentRetrieval_knnChildrenWithParentFilter_shouldReturnKnnParents(VECTOR_FIELD);
  }

  @Test
  public void parentRetrievalByte_knnChildren_shouldReturnKnnParents() {
    super.parentRetrieval_knnChildren_shouldReturnKnnParents(VECTOR_BYTE_FIELD);
  }

  @Test
  public void parentRetrievalByte_knnChildrenWithParentFilter_shouldReturnKnnParents() {
    super.parentRetrieval_knnChildrenWithParentFilter_shouldReturnKnnParents(VECTOR_BYTE_FIELD);
  }

  @Test
  public void
      parentRetrievalByte_topKWithChildTransformer_shouldReturnAllChildren() { // new transformer
    // all vectors
    //super.parentRetrievalByte_topKWithChildTransformer_shouldReturnAllChildren(VECTOR_BYTE_FIELD);
  }

  @Test
  public void
      parentRetrievalByte_topKWithChildTransformerWithFilter_shouldReturnBestChild() { // new
    // trasnformer best vector
    //super.parentRetrievalByte_topKWithChildTransformerWithFilter_shouldReturnBestChild(
       // VECTOR_BYTE_FIELD);
  }
}
