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

public class BlockJoinNestedVectorsTest extends BlockJoinNestedVectorsParentQParserTest {
  protected static String VECTOR_FIELD = "vector";
  protected static String VECTOR_BYTE_FIELD = "vector_byte_encoding";

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
        child.setField(
            "vector_byte_encoding", outDistanceByte(BYTE_QUERY_VECTOR, totalNestedVectors));
        totalNestedVectors--; // the higher the id of the nested document, lower the distance with
        // the query vector
        children.add(child);
      }
      doc.setField("vectors", children);
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
    super.childrenRetrieval_filteringByParentMetadata_shouldReturnKnnChildren(VECTOR_FIELD);
  }

  @Test
  public void childrenRetrievalByte_filteringByParentMetadata_shouldReturnKnnChildren() {
    super.childrenRetrieval_filteringByParentMetadata_shouldReturnKnnChildren(VECTOR_BYTE_FIELD);
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
  public void
      parentRetrievalFloat_knnChildrenWithParentFilterAndChildrenFilter_shouldReturnKnnParents() {
    super.parentRetrieval_knnChildrenWithParentFilterAndChildrenFilter_shouldReturnKnnParents(
        VECTOR_FIELD);
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
      parentRetrievalByte_knnChildrenWithParentFilterAndChildrenFilter_shouldReturnKnnParents() {
    super.parentRetrieval_knnChildrenWithParentFilterAndChildrenFilter_shouldReturnKnnParents(
        VECTOR_BYTE_FIELD);
  }

  @Test
  public void parentRetrievalFloat_topKWithChildTransformerWithFilter_shouldReturnAllChildren() {
    super.parentRetrievalFloat_topKWithChildTransformer_shouldReturnAllChildren(VECTOR_FIELD);
  }

  @Test
  public void parentRetrievalFloat_topKWithChildTransformerWithFilter_shouldReturnBestChild() {
    assertQ(
        req(
            "q",
            "{!parent which=$allParents score=max v=$children.q}",
            "fl",
            "id,score,"
                + VECTORS_PSEUDOFIELD
                + ","
                + VECTOR_FIELD
                + ",[child fl=vector childFilter=$children.q]",
            "children.q",
            "{!knn f="
                + VECTOR_FIELD
                + " topK=3 childrenOf=$someParents allParents=$allParents}"
                + FLOAT_QUERY_VECTOR,
            "allParents",
            "parent_s:[* TO *]",
            "someParents",
            "parent_s:(b c)"),
        "//result[@numFound='3']",
        "//result/doc[1]/str[@name='id'][.='8']",
        "//result/doc[1]/arr[@name='"
            + VECTORS_PSEUDOFIELD
            + "'][1]/doc[1]/arr[@name='"
            + VECTOR_FIELD
            + "']/float[1][.='8.0']",
        "//result/doc[1]/arr[@name='"
            + VECTORS_PSEUDOFIELD
            + "'][1]/doc[1]/arr[@name='"
            + VECTOR_FIELD
            + "']/float[2][.='1.0']",
        "//result/doc[1]/arr[@name='"
            + VECTORS_PSEUDOFIELD
            + "'][1]/doc[1]/arr[@name='"
            + VECTOR_FIELD
            + "']/float[3][.='1.0']",
        "//result/doc[1]/arr[@name='"
            + VECTORS_PSEUDOFIELD
            + "'][1]/doc[1]/arr[@name='"
            + VECTOR_FIELD
            + "']/float[4][.='1.0']",
        "//result/doc[2]/str[@name='id'][.='7']",
        "//result/doc[2]/arr[@name='"
            + VECTORS_PSEUDOFIELD
            + "'][1]/doc[1]/arr[@name='"
            + VECTOR_FIELD
            + "']/float[1][.='11.0']",
        "//result/doc[2]/arr[@name='"
            + VECTORS_PSEUDOFIELD
            + "'][1]/doc[1]/arr[@name='"
            + VECTOR_FIELD
            + "']/float[2][.='1.0']",
        "//result/doc[2]/arr[@name='"
            + VECTORS_PSEUDOFIELD
            + "'][1]/doc[1]/arr[@name='"
            + VECTOR_FIELD
            + "']/float[3][.='1.0']",
        "//result/doc[2]/arr[@name='"
            + VECTORS_PSEUDOFIELD
            + "'][1]/doc[1]/arr[@name='"
            + VECTOR_FIELD
            + "']/float[4][.='1.0']",
        "//result/doc[3]/str[@name='id'][.='2']",
        "//result/doc[3]/arr[@name='"
            + VECTORS_PSEUDOFIELD
            + "'][1]/doc[1]/arr[@name='"
            + VECTOR_FIELD
            + "']/float[1][.='26.0']",
        "//result/doc[3]/arr[@name='"
            + VECTORS_PSEUDOFIELD
            + "'][1]/doc[1]/arr[@name='"
            + VECTOR_FIELD
            + "']/float[2][.='1.0']",
        "//result/doc[3]/arr[@name='"
            + VECTORS_PSEUDOFIELD
            + "'][1]/doc[1]/arr[@name='"
            + VECTOR_FIELD
            + "']/float[3][.='1.0']",
        "//result/doc[3]/arr[@name='"
            + VECTORS_PSEUDOFIELD
            + "'][1]/doc[1]/arr[@name='"
            + VECTOR_FIELD
            + "']/float[4][.='1.0']");
  }

  @Test
  public void parentRetrievalByte_topKWithChildTransformer_shouldReturnAllChildren() {
    super.parentRetrievalByte_topKWithChildTransformer_shouldReturnAllChildren(VECTOR_BYTE_FIELD);
  }

  @Test
  public void parentRetrievalByte_topKWithChildTransformerWithFilter_shouldReturnBestChild() {
    super.parentRetrievalByte_topKWithChildTransformerWithFilter_shouldReturnBestChild(
        VECTOR_BYTE_FIELD);
  }
}
