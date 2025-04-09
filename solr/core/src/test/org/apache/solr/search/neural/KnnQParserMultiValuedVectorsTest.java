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
package org.apache.solr.search.neural;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.util.RandomNoReverseMergePolicyFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TestRule;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.solr.search.neural.KnnQParser.DEFAULT_TOP_K;

public class KnnQParserMultiValuedVectorsTest extends SolrTestCaseJ4 {
  private static final List<Float> FLOAT_QUERY_VECTOR = Arrays.asList(1.0f, 1.0f, 1.0f, 1.0f);
  private static final List<Integer> BYTE_QUERY_VECTOR = Arrays.asList(1, 1, 1, 1);
  
  @ClassRule
  public static final TestRule noReverseMerge = RandomNoReverseMergePolicyFactory.createRule();

  @BeforeClass
  public static void beforeClass() throws Exception {
    /* vectorDimension="4" similarityFunction="cosine" */
    initCore("solrconfig_codec.xml", "schema-densevector.xml");
    prepareIndex();
  }

  public static void prepareIndex() throws Exception {
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
  private static List<SolrInputDocument> prepareDocs() {
    int totalParentDocuments = 10;
    int totalNestedVectors = 30;
    int perParentChildren = totalNestedVectors / totalParentDocuments;
    
    final String[] abcdef = new String[] {"a", "b", "c", "d", "e", "f"};

    List<SolrInputDocument> docs = new ArrayList<>(totalParentDocuments);
    for (int i = 1; i < totalParentDocuments + 1; i++) {
      SolrInputDocument doc = new SolrInputDocument();
      doc.setField("id", i);
      doc.setField("_text_", abcdef[i % abcdef.length]);
      List<List<Float>> floatVectors = new ArrayList<>(perParentChildren);
      List<List<Integer>> byteVectors = new ArrayList<>(perParentChildren);
      // nested vector documents have a distance from the query vector inversely proportional to
      // their id
      for (int j = 0; j < perParentChildren; j++) {
        floatVectors.add(outDistanceFloat(FLOAT_QUERY_VECTOR, totalNestedVectors));
        byteVectors.add(outDistanceByte(BYTE_QUERY_VECTOR, totalNestedVectors));
        totalNestedVectors--; // the higher the id of the nested document, lower the distance with
      }
      doc.setField("vector_multivalued", floatVectors);
      doc.setField("vector_byte_multivalued", byteVectors);

      docs.add(doc);
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
  public void topK_shouldReturnOnlyTopKResults() {
    assertQ(
        req(CommonParams.Q, "{!knn f=vector_multivalued topK=5}" + FLOAT_QUERY_VECTOR, "fl", "id"),
        "//result[@numFound='5']",
        "//result/doc[1]/str[@name='id'][.='10']",
        "//result/doc[2]/str[@name='id'][.='9']",
        "//result/doc[3]/str[@name='id'][.='8']",
        "//result/doc[4]/str[@name='id'][.='7']",
        "//result/doc[5]/str[@name='id'][.='6']");
  }

  @Test
  public void topKWithFilter_shouldReturnOnlyTopKResults() {
    assertQ(
            req(CommonParams.Q, "{!knn f=vector_multivalued topK=5}" + FLOAT_QUERY_VECTOR, "fl", "id,[child childFilter=$allChildren limit=2 fl=id,vector_multivalued]","fq","_text_:(b OR c)","allChildren","_nest_path_:[* TO *]"),
            "//result[@numFound='4']",
            "//result/doc[1]/str[@name='id'][.='8']",
            "//result/doc[2]/str[@name='id'][.='7']",
            "//result/doc[3]/str[@name='id'][.='2']",
            "//result/doc[4]/str[@name='id'][.='1']");
  }

  @Test
  public void topKWithFilterAndChildTransformer_shouldReturnOnlyTopKResults() {
    assertQ(
            req(CommonParams.Q, "{!knn f=vector_multivalued topK=5}" + FLOAT_QUERY_VECTOR, "fl", "id,score,vector_multivalued,[child fl=vector_multivalued]","fq","_text_:(b OR c)","allChildren","_nest_path_:[* TO *]"),
            "//result[@numFound='4']",
            "//result/doc[1]/str[@name='id'][.='8']",
            "//result/doc[2]/str[@name='id'][.='7']",
            "//result/doc[3]/str[@name='id'][.='2']",
            "//result/doc[4]/str[@name='id'][.='1']");
  }
}
