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

public class BlockJoinNestedVectorsParentQParserTest extends SolrTestCaseJ4 {
  protected static final List<Float> FLOAT_QUERY_VECTOR = Arrays.asList(1.0f, 1.0f, 1.0f, 1.0f);
  protected static final List<Integer> BYTE_QUERY_VECTOR = Arrays.asList(1, 1, 1, 1);

  protected static String VECTORS_PSEUDOFIELD = "vectors";

  /**
   * Generate a resulting float vector with a distance from the original vector that is proportional
   * to the value in input (higher the value, higher the distance from the original vector)
   *
   * @param vector a numerical vector
   * @param value a numerical value to be added to the first element of the vector
   * @return a numerical vector that has a distance from the input vector, proportional to the value
   */
  protected static List<Float> outDistanceFloat(List<Float> vector, int value) {
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
  protected static List<Integer> outDistanceByte(List<Integer> vector, int value) {
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

  protected void parentRetrieval_knnChildrenDiversifyingWithNoAllParents_shouldThrowException(
      String vectorField) {
    assertQEx(
        "When running a diversifying children KNN query, 'allParents' parameter is required",
        req(
            "q",
            "{!parent which=$allParents score=max v=$children.q}",
            "fl",
            "id,score",
            "children.q",
            "{!knn f="
                + vectorField
                + " topK=3 parents.preFilter=$someParents}"
                + FLOAT_QUERY_VECTOR,
            "allParents",
            "parent_s:[* TO *]",
            "someParents",
            "parent_s:(a c)"),
        400);
  }

  protected void childrenRetrieval_filteringByParentMetadata_shouldReturnKnnChildren(
      String vectorField) {
    assertQ(
        req(
            "fq", "{!child of=$allParents filters=$parent.fq}",
            "q", "{!knn f=" + vectorField + " topK=5}" + BYTE_QUERY_VECTOR,
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

  protected void parentRetrievalFloat_knnChildrenWithNoDiversifying_shouldReturnOneParent(
      String vectorField) {
    assertQ(
        req(
            "q", "{!parent which=$allParents score=max v=$children.q}",
            "fl", "id,score",
            "children.q", "{!knn f=" + vectorField + " topK=3}" + FLOAT_QUERY_VECTOR,
            "allParents", "parent_s:[* TO *]"),
        "//*[@numFound='1']",
        "//result/doc[1]/str[@name='id'][.='10']");
  }

  protected void parentRetrieval_knnChildren_shouldReturnKnnParents(String vectorByteField) {
    assertQ(
        req(
            "q", "{!parent which=$allParents score=max v=$children.q}",
            "fl", "id,score",
            "children.q",
                "{!knn f="
                    + vectorByteField
                    + " topK=3 childrenOf=$allParents}"
                    + BYTE_QUERY_VECTOR,
            "allParents", "parent_s:[* TO *]"),
        "//*[@numFound='3']",
        "//result/doc[1]/str[@name='id'][.='10']",
        "//result/doc[2]/str[@name='id'][.='9']",
        "//result/doc[3]/str[@name='id'][.='8']");
  }

  protected void parentRetrieval_knnChildrenWithParentFilter_shouldReturnKnnParents(
      String vectorByteField) {
    assertQ(
        req(
            "q", "{!parent which=$allParents score=max v=$children.q}",
            "fl", "id,score",
            "children.q",
                "{!knn f="
                    + vectorByteField
                    + " topK=3 parents.preFilter=$someParents childrenOf=$allParents}"
                    + BYTE_QUERY_VECTOR,
            "allParents", "parent_s:[* TO *]",
            "someParents", "parent_s:(a c)"),
        "//*[@numFound='3']",
        "//result/doc[1]/str[@name='id'][.='8']",
        "//result/doc[2]/str[@name='id'][.='6']",
        "//result/doc[3]/str[@name='id'][.='2']");
  }

  protected void
      parentRetrieval_knnChildrenWithParentFilterAndChildrenFilter_shouldReturnKnnParents(
          String vectorByteField) {
    assertQ(
        req(
            "q", "{!parent which=$allParents score=max v=$children.q}",
            "fl", "id,score",
            "children.q",
                "{!knn f="
                    + vectorByteField
                    + " topK=3 preFilter=child_s:m parents.preFilter=$someParents childrenOf=$allParents}"
                    + BYTE_QUERY_VECTOR,
            "allParents", "parent_s:[* TO *]",
            "someParents", "parent_s:(a c)"),
        "//*[@numFound='2']",
        "//result/doc[1]/str[@name='id'][.='8']",
        "//result/doc[2]/str[@name='id'][.='2']");
  }

  protected void parentRetrievalFloat_topKWithChildTransformer_shouldReturnAllChildren(
      String vectorField) {
    assertQ(
        req(
            "q", "{!parent which=$allParents score=max v=$children.q}",
            "fl",
                "id,score,"
                    + VECTORS_PSEUDOFIELD
                    + ","
                    + vectorField
                    + ",[child limit=2 fl=vector]",
            "children.q",
                "{!knn f="
                    + vectorField
                    + " topK=3 parents.preFilter=$someParents childrenOf=$allParents}"
                    + FLOAT_QUERY_VECTOR,
            "allParents", "parent_s:[* TO *]",
            "someParents", "parent_s:(a c)"),
        "//result[@numFound='3']",
        "//result/doc[1]/str[@name='id'][.='8']",
        "//result/doc[1]/arr[@name='"
            + VECTORS_PSEUDOFIELD
            + "'][1]/doc[1]/arr[@name='"
            + vectorField
            + "']/float[1][.='10.0']",
        "//result/doc[1]/arr[@name='"
            + VECTORS_PSEUDOFIELD
            + "'][1]/doc[1]/arr[@name='"
            + vectorField
            + "']/float[2][.='1.0']",
        "//result/doc[1]/arr[@name='"
            + VECTORS_PSEUDOFIELD
            + "'][1]/doc[1]/arr[@name='"
            + vectorField
            + "']/float[3][.='1.0']",
        "//result/doc[1]/arr[@name='"
            + VECTORS_PSEUDOFIELD
            + "'][1]/doc[1]/arr[@name='"
            + vectorField
            + "']/float[4][.='1.0']",
        "//result/doc[1]/arr[@name='"
            + VECTORS_PSEUDOFIELD
            + "'][1]/doc[2]/arr[@name='"
            + vectorField
            + "']/float[1][.='9.0']",
        "//result/doc[1]/arr[@name='"
            + VECTORS_PSEUDOFIELD
            + "'][1]/doc[2]/arr[@name='"
            + vectorField
            + "']/float[2][.='1.0']",
        "//result/doc[1]/arr[@name='"
            + VECTORS_PSEUDOFIELD
            + "'][1]/doc[2]/arr[@name='"
            + vectorField
            + "']/float[3][.='1.0']",
        "//result/doc[1]/arr[@name='"
            + VECTORS_PSEUDOFIELD
            + "'][1]/doc[2]/arr[@name='"
            + vectorField
            + "']/float[4][.='1.0']",
        "//result/doc[2]/str[@name='id'][.='6']",
        "//result/doc[2]/arr[@name='"
            + VECTORS_PSEUDOFIELD
            + "'][1]/doc[1]/arr[@name='"
            + vectorField
            + "']/float[1][.='16.0']",
        "//result/doc[2]/arr[@name='"
            + VECTORS_PSEUDOFIELD
            + "'][1]/doc[1]/arr[@name='"
            + vectorField
            + "']/float[2][.='1.0']",
        "//result/doc[2]/arr[@name='"
            + VECTORS_PSEUDOFIELD
            + "'][1]/doc[1]/arr[@name='"
            + vectorField
            + "']/float[3][.='1.0']",
        "//result/doc[2]/arr[@name='"
            + VECTORS_PSEUDOFIELD
            + "'][1]/doc[1]/arr[@name='"
            + vectorField
            + "']/float[4][.='1.0']",
        "//result/doc[2]/arr[@name='"
            + VECTORS_PSEUDOFIELD
            + "'][1]/doc[2]/arr[@name='"
            + vectorField
            + "']/float[1][.='15.0']",
        "//result/doc[2]/arr[@name='"
            + VECTORS_PSEUDOFIELD
            + "'][1]/doc[2]/arr[@name='"
            + vectorField
            + "']/float[2][.='1.0']",
        "//result/doc[2]/arr[@name='"
            + VECTORS_PSEUDOFIELD
            + "'][1]/doc[2]/arr[@name='"
            + vectorField
            + "']/float[3][.='1.0']",
        "//result/doc[2]/arr[@name='"
            + VECTORS_PSEUDOFIELD
            + "'][1]/doc[2]/arr[@name='"
            + vectorField
            + "']/float[4][.='1.0']",
        "//result/doc[3]/str[@name='id'][.='2']",
        "//result/doc[3]/arr[@name='"
            + VECTORS_PSEUDOFIELD
            + "'][1]/doc[1]/arr[@name='"
            + vectorField
            + "']/float[1][.='28.0']",
        "//result/doc[3]/arr[@name='"
            + VECTORS_PSEUDOFIELD
            + "'][1]/doc[1]/arr[@name='"
            + vectorField
            + "']/float[2][.='1.0']",
        "//result/doc[3]/arr[@name='"
            + VECTORS_PSEUDOFIELD
            + "'][1]/doc[1]/arr[@name='"
            + vectorField
            + "']/float[3][.='1.0']",
        "//result/doc[3]/arr[@name='"
            + VECTORS_PSEUDOFIELD
            + "'][1]/doc[1]/arr[@name='"
            + vectorField
            + "']/float[4][.='1.0']",
        "//result/doc[3]/arr[@name='"
            + VECTORS_PSEUDOFIELD
            + "'][1]/doc[2]/arr[@name='"
            + vectorField
            + "']/float[1][.='27.0']",
        "//result/doc[3]/arr[@name='"
            + VECTORS_PSEUDOFIELD
            + "'][1]/doc[2]/arr[@name='"
            + vectorField
            + "']/float[2][.='1.0']",
        "//result/doc[3]/arr[@name='"
            + VECTORS_PSEUDOFIELD
            + "'][1]/doc[2]/arr[@name='"
            + vectorField
            + "']/float[3][.='1.0']",
        "//result/doc[3]/arr[@name='"
            + VECTORS_PSEUDOFIELD
            + "'][1]/doc[2]/arr[@name='"
            + vectorField
            + "']/float[4][.='1.0']");
  }

  protected void parentRetrievalByte_topKWithChildTransformer_shouldReturnAllChildren(
      String vectorByteField) {
    assertQ(
        req(
            "q",
            "{!parent which=$allParents score=max v=$children.q}",
            "fl",
            "id,score,"
                + VECTORS_PSEUDOFIELD
                + ","
                + vectorByteField
                + ",[child limit=2 fl="
                + vectorByteField
                + "]",
            "children.q",
            "{!knn f="
                + vectorByteField
                + " topK=3 parents.preFilter=$someParents childrenOf=$allParents}"
                + BYTE_QUERY_VECTOR,
            "allParents",
            "parent_s:[* TO *]",
            "someParents",
            "parent_s:(b c)"),
        "//result[@numFound='3']",
        "//result/doc[1]/str[@name='id'][.='8']",
        "//result/doc[1]/arr[@name='"
            + VECTORS_PSEUDOFIELD
            + "'][1]/doc[1]/arr[@name='"
            + vectorByteField
            + "']/int[1][.='10']",
        "//result/doc[1]/arr[@name='"
            + VECTORS_PSEUDOFIELD
            + "'][1]/doc[1]/arr[@name='"
            + vectorByteField
            + "']/int[2][.='1']",
        "//result/doc[1]/arr[@name='"
            + VECTORS_PSEUDOFIELD
            + "'][1]/doc[1]/arr[@name='"
            + vectorByteField
            + "']/int[3][.='1']",
        "//result/doc[1]/arr[@name='"
            + VECTORS_PSEUDOFIELD
            + "'][1]/doc[1]/arr[@name='"
            + vectorByteField
            + "']/int[4][.='1']",
        "//result/doc[1]/arr[@name='"
            + VECTORS_PSEUDOFIELD
            + "'][1]/doc[2]/arr[@name='"
            + vectorByteField
            + "']/int[1][.='9']",
        "//result/doc[1]/arr[@name='"
            + VECTORS_PSEUDOFIELD
            + "'][1]/doc[2]/arr[@name='"
            + vectorByteField
            + "']/int[2][.='1']",
        "//result/doc[1]/arr[@name='"
            + VECTORS_PSEUDOFIELD
            + "'][1]/doc[2]/arr[@name='"
            + vectorByteField
            + "']/int[3][.='1']",
        "//result/doc[1]/arr[@name='"
            + VECTORS_PSEUDOFIELD
            + "'][1]/doc[2]/arr[@name='"
            + vectorByteField
            + "']/int[4][.='1']",
        "//result/doc[2]/str[@name='id'][.='7']",
        "//result/doc[2]/arr[@name='"
            + VECTORS_PSEUDOFIELD
            + "'][1]/doc[1]/arr[@name='"
            + vectorByteField
            + "']/int[1][.='13']",
        "//result/doc[2]/arr[@name='"
            + VECTORS_PSEUDOFIELD
            + "'][1]/doc[1]/arr[@name='"
            + vectorByteField
            + "']/int[2][.='1']",
        "//result/doc[2]/arr[@name='"
            + VECTORS_PSEUDOFIELD
            + "'][1]/doc[1]/arr[@name='"
            + vectorByteField
            + "']/int[3][.='1']",
        "//result/doc[2]/arr[@name='"
            + VECTORS_PSEUDOFIELD
            + "'][1]/doc[1]/arr[@name='"
            + vectorByteField
            + "']/int[4][.='1']",
        "//result/doc[2]/arr[@name='"
            + VECTORS_PSEUDOFIELD
            + "'][1]/doc[2]/arr[@name='"
            + vectorByteField
            + "']/int[1][.='12']",
        "//result/doc[2]/arr[@name='"
            + VECTORS_PSEUDOFIELD
            + "'][1]/doc[2]/arr[@name='"
            + vectorByteField
            + "']/int[2][.='1']",
        "//result/doc[2]/arr[@name='"
            + VECTORS_PSEUDOFIELD
            + "'][1]/doc[2]/arr[@name='"
            + vectorByteField
            + "']/int[3][.='1']",
        "//result/doc[2]/arr[@name='"
            + VECTORS_PSEUDOFIELD
            + "'][1]/doc[2]/arr[@name='"
            + vectorByteField
            + "']/int[4][.='1']",
        "//result/doc[3]/str[@name='id'][.='2']",
        "//result/doc[3]/arr[@name='"
            + VECTORS_PSEUDOFIELD
            + "'][1]/doc[1]/arr[@name='"
            + vectorByteField
            + "']/int[1][.='28']",
        "//result/doc[3]/arr[@name='"
            + VECTORS_PSEUDOFIELD
            + "'][1]/doc[1]/arr[@name='"
            + vectorByteField
            + "']/int[2][.='1']",
        "//result/doc[3]/arr[@name='"
            + VECTORS_PSEUDOFIELD
            + "'][1]/doc[1]/arr[@name='"
            + vectorByteField
            + "']/int[3][.='1']",
        "//result/doc[3]/arr[@name='"
            + VECTORS_PSEUDOFIELD
            + "'][1]/doc[1]/arr[@name='"
            + vectorByteField
            + "']/int[4][.='1']",
        "//result/doc[3]/arr[@name='"
            + VECTORS_PSEUDOFIELD
            + "'][1]/doc[2]/arr[@name='"
            + vectorByteField
            + "']/int[1][.='27']",
        "//result/doc[3]/arr[@name='"
            + VECTORS_PSEUDOFIELD
            + "'][1]/doc[2]/arr[@name='"
            + vectorByteField
            + "']/int[2][.='1']",
        "//result/doc[3]/arr[@name='"
            + VECTORS_PSEUDOFIELD
            + "'][1]/doc[2]/arr[@name='"
            + vectorByteField
            + "']/int[3][.='1']",
        "//result/doc[3]/arr[@name='"
            + VECTORS_PSEUDOFIELD
            + "'][1]/doc[2]/arr[@name='"
            + vectorByteField
            + "']/int[4][.='1']");
  }

  protected void parentRetrievalByte_topKWithChildTransformerWithFilter_shouldReturnBestChild(
      String vectorByteField) {
    assertQ(
        req(
            "q",
            "{!parent which=$allParents score=max v=$children.q}",
            "fl",
            "id,score,"
                + VECTORS_PSEUDOFIELD
                + ","
                + vectorByteField
                + ",[child fl="
                + vectorByteField
                + " childFilter=$children.q]",
            "children.q",
            "{!knn f="
                + vectorByteField
                + " topK=3 parents.preFilter=$someParents childrenOf=$allParents}"
                + BYTE_QUERY_VECTOR,
            "allParents",
            "parent_s:[* TO *]",
            "someParents",
            "parent_s:(b c)"),
        "//result[@numFound='3']",
        "//result/doc[1]/str[@name='id'][.='8']",
        "//result/doc[1]/arr[@name='"
            + VECTORS_PSEUDOFIELD
            + "'][1]/doc[1]/arr[@name='"
            + vectorByteField
            + "']/int[1][.='8']",
        "//result/doc[1]/arr[@name='"
            + VECTORS_PSEUDOFIELD
            + "'][1]/doc[1]/arr[@name='"
            + vectorByteField
            + "']/int[2][.='1']",
        "//result/doc[1]/arr[@name='"
            + VECTORS_PSEUDOFIELD
            + "'][1]/doc[1]/arr[@name='"
            + vectorByteField
            + "']/int[3][.='1']",
        "//result/doc[1]/arr[@name='"
            + VECTORS_PSEUDOFIELD
            + "'][1]/doc[1]/arr[@name='"
            + vectorByteField
            + "']/int[4][.='1']",
        "//result/doc[2]/str[@name='id'][.='7']",
        "//result/doc[2]/arr[@name='"
            + VECTORS_PSEUDOFIELD
            + "'][1]/doc[1]/arr[@name='"
            + vectorByteField
            + "']/int[1][.='11']",
        "//result/doc[2]/arr[@name='"
            + VECTORS_PSEUDOFIELD
            + "'][1]/doc[1]/arr[@name='"
            + vectorByteField
            + "']/int[2][.='1']",
        "//result/doc[2]/arr[@name='"
            + VECTORS_PSEUDOFIELD
            + "'][1]/doc[1]/arr[@name='"
            + vectorByteField
            + "']/int[3][.='1']",
        "//result/doc[2]/arr[@name='"
            + VECTORS_PSEUDOFIELD
            + "'][1]/doc[1]/arr[@name='"
            + vectorByteField
            + "']/int[4][.='1']",
        "//result/doc[3]/str[@name='id'][.='2']",
        "//result/doc[3]/arr[@name='"
            + VECTORS_PSEUDOFIELD
            + "'][1]/doc[1]/arr[@name='"
            + vectorByteField
            + "']/int[1][.='26']",
        "//result/doc[3]/arr[@name='"
            + VECTORS_PSEUDOFIELD
            + "'][1]/doc[1]/arr[@name='"
            + vectorByteField
            + "']/int[2][.='1']",
        "//result/doc[3]/arr[@name='"
            + VECTORS_PSEUDOFIELD
            + "'][1]/doc[1]/arr[@name='"
            + vectorByteField
            + "']/int[3][.='1']",
        "//result/doc[3]/arr[@name='"
            + VECTORS_PSEUDOFIELD
            + "'][1]/doc[1]/arr[@name='"
            + vectorByteField
            + "']/int[4][.='1']");
  }
}
