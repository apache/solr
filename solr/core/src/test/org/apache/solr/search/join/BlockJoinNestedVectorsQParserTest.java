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

import org.apache.lucene.search.join.ScoreMode;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.core.SolrCore;
import org.apache.solr.metrics.MetricsMap;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.util.BaseTestHarness;
import org.apache.solr.util.RandomNoReverseMergePolicyFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TestRule;

import javax.xml.xpath.XPathConstants;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class BlockJoinNestedVectorsQParserTest extends SolrTestCaseJ4 {

  private static final String[] klm = new String[] {"k", "l", "m"};
  private static final String[] abcdef = new String[] {"a", "b", "c", "d", "e", "f"};
  private static int vectorsIndex = 30;
  private static List<Float> floatQueryVector = Arrays.asList(1.0f, 1.0f, 1.0f, 1.0f);
  private static List<Integer> byteQueryVector = Arrays.asList(1, 1, 1, 1);




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

  private static List<SolrInputDocument> prepareDocs() {
    int parentCount = 10;
    int perParentChildren = 3;
    List<SolrInputDocument> docs = new ArrayList<>(parentCount);
    for (int i = 1; i < parentCount + 1; i++) {
      SolrInputDocument doc = new SolrInputDocument();
      doc.setField("id", i);
      doc.setField("parent_b", true);

      doc.setField("parent_s", abcdef[i % abcdef.length]);
      List<SolrInputDocument> children = new ArrayList<>(perParentChildren);

      for(int j = 0; j < perParentChildren; j++) {
        SolrInputDocument child = new SolrInputDocument();
        child.setField("id", i+""+j);
        child.setField("child_s", klm[i % klm.length]);
        child.setField("vector", perElementAddFloat(floatQueryVector, vectorsIndex));
        child.setField("vector_byte", perElementAddInteger(byteQueryVector, vectorsIndex));
        vectorsIndex--;
        children.add(child);
      }
      doc.setField("vectors",children);
      docs.add(doc);
    }
    
    return docs;
  }

  private static List<Float> perElementAddFloat(List<Float> vector, int value) {
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
  

  private static List<Integer> perElementAddInteger(List<Integer> vector, int value){
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
  public void childrenRetrievalFloat_filteringByParentMetadata_shouldReturnKnnChildren() {
    assertQ(
            req(
                    "fq", "{!child of=$allParents filters=$parent.fq}",
                    "q", "{!knn f=vector topK=5}[1.0, 1.0, 1.0, 1.0]",
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
                    "q", "{!knn f=vector_byte topK=5}[1, 1, 1, 1]",
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
  public void parentRetrievalFloat_knnChildren_shouldReturnKnnChildren() {
    assertQ(
            req(
                   // "fq", "parent_s:(a c)",
                    "q", "{!parent which=$allParents score=max v=$children.q}",
                    "fl", "id,score",
                    "children.q", "{!knn f=vector topK=3}[1.0, 1.0, 1.0, 1.0]",
                    "allParents", "parent_s:[* TO *]"),
            "//*[@numFound='3']",
            "//result/doc[1]/str[@name='id'][.='10']",
            "//result/doc[2]/str[@name='id'][.='9']",
            "//result/doc[3]/str[@name='id'][.='8']");
  }

  @Test
  public void parentRetrievalFloat_knnChildrenWithParentFilter_shouldReturnKnnChildren() {
    assertQ(
            req(
                    "fq", "parent_s:(a c)",
                    "q", "{!parent which=$allParents score=max v=$children.q}",
                    "fl", "id,score",
                    "children.q", "{!knn f=vector topK=3}[1.0, 1.0, 1.0, 1.0]",
                    "allParents", "parent_s:[* TO *]"),
            "//*[@numFound='3']",
            "//result/doc[1]/str[@name='id'][.='8']",
            "//result/doc[2]/str[@name='id'][.='6']",
            "//result/doc[3]/str[@name='id'][.='2']");
  }

  @Test
  public void parentRetrievalByte_knnChildren_shouldReturnKnnChildren() {
    assertQ(
            req(
                    // "fq", "parent_s:(a c)",
                    "q", "{!parent which=$allParents score=max v=$children.q}",
                    "fl", "id,score",
                    "children.q", "{!knn f=vector_byte topK=3}[1, 1, 1, 1]",
                    "allParents", "parent_s:[* TO *]"),
            "//*[@numFound='3']",
            "//result/doc[1]/str[@name='id'][.='10']",
            "//result/doc[2]/str[@name='id'][.='9']",
            "//result/doc[3]/str[@name='id'][.='8']");
  }

  @Test
  public void parentRetrievalByte_knnChildrenWithParentFilter_shouldReturnKnnChildren() {
    assertQ(
            req(
                    "fq", "parent_s:(a c)",
                    "q", "{!parent which=$allParents score=max v=$children.q}",
                    "fl", "id,score",
                    "children.q", "{!knn f=vector_byte topK=3}[1, 1, 1, 1]",
                    "allParents", "parent_s:[* TO *]"),
            "//*[@numFound='3']",
            "//result/doc[1]/str[@name='id'][.='8']",
            "//result/doc[2]/str[@name='id'][.='6']",
            "//result/doc[3]/str[@name='id'][.='2']");
  }
  
  
}
