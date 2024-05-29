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
package org.apache.solr.search.combining;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.search.neural.KnnBaseTest;
import org.apache.solr.search.neural.KnnQParserTest;
import org.junit.Test;

public class CombiningIntegrationTest extends KnnBaseTest {

  @Test
  public void reciprocalRankFusion_lexicalVector_shouldReturnHybridRanking() {
    String vectorToSearch = "[1.0, 2.0, 3.0, 4.0]";
    // lexical Ranking: [10,2]
    // vector-based Ranking: [1,4,2,10,3]
    assertQ(
            req(
                    CommonParams.JSON,
                    "{ \"queries\":{"
                            + " \"lexical\": { \"lucene\": { \"query\": \"id:(10^=2 OR 2^=1)\" }},"
                            + " \"vector-based\": { \"knn\": { \"f\": \"vector\", \"topK\": 5, \"query\": \"" + vectorToSearch + "\" }}"
                            + "},"
                            + " \"limit\": 10,"
                            + " \"fields\": [id,score],"
                            + " \"params\":{\"combiner\": true}}"),
            "//result[@numFound='5']",
            "//result/doc[1]/str[@name='id'][.='10']",
            "//result/doc[2]/str[@name='id'][.='2']",
            "//result/doc[3]/str[@name='id'][.='1']",
            "//result/doc[4]/str[@name='id'][.='4']",
            "//result/doc[5]/str[@name='id'][.='3']");
  }
  
  @Test
  public void reciprocalRankFusion_lexicalVectorWithUpTo_shouldOnlyConsiderUpToPerRankedList() {
    String vectorToSearch = "[1.0, 2.0, 3.0, 4.0]";
    // lexical Ranking: [4,2]
    // vector-based Ranking: [1,4,2,10,3]
    assertQ(
        req(
            CommonParams.JSON,
            "{ \"queries\":{"
                + " \"lexical\": { \"lucene\": { \"query\": \"id:(4^=2 OR 2^=1)\" }},"
                + " \"vector-based\": { \"knn\": { \"f\": \"vector\", \"topK\": 10, \"query\": \"" + vectorToSearch + "\" }}"
                + "},"
                + " \"limit\": 10,"
                + " \"fields\": [id,score],"
                + " \"params\":{\"combiner\": true,\"combiner.upTo\": 2}}"),
        "//result[@numFound='3']",
        "//result/doc[1]/str[@name='id'][.='4']",
        "//result/doc[2]/str[@name='id'][.='1']",
        "//result/doc[3]/str[@name='id'][.='2']");
  }

  @Test
  public void reciprocalRankFusion_lexicalLexical_shouldReturnHybridRanking() {
    // lexical Ranking: [10,2,4]
    // lexical2 Ranking: [2,4,3]
    assertQ(
        req(
            CommonParams.JSON,
            "{ \"queries\": {"
                + " \"lexical1\": { \"lucene\": { \"query\": \"id:(10^=2 OR 2^=1 OR 4^=0.5)\" }},"
                + " \"lexical2\": { \"lucene\": { \"query\": \"id:(2^=2 OR 4^=1 OR 3^=0.5)\" }}"
                + "},"
                + " \"limit\": 10, \"fields\": [id,score],"
                + " \"params\":{\"combiner\": true}}"),
        "//result[@numFound='4']",
        "//result/doc[1]/str[@name='id'][.='2']",
        "//result/doc[2]/str[@name='id'][.='4']",
        "//result/doc[3]/str[@name='id'][.='10']",
        "//result/doc[4]/str[@name='id'][.='3']");
  }

  @Test
  public void reciprocalRankFusion_faceting_shouldReturnFacetsOnCombinedResults() {
    final String facet_xpath = "//lst[@name='facet_fields']/lst[@name='id']/int";
    
    // lexical Ranking: [10,2,4]
    // lexical2 Ranking: [2,4,3]
    assertQ(
            req(
                    CommonParams.JSON,
                    "{ \"queries\": {"
                            + " \"lexical1\": { \"lucene\": { \"query\": \"id:(10^=2 OR 2^=1 OR 4^=0.5)\" }},"
                            + " \"lexical2\": { \"lucene\": { \"query\": \"id:(2^=2 OR 4^=1 OR 3^=0.5)\" }}"
                            + "},"
                            + " \"limit\": 10, \"fields\": [id,score],"
                            + " \"params\":{\"combiner\": true,\"combiner.upTo\": 5,\"facet\": true,\"facet.field\":\"id\",\"facet.mincount\":1}}"),
            "//result[@numFound='4']",
            "//result/doc[1]/str[@name='id'][.='2']",
            "//result/doc[2]/str[@name='id'][.='4']",
            "//result/doc[3]/str[@name='id'][.='10']",
            "//result/doc[4]/str[@name='id'][.='3']",
            "*[count(" + facet_xpath + ")=4]",
            facet_xpath + "[@name='2'][.='1']",
            facet_xpath + "[@name='3'][.='1']",
            facet_xpath + "[@name='4'][.='1']",
            facet_xpath + "[@name='10'][.='1']");
  }

  @Test
  public void reciprocalRankFusion_queriesWithPostFilters_shouldPostFilter() {

    // lexical Ranking: [10,2,4]
    // lexical2 Ranking: [2,4,3]
    assertQ(
            req(
                    CommonParams.JSON,
                    "{ \"queries\": {"
                            + " \"lexical1\": { \"lucene\": { \"query\": \"id:(10^=2 OR 2^=1 OR 4^=0.5)\" }},"
                            + " \"lexical2\": { \"lucene\": { \"query\": \"id:(2^=2 OR 4^=1 OR 3^=0.5)\" }}"
                            + "},"
                            + " \"limit\": 10, \"fields\": [id,score],"
                            + " \"filter\": \"id:(4 OR 3)\","
                            + " \"params\":{\"combiner\": true}}"),
            "//result[@numFound='2']",
            "//result/doc[1]/str[@name='id'][.='4']",
            "//result/doc[2]/str[@name='id'][.='3']");
  }

  @Test
  public void reciprocalRankFusion_lexicalVectorWithFilters_shouldReturnHybridRanking() {//TO DO
    String vectorToSearch = "[1.0, 2.0, 3.0, 4.0]";
    // lexical Ranking: [8,7]
    // vector-based Ranking: [10,7,9,8]
    assertQ(
            req(
                    CommonParams.JSON,
                    "{ \"queries\":{"
                            + " \"lexical\": { \"lucene\": { \"query\": \"id:(8^=2 OR 7^=1)\" }},"
                            + " \"vector-based\": { \"knn\": { \"f\": \"vector\", \"topK\": 5, \"query\": \"" + vectorToSearch + "\" }}"
                            + "},"
                            + " \"limit\": 10,"
                            + " \"fields\": [id,score],"
                            + " \"filter\": \"id:(7 OR 8 OR 9 OR 10)\","
                            + " \"params\":{\"combiner\": true}}"),
            "//result[@numFound='4']",
            "//result/doc[1]/str[@name='id'][.='7']",
            "//result/doc[2]/str[@name='id'][.='8']",
            "//result/doc[3]/str[@name='id'][.='10']",
            "//result/doc[4]/str[@name='id'][.='9']");
  }
  
  @Test
  public void reciprocalRankFusion_oneMalformedQuery_shouldRaiseException() {
    String vectorToSearch = "2.0, 4.4, 3.5, 6.4";
    // lexical Ranking: [4,2]
    // vector-based Ranking: Exception
    
    assertQEx(
            "incorrect vector to search should throw Exception",
            "incorrect vector format. The expected format is:'[f1,f2..f3]' where each element f is a float",
            req(
                    CommonParams.JSON,
                    "{ \"queries\":{"
                            + " \"lexical\": { \"lucene\": { \"query\": \"id:(4^=2 OR 2^=1)\" }},"
                            + " \"vector-based\": { \"knn\": { \"f\": \"vector\", \"topK\": 10, \"query\": \"" + vectorToSearch + "\" }}"
                            + "},"
                            + " \"limit\": 10,"
                            + " \"fields\": [id,score],"
                            + " \"params\":{\"combiner\": true}}"),
            SolrException.ErrorCode.BAD_REQUEST);
  }

  @Test
  public void reciprocalRankFusion_oneQueryWithException_shouldRaiseException() {
    String vectorToSearch = "[1.0, 2.0, 3.0, 4.0]";
    // lexical Ranking: [4,2]
    // vector-based Ranking: Exception
    
    assertQEx(
            "Undefined vector field should throw Exception",
            "undefined field: \"notExistent\"",
            req(
                    CommonParams.JSON,
                    "{ \"queries\":{"
                            + " \"lexical\": { \"lucene\": { \"query\": \"id:(4^=2 OR 2^=1)\" }},"
                            + " \"vector-based\": { \"knn\": { \"f\": \"notExistent\", \"topK\": 10, \"query\": \"" + vectorToSearch + "\" }}"
                            + "},"
                            + " \"limit\": 10,"
                            + " \"fields\": [id,score],"
                            + " \"params\":{\"combiner\": true}}"),
            SolrException.ErrorCode.BAD_REQUEST);
  }
}
