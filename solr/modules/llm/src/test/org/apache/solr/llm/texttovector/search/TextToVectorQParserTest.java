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
package org.apache.solr.llm.texttovector.search;

import java.util.Arrays;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.llm.TestLlmBase;
import org.junit.BeforeClass;
import org.junit.Test;

public class TextToVectorQParserTest extends TestLlmBase {
  @BeforeClass
  public static void init() throws Exception {
    setupTest("solrconfig-llm.xml", "schema.xml", true, false);
    loadModel("dummy-model.json");
  }

  @Test
  public void notExistentModel_shouldThrowException() throws Exception {
    final String solrQuery = "{!knn_text_to_vector model=not-exist f=vector topK=5}hello world";
    final SolrQuery query = new SolrQuery();
    query.setQuery(solrQuery);
    query.add("fl", "id");

    assertJQ(
        "/query" + query.toQueryString(),
        "/error/msg=='The model requested \\'not-exist\\' can\\'t be found in the store: /schema/text-to-vector-model-store'",
        "/error/code==400");
  }

  @Test
  public void missingModelParam_shouldThrowException() throws Exception {
    final String solrQuery = "{!knn_text_to_vector f=vector topK=5}hello world";
    final SolrQuery query = new SolrQuery();
    query.setQuery(solrQuery);
    query.add("fl", "id");

    assertJQ(
        "/query" + query.toQueryString(),
        "/error/msg=='The \\'model\\' parameter is missing'",
        "/error/code==400");
  }

  @Test
  public void incorrectVectorFieldType_shouldThrowException() throws Exception {
    final String solrQuery = "{!knn_text_to_vector model=dummy-1 f=id topK=5}hello world";
    final SolrQuery query = new SolrQuery();
    query.setQuery(solrQuery);
    query.add("fl", "id");

    assertJQ(
        "/query" + query.toQueryString(),
        "/error/msg=='only DenseVectorField is compatible with Vector Query Parsers'",
        "/error/code==400");
  }

  @Test
  public void undefinedVectorField_shouldThrowException() throws Exception {
    final String solrQuery = "{!knn_text_to_vector model=dummy-1 f=notExistent topK=5}hello world";
    final SolrQuery query = new SolrQuery();
    query.setQuery(solrQuery);
    query.add("fl", "id");

    assertJQ(
        "/query" + query.toQueryString(),
        "/error/msg=='undefined field: \"notExistent\"'",
        "/error/code==400");
  }

  @Test
  public void missingVectorFieldParam_shouldThrowException() throws Exception {
    final String solrQuery = "{!knn_text_to_vector model=dummy-1 topK=5}hello world";
    final SolrQuery query = new SolrQuery();
    query.setQuery(solrQuery);
    query.add("fl", "id");

    assertJQ(
        "/query" + query.toQueryString(),
        "/error/msg=='the Dense Vector field \\'f\\' is missing'",
        "/error/code==400");
  }

  @Test
  public void vectorByteEncodingField_shouldRaiseException() throws Exception {
    final String solrQuery =
        "{!knn_text_to_vector model=dummy-1 f=vector_byte_encoding topK=5}hello world";
    final SolrQuery query = new SolrQuery();
    query.setQuery(solrQuery);
    query.add("fl", "id");

    assertJQ(
        "/query" + query.toQueryString(),
        "/error/msg=='Vector Encoding not supported : BYTE'",
        "/error/code==500");
  }

  @Test
  public void missingQueryToEmbed_shouldThrowException() throws Exception {
    final String solrQuery = "{!knn_text_to_vector model=dummy-1 f=vector topK=5}";
    final SolrQuery query = new SolrQuery();
    query.setQuery(solrQuery);
    query.add("fl", "id");

    assertJQ(
        "/query" + query.toQueryString(),
        "/error/msg=='Query string is empty, nothing to vectorise'",
        "/error/code==400");
  }

  @Test
  public void incorrectVectorToSearchDimension_shouldThrowException() throws Exception {
    final String solrQuery =
        "{!knn_text_to_vector model=dummy-1 f=2048_float_vector topK=5}hello world";
    final SolrQuery query = new SolrQuery();
    query.setQuery(solrQuery);
    query.add("fl", "id");

    assertJQ(
        "/query" + query.toQueryString(),
        "/error/msg=='incorrect vector dimension. The vector value has size 4 while it is expected a vector with size 2048'",
        "/error/code==400");
  }

  @Test
  public void topK_shouldEmbedAndReturnOnlyTopKResults() throws Exception {
    final String solrQuery = "{!knn_text_to_vector model=dummy-1 f=vector topK=5}hello world";
    final SolrQuery query = new SolrQuery();
    query.setQuery(solrQuery);
    query.add("fl", "id");

    assertJQ(
        "/query" + query.toQueryString(),
        "/response/numFound==5]",
        "/response/docs/[0]/id=='1'",
        "/response/docs/[1]/id=='4'",
        "/response/docs/[2]/id=='2'",
        "/response/docs/[3]/id=='10'",
        "/response/docs/[4]/id=='3'");
  }

  @Test
  public void vectorFieldParam_shouldSearchOnThatField() throws Exception {
    final String solrQuery = "{!knn_text_to_vector model=dummy-1 f=vector2 topK=5}hello world";
    final SolrQuery query = new SolrQuery();
    query.setQuery(solrQuery);
    query.add("fl", "id");

    assertJQ(
        "/query" + query.toQueryString(),
        "/response/numFound==3]",
        "/response/docs/[0]/id=='11'",
        "/response/docs/[1]/id=='13'",
        "/response/docs/[2]/id=='12'");
  }

  @Test
  public void embeddedQuery_shouldRankBySimilarityFunction() throws Exception {
    final String solrQuery = "{!knn_text_to_vector model=dummy-1 f=vector topK=10}hello world";
    final SolrQuery query = new SolrQuery();
    query.setQuery(solrQuery);
    query.add("fl", "id");

    assertJQ(
        "/query" + query.toQueryString(),
        "/response/numFound==10]",
        "/response/docs/[0]/id=='1'",
        "/response/docs/[1]/id=='4'",
        "/response/docs/[2]/id=='2'",
        "/response/docs/[3]/id=='10'",
        "/response/docs/[4]/id=='3'",
        "/response/docs/[5]/id=='7'",
        "/response/docs/[6]/id=='5'",
        "/response/docs/[7]/id=='6'",
        "/response/docs/[8]/id=='9'",
        "/response/docs/[9]/id=='8'");
  }

  @Test
  public void embeddedQueryUsedInFilter_shouldFilterResultsBeforeTheQueryExecution()
      throws Exception {
    final String solrQuery = "{!knn_text_to_vector model=dummy-1 f=vector topK=4}hello world";
    final SolrQuery query = new SolrQuery();
    query.setQuery("id:(3 4 9 2)");
    query.setFilterQueries(solrQuery);
    query.add("fl", "id");

    assertJQ(
        "/query" + query.toQueryString(),
        "/response/numFound==2]",
        "/response/docs/[0]/id=='2'",
        "/response/docs/[1]/id=='4'");
  }

  @Test
  public void embeddedQueryUsedInFilters_shouldFilterResultsBeforeTheQueryExecution()
      throws Exception {
    final String solrQuery = "{!knn_text_to_vector model=dummy-1 f=vector topK=4}hello world";
    final SolrQuery query = new SolrQuery();
    query.setQuery("id:(3 4 9 2)");
    query.setFilterQueries(solrQuery, "id:(4 20 9)");
    query.add("fl", "id");

    // topK=4 -> 1,4,2,10
    assertJQ(
        "/query" + query.toQueryString(), "/response/numFound==1]", "/response/docs/[0]/id=='4'");
  }

  @Test
  public void embeddedQueryUsedInFiltersWithPreFilter_shouldFilterResultsBeforeTheQueryExecution()
      throws Exception {
    final String solrQuery =
        "{!knn_text_to_vector model=dummy-1 f=vector topK=4 preFilter='id:(1 4 7 8 9)'}hello world";
    final SolrQuery query = new SolrQuery();
    query.setQuery("id:(3 4 9 2)");
    query.setFilterQueries(solrQuery, "id:(4 20 9)");
    query.add("fl", "id");

    // topK=4 w/localparam preFilter -> 1,4,7,9
    assertJQ(
        "/query" + query.toQueryString(),
        "/response/numFound==2]",
        "/response/docs/[0]/id=='4'",
        "/response/docs/[1]/id=='9'");
  }

  @Test
  public void embeddedQueryUsedInFilters_rejectIncludeExclude() throws Exception {
    for (String fq :
        Arrays.asList(
            "{!knn_text_to_vector model=dummy-1 f=vector topK=5 includeTags=xxx}hello world",
            "{!knn_text_to_vector model=dummy-1 f=vector topK=5 excludeTags=xxx}hello world")) {
      final SolrQuery query = new SolrQuery();
      query.setQuery("*:*");
      query.setFilterQueries(fq);
      query.add("fl", "id");

      assertJQ(
          "/query" + query.toQueryString(),
          "/error/msg=='Knn Query Parser used as a filter does not support includeTags or excludeTags localparams'",
          "/error/code==400");
    }
  }

  @Test
  public void embeddedQueryAsSubQuery() throws Exception {
    final String solrQuery =
        "*:* AND {!knn_text_to_vector model=dummy-1 f=vector topK=5 v='hello world'}";
    final SolrQuery query = new SolrQuery();
    query.setQuery(solrQuery);
    query.setFilterQueries("id:(2 4 7 9 8 20 3)");
    query.add("fl", "id");

    // When knn parser is a subquery, it should not pre-filter on any global fq params
    // topK -> 1,4,2,10,3 -> fq -> 4,2,3
    assertJQ(
        "/query" + query.toQueryString(),
        "/response/numFound==3]",
        "/response/docs/[0]/id=='4'",
        "/response/docs/[1]/id=='2'",
        "/response/docs/[2]/id=='3'");
  }

  @Test
  public void embeddedQueryAsSubQuery_withPreFilter() throws Exception {
    final String solrQuery =
        "*:* AND {!knn_text_to_vector model=dummy-1 f=vector topK=5 preFilter='id:(2 4 7 9 8 20 3)' v='hello world'}";
    final SolrQuery query = new SolrQuery();
    query.setQuery(solrQuery);
    query.add("fl", "id");

    // knn subquery should still accept `preFilter` local param
    // filt -> topK -> 4,2,3,7,9
    assertJQ(
        "/query" + query.toQueryString(),
        "/response/numFound==5]",
        "/response/docs/[0]/id=='4'",
        "/response/docs/[1]/id=='2'",
        "/response/docs/[2]/id=='3'",
        "/response/docs/[3]/id=='7'",
        "/response/docs/[4]/id=='9'");
  }

  @Test
  public void embeddedQueryAsSubQuery_rejectIncludeExclude() throws Exception {
    for (String q :
        Arrays.asList(
            "{!knn_text_to_vector model=dummy-1 f=vector topK=5 includeTags=xxx}hello world",
            "{!knn_text_to_vector model=dummy-1 f=vector topK=5 excludeTags=xxx}hello world")) {
      final SolrQuery query = new SolrQuery();
      query.setQuery("*:* OR " + q);
      query.add("fl", "id");

      assertJQ(
          "/query" + query.toQueryString(),
          "/error/msg=='Knn Query Parser used as a sub-query does not support includeTags or excludeTags localparams'",
          "/error/code==400");
    }
  }

  @Test
  public void embeddedQueryWithCostlyFq_shouldPerformKnnSearchWithPostFilter() throws Exception {
    final String solrQuery = "{!knn_text_to_vector model=dummy-1 f=vector topK=10}hello world";
    final SolrQuery query = new SolrQuery();
    query.setQuery(solrQuery);
    query.setFilterQueries("{!frange cache=false l=0.99}$q");
    query.add("fl", "id");

    assertJQ(
        "/query" + query.toQueryString(),
        "/response/numFound==5]",
        "/response/docs/[0]/id=='1'",
        "/response/docs/[1]/id=='4'",
        "/response/docs/[2]/id=='2'",
        "/response/docs/[3]/id=='10'",
        "/response/docs/[4]/id=='3'");
  }

  @Test
  public void embeddedQueryWithFilterQueries_shouldPerformKnnSearchWithPreFiltersAndPostFilters()
      throws Exception {
    final String solrQuery = "{!knn_text_to_vector model=dummy-1 f=vector topK=4}hello world";
    final SolrQuery query = new SolrQuery();
    query.setQuery(solrQuery);
    query.setFilterQueries("id:(3 4 9 2)", "{!frange cache=false l=0.99}$q");
    query.add("fl", "id");

    assertJQ(
        "/query" + query.toQueryString(),
        "/response/numFound==2]",
        "/response/docs/[0]/id=='4'",
        "/response/docs/[1]/id=='2'");
  }

  @Test
  public void embeddedQueryWithNegativeFilterQuery_shouldPerformKnnSearchInPreFilteredResults()
      throws Exception {
    final String solrQuery = "{!knn_text_to_vector model=dummy-1 f=vector topK=4}hello world";
    final SolrQuery query = new SolrQuery();
    query.setQuery(solrQuery);
    query.setFilterQueries("-id:4");
    query.add("fl", "id");

    assertJQ(
        "/query" + query.toQueryString(),
        "/response/numFound==4]",
        "/response/docs/[0]/id=='1'",
        "/response/docs/[1]/id=='2'",
        "/response/docs/[2]/id=='10'",
        "/response/docs/[3]/id=='3'");
  }

  /**
   * See {@link org.apache.solr.search.ReRankQParserPlugin.ReRankQueryRescorer#combine(float,
   * boolean, float)}} for more details.
   */
  @Test
  public void embeddedQueryAsRerank_shouldAddSimilarityFunctionScore() throws Exception {
    final SolrQuery query = new SolrQuery();
    query.set("rq", "{!rerank reRankQuery=$rqq reRankDocs=4 reRankWeight=1}");
    query.set("rqq", "{!knn_text_to_vector model=dummy-1 f=vector topK=4}hello world");
    query.setQuery("id:(3 4 9 2)");
    query.add("fl", "id");

    assertJQ(
        "/query" + query.toQueryString(),
        "/response/numFound==4]",
        "/response/docs/[0]/id=='4'",
        "/response/docs/[1]/id=='2'",
        "/response/docs/[2]/id=='3'",
        "/response/docs/[3]/id=='9'");
  }
}
