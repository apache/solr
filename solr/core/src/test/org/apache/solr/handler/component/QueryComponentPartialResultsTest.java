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
package org.apache.solr.handler.component;

import org.apache.lucene.search.SortField;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.search.SortSpec;
import org.junit.BeforeClass;
import org.junit.Test;

public class QueryComponentPartialResultsTest extends SolrTestCaseJ4 {
  private static final String SORT_FIELD_NAME = "category";
  private static final int shard1Size = 2;
  private static final int shard2Size = 3;

  private static int id = 0;
  private static ShardRequest shardRequestWithPartialResults;

  @BeforeClass
  public static void setup() {
    assumeWorkingMockito();
    shardRequestWithPartialResults = createShardRequestWithPartialResults();
  }

  @Test
  public void includesPartialShardResultWhenUsingImplicitScoreSort() {
    SortSpec sortSpec =
        MockSortSpecBuilder.create().withIncludesNonScoreOrDocSortField(false).build();
    testPartialResultsForSortSpec(sortSpec, true);
  }

  @Test
  public void includesPartialShardResultWhenUsingExplicitScoreSort() {
    SortSpec sortSpec =
        MockSortSpecBuilder.create()
            .withSortFields(new SortField[] {SortField.FIELD_SCORE})
            .withIncludesNonScoreOrDocSortField(false)
            .build();
    testPartialResultsForSortSpec(sortSpec, true);
  }

  @Test
  public void includesPartialShardResultWhenUsingExplicitDocSort() {
    SortSpec sortSpec =
        MockSortSpecBuilder.create()
            .withSortFields(new SortField[] {SortField.FIELD_DOC})
            .withIncludesNonScoreOrDocSortField(false)
            .build();
    testPartialResultsForSortSpec(sortSpec, true);
  }

  @Test
  public void excludesPartialShardResultWhenUsingNonScoreOrDocSortField() {
    SortField sortField = new SortField(SORT_FIELD_NAME, SortField.Type.INT);
    SortSpec sortSpec =
        MockSortSpecBuilder.create()
            .withSortFields(new SortField[] {sortField})
            .withIncludesNonScoreOrDocSortField(true)
            .build();
    testPartialResultsForSortSpec(sortSpec, false);
  }

  @Test
  public void skippedPartialShardDocsAreAbsent() {
    MockResponseBuilder responseBuilder = mergeWithSkippedPartialShard();
    assertEquals(shard2Size, responseBuilder.getResponseDocs().size());
  }

  @Test
  public void mergedNumFoundCountsOnlyMergedShards() {
    MockResponseBuilder responseBuilder = mergeWithSkippedPartialShard();
    SolrDocumentList responseDocs = responseBuilder.getResponseDocs();
    // the skipped shard contributes nothing to the merged totals: they describe
    // exactly the shard whose documents were merged
    assertEquals(shard2Size, responseDocs.getNumFound());
    assertEquals(Boolean.TRUE, responseDocs.getNumFoundExact());
  }

  @Test
  public void mergedMaxScoreComesFromMergedShards() {
    MockResponseBuilder responseBuilder = mergeWithSkippedPartialShard();
    assertEquals(Float.valueOf(3.0f), responseBuilder.getResponseDocs().getMaxScore());
  }

  @Test
  public void mergedApproximateTotalHitsExcludesSkippedShard() {
    MockResponseBuilder responseBuilder = mergeWithSkippedPartialShard();
    assertEquals(
        3L,
        responseBuilder
            .rsp
            .getResponseHeader()
            .get(SolrQueryResponse.RESPONSE_HEADER_APPROXIMATE_TOTAL_HITS_KEY));
  }

  private MockResponseBuilder mergeWithSkippedPartialShard() {
    SortField sortField = new SortField(SORT_FIELD_NAME, SortField.Type.INT);
    SortSpec sortSpec =
        MockSortSpecBuilder.create()
            .withSortFields(new SortField[] {sortField})
            .withIncludesNonScoreOrDocSortField(true)
            .build();
    MockResponseBuilder responseBuilder = MockResponseBuilder.create().withSortSpec(sortSpec);

    // shard1 answers with partial results and no sort_values, so its documents are
    // skipped at merge time; it still reports its own totals in its response
    final NamedList<Object> shard1ResponseHeader = new NamedList<>();
    shard1ResponseHeader.add(SolrQueryResponse.RESPONSE_HEADER_PARTIAL_RESULTS_KEY, Boolean.TRUE);
    shard1ResponseHeader.add(
        SolrQueryResponse.RESPONSE_HEADER_MAX_HITS_TERMINATED_EARLY_KEY, Boolean.TRUE);
    shard1ResponseHeader.add(SolrQueryResponse.RESPONSE_HEADER_APPROXIMATE_TOTAL_HITS_KEY, 7L);
    SolrDocumentList shard1Docs = createSolrDocumentList(shard1Size);
    shard1Docs.setNumFound(shard1Size);
    shard1Docs.setNumFoundExact(true);
    shard1Docs.setMaxScore(99.0f);

    final NamedList<Object> shard2ResponseHeader = new NamedList<>();
    shard2ResponseHeader.add(SolrQueryResponse.RESPONSE_HEADER_APPROXIMATE_TOTAL_HITS_KEY, 3L);
    SolrDocumentList shard2Docs = createSolrDocumentList(shard2Size);
    shard2Docs.setNumFound(shard2Size);
    shard2Docs.setNumFoundExact(true);
    shard2Docs.setMaxScore(3.0f);

    ShardRequest shardRequest =
        MockShardRequest.create()
            .withShardResponse(shard1ResponseHeader, shard1Docs)
            .withShardResponse(shard2ResponseHeader, shard2Docs);

    QueryComponent queryComponent = new QueryComponent();
    queryComponent.mergeIds(responseBuilder, shardRequest);
    return responseBuilder;
  }

  private void testPartialResultsForSortSpec(
      SortSpec sortSpec, boolean shouldIncludePartialShardResult) {

    MockResponseBuilder responseBuilder = MockResponseBuilder.create().withSortSpec(sortSpec);

    QueryComponent queryComponent = new QueryComponent();
    queryComponent.mergeIds(responseBuilder, shardRequestWithPartialResults);

    // do we have the expected document count?
    // if results are not merged for the partial results shard, then the total doc count will
    // exclude them
    if (shouldIncludePartialShardResult) {
      assertEquals(shard1Size + shard2Size, responseBuilder.getResponseDocs().size());
    } else {
      assertEquals(shard2Size, responseBuilder.getResponseDocs().size());
    }
  }

  private static ShardRequest createShardRequestWithPartialResults() {
    final NamedList<Object> shard1ResponseHeader = new NamedList<>();
    final NamedList<Object> shard2ResponseHeader = new NamedList<>();

    // the results from shard1 are marked partial
    shard1ResponseHeader.add(SolrQueryResponse.RESPONSE_HEADER_PARTIAL_RESULTS_KEY, Boolean.TRUE);

    return MockShardRequest.create()
        .withShardResponse(shard1ResponseHeader, createSolrDocumentList(shard1Size))
        .withShardResponse(shard2ResponseHeader, createSolrDocumentList(shard2Size));
  }

  private static SolrDocumentList createSolrDocumentList(int size) {
    SolrDocumentList solrDocuments = new SolrDocumentList();
    for (int i = 0; i < size; i++) {
      SolrDocument solrDocument = new SolrDocument();
      solrDocument.addField("id", id++);
      solrDocument.addField("score", (float) id);
      solrDocument.addField(SORT_FIELD_NAME, id);
      solrDocuments.add(solrDocument);
    }
    return solrDocuments;
  }
}
