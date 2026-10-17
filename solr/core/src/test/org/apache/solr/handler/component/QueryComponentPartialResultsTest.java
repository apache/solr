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
    // exactly the shard whose documents were merged, and the count is a lower bound
    // because the skipped shard's hits are not in it
    assertEquals(shard2Size, responseDocs.getNumFound());
    assertEquals(Boolean.FALSE, responseDocs.getNumFoundExact());
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
        4L,
        responseBuilder
            .rsp
            .getResponseHeader()
            .get(SolrQueryResponse.RESPONSE_HEADER_APPROXIMATE_TOTAL_HITS_KEY));
  }

  @Test
  public void approximateTotalHitsFallsBackToEachShardsNumFound() {
    SortSpec sortSpec =
        MockSortSpecBuilder.create().withIncludesNonScoreOrDocSortField(false).build();
    MockResponseBuilder responseBuilder = MockResponseBuilder.create().withSortSpec(sortSpec);

    // neither shard sends an approximateTotalHits header, so the merge falls back
    // to each shard's numFound; shard1 flags maxHitsTerminatedEarly so the merged
    // approximate total is written to the response header
    final NamedList<Object> shard1ResponseHeader = new NamedList<>();
    shard1ResponseHeader.add(
        SolrQueryResponse.RESPONSE_HEADER_MAX_HITS_TERMINATED_EARLY_KEY, Boolean.TRUE);
    SolrDocumentList shard1Docs = createSolrDocumentList(shard1Size);
    shard1Docs.setNumFound(shard1Size);
    shard1Docs.setNumFoundExact(true);

    // shard2 returns fewer documents than it matched, as a shard does for any query with more
    // hits than rows, so a fallback that added the number of returned documents would be wrong
    final long shard2NumFound = 8;
    final NamedList<Object> shard2ResponseHeader = new NamedList<>();
    SolrDocumentList shard2Docs = createSolrDocumentList(shard2Size);
    shard2Docs.setNumFound(shard2NumFound);
    shard2Docs.setNumFoundExact(true);

    ShardRequest shardRequest =
        MockShardRequest.create()
            .withShardResponse(shard1ResponseHeader, shard1Docs)
            .withShardResponse(shard2ResponseHeader, shard2Docs);

    QueryComponent queryComponent = new QueryComponent();
    queryComponent.mergeIds(responseBuilder, shardRequest);

    assertEquals(shard1Size + shard2NumFound, responseBuilder.getResponseDocs().getNumFound());
    assertEquals(
        shard1Size + shard2NumFound,
        responseBuilder
            .rsp
            .getResponseHeader()
            .get(SolrQueryResponse.RESPONSE_HEADER_APPROXIMATE_TOTAL_HITS_KEY));
  }

  @Test
  public void skippedShardWithExactZeroLeavesMergedCountExact() {
    SortField sortField = new SortField(SORT_FIELD_NAME, SortField.Type.INT);
    SortSpec sortSpec =
        MockSortSpecBuilder.create()
            .withSortFields(new SortField[] {sortField})
            .withIncludesNonScoreOrDocSortField(true)
            .build();
    MockResponseBuilder responseBuilder = MockResponseBuilder.create().withSortSpec(sortSpec);

    // shard1 is skipped at merge time and reports an exact zero, so it holds no matches that are
    // missing from the merged count; the merged count of shard2 alone is exact
    final NamedList<Object> shard1ResponseHeader = new NamedList<>();
    shard1ResponseHeader.add(SolrQueryResponse.RESPONSE_HEADER_PARTIAL_RESULTS_KEY, Boolean.TRUE);
    SolrDocumentList shard1Docs = createSolrDocumentList(0);
    shard1Docs.setNumFound(0);
    shard1Docs.setNumFoundExact(true);

    final NamedList<Object> shard2ResponseHeader = new NamedList<>();
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

    SolrDocumentList responseDocs = responseBuilder.getResponseDocs();
    assertEquals(shard2Size, responseDocs.getNumFound());
    assertEquals(Boolean.TRUE, responseDocs.getNumFoundExact());
  }

  @Test
  public void skippedShardWithHitsMakesMergedCountInexact() {
    SortField sortField = new SortField(SORT_FIELD_NAME, SortField.Type.INT);
    SortSpec sortSpec =
        MockSortSpecBuilder.create()
            .withSortFields(new SortField[] {sortField})
            .withIncludesNonScoreOrDocSortField(true)
            .build();
    MockResponseBuilder responseBuilder = MockResponseBuilder.create().withSortSpec(sortSpec);

    // shard1 is skipped and reported hits that are not in the merged count, so the
    // merged count of shard2 alone is a lower bound and must not be reported exact
    final NamedList<Object> shard1ResponseHeader = new NamedList<>();
    shard1ResponseHeader.add(SolrQueryResponse.RESPONSE_HEADER_PARTIAL_RESULTS_KEY, Boolean.TRUE);
    SolrDocumentList shard1Docs = createSolrDocumentList(shard1Size);
    shard1Docs.setNumFound(shard1Size);
    shard1Docs.setNumFoundExact(true);

    final NamedList<Object> shard2ResponseHeader = new NamedList<>();
    SolrDocumentList shard2Docs = createSolrDocumentList(shard2Size);
    shard2Docs.setNumFound(shard2Size);
    shard2Docs.setNumFoundExact(true);

    ShardRequest shardRequest =
        MockShardRequest.create()
            .withShardResponse(shard1ResponseHeader, shard1Docs)
            .withShardResponse(shard2ResponseHeader, shard2Docs);

    QueryComponent queryComponent = new QueryComponent();
    queryComponent.mergeIds(responseBuilder, shardRequest);

    SolrDocumentList responseDocs = responseBuilder.getResponseDocs();
    assertEquals(shard2Size, responseDocs.getNumFound());
    assertEquals(Boolean.FALSE, responseDocs.getNumFoundExact());
  }

  @Test
  public void allShardsSkippedDoesNotClaimExactZero() {
    SortField sortField = new SortField(SORT_FIELD_NAME, SortField.Type.INT);
    SortSpec sortSpec =
        MockSortSpecBuilder.create()
            .withSortFields(new SortField[] {sortField})
            .withIncludesNonScoreOrDocSortField(true)
            .build();
    MockResponseBuilder responseBuilder = MockResponseBuilder.create().withSortSpec(sortSpec);

    // both shards answer partial with no sort_values, so both are skipped;
    // both report hits, so a merged count of zero must not be reported exact
    final NamedList<Object> shard1ResponseHeader = new NamedList<>();
    shard1ResponseHeader.add(SolrQueryResponse.RESPONSE_HEADER_PARTIAL_RESULTS_KEY, Boolean.TRUE);
    SolrDocumentList shard1Docs = createSolrDocumentList(shard1Size);
    shard1Docs.setNumFound(shard1Size);
    shard1Docs.setNumFoundExact(true);

    final NamedList<Object> shard2ResponseHeader = new NamedList<>();
    shard2ResponseHeader.add(SolrQueryResponse.RESPONSE_HEADER_PARTIAL_RESULTS_KEY, Boolean.TRUE);
    SolrDocumentList shard2Docs = createSolrDocumentList(shard2Size);
    shard2Docs.setNumFound(shard2Size);
    shard2Docs.setNumFoundExact(true);

    ShardRequest shardRequest =
        MockShardRequest.create()
            .withShardResponse(shard1ResponseHeader, shard1Docs)
            .withShardResponse(shard2ResponseHeader, shard2Docs);

    QueryComponent queryComponent = new QueryComponent();
    queryComponent.mergeIds(responseBuilder, shardRequest);

    SolrDocumentList responseDocs = responseBuilder.getResponseDocs();
    assertEquals(0, responseDocs.size());
    assertEquals(0, responseDocs.getNumFound());
    assertEquals(Boolean.FALSE, responseDocs.getNumFoundExact());
  }

  @Test
  public void approximateTotalHitsMixesHeaderAndFallbackShards() {
    SortSpec sortSpec =
        MockSortSpecBuilder.create().withIncludesNonScoreOrDocSortField(false).build();
    MockResponseBuilder responseBuilder = MockResponseBuilder.create().withSortSpec(sortSpec);

    // shard1 sends an approximateTotalHits header and shard2 does not, so the
    // merged total is the header value plus shard2's own numFound
    final NamedList<Object> shard1ResponseHeader = new NamedList<>();
    shard1ResponseHeader.add(
        SolrQueryResponse.RESPONSE_HEADER_MAX_HITS_TERMINATED_EARLY_KEY, Boolean.TRUE);
    shard1ResponseHeader.add(SolrQueryResponse.RESPONSE_HEADER_APPROXIMATE_TOTAL_HITS_KEY, 11L);
    SolrDocumentList shard1Docs = createSolrDocumentList(shard1Size);
    shard1Docs.setNumFound(shard1Size);
    shard1Docs.setNumFoundExact(true);

    final NamedList<Object> shard2ResponseHeader = new NamedList<>();
    SolrDocumentList shard2Docs = createSolrDocumentList(shard2Size);
    shard2Docs.setNumFound(shard2Size);
    shard2Docs.setNumFoundExact(true);

    ShardRequest shardRequest =
        MockShardRequest.create()
            .withShardResponse(shard1ResponseHeader, shard1Docs)
            .withShardResponse(shard2ResponseHeader, shard2Docs);

    QueryComponent queryComponent = new QueryComponent();
    queryComponent.mergeIds(responseBuilder, shardRequest);

    assertEquals(shard1Size + shard2Size, responseBuilder.getResponseDocs().getNumFound());
    assertEquals(
        11L + shard2Size,
        responseBuilder
            .rsp
            .getResponseHeader()
            .get(SolrQueryResponse.RESPONSE_HEADER_APPROXIMATE_TOTAL_HITS_KEY));
  }

  @Test
  public void mergedMaxScoreIsTheMaximumAcrossMergedShards() {
    SortSpec sortSpec =
        MockSortSpecBuilder.create().withIncludesNonScoreOrDocSortField(false).build();
    MockResponseBuilder responseBuilder = MockResponseBuilder.create().withSortSpec(sortSpec);

    // the highest score sits on the first merged shard, so the merged maxScore
    // is neither the last shard's value nor the minimum
    final NamedList<Object> shard1ResponseHeader = new NamedList<>();
    SolrDocumentList shard1Docs = createSolrDocumentList(shard1Size);
    shard1Docs.setNumFound(shard1Size);
    shard1Docs.setNumFoundExact(true);
    shard1Docs.setMaxScore(5.0f);

    final NamedList<Object> shard2ResponseHeader = new NamedList<>();
    SolrDocumentList shard2Docs = createSolrDocumentList(shard2Size);
    shard2Docs.setNumFound(shard2Size);
    shard2Docs.setNumFoundExact(true);
    shard2Docs.setMaxScore(2.0f);

    ShardRequest shardRequest =
        MockShardRequest.create()
            .withShardResponse(shard1ResponseHeader, shard1Docs)
            .withShardResponse(shard2ResponseHeader, shard2Docs);

    QueryComponent queryComponent = new QueryComponent();
    queryComponent.mergeIds(responseBuilder, shardRequest);

    SolrDocumentList responseDocs = responseBuilder.getResponseDocs();
    assertEquals(shard1Size + shard2Size, responseDocs.getNumFound());
    assertEquals(Float.valueOf(5.0f), responseDocs.getMaxScore());
  }

  @Test
  public void mergedShardInexactCountMakesMergedFlagInexact() {
    SortSpec sortSpec =
        MockSortSpecBuilder.create().withIncludesNonScoreOrDocSortField(false).build();
    MockResponseBuilder responseBuilder = MockResponseBuilder.create().withSortSpec(sortSpec);

    // both shards are merged; shard2 reports an inexact count, so the merged
    // count is inexact too. This pins the merged exactness fold itself: if the
    // fold were deleted, the merged flag would wrongly stay exact.
    final NamedList<Object> shard1ResponseHeader = new NamedList<>();
    SolrDocumentList shard1Docs = createSolrDocumentList(shard1Size);
    shard1Docs.setNumFound(shard1Size);
    shard1Docs.setNumFoundExact(true);

    final NamedList<Object> shard2ResponseHeader = new NamedList<>();
    SolrDocumentList shard2Docs = createSolrDocumentList(shard2Size);
    shard2Docs.setNumFound(shard2Size);
    shard2Docs.setNumFoundExact(false);

    ShardRequest shardRequest =
        MockShardRequest.create()
            .withShardResponse(shard1ResponseHeader, shard1Docs)
            .withShardResponse(shard2ResponseHeader, shard2Docs);

    QueryComponent queryComponent = new QueryComponent();
    queryComponent.mergeIds(responseBuilder, shardRequest);

    SolrDocumentList responseDocs = responseBuilder.getResponseDocs();
    assertEquals(shard1Size + shard2Size, responseDocs.getNumFound());
    assertEquals(Boolean.FALSE, responseDocs.getNumFoundExact());
  }

  @Test
  public void mergedPartialShardTotalsAreCounted() {
    SortSpec sortSpec =
        MockSortSpecBuilder.create().withIncludesNonScoreOrDocSortField(false).build();
    MockResponseBuilder responseBuilder = MockResponseBuilder.create().withSortSpec(sortSpec);

    // shard1 answers with partial results, but the sort needs no unmarshalling,
    // so the shard is merged rather than skipped and its totals count in full
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
    shard2ResponseHeader.add(SolrQueryResponse.RESPONSE_HEADER_APPROXIMATE_TOTAL_HITS_KEY, 4L);
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

    SolrDocumentList responseDocs = responseBuilder.getResponseDocs();
    assertEquals(shard1Size + shard2Size, responseDocs.size());
    assertEquals(shard1Size + shard2Size, responseDocs.getNumFound());
    assertEquals(Boolean.TRUE, responseDocs.getNumFoundExact());
    assertEquals(Float.valueOf(99.0f), responseDocs.getMaxScore());
    assertEquals(
        7L + 4L,
        responseBuilder
            .rsp
            .getResponseHeader()
            .get(SolrQueryResponse.RESPONSE_HEADER_APPROXIMATE_TOTAL_HITS_KEY));
  }

  @Test
  public void mergedMaxScoreIsTheMaximumWhenHighestIsOnLaterShard() {
    SortSpec sortSpec =
        MockSortSpecBuilder.create().withIncludesNonScoreOrDocSortField(false).build();
    MockResponseBuilder responseBuilder = MockResponseBuilder.create().withSortSpec(sortSpec);

    // the highest score sits on the second merged shard, so the merged maxScore
    // is neither the first shard's value nor the minimum
    final NamedList<Object> shard1ResponseHeader = new NamedList<>();
    SolrDocumentList shard1Docs = createSolrDocumentList(shard1Size);
    shard1Docs.setNumFound(shard1Size);
    shard1Docs.setNumFoundExact(true);
    shard1Docs.setMaxScore(2.0f);

    final NamedList<Object> shard2ResponseHeader = new NamedList<>();
    SolrDocumentList shard2Docs = createSolrDocumentList(shard2Size);
    shard2Docs.setNumFound(shard2Size);
    shard2Docs.setNumFoundExact(true);
    shard2Docs.setMaxScore(5.0f);

    ShardRequest shardRequest =
        MockShardRequest.create()
            .withShardResponse(shard1ResponseHeader, shard1Docs)
            .withShardResponse(shard2ResponseHeader, shard2Docs);

    QueryComponent queryComponent = new QueryComponent();
    queryComponent.mergeIds(responseBuilder, shardRequest);

    SolrDocumentList responseDocs = responseBuilder.getResponseDocs();
    assertEquals(shard1Size + shard2Size, responseDocs.getNumFound());
    assertEquals(Float.valueOf(5.0f), responseDocs.getMaxScore());
  }

  @Test
  public void allShardsSkippedWithNoHitsKeepsExactZero() {
    SortField sortField = new SortField(SORT_FIELD_NAME, SortField.Type.INT);
    SortSpec sortSpec =
        MockSortSpecBuilder.create()
            .withSortFields(new SortField[] {sortField})
            .withIncludesNonScoreOrDocSortField(true)
            .build();
    MockResponseBuilder responseBuilder = MockResponseBuilder.create().withSortSpec(sortSpec);

    // both shards are skipped and each reports an exact zero, so no matches are missing from the
    // merged count and the merged zero stays exact
    final NamedList<Object> shard1ResponseHeader = new NamedList<>();
    shard1ResponseHeader.add(SolrQueryResponse.RESPONSE_HEADER_PARTIAL_RESULTS_KEY, Boolean.TRUE);
    SolrDocumentList shard1Docs = createSolrDocumentList(0);
    shard1Docs.setNumFound(0);
    shard1Docs.setNumFoundExact(true);

    final NamedList<Object> shard2ResponseHeader = new NamedList<>();
    shard2ResponseHeader.add(SolrQueryResponse.RESPONSE_HEADER_PARTIAL_RESULTS_KEY, Boolean.TRUE);
    SolrDocumentList shard2Docs = createSolrDocumentList(0);
    shard2Docs.setNumFound(0);
    shard2Docs.setNumFoundExact(true);

    ShardRequest shardRequest =
        MockShardRequest.create()
            .withShardResponse(shard1ResponseHeader, shard1Docs)
            .withShardResponse(shard2ResponseHeader, shard2Docs);

    QueryComponent queryComponent = new QueryComponent();
    queryComponent.mergeIds(responseBuilder, shardRequest);

    SolrDocumentList responseDocs = responseBuilder.getResponseDocs();
    assertEquals(0, responseDocs.size());
    assertEquals(0, responseDocs.getNumFound());
    assertEquals(Boolean.TRUE, responseDocs.getNumFoundExact());
  }

  @Test
  public void allShardsSkippedWithInexactZeroDoesNotClaimExactZero() {
    SortField sortField = new SortField(SORT_FIELD_NAME, SortField.Type.INT);
    SortSpec sortSpec =
        MockSortSpecBuilder.create()
            .withSortFields(new SortField[] {sortField})
            .withIncludesNonScoreOrDocSortField(true)
            .build();
    MockResponseBuilder responseBuilder = MockResponseBuilder.create().withSortSpec(sortSpec);

    // both shards are skipped and report zero hits; shard1's zero is exact, but shard2 reports
    // its zero as inexact, so the merged zero must not be reported exact
    final NamedList<Object> shard1ResponseHeader = new NamedList<>();
    shard1ResponseHeader.add(SolrQueryResponse.RESPONSE_HEADER_PARTIAL_RESULTS_KEY, Boolean.TRUE);
    SolrDocumentList shard1Docs = createSolrDocumentList(0);
    shard1Docs.setNumFound(0);
    shard1Docs.setNumFoundExact(true);

    final NamedList<Object> shard2ResponseHeader = new NamedList<>();
    shard2ResponseHeader.add(SolrQueryResponse.RESPONSE_HEADER_PARTIAL_RESULTS_KEY, Boolean.TRUE);
    SolrDocumentList shard2Docs = createSolrDocumentList(0);
    shard2Docs.setNumFound(0);
    shard2Docs.setNumFoundExact(false);

    ShardRequest shardRequest =
        MockShardRequest.create()
            .withShardResponse(shard1ResponseHeader, shard1Docs)
            .withShardResponse(shard2ResponseHeader, shard2Docs);

    QueryComponent queryComponent = new QueryComponent();
    queryComponent.mergeIds(responseBuilder, shardRequest);

    SolrDocumentList responseDocs = responseBuilder.getResponseDocs();
    assertEquals(0, responseDocs.size());
    assertEquals(0, responseDocs.getNumFound());
    assertEquals(Boolean.FALSE, responseDocs.getNumFoundExact());
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
    // the header value differs from shard2's numFound on purpose, so the test
    // fails if the merge sums numFound instead of using the header values
    shard2ResponseHeader.add(SolrQueryResponse.RESPONSE_HEADER_APPROXIMATE_TOTAL_HITS_KEY, 4L);
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
