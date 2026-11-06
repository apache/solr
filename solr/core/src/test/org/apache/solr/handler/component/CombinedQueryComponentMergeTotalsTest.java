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

import static org.apache.solr.SolrTestCaseJ4.assumeWorkingMockito;
import static org.apache.solr.handler.component.MockShardRequest.header;

import java.util.Arrays;
import java.util.List;
import org.apache.lucene.search.SortField;
import org.apache.solr.SolrTestCase;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.IntPointField;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.SortSpec;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Tests for the totals that {@link CombinedQueryComponent#mergeIds} reports: a partial shard that
 * the merge skips contributes nothing to its query's totals, the approximateTotalHits fallback adds
 * each shard's own count, and the merged exactness flag is true only when every contributing shard
 * reports an exact count and no shard's hits were left out. Fabricated shard responses are driven
 * through {@code mergeIds} directly.
 */
public class CombinedQueryComponentMergeTotalsTest extends SolrTestCase {

  private final CombinedQueryComponent cqc = combinedQueryComponent();

  @BeforeClass
  public static void setup() {
    assumeWorkingMockito();
  }

  private static CombinedQueryComponent combinedQueryComponent() {
    CombinedQueryComponent cqc = new CombinedQueryComponent();
    cqc.init(new NamedList<>());
    cqc.inform(Mockito.mock(SolrCore.class));
    return cqc;
  }

  private static SolrDocument doc(String id, float score) {
    SolrDocument doc = new SolrDocument();
    doc.addField("id", id);
    doc.addField("score", score);
    return doc;
  }

  private static SolrDocumentList docList(long numFound, Boolean exact, SolrDocument... docs) {
    SolrDocumentList list = new SolrDocumentList();
    for (SolrDocument doc : docs) {
      list.add(doc);
    }
    list.setNumFound(numFound);
    if (exact != null) {
      list.setNumFoundExact(exact);
    }
    return list;
  }

  private static SortSpec scoreSortSpec() {
    return MockSortSpecBuilder.create()
        .withSortFields(new SortField[] {SortField.FIELD_SCORE})
        .withSchemaFields(Arrays.asList((SchemaField) null))
        .build();
  }

  private static SortSpec intSortSpec() {
    return MockSortSpecBuilder.create()
        .withSortFields(new SortField[] {new SortField("intDefault", SortField.Type.INT)})
        .withSchemaFields(List.of(new SchemaField("intDefault", new IntPointField())))
        .withIncludesNonScoreOrDocSortField(true)
        .build();
  }

  /** sort_values_0 is keyed by field name; the value list is per doc, in doc order. */
  private static NamedList<List<Object>> intSortValues() {
    NamedList<List<Object>> sortValues = new NamedList<>();
    sortValues.add("intDefault", List.<Object>of("1", "2", "3"));
    return sortValues;
  }

  /** The fallback adds each shard's own count (2 and 3), not the running total (2 then 7). */
  @Test
  public void fallbackUsesEachShardsOwnCount() {
    SolrDocument a1 = doc("a1", 2.0f), a2 = doc("a2", 1.0f);
    SolrDocument b1 = doc("b1", 3.0f), b2 = doc("b2", 2.0f), b3 = doc("b3", 1.0f);
    MockShardRequest sreq =
        MockShardRequest.create()
            .withShardResponse(
                "shard1",
                header(
                    SolrQueryResponse.RESPONSE_HEADER_MAX_HITS_TERMINATED_EARLY_KEY, Boolean.TRUE),
                docList(2, true, a1, a2),
                List.of(docList(2, true, a1, a2)),
                null)
            .withShardResponse(
                "shard2",
                header(),
                docList(3, true, b1, b2, b3),
                List.of(docList(3, true, b1, b2, b3)),
                null);
    MockResponseBuilder rb =
        MockResponseBuilder.create().withSortSpec(scoreSortSpec()).withCombinerQueries("q0");
    cqc.mergeIds(rb, sreq);
    assertEquals("numFound", 5, rb.getResponseDocs().getNumFound());
    assertEquals(
        "approximateTotalHits",
        5L,
        rb.rsp
            .getResponseHeader()
            .get(SolrQueryResponse.RESPONSE_HEADER_APPROXIMATE_TOTAL_HITS_KEY));
  }

  /** A skipped partial shard contributes nothing to the totals of the query it was skipped for. */
  @Test
  public void skippedPartialShardContributesNothing() {
    SolrDocument a1 = doc("a1", 2.0f), a2 = doc("a2", 1.0f);
    SolrDocument b1 = doc("b1", 3.0f), b2 = doc("b2", 2.0f), b3 = doc("b3", 1.0f);
    MockShardRequest sreq =
        MockShardRequest.create()
            .withShardResponse(
                "shard1",
                header(
                    SolrQueryResponse.RESPONSE_HEADER_PARTIAL_RESULTS_KEY,
                    Boolean.TRUE,
                    SolrQueryResponse.RESPONSE_HEADER_MAX_HITS_TERMINATED_EARLY_KEY,
                    Boolean.TRUE,
                    SolrQueryResponse.RESPONSE_HEADER_APPROXIMATE_TOTAL_HITS_KEY,
                    10L),
                docList(4, true, a1, a2),
                List.of(docList(4, true, a1, a2)),
                null)
            .withShardResponse(
                "shard2",
                header(SolrQueryResponse.RESPONSE_HEADER_APPROXIMATE_TOTAL_HITS_KEY, 5L),
                docList(3, true, b1, b2, b3),
                List.of(docList(3, true, b1, b2, b3)),
                intSortValues());
    MockResponseBuilder rb =
        MockResponseBuilder.create().withSortSpec(intSortSpec()).withCombinerQueries("q0");
    cqc.mergeIds(rb, sreq);
    assertEquals("numFound", 3, rb.getResponseDocs().getNumFound());
    assertEquals("numFoundExact", Boolean.FALSE, rb.getResponseDocs().getNumFoundExact());
    assertEquals(
        "approximateTotalHits",
        5L,
        rb.rsp
            .getResponseHeader()
            .get(SolrQueryResponse.RESPONSE_HEADER_APPROXIMATE_TOTAL_HITS_KEY));
    assertEquals("response size", 3, rb.getResponseDocs().size());
  }

  @Test
  public void skippedPartialShardWithHitsClearsExactness() {
    SolrDocument a1 = doc("a1", 2.0f), a2 = doc("a2", 1.0f);
    MockShardRequest sreq =
        MockShardRequest.create()
            .withShardResponse(
                "shard1",
                header(SolrQueryResponse.RESPONSE_HEADER_PARTIAL_RESULTS_KEY, Boolean.TRUE),
                docList(2, true, a1, a2),
                List.of(docList(2, true, a1, a2)),
                null)
            .withShardResponse(
                "shard2",
                header(SolrQueryResponse.RESPONSE_HEADER_PARTIAL_RESULTS_KEY, Boolean.TRUE),
                docList(0, true),
                List.of(docList(0, true)),
                null);
    MockResponseBuilder rb =
        MockResponseBuilder.create().withSortSpec(intSortSpec()).withCombinerQueries("q0");
    cqc.mergeIds(rb, sreq);
    assertEquals("numFound", 0, rb.getResponseDocs().getNumFound());
    assertEquals("numFoundExact", Boolean.FALSE, rb.getResponseDocs().getNumFoundExact());
  }

  /** A skipped partial shard that sent no per-query entry reports no count: inexact. */
  @Test
  public void skippedPartialShardWithoutCountClearsExactness() {
    SolrDocument b1 = doc("b1", 3.0f), b2 = doc("b2", 2.0f), b3 = doc("b3", 1.0f);
    MockShardRequest sreq =
        MockShardRequest.create()
            .withShardResponse(
                "shard1",
                header(SolrQueryResponse.RESPONSE_HEADER_PARTIAL_RESULTS_KEY, Boolean.TRUE),
                docList(0, true),
                null,
                null)
            .withShardResponse(
                "shard2",
                header(),
                docList(3, true, b1, b2, b3),
                List.of(docList(3, true, b1, b2, b3)),
                intSortValues());
    MockResponseBuilder rb =
        MockResponseBuilder.create().withSortSpec(intSortSpec()).withCombinerQueries("q0");
    cqc.mergeIds(rb, sreq);
    assertEquals("numFound", 3, rb.getResponseDocs().getNumFound());
    assertEquals("numFoundExact", Boolean.FALSE, rb.getResponseDocs().getNumFoundExact());
    assertEquals("response size", 3, rb.getResponseDocs().size());
  }

  /** A skipped partial shard that reported no hits still leaves the count inexact. */
  @Test
  public void skippedPartialShardWithoutHitsClearsExactness() {
    SolrDocument b1 = doc("b1", 3.0f), b2 = doc("b2", 2.0f), b3 = doc("b3", 1.0f);
    MockShardRequest sreq =
        MockShardRequest.create()
            .withShardResponse(
                "shard1",
                header(SolrQueryResponse.RESPONSE_HEADER_PARTIAL_RESULTS_KEY, Boolean.TRUE),
                docList(0, true),
                List.of(docList(0, true)),
                null)
            .withShardResponse(
                "shard2",
                header(),
                docList(3, true, b1, b2, b3),
                List.of(docList(3, true, b1, b2, b3)),
                intSortValues());
    MockResponseBuilder rb =
        MockResponseBuilder.create().withSortSpec(intSortSpec()).withCombinerQueries("q0");
    cqc.mergeIds(rb, sreq);
    assertEquals("numFound", 3, rb.getResponseDocs().getNumFound());
    assertEquals("numFoundExact", Boolean.FALSE, rb.getResponseDocs().getNumFoundExact());
    assertEquals("response size", 3, rb.getResponseDocs().size());
  }

  /** Both shards report exact counts; the merged flag stays exact. */
  @Test
  public void exactnessAllExactStaysExact() {
    SolrDocument a1 = doc("a1", 2.0f), a2 = doc("a2", 1.0f);
    SolrDocument b1 = doc("b1", 3.0f), b2 = doc("b2", 2.0f), b3 = doc("b3", 1.0f);
    MockShardRequest sreq =
        MockShardRequest.create()
            .withShardResponse(
                "shard1",
                header(),
                docList(2, true, a1, a2),
                List.of(docList(2, true, a1, a2)),
                null)
            .withShardResponse(
                "shard2",
                header(),
                docList(3, true, b1, b2, b3),
                List.of(docList(3, true, b1, b2, b3)),
                null);
    MockResponseBuilder rb =
        MockResponseBuilder.create().withSortSpec(scoreSortSpec()).withCombinerQueries("q0");
    cqc.mergeIds(rb, sreq);
    assertEquals("numFoundExact", Boolean.TRUE, rb.getResponseDocs().getNumFoundExact());
  }

  /** Both shards report inexact counts; the merged flag is inexact. */
  @Test
  public void exactnessAllInexactStaysInexact() {
    SolrDocument a1 = doc("a1", 2.0f), a2 = doc("a2", 1.0f);
    SolrDocument b1 = doc("b1", 3.0f), b2 = doc("b2", 2.0f), b3 = doc("b3", 1.0f);
    MockShardRequest sreq =
        MockShardRequest.create()
            .withShardResponse(
                "shard1",
                header(),
                docList(2, false, a1, a2),
                List.of(docList(2, false, a1, a2)),
                null)
            .withShardResponse(
                "shard2",
                header(),
                docList(3, false, b1, b2, b3),
                List.of(docList(3, false, b1, b2, b3)),
                null);
    MockResponseBuilder rb =
        MockResponseBuilder.create().withSortSpec(scoreSortSpec()).withCombinerQueries("q0");
    cqc.mergeIds(rb, sreq);
    assertEquals("numFoundExact", Boolean.FALSE, rb.getResponseDocs().getNumFoundExact());
  }

  /** A partial shard with no per-query entry under a score-only sort is skipped: inexact. */
  @Test
  public void scoreOnlyPartialShardWithoutPerQueryListIsSkipped() {
    SolrDocument b1 = doc("b1", 3.0f), b2 = doc("b2", 2.0f), b3 = doc("b3", 1.0f);
    MockShardRequest sreq =
        MockShardRequest.create()
            .withShardResponse(
                "shard1",
                header(SolrQueryResponse.RESPONSE_HEADER_PARTIAL_RESULTS_KEY, Boolean.TRUE),
                docList(0, true),
                null,
                null)
            .withShardResponse(
                "shard2",
                header(),
                docList(3, true, b1, b2, b3),
                List.of(docList(3, true, b1, b2, b3)),
                null);
    MockResponseBuilder rb =
        MockResponseBuilder.create().withSortSpec(scoreSortSpec()).withCombinerQueries("q0");
    cqc.mergeIds(rb, sreq);
    assertEquals("numFound", 3, rb.getResponseDocs().getNumFound());
    assertEquals("numFoundExact", Boolean.FALSE, rb.getResponseDocs().getNumFoundExact());
    assertEquals("response size", 3, rb.getResponseDocs().size());
    assertEquals(
        "partialResults",
        Boolean.TRUE,
        rb.rsp.getResponseHeader().get(SolrQueryResponse.RESPONSE_HEADER_PARTIAL_RESULTS_KEY));
  }

  @Test
  public void multipleContributingSubQueriesCountDistinctDocsExactly() {
    SolrDocument c1 = doc("c1", 5.0f), c2 = doc("c2", 4.0f), c3 = doc("c3", 3.0f);
    SolrDocument d1 = doc("d1", 2.0f), d2 = doc("d2", 1.0f);
    MockShardRequest sreq =
        MockShardRequest.create()
            .withShardResponse(
                "shard1",
                header(),
                docList(5, true, c1, c2, c3, d1, d2),
                List.of(docList(3, true, c1, c2, c3), docList(3, true, c1, d1, d2)),
                null);
    MockResponseBuilder rb =
        MockResponseBuilder.create().withSortSpec(scoreSortSpec()).withCombinerQueries("q0", "q1");
    cqc.mergeIds(rb, sreq);
    assertEquals("numFound", 5, rb.getResponseDocs().getNumFound());
    assertEquals("numFoundExact", Boolean.TRUE, rb.getResponseDocs().getNumFoundExact());
    assertEquals("response size", 5, rb.getResponseDocs().size());
  }

  /** A shard that errored leaves its hits out, so the merged count is inexact. */
  @Test
  public void erroredShardClearsExactness() {
    SolrDocument b1 = doc("b1", 3.0f), b2 = doc("b2", 2.0f), b3 = doc("b3", 1.0f);
    MockShardRequest sreq =
        MockShardRequest.create()
            .withErroredShard("shard1", new RuntimeException("shard unavailable"))
            .withShardResponse(
                "shard2",
                header(),
                docList(3, true, b1, b2, b3),
                List.of(docList(3, true, b1, b2, b3)),
                null);
    MockResponseBuilder rb =
        MockResponseBuilder.create()
            .withSortSpec(scoreSortSpec())
            .withCombinerQueries("q0")
            .withShardsTolerant();
    cqc.mergeIds(rb, sreq);
    assertEquals("numFound", 3, rb.getResponseDocs().getNumFound());
    assertEquals("numFoundExact", Boolean.FALSE, rb.getResponseDocs().getNumFoundExact());
    assertEquals(
        "partialResults",
        Boolean.TRUE,
        rb.rsp.getResponseHeader().get(SolrQueryResponse.RESPONSE_HEADER_PARTIAL_RESULTS_KEY));
  }
}
