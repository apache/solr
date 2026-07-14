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
package org.apache.solr.ltr;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakLingering;
import java.util.List;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.SolrQuery;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.search.AbstractReRankQuery;
import org.junit.Test;

@ThreadLeakLingering(linger = 10)
public class TestLTRReRankCutoffOnSolrCloud extends AbstractLTRSolrCloudTestBase {

  private static final int NUM_SHARDS = 3;
  private static final int NUM_REPLICAS = 2;

  @Override
  protected int numberOfShards() {
    return NUM_SHARDS;
  }

  @Override
  protected int numberOfReplicas() {
    return NUM_REPLICAS;
  }

  @Test
  public void distributedRerankCutoffScore_shouldBeReturnedForShardsWithHits() throws Exception {
    final SolrQuery query = newBaseRerankQuery();
    query.add("rq", "{!ltr model=powpularityS-model reRankDocs=3 echoReRankCutoff=true}");

    final QueryRequest queryRequest = new QueryRequest(query);
    queryRequest.setPath("/query");
    final QueryResponse queryResponse =
        queryRequest.process(solrCluster.getSolrClient(), COLLECTION);

    @SuppressWarnings("unchecked")
    final NamedList<Object> perShardCutoff =
        (NamedList<Object>)
            queryResponse
                .getResponseHeader()
                .get(AbstractReRankQuery.RERANK_CUTOFF_BY_SHARD_RESPONSE_HEADER_KEY);
    assertNotNull(perShardCutoff);

    for (int i = 0; i < perShardCutoff.size(); i++) {
      assertNotNull(perShardCutoff.getName(i));
      assertNotNull(perShardCutoff.getVal(i));
      assertTrue(perShardCutoff.getVal(i) instanceof List);
      assertEquals(2, ((List<?>) perShardCutoff.getVal(i)).size());
    }
  }

  @Test
  public void distributedRerankCutoffScore_defaultShouldNotBeReturnedPerShard() throws Exception {
    final SolrQuery query = newBaseRerankQuery();
    query.add("rq", "{!ltr model=powpularityS-model reRankDocs=3}");

    final QueryRequest queryRequest = new QueryRequest(query);
    queryRequest.setPath("/query");
    final QueryResponse queryResponse =
        queryRequest.process(solrCluster.getSolrClient(), COLLECTION);
    assertNull(
        queryResponse
            .getResponseHeader()
            .get(AbstractReRankQuery.RERANK_CUTOFF_BY_SHARD_RESPONSE_HEADER_KEY));
  }

  @Test
  public void distributedRerankCutoffScore_falseShouldNotBeReturnedPerShard() throws Exception {
    final SolrQuery query = newBaseRerankQuery();
    query.add("rq", "{!ltr model=powpularityS-model reRankDocs=3 echoReRankCutoff=false}");

    final QueryRequest queryRequest = new QueryRequest(query);
    queryRequest.setPath("/query");
    final QueryResponse queryResponse =
        queryRequest.process(solrCluster.getSolrClient(), COLLECTION);
    assertNull(
        queryResponse
            .getResponseHeader()
            .get(AbstractReRankQuery.RERANK_CUTOFF_BY_SHARD_RESPONSE_HEADER_KEY));
  }

  private SolrQuery newBaseRerankQuery() {
    final SolrQuery query = new SolrQuery("{!func}sub(8,field(popularity))");
    query.setFields("id", "score");
    query.setRows(4);
    query.setParam("sort", "score desc,id asc");
    return query;
  }
}
