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

import java.util.List;
import java.util.Set;
import org.apache.lucene.index.Term;
import org.apache.solr.SolrTestCase;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrQueryRequestBase;
import org.apache.solr.search.stats.ExactStatsCache;
import org.apache.solr.search.stats.StatsCache;
import org.apache.solr.search.stats.StatsUtil;

/**
 * The stock state for a shard no server answered: an exception on the ShardResponse and a body that
 * was never set. With shards.tolerant=true the merge skips such a shard on the exception check,
 * before it reads the body.
 */
public class ExactStatsCacheNoAnswerTest extends SolrTestCase {

  public void testTolerantNoAnswerShardIsSkippedBeforeBodyRead() {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set(ShardParams.SHARDS_TOLERANT, "true");
    params.set(CommonParams.PARTIAL_RESULTS, "true");
    SolrQueryRequest req = new SolrQueryRequestBase(null, params) {};

    ShardResponse noServer = new ShardResponse();
    noServer.setSolrResponse(new HttpShardHandler.SimpleSolrResponse());
    noServer.setException(
        new SolrException(SolrException.ErrorCode.SERVICE_UNAVAILABLE, "no servers hosting shard"));
    ShardResponse answered = new ShardResponse();
    answered.setSolrResponse(responseWithTerm(new Term("field", "a")));

    new ExactStatsCache().mergeToGlobalStats(req, List.of(noServer, answered));

    assertEquals(Set.of(new Term("field", "a")), mergedTerms(req));
  }

  private static QueryResponse responseWithTerm(Term term) {
    NamedList<Object> body = new NamedList<>();
    body.add(StatsCache.TERMS_KEY, StatsUtil.termsToEncodedString(List.of(term)));
    QueryResponse rsp = new QueryResponse();
    rsp.setResponse(body);
    return rsp;
  }

  private static Set<Term> mergedTerms(SolrQueryRequest req) {
    return StatsUtil.termsFromEncodedString((String) req.getContext().get(StatsCache.TERMS_KEY));
  }
}
