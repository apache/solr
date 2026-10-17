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
package org.apache.solr.search.stats;

import java.util.List;
import java.util.Set;
import org.apache.lucene.index.Term;
import org.apache.solr.SolrTestCase;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.handler.component.ShardResponse;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrQueryRequestBase;

/** A shard that never answered (no live replica) must not break the global stats merge. */
public class ExactStatsCacheMergeTest extends SolrTestCase {

  public void testShardWithoutResponseIsSkipped() {
    SolrQueryRequest req = new SolrQueryRequestBase(null, new ModifiableSolrParams()) {};
    ShardResponse noAnswer = new ShardResponse();
    noAnswer.setSolrResponse(new QueryResponse()); // getResponse() == null, no exception
    ShardResponse answered = new ShardResponse();
    answered.setSolrResponse(responseWithTerm(new Term("field", "a")));

    new ExactStatsCache().mergeToGlobalStats(req, List.of(noAnswer, answered));

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
