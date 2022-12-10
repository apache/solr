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
package org.apache.solr.util.querytransfer;

import java.io.IOException;
import java.util.Base64;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.solr.SolrTestCase;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.search.QParserPlugin;
import org.apache.solr.search.SyntaxError;
import org.junit.Test;

public class QueryTransferTest extends SolrTestCase {
  @Test
  public void testTermQuery() throws IOException, SyntaxError {
    final TermQuery termQuery = randomTermQuery();
    assertTransfer(termQuery);
  }

  private static TermQuery randomTermQuery() {
    return new TermQuery(
        new Term(TestUtil.randomSimpleString(random()), TestUtil.randomUnicodeString(random())));
  }

  @Test
  public void testBoostQuery() throws IOException, SyntaxError {
    final TermQuery termQuery = randomTermQuery();
    final BoostQuery boostQuery = randomBoostQuery(termQuery);
    assertTransfer(boostQuery);
  }

  final BooleanClause.Occur[] occurs = BooleanClause.Occur.values();

  @Test
  public void testBoolQuery() throws IOException, SyntaxError {
    final BooleanQuery.Builder builder = new BooleanQuery.Builder();
    Set<Query> noDupes = new HashSet<>();
    int clauses = 1 + random().nextInt(5);
    for (int i = 0; i < clauses; i++) {
      Query termQuery = randomTermQuery();
      if (random().nextBoolean()) {
        termQuery = randomBoostQuery(termQuery);
      }
      if (noDupes.add(termQuery)) {
        builder.add(termQuery, occurs[random().nextInt(occurs.length)]);
      }
    }
    if (random().nextBoolean()) {
      builder.setMinimumNumberShouldMatch(random().nextInt(5));
    }
    assertTransfer(builder.build());
  }

  private static BoostQuery randomBoostQuery(Query termQuery) {
    return new BoostQuery(termQuery, random().nextFloat() * random().nextInt(10));
  }

  private static void assertTransfer(Query termQuery) throws IOException, SyntaxError {
    final Query mltQ = mltRemoteReduce(termQuery);
    assertEquals(termQuery, mltQ);
    assertNotSame(termQuery, mltQ);
  }

  private static Query mltRemoteReduce(Query termQuery) throws IOException, SyntaxError {
    final byte[] blob = QueryTransfer.transfer(termQuery);
    final LocalSolrQueryRequest req = new LocalSolrQueryRequest(null, new NamedList<>());
    final String qstr = Base64.getEncoder().encodeToString(blob);
    final Query mltQ;
    try (QParserPlugin plugin = new QueryTransferQParserPlugin()) {
      mltQ =
          plugin
              .createParser(qstr, new MapSolrParams(Map.of()), new MapSolrParams(Map.of()), req)
              .getQuery();
      req.close();
    }
    return mltQ;
  }
}
