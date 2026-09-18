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
package org.apache.solr.search.vector;

import java.io.IOException;
import org.apache.lucene.search.DoubleValuesSource;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.RescoreTopNQuery;

/**
 * A {@link RescoreTopNQuery} that tolerates an inner query matching no documents.
 *
 * <p>{@link RescoreTopNQuery#rewrite} unconditionally hands its collected hits to {@code
 * DocAndScoreQuery#createDocAndScoreQuery}, which requires at least one hit: it asserts as much,
 * and without assertions enabled it reads element zero of an empty array. {@code
 * AbstractKnnVectorQuery#rewrite} guards the very same call by returning {@link MatchNoDocsQuery},
 * but the re-ranking query has no equivalent guard, so an oversampled knn query that matches
 * nothing (a restrictive {@code preFilter}, say) fails instead of returning no results.
 *
 * <p>TODO: remove this class once the guard is added upstream in Lucene and Solr picks up a release
 * containing it.
 */
public class SolrRescoreTopNQuery extends RescoreTopNQuery {

  private final Query innerQuery;
  private final DoubleValuesSource valuesSource;
  private final int n;

  public SolrRescoreTopNQuery(Query query, DoubleValuesSource valuesSource, int n) {
    super(query, valuesSource, n);
    this.innerQuery = query;
    this.valuesSource = valuesSource;
    this.n = n;
  }

  @Override
  public Query rewrite(IndexSearcher indexSearcher) throws IOException {
    final Query rewrittenInner = indexSearcher.rewrite(innerQuery);
    if (rewrittenInner instanceof MatchNoDocsQuery) {
      return rewrittenInner;
    }
    // Delegate using the already rewritten inner query rather than calling super.rewrite(), which
    // would rewrite innerQuery a second time. That matters because rewriting a knn query runs the
    // whole vector search; rewriting its result is a no-op.
    return new RescoreTopNQuery(rewrittenInner, valuesSource, n).rewrite(indexSearcher);
  }
}
