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
package org.apache.solr.search;

import java.io.IOException;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;

/**
 * @lucene.experimental
 */
public interface DocSetProducer {
  public DocSet createDocSet(SolrIndexSearcher searcher) throws IOException;

  /**
   * Returns true if any invocation of this Query is guaranteed to create a DocSet. If <code>true
   * </code>, callers may in some cases choose to call {@link #createDocSet(SolrIndexSearcher)}
   * where they otherwise might have default to, e.g., {@link Query#createWeight(IndexSearcher,
   * ScoreMode, float)}.
   */
  default boolean alwaysCreatesDocSet() {
    return false;
  }

  /**
   * Checks the specified instance (unwrapping if necessary), to determine whether any invocation of
   * the query is guaranteed to generate a {@link DocSet}. See {@link #alwaysCreatesDocSet()}.
   */
  static boolean alwaysCreatesDocSet(Query q) {
    for (; ; ) {
      if (q instanceof DocSetProducer) {
        return ((DocSetProducer) q).alwaysCreatesDocSet();
      } else if (q instanceof WrappedQuery) {
        q = ((WrappedQuery) q).getWrappedQuery();
      } else if (q instanceof BoostQuery) {
        q = ((BoostQuery) q).getQuery();
      } else {
        return false;
      }
    }
  }
}
