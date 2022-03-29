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
import java.util.Objects;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;

/**
 * A Query based on a {@link DocSet}. The un-boosted score is always 1.
 *
 * @see DocSet#makeQuery()
 * @since 9.0
 */
class DocSetQuery extends Query implements DocSetProducer {
  private final DocSet docSet;

  DocSetQuery(DocSet docSet) {
    this.docSet = Objects.requireNonNull(docSet);
  }

  @Override
  public String toString(String field) {
    return "DocSetQuery(" + field + ")";
  }

  @Override
  public void visit(QueryVisitor visitor) {
    visitor.visitLeaf(this);
  }

  @Override
  public boolean equals(Object obj) {
    return sameClassAs(obj) && equalsTo(getClass().cast(obj));
  }

  private boolean equalsTo(DocSetQuery other) {
    return Objects.equals(docSet, other.docSet);
  }

  @Override
  public int hashCode() {
    return classHash() * 31 + docSet.hashCode();
  }

  /**
   * @param searcher is not used because we already have a DocSet created in DocSetQuery
   * @return the DocSet created in DocSetQuery
   */
  @Override
  public DocSet createDocSet(SolrIndexSearcher searcher) throws IOException {
    return docSet;
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost)
      throws IOException {
    return new ConstantScoreWeight(this, boost) {
      @Override
      public Scorer scorer(LeafReaderContext context) {
        DocIdSetIterator disi = docSet.iterator(context);
        if (disi == null) {
          return null;
        }
        return new ConstantScoreScorer(this, score(), scoreMode, disi);
      }

      @Override
      public boolean isCacheable(LeafReaderContext ctx) {
        return true;
      }
    };
  }
}
