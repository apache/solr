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
package org.apache.solr.parser;

import java.io.IOException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;

/**
 * Wraps a PhraseQuery and stores an Integer startOffset taken from the Token that gave rise to the
 * contained Terms.
 */
public final class PhraseQueryWithOffset extends Query implements OffsetHolder {

  private final PhraseQuery query;

  private final Integer startOffset;

  public PhraseQueryWithOffset(PhraseQuery query, Integer offset) {
    this.query = query;
    this.startOffset = offset;
  }

  public int getSlop() {
    return query.getSlop();
  }

  /** Returns the field this query applies to */
  public String getField() {
    return query.getField();
  }

  /** Returns the list of terms in this phrase. */
  public Term[] getTerms() {
    return query.getTerms();
  }

  /** Returns the relative positions of terms in this phrase. */
  public int[] getPositions() {
    return query.getPositions();
  }

  public PhraseQuery getQuery() {
    return query;
  }

  public Integer getStartOffset() {
    return startOffset;
  }

  public boolean hasStartOffset() {
    return startOffset != null;
  }

  @Override
  public String toString(String field) {
    return query.toString();
  }

  @Override
  public void visit(QueryVisitor visitor) {
    query.visit(visitor);
  }

  /**
   * Equality is based on the contained PhraseQuery and ignores the startOffset. A
   * PhraseQueryWithOffset will consider itself equal to the PhraseQuery that it contains.
   *
   * <p>Note that this relationship is not currently symmetric. A PhraseQuery that will not consider
   * itself equal to any PhraseQueryWithOffset because SPhraseQuery.equals() requires class
   * equality. This could be fixed by updating PhraseQuery.equals() inside the lucene codebase.
   */
  @Override
  public boolean equals(Object obj) {
    if (obj instanceof PhraseQueryWithOffset) {
      return query.equals(((PhraseQueryWithOffset) obj).getQuery());
    }
    return query.equals(obj);
  }

  @Override
  public int hashCode() {
    return query.hashCode();
  }

  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    return query.rewrite(reader);
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost)
      throws IOException {
    return query.createWeight(searcher, scoreMode, boost);
  }
}
