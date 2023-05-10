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
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.SynonymQuery;
import org.apache.lucene.search.Weight;

/**
 * Wraps a SynonymQuery and stores an Integer startOffset taken from the Token that gave rise to the
 * contained Terms.
 */
public final class SynonymQueryWithOffset extends Query implements OffsetHolder {

  private final SynonymQuery query;

  private final Integer startOffset;

  public SynonymQueryWithOffset(SynonymQuery query, Integer offset) {
    this.query = query;
    this.startOffset = offset;
  }

  public SynonymQuery getQuery() {
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
   * Equality is based on the contained SynonymQuery and ignores the startOffset. A
   * SynonymQueryWithOffset will consider itself equal to the SynonymQuery that it contains.
   *
   * <p>Note that this relationship is not currently symmetric. A SynonymQuery that will not
   * consider itself equal to any SynonymQueryWithOffset because SynonymQuery.equals() requires
   * class equality. This could be fixed by updating SynonymQuery.equals() inside the lucene
   * codebase.
   */
  @Override
  public boolean equals(Object obj) {
    if (obj instanceof SynonymQueryWithOffset) {
      return query.equals(((SynonymQueryWithOffset) obj).getQuery());
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
