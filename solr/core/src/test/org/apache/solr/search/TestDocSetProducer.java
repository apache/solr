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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.Weight;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.core.SolrCore;
import org.junit.BeforeClass;
import org.junit.Test;

/** Verify behavior of {@link DocSetProducer} */
public class TestDocSetProducer extends SolrTestCaseJ4 {

  private static final int NUM_DOCS = 100;
  private static final List<Integer> VALS;

  static {
    final Integer[] vals = new Integer[NUM_DOCS];
    for (int i = 0; i < NUM_DOCS; i++) {
      vals[i] = i;
    }
    VALS = List.of(vals);
  }

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml", "schema_latest.xml");
    for (int doc : VALS) {
      assertU(adoc("id", Integer.toString(doc), "field_s", "d" + doc));
    }
  }

  private static Query getQuery(int count, Random r) {
    List<Integer> vals = new ArrayList<>(VALS);
    Collections.shuffle(vals, r);
    BooleanQuery.Builder builder = new BooleanQuery.Builder();
    vals.subList(0, count)
        .forEach(
            (i) -> {
              builder.add(new TermQuery(new Term("field_s", "d" + i)), BooleanClause.Occur.SHOULD);
            });
    return builder.build();
  }

  private static final class TestAlwaysDocSet extends ExtendedQueryBase implements DocSetProducer {
    private final Query delegate;
    private final boolean alwaysCreateDocSet;

    private boolean calledCreateDocSet = false;
    private boolean calledCreateWeight = false;

    private TestAlwaysDocSet(Query delegate, boolean alwaysCreateDocSet) {
      this.delegate = delegate;
      this.alwaysCreateDocSet = alwaysCreateDocSet;
    }

    @Override
    public void visit(QueryVisitor visitor) {
      delegate.visit(visitor);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      TestAlwaysDocSet that = (TestAlwaysDocSet) o;
      return Objects.equals(delegate, that.delegate)
          && alwaysCreateDocSet == that.alwaysCreateDocSet;
    }

    @Override
    public int hashCode() {
      return classHash() ^ Objects.hashCode(delegate) ^ Boolean.hashCode(alwaysCreateDocSet);
    }

    @Override
    public DocSet createDocSet(SolrIndexSearcher searcher) throws IOException {
      calledCreateDocSet = true;
      return searcher.getDocSet(delegate);
    }

    @Override
    public boolean getCache() {
      return false;
    }

    @Override
    public boolean alwaysCreatesDocSet() {
      return alwaysCreateDocSet;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost)
        throws IOException {
      calledCreateWeight = true;
      return delegate.createWeight(searcher, scoreMode, boost);
    }
  }

  /** Verify expected behavior of {@link DocSetProducer#alwaysCreatesDocSet()}. */
  @Test
  public void testAlwaysCreateDocSet() throws Exception {
    Random r = random();
    h.reload();
    SolrCore core = h.getCore();
    for (int i = 100; i > 0; i--) {
      boolean b1 = r.nextBoolean();
      boolean b2 = r.nextBoolean();
      Query backingQ1 = getQuery(((NUM_DOCS >> 1) + 1), r);
      Query backingQ2 = getQuery(((NUM_DOCS >> 1)), r);
      TestAlwaysDocSet q1 = new TestAlwaysDocSet(backingQ1, b1);
      TestAlwaysDocSet q2 = new TestAlwaysDocSet(backingQ2, b2);
      TestAlwaysDocSet q3 = new TestAlwaysDocSet(backingQ1, !b1);
      TestAlwaysDocSet q4 = new TestAlwaysDocSet(backingQ2, !b2);
      QueryResult result =
          core.withSearcher(
              (s) -> {
                QueryCommand cmd = new QueryCommand();
                cmd.setQuery(new MatchAllDocsQuery());
                cmd.setFilterList(List.of(q1, q2));
                return s.search(cmd);
              });
      QueryResult result2 =
          core.withSearcher(
              (s) -> {
                QueryCommand cmd = new QueryCommand();
                cmd.setQuery(new MatchAllDocsQuery());
                cmd.setFilterList(List.of(q3, q4));
                return s.search(cmd);
              });
      assertTrue(result.getDocList().matches() > 0);
      assertEquals(result.getDocList().matches(), result2.getDocList().matches()); // parity
      if (b1) {
        assertTrue(q1.calledCreateDocSet);
        assertFalse(q1.calledCreateWeight);
        assertFalse(q3.calledCreateDocSet);
        assertTrue(q3.calledCreateWeight);
      } else {
        assertFalse(q1.calledCreateDocSet);
        assertTrue(q1.calledCreateWeight);
        assertTrue(q3.calledCreateDocSet);
        assertFalse(q3.calledCreateWeight);
      }
      if (b2) {
        assertTrue(q2.calledCreateDocSet);
        assertFalse(q2.calledCreateWeight);
        assertFalse(q4.calledCreateDocSet);
        assertTrue(q4.calledCreateWeight);
      } else {
        assertFalse(q2.calledCreateDocSet);
        assertTrue(q2.calledCreateWeight);
        assertTrue(q4.calledCreateDocSet);
        assertFalse(q4.calledCreateWeight);
      }
    }
  }
}
