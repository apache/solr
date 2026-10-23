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
import java.util.List;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.Scorable;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;

/**
 * Test-only {@code {!releaseoncomplete}} post filter, shaped like an access-control filter. It
 * holds every document it is offered and releases only the permitted ones (those with {@code
 * sort_i} below 10) when the collector chain is completed.
 */
public class ReleaseOnCompleteQParserPlugin extends QParserPlugin {

  @Override
  public QParser createParser(
      String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
    return new QParser(qstr, localParams, params, req) {
      @Override
      public Query parse() {
        return new ReleaseOnCompleteFilter();
      }
    };
  }

  private static final class ReleaseOnCompleteFilter extends ExtendedQueryBase
      implements PostFilter {

    @Override
    public boolean getCache() {
      return false;
    }

    @Override
    public int getCost() {
      return Math.max(super.getCost(), 100);
    }

    @Override
    public DelegatingCollector getFilterCollector(IndexSearcher searcher) {
      return new HoldingCollector();
    }

    @Override
    public String toString(String field) {
      return "ReleaseOnCompleteFilter";
    }

    @Override
    public void visit(QueryVisitor visitor) {
      visitor.visitLeaf(this);
    }

    @Override
    public boolean equals(Object o) {
      return this == o;
    }

    @Override
    public int hashCode() {
      return System.identityHashCode(this);
    }
  }

  private static final class HoldingCollector extends DelegatingCollector {
    private record Segment(LeafReaderContext context, LeafCollector leaf, List<Integer> docs) {}

    private final List<Segment> segments = new ArrayList<>();
    private Segment current;

    @Override
    protected void doSetNextReader(LeafReaderContext context) throws IOException {
      current = new Segment(context, delegate.getLeafCollector(context), new ArrayList<>());
      segments.add(current);
    }

    @Override
    public void collect(int doc) {
      current.docs().add(doc);
    }

    @Override
    public void complete() throws IOException {
      for (Segment segment : segments) {
        for (int doc : segment.docs()) {
          if (permitted(segment.context(), doc)) {
            segment.leaf().setScorer(constantScore());
            segment.leaf().collect(doc);
          }
        }
      }
      super.complete();
    }

    private static boolean permitted(LeafReaderContext context, int doc) throws IOException {
      return Integer.parseInt(context.reader().storedFields().document(doc).get("sort_i")) < 10;
    }

    private static Scorable constantScore() {
      return new Scorable() {
        @Override
        public float score() {
          return 1f;
        }
      };
    }
  }
}
