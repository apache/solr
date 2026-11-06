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
import org.apache.solr.search.DelegatingCollector;
import org.apache.solr.search.ExtendedQueryBase;
import org.apache.solr.search.PostFilter;
import org.apache.solr.search.QParser;
import org.apache.solr.search.QParserPlugin;

/**
 * Test-only {@code {!releaseoncomplete}} post filter, shaped like an access-control filter. It
 * holds every document it is offered and releases only the permitted ones (those with {@code
 * test_i} below 10) when the collector chain is completed.
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
    private record Held(LeafReaderContext context, int doc) {}

    private final List<Held> held = new ArrayList<>();

    @Override
    protected void doSetNextReader(LeafReaderContext context) {
      this.context = context;
      this.docBase = context.docBase;
    }

    @Override
    public void collect(int doc) {
      held.add(new Held(context, doc));
    }

    @Override
    public void complete() throws IOException {
      LeafReaderContext current = null;
      LeafCollector leaf = null;
      for (Held h : held) {
        if (h.context() != current) {
          current = h.context();
          leaf = delegate.getLeafCollector(current);
        }
        if (permitted(h)) {
          leaf.setScorer(constantScore());
          leaf.collect(h.doc());
        }
      }
      super.complete();
    }

    private static boolean permitted(Held h) throws IOException {
      return Integer.parseInt(h.context().reader().storedFields().document(h.doc()).get("test_i"))
          < 10;
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
