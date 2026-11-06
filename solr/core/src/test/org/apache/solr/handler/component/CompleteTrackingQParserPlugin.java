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
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.DelegatingCollector;
import org.apache.solr.search.ExtendedQueryBase;
import org.apache.solr.search.PostFilter;
import org.apache.solr.search.QParser;
import org.apache.solr.search.QParserPlugin;

/**
 * Test-only {@code {!completetracking}} post filter. It passes every document through and counts
 * how many collectors it created and how many of those had {@link DelegatingCollector#complete()}
 * called, so a test can tell whether a component finishes the collector chain it builds.
 */
public class CompleteTrackingQParserPlugin extends QParserPlugin {

  public static final AtomicInteger CREATED = new AtomicInteger();
  public static final AtomicInteger COMPLETED = new AtomicInteger();

  public static void reset() {
    CREATED.set(0);
    COMPLETED.set(0);
  }

  @Override
  public QParser createParser(
      String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
    return new QParser(qstr, localParams, params, req) {
      @Override
      public Query parse() {
        return new TrackingPostFilter();
      }
    };
  }

  private static final class TrackingPostFilter extends ExtendedQueryBase implements PostFilter {

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
      CREATED.incrementAndGet();
      return new DelegatingCollector() {
        @Override
        public void complete() throws IOException {
          COMPLETED.incrementAndGet();
          super.complete();
        }
      };
    }

    @Override
    public String toString(String field) {
      return "TrackingPostFilter";
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
}
