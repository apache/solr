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
import java.util.Map;
import java.util.Objects;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.ValueSourceScorer;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;
import org.apache.solr.common.SolrException;
import org.apache.solr.search.function.ValueSourceRangeFilter;

// This class works as either an ExtendedQuery, or as a PostFilter using a collector
public class FunctionRangeQuery extends ExtendedQueryBase implements PostFilter {

  final ValueSourceRangeFilter rangeFilt;

  public FunctionRangeQuery(ValueSourceRangeFilter filter) {
    super();
    this.rangeFilt = filter;
    super.setCost(100); // default behavior should be PostFiltering
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost)
      throws IOException {
    return rangeFilt.createWeight(searcher, scoreMode, boost);
  }

  @Override
  public DelegatingCollector getFilterCollector(IndexSearcher searcher) {
    Map<Object, Object> fcontext = ValueSource.newContext(searcher);
    Weight weight = null;
    try {
      weight = rangeFilt.createWeight(searcher, ScoreMode.COMPLETE, 1);
    } catch (IOException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }
    return new FunctionRangeCollector(fcontext, weight);
  }

  class FunctionRangeCollector extends DelegatingCollector {
    final Map<Object, Object> fcontext;
    final Weight weight;
    ValueSourceScorer valueSourceScorer;
    int maxdoc;

    public FunctionRangeCollector(Map<Object, Object> fcontext, Weight weight) {
      this.fcontext = fcontext;
      this.weight = weight;
    }

    @Override
    public void collect(int doc) throws IOException {
      assert doc < maxdoc;
      if (valueSourceScorer.matches(doc)) {
        leafDelegate.collect(doc);
      }
    }

    @Override
    protected void doSetNextReader(LeafReaderContext context) throws IOException {
      super.doSetNextReader(context);
      maxdoc = context.reader().maxDoc();
      FunctionValues dv = rangeFilt.getValueSource().getValues(fcontext, context);
      valueSourceScorer =
          dv.getRangeScorer(
              weight,
              context,
              rangeFilt.getLowerVal(),
              rangeFilt.getUpperVal(),
              rangeFilt.isIncludeLower(),
              rangeFilt.isIncludeUpper());
    }
  }

  @Override
  public String toString(String field) {
    return "FunctionRangeQuery(" + field + ")";
  }

  @Override
  public void visit(QueryVisitor visitor) {
    visitor.visitLeaf(this);
  }

  @Override
  public boolean equals(Object obj) {
    return sameClassAs(obj) && Objects.equals(rangeFilt, ((FunctionRangeQuery) obj).rangeFilt);
  }

  @Override
  public int hashCode() {
    return 31 * classHash() + rangeFilt.hashCode();
  }
}
