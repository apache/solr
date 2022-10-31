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
package org.apache.solr.search.function;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.ValueSourceScorer;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;

/** RangeFilter over a ValueSource. */
public class ValueSourceRangeFilter extends Query {
  private final ValueSource valueSource;
  private final String lowerVal;
  private final String upperVal;
  private final boolean includeLower;
  private final boolean includeUpper;

  public ValueSourceRangeFilter(
      ValueSource valueSource,
      String lowerVal,
      String upperVal,
      boolean includeLower,
      boolean includeUpper) {
    this.valueSource = valueSource;
    this.lowerVal = lowerVal;
    this.upperVal = upperVal;
    this.includeLower = includeLower;
    this.includeUpper = includeUpper;
    //    this.includeLower = lowerVal != null && includeLower;
    //    this.includeUpper = upperVal != null && includeUpper;
  }

  public ValueSource getValueSource() {
    return valueSource;
  }

  public String getLowerVal() {
    return lowerVal;
  }

  public String getUpperVal() {
    return upperVal;
  }

  public boolean isIncludeLower() {
    return includeLower;
  }

  public boolean isIncludeUpper() {
    return includeUpper;
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost)
      throws IOException {
    return new FunctionRangeWeight(searcher, scoreMode, boost);
  }

  private class FunctionRangeWeight extends ConstantScoreWeight {
    private final Map<Object, Object> vsContext;
    private final ScoreMode scoreMode;

    public FunctionRangeWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost)
        throws IOException {
      super(ValueSourceRangeFilter.this, boost);
      this.scoreMode = scoreMode;
      vsContext = ValueSource.newContext(searcher);
      valueSource.createWeight(vsContext, searcher); // callback on valueSource tree
    }

    @Override
    public Explanation explain(LeafReaderContext context, int doc) throws IOException {
      FunctionValues functionValues = valueSource.getValues(vsContext, context);
      ValueSourceScorer scorer =
          functionValues.getRangeScorer(
              this, context, lowerVal, upperVal, includeLower, includeUpper);
      if (scorer.matches(doc)) {
        scorer.iterator().advance(doc);
        return Explanation.match(
            scorer.score(), ValueSourceRangeFilter.this.toString(), functionValues.explain(doc));
      } else {
        return Explanation.noMatch(
            ValueSourceRangeFilter.this.toString(), functionValues.explain(doc));
      }
    }

    @Override
    public Scorer scorer(LeafReaderContext context) throws IOException {
      ValueSourceScorer scorer =
          valueSource
              .getValues(vsContext, context)
              .getRangeScorer(this, context, lowerVal, upperVal, includeLower, includeUpper);
      return new ConstantScoreScorer(this, score(), scoreMode, scorer.iterator());
    }

    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
      return false;
    }
  }

  @Override
  public String toString(String field) {
    StringBuilder sb = new StringBuilder();
    sb.append("frange(");
    sb.append(valueSource);
    sb.append("):");
    sb.append(includeLower ? '[' : '{');
    sb.append(lowerVal == null ? "*" : lowerVal);
    sb.append(" TO ");
    sb.append(upperVal == null ? "*" : upperVal);
    sb.append(includeUpper ? ']' : '}');
    return sb.toString();
  }

  @Override
  public void visit(QueryVisitor visitor) {
    visitor.visitLeaf(this);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof ValueSourceRangeFilter)) return false;

    ValueSourceRangeFilter other = (ValueSourceRangeFilter) o;
    return Objects.equals(this.valueSource, other.valueSource)
        && this.includeLower == other.includeLower
        && this.includeUpper == other.includeUpper
        && Objects.equals(this.lowerVal, other.lowerVal)
        && Objects.equals(this.upperVal, other.upperVal);
  }

  @Override
  public int hashCode() {
    int h = valueSource.hashCode();
    h += lowerVal != null ? lowerVal.hashCode() : 0x572353db;
    h = (h << 16) | (h >>> 16); // rotate to distinguish lower from upper
    h += (upperVal != null ? (upperVal.hashCode()) : 0xe16fe9e7);
    h += (includeLower ? 0xdaa47978 : 0) + (includeUpper ? 0x9e634b57 : 0);
    return h;
  }
}
