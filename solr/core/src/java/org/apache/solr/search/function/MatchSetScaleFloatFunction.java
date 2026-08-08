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
import java.util.List;
import java.util.Map;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.docvalues.FloatDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.search.DocSet;
import org.apache.solr.search.SolrIndexSearcher;

/**
 * Linearly scales {@code source} into {@code [targetMin, targetMax]} using the observed min/max of
 * {@code source} over the <b>current request's matching DocSet</b>.
 *
 * <p>Differs from Lucene's {@code ScaleFloatFunction} in two ways:
 *
 * <ul>
 *   <li>Bounds are computed over only the request's matching set (intersection of {@code q} and all
 *       {@code fq}s), not every doc in every segment. For narrowly filtered queries this can be
 *       orders of magnitude faster.
 *   <li>Output is clamped to {@code [targetMin, targetMax]}.
 * </ul>
 *
 * <p>Falls back to a full index scan when a Solr request context is not available — e.g. when
 * invoked from Lucene-level tests or embedded tool usage.
 */
public class MatchSetScaleFloatFunction extends ValueSource {
  protected final ValueSource source;
  protected final float targetMin;
  protected final float targetMax;

  public MatchSetScaleFloatFunction(ValueSource source, float targetMin, float targetMax) {
    this.source = source;
    this.targetMin = targetMin;
    this.targetMax = targetMax;
  }

  @Override
  public String description() {
    return "matchset_scale(" + source.description() + "," + targetMin + "," + targetMax + ")";
  }

  private static final class Bounds {
    float min;
    float max;
  }

  @Override
  public void createWeight(Map<Object, Object> context, IndexSearcher searcher) throws IOException {
    source.createWeight(context, searcher);
  }

  private Bounds computeBounds(Map<Object, Object> context, LeafReaderContext readerContext)
      throws IOException {
    float minVal = Float.POSITIVE_INFINITY;
    float maxVal = Float.NEGATIVE_INFINITY;

    List<LeafReaderContext> leaves = ReaderUtil.getTopLevelContext(readerContext).leaves();
    DocSet matchSet = findMatchSet();

    if (matchSet != null) {
      for (LeafReaderContext leaf : leaves) {
        DocIdSetIterator it = matchSet.iterator(leaf);
        if (it == null) continue;
        FunctionValues vals = source.getValues(context, leaf);
        for (int doc = it.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = it.nextDoc()) {
          float v = vals.floatVal(doc);
          if ((Float.floatToRawIntBits(v) & (0xff << 23)) == 0xff << 23) {
            continue;
          }
          if (v < minVal) minVal = v;
          if (v > maxVal) maxVal = v;
        }
      }
    } else {
      for (LeafReaderContext leaf : leaves) {
        int maxDoc = leaf.reader().maxDoc();
        FunctionValues vals = source.getValues(context, leaf);
        for (int i = 0; i < maxDoc; i++) {
          if (!vals.exists(i)) continue;
          float v = vals.floatVal(i);
          if ((Float.floatToRawIntBits(v) & (0xff << 23)) == 0xff << 23) {
            continue;
          }
          if (v < minVal) minVal = v;
          if (v > maxVal) maxVal = v;
        }
      }
    }

    if (minVal == Float.POSITIVE_INFINITY) {
      minVal = maxVal = 0f;
    }

    Bounds b = new Bounds();
    b.min = minVal;
    b.max = maxVal;
    context.put(MatchSetScaleFloatFunction.this, b);
    return b;
  }

  // Guards against reentrant DocSet materialization when matchset_scale appears (directly or
  // nested) inside the main query being materialized. Each recursive FunctionQuery creates a
  // fresh ValueSource context, so the per-context Bounds cache cannot prevent recursion — the
  // guard lives in the per-request context instead.
  private static final String COMPUTE_GUARD_KEY = "matchset_scale.computing";

  private DocSet findMatchSet() throws IOException {
    SolrRequestInfo reqInfo = SolrRequestInfo.getRequestInfo();
    if (reqInfo == null) return null;
    ResponseBuilder rb = reqInfo.getResponseBuilder();
    if (rb == null) return null;

    if (rb.getResults() != null && rb.getResults().docSet != null) {
      return rb.getResults().docSet;
    }

    SolrQueryRequest req = reqInfo.getReq();
    if (req == null) return null;
    Map<Object, Object> reqCtx = req.getContext();
    if (reqCtx != null && reqCtx.containsKey(COMPUTE_GUARD_KEY)) {
      // Reentrant call from inside our own DocSet materialization — fall back to a full scan.
      return null;
    }

    SolrIndexSearcher sis = req.getSearcher();
    if (sis == null) return null;
    Query q = rb.getQuery();
    if (q == null) return null;
    List<Query> filters = rb.getFilters();

    if (reqCtx != null) reqCtx.put(COMPUTE_GUARD_KEY, Boolean.TRUE);
    try {
      if (filters == null || filters.isEmpty()) {
        return sis.getDocSet(q);
      }
      return sis.getDocSet(q, sis.getDocSet(filters));
    } finally {
      if (reqCtx != null) reqCtx.remove(COMPUTE_GUARD_KEY);
    }
  }

  @Override
  public FunctionValues getValues(Map<Object, Object> context, LeafReaderContext readerContext)
      throws IOException {
    Bounds b = (Bounds) context.get(MatchSetScaleFloatFunction.this);
    if (b == null) {
      b = computeBounds(context, readerContext);
    }

    final float minObs = b.min;
    final float maxObs = b.max;
    final float outMin = targetMin;
    final float outMax = targetMax;
    final float obsRange = maxObs - minObs;
    final float scale = (obsRange == 0f) ? 0f : (outMax - outMin) / obsRange;

    final FunctionValues vals = source.getValues(context, readerContext);

    return new FloatDocValues(this) {
      @Override
      public boolean exists(int doc) throws IOException {
        return vals.exists(doc);
      }

      @Override
      public float floatVal(int doc) throws IOException {
        if (obsRange == 0f) {
          return outMin;
        }
        float v = (vals.floatVal(doc) - minObs) * scale + outMin;
        if (v < outMin) return outMin;
        if (v > outMax) return outMax;
        return v;
      }

      @Override
      public String toString(int doc) throws IOException {
        return "matchset_scale("
            + vals.toString(doc)
            + ",toMin="
            + outMin
            + ",toMax="
            + outMax
            + ",fromMin="
            + minObs
            + ",fromMax="
            + maxObs
            + ")";
      }
    };
  }

  @Override
  public int hashCode() {
    int h = Float.floatToIntBits(targetMin);
    h = h * 29;
    h += Float.floatToIntBits(targetMax);
    h = h * 29;
    h += source.hashCode();
    return h ^ MatchSetScaleFloatFunction.class.hashCode();
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || MatchSetScaleFloatFunction.class != o.getClass()) return false;
    MatchSetScaleFloatFunction other = (MatchSetScaleFloatFunction) o;
    return this.targetMin == other.targetMin
        && this.targetMax == other.targetMax
        && this.source.equals(other.source);
  }
}
