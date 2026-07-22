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
package org.apache.solr.schema.numericrange;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;
import org.apache.lucene.document.RangeFieldQuery;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.ArrayUtil.ByteArrayComparator;
import org.apache.lucene.util.BytesRef;

/**
 * A docValues-backed range query over {@link BinaryDocValues} for numeric range field types
 * (single- or multiValued).
 *
 * <p>A document's range(s) are packed into a single {@code BinaryDocValues} value as one or more
 * concatenated fixed-width {@code [min... | max...]} blobs (see {@code
 * AbstractNumericRangeField.encodePackedValues}). Because every range for a field is the same width
 * ({@code stride = 2 * numDims * bytesPerDim}), the count is recovered as {@code value.length /
 * stride}.
 *
 * <p>Reading the flat per-doc blob avoids the dictionary/ordinal dereference a SortedSet-backed
 * docValues range query would pay, so it stays cheap even for high-cardinality range data. It is a
 * candidate to push down to Lucene, which currently lacks a multi-valued binary range docValues
 * field.
 */
final class MultiBinaryRangeDocValuesQuery extends Query {

  private final String field;
  private final byte[] queryPackedValue;
  private final int numDims;
  private final int bytesPerDim;
  private final RangeFieldQuery.QueryType queryType;
  private final ByteArrayComparator comparator;

  /**
   * @param field the field name
   * @param queryPackedValue the query range, encoded as {@code [min... | max...]} the same way the
   *     indexed ranges are
   * @param numDims number of dimensions (1-4)
   * @param bytesPerDim bytes per dimension value (e.g. {@code Integer.BYTES})
   * @param queryType the relation to test ({@code INTERSECTS}, {@code CONTAINS}, {@code WITHIN},
   *     {@code CROSSES})
   */
  MultiBinaryRangeDocValuesQuery(
      String field,
      byte[] queryPackedValue,
      int numDims,
      int bytesPerDim,
      RangeFieldQuery.QueryType queryType) {
    this.field = Objects.requireNonNull(field, "field must not be null");
    this.queryPackedValue =
        Objects.requireNonNull(queryPackedValue, "queryPackedValue must not be null");
    this.numDims = numDims;
    this.bytesPerDim = bytesPerDim;
    this.queryType = Objects.requireNonNull(queryType, "queryType must not be null");
    this.comparator = ArrayUtil.getUnsignedComparator(bytesPerDim);
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) {
    return new ConstantScoreWeight(this, boost) {
      @Override
      public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
        LeafReader reader = context.reader();
        if (reader.getFieldInfos().fieldInfo(field) == null) {
          return null;
        }
        final BinaryDocValues values = DocValues.getBinary(reader, field);
        final int stride = 2 * numDims * bytesPerDim;
        final byte[] docPacked = new byte[stride];
        final TwoPhaseIterator iterator =
            new TwoPhaseIterator(values) {
              @Override
              public boolean matches() throws IOException {
                // The doc's value is N concatenated stride-byte ranges; match if ANY satisfies.
                // Copy each range to a 0-based buffer because QueryType.matches indexes from 0.
                BytesRef b = values.binaryValue();
                for (int off = b.offset; off < b.offset + b.length; off += stride) {
                  System.arraycopy(b.bytes, off, docPacked, 0, stride);
                  if (queryType.matches(
                      queryPackedValue, docPacked, numDims, bytesPerDim, comparator)) {
                    return true;
                  }
                }
                return false;
              }

              @Override
              public float matchCost() {
                return stride; // ~ one packed-value comparison per range
              }
            };

        Scorer scorer = new ConstantScoreScorer(score(), scoreMode, iterator);
        return new DefaultScorerSupplier(scorer);
      }

      @Override
      public boolean isCacheable(LeafReaderContext ctx) {
        return DocValues.isCacheable(ctx, field);
      }
    };
  }

  @Override
  public void visit(QueryVisitor visitor) {
    if (visitor.acceptField(field)) {
      visitor.visitLeaf(this);
    }
  }

  @Override
  public String toString(String f) {
    return getClass().getSimpleName()
        + "(field="
        + field
        + ", type="
        + queryType
        + ", value="
        + Arrays.toString(queryPackedValue)
        + ")";
  }

  @Override
  public boolean equals(Object obj) {
    if (!sameClassAs(obj)) {
      return false;
    }
    MultiBinaryRangeDocValuesQuery that = (MultiBinaryRangeDocValuesQuery) obj;
    // queryType MUST be part of identity: the same field/range under different relations are
    // different queries and must not collide in the query cache.
    return numDims == that.numDims
        && bytesPerDim == that.bytesPerDim
        && queryType == that.queryType
        && field.equals(that.field)
        && Arrays.equals(queryPackedValue, that.queryPackedValue);
  }

  @Override
  public int hashCode() {
    int h = classHash();
    h = 31 * h + field.hashCode();
    h = 31 * h + Arrays.hashCode(queryPackedValue);
    h = 31 * h + numDims;
    h = 31 * h + bytesPerDim;
    h = 31 * h + queryType.hashCode();
    return h;
  }
}
