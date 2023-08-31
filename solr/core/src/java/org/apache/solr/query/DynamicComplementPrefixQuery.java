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
package org.apache.solr.query;

import java.io.IOException;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.FilteredTermsEnum;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocValuesFieldExistsQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.UnicodeUtil;

/**
 * A Query that matches documents containing terms with a specified prefix. A PrefixQuery is built
 * by QueryParser for input like <code>app*</code>.
 *
 * <p>This query differs from {@link org.apache.lucene.search.PrefixQuery} in two ways:
 *
 * <ol>
 *   <li>It does _not_ build an automaton representing the prefix; rather, it builds {@link
 *       FilteredTermsEnum} instances that are optimized for common data patterns (specifically, it
 *       compares the _end_ of BytesRefs first).
 *   <li>In the event that the prefix matches a large proportion of terms in the index, an inverse
 *       query is built against terms that do _not_ match the prefix.
 * </ol>
 */
public class DynamicComplementPrefixQuery extends MultiTermQuery {

  private final Term termPrefix;
  private final BytesRef prefix;
  private final BytesRef limit;
  private final boolean multiValued;
  private final MultiTermQuery inverted;
  private final boolean forceCacheFieldExists;

  /** Constructs a query for terms starting with <code>prefix</code>. */
  public DynamicComplementPrefixQuery(Term prefix, boolean noInvert, boolean multiValued) {
    this(prefix, noInvert, multiValued, false);
  }

  public DynamicComplementPrefixQuery(
      Term prefix, boolean noInvert, boolean multiValued, boolean forceCacheFieldExists) {
    super(
        prefix.field(),
        noInvert
            ? CONSTANT_SCORE_REWRITE
            : new InvertingRewriteMethod(CONSTANT_SCORE_REWRITE, multiValued));
    this.termPrefix = prefix;
    BytesRef tmp = prefix.bytes();
    byte[] backing = new byte[tmp.length + UnicodeUtil.BIG_TERM.length];
    System.arraycopy(tmp.bytes, tmp.offset, backing, 0, tmp.length);
    System.arraycopy(
        UnicodeUtil.BIG_TERM.bytes, 0, backing, tmp.length, UnicodeUtil.BIG_TERM.length);
    this.prefix = new BytesRef(backing, 0, tmp.length);
    this.limit = new BytesRef(backing);
    this.multiValued = multiValued;
    this.forceCacheFieldExists = forceCacheFieldExists;
    inverted =
        noInvert
            ? null
            : new MultiTermQuery(field, CONSTANT_SCORE_REWRITE) {
              @Override
              protected TermsEnum getTermsEnum(Terms terms, AttributeSource atts)
                  throws IOException {
                TermsEnum te = terms.iterator();
                BytesRef limit;
                TermState limitTermState;
                if (te.seekCeil(DynamicComplementPrefixQuery.this.limit)
                    == TermsEnum.SeekStatus.END) {
                  limit = null;
                  limitTermState = null;
                } else {
                  limit = BytesRef.deepCopyOf(te.term());
                  limitTermState = te.termState();
                }
                if (te.seekCeil(DynamicComplementPrefixQuery.this.prefix)
                    == TermsEnum.SeekStatus.END) {
                  return TermsEnum.EMPTY;
                }
                BytesRef start = BytesRef.deepCopyOf(te.term());
                te.seekCeil(new BytesRef());
                if (start.bytesEquals(te.term())) {
                  // there exist no terms before prefix
                  if (limitTermState == null) {
                    // there exist no terms after
                    return TermsEnum.EMPTY;
                  } else {
                    // pre-position `te` and set `start` term to null. This will cause
                    // `InvertPrefixTermsEnum`
                    // to only (and unconditionally) iterate over the "tail" terms -- greater than
                    // would match
                    // the specified prefix.
                    te.seekExact(limit, limitTermState);
                    start = null;
                  }
                }
                return new InvertPrefixTermsEnum(te, start, limit, limitTermState);
              }

              @Override
              public String toString(String field) {
                return DynamicComplementPrefixQuery.this.toString(field) + " (inverted)";
              }

              @Override
              public void visit(QueryVisitor visitor) {
                // NOTE: we obviously _do_ match on terms, but fully enumerating all matching terms
                // here
                // could be prohibitively expensive, so we pretend we _don't_ match on terms.
                visitor.visitLeaf(this);
              }
            };
  }

  @Override
  protected TermsEnum getTermsEnum(Terms terms, AttributeSource atts) throws IOException {
    TermsEnum te = terms.iterator();
    int limitLength;
    int determinantIdx;
    int determinant;
    if (te.seekCeil(DynamicComplementPrefixQuery.this.limit) == TermsEnum.SeekStatus.END) {
      limitLength = -1;
      determinantIdx = -1;
      determinant = -1;
    } else {
      BytesRef limit = te.term();
      limitLength = limit.length;
      determinantIdx = getThresholdDeterminantIdx(DynamicComplementPrefixQuery.this.prefix, limit);
      determinant = Byte.toUnsignedInt(limit.bytes[limit.offset + determinantIdx]);
    }
    if (te.seekCeil(DynamicComplementPrefixQuery.this.prefix) == TermsEnum.SeekStatus.END) {
      return TermsEnum.EMPTY;
    }
    return new DirectPrefixTermsEnum(te, limitLength, determinantIdx, determinant);
  }

  /**
   * Find an index that differs between the prefix and limit. The particular index is arbitrary, but
   * there is guaranteed to be at least one determinant index. Once we have this index, it will
   * suffice to check this index only (regardless of how long the prefix or limit threshold term
   * is).
   */
  private static int getThresholdDeterminantIdx(BytesRef prefix, BytesRef limit) {
    for (int i = Math.min(prefix.length, limit.length) - 1; i >= 0; i--) {
      if (prefix.bytes[i] != limit.bytes[limit.offset + i]) {
        return i;
      }
    }
    throw new IllegalStateException("`limit` must not start with `prefix`");
  }

  private static final class DirectPrefixTermsEnum extends FilteredTermsEnum {
    private boolean unpositioned = true;
    private final int limitLength;
    private final int determinantIdx;
    private final int determinant;

    public DirectPrefixTermsEnum(
        TermsEnum tenum, int limitLength, int determinantIdx, int determinant) {
      super(tenum);
      this.limitLength = limitLength;
      this.determinantIdx = determinantIdx;
      this.determinant = determinant;
    }

    @Override
    public BytesRef next() throws IOException {
      BytesRef candidate;
      if (unpositioned) {
        unpositioned = false;
        candidate = tenum.term();
      } else {
        candidate = tenum.next();
      }
      if (limitLength != candidate.length
          || determinant
              != Byte.toUnsignedInt(candidate.bytes[candidate.offset + determinantIdx])) {
        return candidate;
      } else {
        return null;
      }
    }

    @Override
    protected BytesRef nextSeekTerm(BytesRef currentTerm) {
      throw new UnsupportedOperationException();
    }

    @Override
    protected AcceptStatus accept(BytesRef term) {
      throw new UnsupportedOperationException();
    }
  }

  private static final class InvertPrefixTermsEnum extends FilteredTermsEnum {
    private boolean unpositioned = true;
    private final BytesRef initial;
    private final BytesRef limit;
    private final TermState limitTermState;
    private boolean tail;

    public InvertPrefixTermsEnum(
        TermsEnum tenum, BytesRef initial, BytesRef limit, TermState limitTermState) {
      super(tenum, false);
      this.initial = initial;
      this.limit = limit;
      this.limitTermState = limitTermState;
      this.tail = initial == null;
    }

    @Override
    protected BytesRef nextSeekTerm(BytesRef currentTerm) {
      throw new UnsupportedOperationException();
    }

    @Override
    public BytesRef next() throws IOException {
      final BytesRef candidate;
      if (unpositioned) {
        unpositioned = false;
        candidate = tenum.term();
      } else {
        candidate = tenum.next();
      }
      if (candidate == null) {
        return null;
      } else if (tail || noMatchThreshold(initial, candidate)) {
        return candidate;
      } else {
        // seek the exit threshold
        tail = true;
        if (limit == null) {
          return null;
        } else {
          tenum.seekExact(limit, limitTermState);
          return tenum.term();
        }
      }
    }

    @Override
    protected AcceptStatus accept(BytesRef term) {
      throw new UnsupportedOperationException();
    }
  }

  private static boolean noMatchThreshold(BytesRef ref, BytesRef test) {
    if (test.length != ref.length) {
      return true;
    }
    for (int i = ref.length - 1; i >= 0; i--) {
      if (test.bytes[test.offset + i] != ref.bytes[i]) {
        return true;
      }
    }
    return false;
  }

  private static class InvertingRewriteMethod extends RewriteMethod {
    private final RewriteMethod backing;
    private final boolean multiValued;

    private InvertingRewriteMethod(RewriteMethod backing, boolean multiValued) {
      this.backing = backing;
      this.multiValued = multiValued;
    }

    @Override
    public int hashCode() {
      return backing.hashCode() ^ InvertingRewriteMethod.class.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
      return obj instanceof InvertingRewriteMethod
          && backing.equals(((InvertingRewriteMethod) obj).backing);
    }

    @Override
    public Query rewrite(IndexReader reader, MultiTermQuery query) throws IOException {
      return ((DynamicComplementPrefixQuery) query).rewriteInverting(reader, backing, multiValued);
    }
  }

  @Override
  @Deprecated
  public void setRewriteMethod(RewriteMethod method) {
    super.setRewriteMethod(
        inverted == null ? method : new InvertingRewriteMethod(method, multiValued));
  }

  private Query rewriteInverting(
      IndexReader reader, RewriteMethod internalRewriteMethod, boolean multiValued)
      throws IOException {
    Query direct = internalRewriteMethod.rewrite(reader, this);
    BooleanQuery.Builder builder = new BooleanQuery.Builder();
    Query fieldExistsQuery = new DocValuesFieldExistsQuery(termPrefix.field());
    if (forceCacheFieldExists) {
      fieldExistsQuery = new FilterQuery(fieldExistsQuery);
    }
    builder.add(fieldExistsQuery, BooleanClause.Occur.FILTER);
    builder.add(internalRewriteMethod.rewrite(reader, this.inverted), BooleanClause.Occur.MUST_NOT);
    Query inverted = builder.build();
    return new DynamicallyInvertingPrefixQuery(direct, inverted, multiValued, field, prefix, limit);
  }

  private static final class DynamicallyInvertingPrefixQuery extends Query {
    private final Query direct;
    private final Query inverted;
    private final boolean multiValued;
    private final String field;
    private final BytesRef prefix;
    private final BytesRef limit;

    private DynamicallyInvertingPrefixQuery(
        Query direct,
        Query inverted,
        boolean multiValued,
        String field,
        BytesRef prefix,
        BytesRef limit) {
      this.direct = direct;
      this.inverted = inverted;
      this.multiValued = multiValued;
      this.field = field;
      this.prefix = prefix;
      this.limit = limit;
    }

    @Override
    public String toString(String field) {
      StringBuilder sb = new StringBuilder();
      sb.append(DynamicallyInvertingPrefixQuery.class.getSimpleName()).append('/');
      if (!this.field.equals(field)) {
        sb.append(this.field);
        sb.append(':');
      }
      sb.append(prefix.utf8ToString());
      sb.append('*');
      return sb.toString();
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost)
        throws IOException {
      Weight directWeight = direct.createWeight(searcher, scoreMode, boost);
      Weight invertedWeight = inverted.createWeight(searcher, scoreMode, boost);
      return new ConstantScoreWeight(this, boost) {
        @Override
        public Scorer scorer(LeafReaderContext context) throws IOException {
          LeafReader r = context.reader();
          final SortedDocValues sdv;
          if (!multiValued) {
            sdv = r.getSortedDocValues(field);
            if (sdv == null) {
              return null;
            }
          } else {
            SortedSetDocValues ssdv = r.getSortedSetDocValues(field);
            if (ssdv == null) {
              return null;
            }
            sdv = DocValues.unwrapSingleton(ssdv);
            if (sdv == null) {
              return directWeight.scorer(context);
            }
          }
          TermsEnum te = sdv.termsEnum();
          if (te.seekCeil(prefix) == TermsEnum.SeekStatus.END) {
            return null;
          }
          final long minOrd = te.ord();
          final long maxOrd;
          final long limitOrd = sdv.getValueCount();
          if (te.seekCeil(limit) == TermsEnum.SeekStatus.END) {
            maxOrd = limitOrd;
          } else {
            maxOrd = te.ord();
          }
          long range = maxOrd - minOrd;
          if (range < 1) {
            return null;
          }
          if (range <= sdv.getValueCount() >> 1) {
            return directWeight.scorer(context);
          } else {
            return invertedWeight.scorer(context);
          }
        }

        @Override
        public boolean isCacheable(LeafReaderContext ctx) {
          return true;
        }
      };
    }

    @Override
    public void visit(QueryVisitor visitor) {
      // NOTE: we obviously _do_ match on terms, but fully enumerating all matching terms here
      // could be prohibitively expensive, so we pretend we _don't_ match on terms.
      visitor.visitLeaf(this);
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof DynamicallyInvertingPrefixQuery)) {
        return false;
      }
      DynamicallyInvertingPrefixQuery other = (DynamicallyInvertingPrefixQuery) obj;
      return field.equals(other.field) && prefix.equals(other.prefix);
    }

    @Override
    public int hashCode() {
      return classHash() ^ field.hashCode() ^ prefix.hashCode();
    }
  }

  /** Returns the prefix of this query. */
  public Term getPrefix() {
    return termPrefix;
  }

  /** Prints a user-readable version of this query. */
  @Override
  public String toString(String field) {
    StringBuilder buffer = new StringBuilder();
    if (!getField().equals(field)) {
      buffer.append(getField());
      buffer.append(':');
    }
    buffer.append(termPrefix.text());
    buffer.append('*');
    return buffer.toString();
  }

  @Override
  public void visit(QueryVisitor visitor) {
    // NOTE: we obviously _do_ match on terms, but fully enumerating all matching terms here
    // could be prohibitively expensive, so we pretend we _don't_ match on terms.
    visitor.visitLeaf(this);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + termPrefix.hashCode();
    result = result ^ Boolean.hashCode(inverted == null);
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!super.equals(obj)) {
      return false;
    }
    // super.equals() ensures we are the same class
    DynamicComplementPrefixQuery other = (DynamicComplementPrefixQuery) obj;
    if (!termPrefix.equals(other.termPrefix)) {
      return false;
    }
    if (inverted == null ^ other.inverted == null) {
      // must have same invert capability to be considered equal
      return false;
    }
    return true;
  }
}
