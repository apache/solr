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
package org.apache.solr.search.join;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.search.FixedBitSetIntersectionIterator;
import org.apache.solr.util.SolrDefaultScorerSupplier;

/**
 * Matches the documents matched by a wrapped query plus all of their nested descendants, at any
 * depth. Primarily used to make delete-by-query safe for nested documents. Scores are constant.
 *
 * <p>A nested document tree is indexed as a contiguous block, each document after all of its
 * descendants (post-order), with the root last. The depth of a document is the number of {@code /}
 * in its indexed {@link IndexSchema#NEST_PATH_FIELD_NAME} term; root documents have no term (depth
 * 0). Consequently, the descendants of a document at depth {@code k} are exactly the contiguous
 * documents immediately preceding it that are deeper than {@code k}. For example:
 *
 * <pre>
 * docId:  0       1       2         3      4       5
 * path:   /a/b    /a      /a/b      /a     /c      (root)
 * depth:  2       1       2         1      1       0
 * </pre>
 *
 * Doc 3's descendants are doc 2 (doc 1 is not deeper); doc 5's are docs 0-4.
 *
 * <p>Per segment, there are two strategies:
 *
 * <ol>
 *   <li>If every matched document is a root, which is the common case, a root's descendants are
 *       simply all documents after the previous root. That's found with the given root documents
 *       filter, which should be cached so that its per-segment bitset is reused.
 *   <li>Otherwise, the (typically few) unique nest path terms are grouped by depth and processed
 *       from the deepest level up, accumulating the documents deeper than the current level. For
 *       each matched document at the current level, walk backwards while documents are deeper,
 *       marking them. The cost is proportional to the number of child documents in the segment plus
 *       the number of documents marked.
 * </ol>
 */
public class WithDescendantsQuery extends Query {

  private final Query query;
  private final BitSetProducer rootsFilter;

  /**
   * @param query the documents whose descendants should also match
   * @param rootsFilter matches all root documents, i.e. those without a nest path
   */
  public WithDescendantsQuery(Query query, BitSetProducer rootsFilter) {
    this.query = Objects.requireNonNull(query);
    this.rootsFilter = Objects.requireNonNull(rootsFilter);
  }

  public Query getQuery() {
    return query;
  }

  @Override
  public Query rewrite(IndexSearcher indexSearcher) throws IOException {
    Query rewritten = query.rewrite(indexSearcher);
    if (rewritten instanceof MatchNoDocsQuery || rewritten instanceof MatchAllDocsQuery) {
      return rewritten; // descendants wouldn't add anything
    }
    return rewritten.equals(query) ? this : new WithDescendantsQuery(rewritten, rootsFilter);
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost)
      throws IOException {
    final Weight innerWeight = searcher.createWeight(query, ScoreMode.COMPLETE_NO_SCORES, 1f);
    return new ConstantScoreWeight(this, boost) {
      @Override
      public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
        final Scorer innerScorer = innerWeight.scorer(context);
        if (innerScorer == null) {
          return null;
        }
        final LeafReader reader = context.reader();
        final Terms terms = reader.terms(IndexSchema.NEST_PATH_FIELD_NAME);
        final DocIdSetIterator disi;
        if (terms == null) { // no nested docs in this segment
          disi = innerScorer.iterator();
        } else {
          final FixedBitSet result = matchWithDescendants(context, terms, innerScorer.iterator());
          if (result == null) {
            return null;
          }
          disi = new BitSetIterator(result, result.cardinality());
        }
        return new SolrDefaultScorerSupplier(new ConstantScoreScorer(score(), scoreMode, disi));
      }

      @Override
      public boolean isCacheable(LeafReaderContext ctx) {
        return innerWeight.isCacheable(ctx);
      }
    };
  }

  /** Returns null if nothing matches. */
  private FixedBitSet matchWithDescendants(
      LeafReaderContext context, Terms terms, DocIdSetIterator innerIter) throws IOException {
    final int first = innerIter.nextDoc();
    if (first == DocIdSetIterator.NO_MORE_DOCS) {
      return null;
    }
    final int maxDoc = context.reader().maxDoc();
    final FixedBitSet matched = new FixedBitSet(maxDoc);
    innerIter.intoBitSet(DocIdSetIterator.NO_MORE_DOCS, matched, 0);
    final BitSet roots = rootsFilter.getBitSet(context);
    final FixedBitSet result = matched.clone();

    if (roots != null && isSubset(matched, roots)) {
      // a root's descendants are all docs after the previous root
      final DocIdSetIterator iter = new BitSetIterator(matched, 0);
      for (int doc = iter.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = iter.nextDoc()) {
        final int start = doc == 0 ? 0 : roots.prevSetBit(doc - 1) + 1;
        if (start < doc) {
          result.set(start, doc);
        }
      }
      return result;
    }

    // Group the (few) unique nest paths by depth
    final TreeMap<Integer, List<BytesRef>> termsByDepth = new TreeMap<>();
    final TermsEnum termsEnum = terms.iterator();
    for (BytesRef term = termsEnum.next(); term != null; term = termsEnum.next()) {
      termsByDepth
          .computeIfAbsent(depthOf(term), k -> new ArrayList<>())
          .add(BytesRef.deepCopyOf(term));
    }

    // Process from the deepest level up; "deeper" accumulates docs below the current depth.
    final FixedBitSet deeper = new FixedBitSet(maxDoc);
    final FixedBitSet atDepth = new FixedBitSet(maxDoc);
    PostingsEnum postings = null;
    for (Map.Entry<Integer, List<BytesRef>> entry : termsByDepth.descendingMap().entrySet()) {
      atDepth.clear();
      for (BytesRef term : entry.getValue()) {
        if (termsEnum.seekExact(term)) {
          postings = termsEnum.postings(postings, PostingsEnum.NONE);
          atDepth.or(postings);
        }
      }
      // matched docs at this depth
      final DocIdSetIterator iter = new FixedBitSetIntersectionIterator(atDepth, matched);
      for (int doc = iter.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = iter.nextDoc()) {
        addDescendants(doc, deeper, result);
      }
      deeper.or(atDepth);
    }

    // remaining matched docs are roots (no nest path); "deeper" now holds all child docs
    matched.andNot(deeper);
    final DocIdSetIterator iter = new BitSetIterator(matched, 0);
    for (int doc = iter.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = iter.nextDoc()) {
      addDescendants(doc, deeper, result);
    }
    return result;
  }

  /** Is {@code a}, which must not be empty, a subset of {@code b}? */
  private static boolean isSubset(FixedBitSet a, BitSet b) {
    if (!b.get(a.nextSetBit(0))) { // cheap early exit
      return false;
    }
    if (b instanceof FixedBitSet fixedB) {
      return FixedBitSet.andNotCount(a, fixedB) == 0;
    }
    // if b is smaller, a can't be a subset; otherwise a is the cheaper one to iterate
    if (b.cardinality() < a.cardinality()) {
      return false;
    }
    final int length = a.length();
    for (int doc = a.nextSetBit(0);
        doc < length;
        doc = doc + 1 < length ? a.nextSetBit(doc + 1) : length) {
      if (!b.get(doc)) {
        return false;
      }
    }
    return true;
  }

  /** Marks the contiguous docs preceding {@code doc} that are deeper than it. */
  private static void addDescendants(int doc, FixedBitSet deeper, FixedBitSet result) {
    for (int d = doc - 1; d >= 0 && deeper.get(d); d--) {
      result.set(d);
    }
  }

  private static int depthOf(BytesRef term) {
    int depth = 0;
    for (int i = term.offset, end = term.offset + term.length; i < end; i++) {
      if (term.bytes[i] == '/') {
        depth++;
      }
    }
    return depth;
  }

  @Override
  public void visit(QueryVisitor visitor) {
    query.visit(visitor.getSubVisitor(BooleanClause.Occur.MUST, this));
  }

  @Override
  public String toString(String field) {
    return "WithDescendants(" + query.toString(field) + ", roots=" + rootsFilter + ")";
  }

  @Override
  public boolean equals(Object other) {
    return sameClassAs(other)
        && query.equals(((WithDescendantsQuery) other).query)
        && rootsFilter.equals(((WithDescendantsQuery) other).rootsFilter);
  }

  @Override
  public int hashCode() {
    return 31 * (31 * classHash() + query.hashCode()) + rootsFilter.hashCode();
  }
}
