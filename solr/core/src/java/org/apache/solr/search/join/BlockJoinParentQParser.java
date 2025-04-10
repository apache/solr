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
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Objects;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnByteVectorQuery;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.search.join.DiversifyingChildrenByteKnnVectorQuery;
import org.apache.lucene.search.join.DiversifyingChildrenFloatKnnVectorQuery;
import org.apache.lucene.search.join.QueryBitSetProducer;
import org.apache.lucene.search.join.ScoreMode;
import org.apache.lucene.search.join.ToChildBlockJoinQuery;
import org.apache.lucene.search.join.ToParentBlockJoinQuery;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.BitSetIterator;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.transform.ChildDocTransformer;
import org.apache.solr.response.transform.DocTransformer;
import org.apache.solr.response.transform.DocTransformers;
import org.apache.solr.search.ExtendedQueryBase;
import org.apache.solr.search.QParser;
import org.apache.solr.search.QueryLimits;
import org.apache.solr.search.QueryUtils;
import org.apache.solr.search.ReturnFields;
import org.apache.solr.search.SolrCache;
import org.apache.solr.search.SyntaxError;

public class BlockJoinParentQParser extends FiltersQParser {
  /** implementation detail subject to change */
  public static final String CACHE_NAME = "perSegFilter";

  protected String getParentFilterLocalParamName() {
    return "which";
  }

  @Override
  protected String getFiltersParamName() {
    return "filters";
  }

  BlockJoinParentQParser(
      String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
    super(qstr, localParams, params, req);
  }

  protected Query parseParentFilter() throws SyntaxError {
    String filter = localParams.get(getParentFilterLocalParamName());
    QParser parentParser = subQuery(filter, null);
    Query parentQ = parentParser.getQuery();
    return parentQ;
  }

  @Override
  protected Query wrapSubordinateClause(BooleanQuery subordinate) throws SyntaxError {
    String scoreMode = localParams.get("score", ScoreMode.None.name());
    Query parentQ = parseParentFilter();
    return createQuery(parentQ, subordinate, scoreMode);
  }

  @Override
  protected Query noClausesQuery() throws SyntaxError {
    return new BitSetProducerQuery(getBitSetProducer(parseParentFilter()));
  }

  protected Query createQuery(final Query allParents, BooleanQuery childrenQuery, String scoreMode)
      throws SyntaxError {
      try {
          List<BooleanClause> childrenClauses = childrenQuery.clauses();
          if (isByteKnnQuery(childrenClauses)) {
            BitSetProducer allParentsBitSet = getBitSetProducer(allParents);
            BooleanQuery parentsFilter = getParentsFilter();
      
            KnnByteVectorQuery knnChildrenQuery = (KnnByteVectorQuery) childrenClauses.get(0).getQuery();
            String vectorField = knnChildrenQuery.getField();
            byte[] queryVector = knnChildrenQuery.getTargetCopy();
            int topK = knnChildrenQuery.getK();
      
            Query acceptedChildren =
                getChildrenFilter(knnChildrenQuery.getFilter(), parentsFilter, allParentsBitSet);
      
            Query knnChildren =
                new DiversifyingChildrenByteKnnVectorQuery(
                    vectorField, queryVector, acceptedChildren, topK, allParentsBitSet);
            knnChildren = knnChildren.rewrite(req.getSearcher());
            this.setAppropriateChildrenListingTransformer(req,knnChildren);
            
            return new ToParentBlockJoinQuery(
                knnChildren, allParentsBitSet, ScoreModeParser.parse(scoreMode));
          } else if (isFloatKnnQuery(childrenClauses)) {
            BitSetProducer allParentsBitSet = getBitSetProducer(allParents);
            BooleanQuery parentsFilter = getParentsFilter();
      
            KnnFloatVectorQuery knnChildrenQuery =
                (KnnFloatVectorQuery) childrenClauses.get(0).getQuery();
            String vectorField = knnChildrenQuery.getField();
            float[] queryVector = knnChildrenQuery.getTargetCopy();
            int topK = knnChildrenQuery.getK();
      
            Query childrenFilter =
                getChildrenFilter(knnChildrenQuery.getFilter(), parentsFilter, allParentsBitSet);
      
            Query knnChildren =
                new DiversifyingChildrenFloatKnnVectorQuery(
                    vectorField, queryVector, childrenFilter, topK, allParentsBitSet);
            knnChildren = knnChildren.rewrite(req.getSearcher());
            this.setAppropriateChildrenListingTransformer(req,knnChildren);
      
            return new ToParentBlockJoinQuery(
                knnChildren, allParentsBitSet, ScoreModeParser.parse(scoreMode));
          } else {
            return new AllParentsAware(
                childrenQuery,
                getBitSetProducer(allParents),
                ScoreModeParser.parse(scoreMode),
                allParents);
          }
      } catch (IOException e) {
          throw new RuntimeException(e);
      }
  }

  private void setAppropriateChildrenListingTransformer(SolrQueryRequest request, Query knnOnVectorField) throws IOException {
    QueryLimits currentLimits = QueryLimits.getCurrentLimits();
    ReturnFields returnFields = currentLimits.getRsp().getReturnFields();
    DocTransformer originalTransformer = returnFields.getTransformer();

    if (originalTransformer instanceof DocTransformers) {
      DocTransformers transformers = (DocTransformers) originalTransformer;
      boolean noChildTransformer = true;
      for (int i = 0; i < transformers.size() && noChildTransformer; i++) {
        DocTransformer t = transformers.getTransformer(i);
        if (t instanceof ChildDocTransformer) {
          ChildDocTransformer childTransformer = (ChildDocTransformer) t;
          if (childTransformer.getChildDocSet() == null) {
            childTransformer.setChildDocSet(request.getSearcher().getDocSet(knnOnVectorField));
          }
          noChildTransformer = false;
        }
      }
    } else {
      if ((originalTransformer instanceof ChildDocTransformer)) {
        ChildDocTransformer childTransformer = (ChildDocTransformer) originalTransformer;
        if (childTransformer.getChildDocSet() == null) {
          childTransformer.setChildDocSet(request.getSearcher().getDocSet(knnOnVectorField));
        }
      }
    }
  }

  private boolean isFloatKnnQuery(List<BooleanClause> childrenClauses) {
    return childrenClauses.size() == 1
        && childrenClauses.get(0).getQuery().getClass().equals(KnnFloatVectorQuery.class);
  }

  private boolean isByteKnnQuery(List<BooleanClause> childrenClauses) {
    return childrenClauses.size() == 1
        && childrenClauses.get(0).getQuery().getClass().equals(KnnByteVectorQuery.class);
  }

  private Query getChildrenFilter(
      Query childrenKnnPreFilter, BooleanQuery parentsFilter, BitSetProducer allParentsBitSet) {
    Query childrenFilter = childrenKnnPreFilter;

    if (parentsFilter.clauses().size() > 0) {
      Query acceptedChildrenBasedOnParentsFilter =
          new ToChildBlockJoinQuery(parentsFilter, allParentsBitSet); // no scoring happens here
      BooleanQuery.Builder acceptedChildrenBuilder = createBuilder();
      if (childrenFilter != null) {
        acceptedChildrenBuilder.add(childrenFilter, BooleanClause.Occur.FILTER);
      }
      acceptedChildrenBuilder.add(acceptedChildrenBasedOnParentsFilter, BooleanClause.Occur.FILTER);

      childrenFilter = acceptedChildrenBuilder.build();
    }
    return childrenFilter;
  }

  private BooleanQuery getParentsFilter() throws SyntaxError {
    List<Query> parentFilterQueries = QueryUtils.parseFilterQueries(req);
    BooleanQuery.Builder acceptedParentsBuilder = createBuilder();
    for (Query filter : parentFilterQueries) {
      acceptedParentsBuilder.add(filter, BooleanClause.Occur.FILTER);
    }
    BooleanQuery acceptedParents = acceptedParentsBuilder.build();
    return acceptedParents;
  }

  BitSetProducer getBitSetProducer(Query query) {
    return getCachedBitSetProducer(req, query);
  }

  public static BitSetProducer getCachedBitSetProducer(
      final SolrQueryRequest request, Query query) {
    @SuppressWarnings("unchecked")
    SolrCache<Query, BitSetProducer> parentCache = request.getSearcher().getCache(CACHE_NAME);
    // lazily retrieve from solr cache
    if (parentCache != null) {
      try {
        return parentCache.computeIfAbsent(query, QueryBitSetProducer::new);
      } catch (IOException e) {
        throw new UncheckedIOException(e); // Shouldn't happen because QBSP doesn't throw
      }
    } else {
      return new QueryBitSetProducer(query);
    }
  }

  static final class AllParentsAware extends ToParentBlockJoinQuery {
    private final Query parentQuery;

    private AllParentsAware(
        Query childQuery, BitSetProducer parentsFilter, ScoreMode scoreMode, Query parentList) {
      super(childQuery, parentsFilter, scoreMode);
      parentQuery = parentList;
    }

    public Query getParentQuery() {
      return parentQuery;
    }
  }

  /** A constant score query based on a {@link BitSetProducer}. */
  static class BitSetProducerQuery extends ExtendedQueryBase {

    final BitSetProducer bitSetProducer;

    public BitSetProducerQuery(BitSetProducer bitSetProducer) {
      this.bitSetProducer = bitSetProducer;
      setCache(false); // because we assume the bitSetProducer is itself cached
    }

    @Override
    public String toString(String field) {
      return getClass().getSimpleName() + "(" + bitSetProducer + ")";
    }

    @Override
    public boolean equals(Object other) {
      return sameClassAs(other)
          && Objects.equals(bitSetProducer, getClass().cast(other).bitSetProducer);
    }

    @Override
    public int hashCode() {
      return classHash() + bitSetProducer.hashCode();
    }

    @Override
    public void visit(QueryVisitor visitor) {
      visitor.visitLeaf(this);
    }

    @Override
    public Weight createWeight(
        IndexSearcher searcher, org.apache.lucene.search.ScoreMode scoreMode, float boost)
        throws IOException {
      return new ConstantScoreWeight(BitSetProducerQuery.this, boost) {
        @Override
        public Scorer scorer(LeafReaderContext context) throws IOException {
          BitSet bitSet = bitSetProducer.getBitSet(context);
          if (bitSet == null) {
            return null;
          }
          DocIdSetIterator disi = new BitSetIterator(bitSet, bitSet.approximateCardinality());
          return new ConstantScoreScorer(this, boost, scoreMode, disi);
        }

        @Override
        public boolean isCacheable(LeafReaderContext ctx) {
          return getCache();
        }
      };
    }
  }
}
