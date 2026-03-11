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
import java.util.Objects;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.search.join.QueryBitSetProducer;
import org.apache.lucene.search.join.ScoreMode;
import org.apache.lucene.search.join.ToParentBlockJoinQuery;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.BitSetIterator;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.search.ExtendedQueryBase;
import org.apache.solr.search.QParser;
import org.apache.solr.search.SolrCache;
import org.apache.solr.search.SyntaxError;
import org.apache.solr.util.SolrDefaultScorerSupplier;

/** Matches parent documents based on child doc criteria. */
public class BlockJoinParentQParser extends FiltersQParser {
  /** implementation detail subject to change */
  public static final String CACHE_NAME = "perSegFilter";

  /**
   * Optional localparam that, when specified, makes this parser natively aware of the {@link
   * IndexSchema#NEST_PATH_FIELD_NAME} field to automatically derive the parent filter (the {@code
   * which} param). The value must be an absolute path starting with {@code /} using {@code /} as
   * separator, e.g. {@code /} for root-level parents or {@code /skus} for parents nested at that
   * path. When specified, the {@code which} param must not also be specified.
   *
   * @see <a href="https://issues.apache.org/jira/browse/SOLR-14687">SOLR-14687</a>
   */
  public static final String PARENT_PATH_PARAM = "parentPath";

  /**
   * Optional localparam, only valid together with {@link #PARENT_PATH_PARAM} on the {@code parent}
   * parser. When specified, the subordinate (child) query is constrained to docs at exactly the
   * path formed by concatenating {@code parentPath + "/" + childPath}, instead of the default
   * behavior of matching all descendants. For example, {@code parentPath="/skus"
   * childPath="manuals"} constrains children to docs whose {@code _nest_path_} is exactly {@code
   * /skus/manuals}.
   */
  public static final String CHILD_PATH_PARAM = "childPath";

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

  @Override
  public Query parse() throws SyntaxError {
    String parentPath = localParams.get(PARENT_PATH_PARAM);
    if (parentPath != null) {
      if (localParams.get(getParentFilterLocalParamName()) != null) {
        throw new SolrException(
            SolrException.ErrorCode.BAD_REQUEST,
            PARENT_PATH_PARAM
                + " and "
                + getParentFilterLocalParamName()
                + " local params are mutually exclusive");
      }
      if (!parentPath.startsWith("/")) {
        throw new SolrException(
            SolrException.ErrorCode.BAD_REQUEST, PARENT_PATH_PARAM + " must start with '/'");
      }
      // strip trailing slash (except for root "/")
      if (parentPath.length() > 1 && parentPath.endsWith("/")) {
        parentPath = parentPath.substring(0, parentPath.length() - 1);
      }
      return parseUsingParentPath(parentPath);
    }
    if (localParams.get(CHILD_PATH_PARAM) != null) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST, CHILD_PATH_PARAM + " requires " + PARENT_PATH_PARAM);
    }
    return super.parse();
  }

  /**
   * Parses the query using the {@code parentPath} localparam to automatically derive the parent
   * filter and child query constraints from {@link IndexSchema#NEST_PATH_FIELD_NAME}.
   *
   * <p>For the {@code parent} parser with {@code parentPath="/a/b/c"}:
   *
   * <pre>NEW: q={!parent parentPath="/a/b/c"}c_title:son
   *
   * OLD: q=(+{!field f="_nest_path_" v="/a/b/c"} +{!parent which=$ff v=$vv})
   *      ff=(*:* -{!prefix f="_nest_path_" v="/a/b/c/"})
   *      vv=(+c_title:son +{!prefix f="_nest_path_" v="/a/b/c/"})</pre>
   *
   * <p>For {@code parentPath="/"}:
   *
   * <pre>NEW: q={!parent parentPath="/"}c_title:son
   *
   * OLD: q=(+(*:* -_nest_path_:*) +{!parent which=$ff v=$vv})
   *      ff=(*:* -_nest_path_:*)
   *      vv=(+c_title:son +_nest_path_:*)</pre>
   *
   * @param parentPath the normalized parent path (starts with "/", no trailing slash except for
   *     root "/")
   */
  protected Query parseUsingParentPath(String parentPath) throws SyntaxError {
    // allParents filter: (*:* -{!prefix f="_nest_path_" v="<parentPath>/"})
    // For root: (*:* -_nest_path_:*)
    final Query allParentsFilter = buildAllParentsFilterFromPath(parentPath);

    String childPath = localParams.get(CHILD_PATH_PARAM);
    if (childPath != null) {
      // strip leading slash if present
      if (childPath.startsWith("/")) {
        childPath = childPath.substring(1);
      }
      if (childPath.isEmpty()) {
        throw new SolrException(
            SolrException.ErrorCode.BAD_REQUEST, CHILD_PATH_PARAM + " must not be empty");
      }
    }

    final BooleanQuery parsedChildQuery = parseImpl();

    if (parsedChildQuery.clauses().isEmpty()) {
      // no child query: return all "parent" docs at this level
      return wrapWithParentPathConstraint(parentPath, allParentsFilter);
    }

    // constrain child query: (+<original_child> +{!prefix f="_nest_path_" v="<parentPath>/"})
    // For root: (+<original_child> +_nest_path_:*)
    // If childPath specified: (+<original_child> +{!term f="_nest_path_"
    // v="<parentPath>/<childPath>"})
    final Query constrainedChildQuery =
        buildChildQueryWithPathConstraint(parentPath, childPath, parsedChildQuery);

    final String scoreMode = localParams.get("score", ScoreMode.None.name());
    final Query parentJoinQuery = createQuery(allParentsFilter, constrainedChildQuery, scoreMode);

    // wrap result: (+<parent_join> +{!field f="_nest_path_" v="<parentPath>"})
    // For root: (+<parent_join> -_nest_path_:*)
    return wrapWithParentPathConstraint(parentPath, parentJoinQuery);
  }

  /**
   * Builds the "all parents" filter query from the given {@code parentPath}. This query matches all
   * documents that are NOT strictly below (nested inside) the given path. This includes:
   *
   * <ul>
   *   <li>documents without any {@code _nest_path_} (root-level, non-nested docs)
   *   <li>documents at the same level as {@code parentPath} (i.e. with exactly that path)
   *   <li>documents at levels above {@code parentPath}
   *   <li>documents at completely orthogonal paths (e.g. {@code /x/y/z} when parentPath is {@code
   *       /a/b/c})
   * </ul>
   *
   * <p>Equivalent to: {@code (*:* -{!prefix f="_nest_path_" v="<parentPath>/"})} For root ({@code
   * /}): {@code (*:* -_nest_path_:*)}
   */
  protected static Query buildAllParentsFilterFromPath(String parentPath) {
    final Query excludeQuery;
    if (parentPath.equals("/")) {
      excludeQuery = new FieldExistsQuery(IndexSchema.NEST_PATH_FIELD_NAME);
    } else {
      excludeQuery = new PrefixQuery(new Term(IndexSchema.NEST_PATH_FIELD_NAME, parentPath + "/"));
    }
    return new BooleanQuery.Builder()
        .add(new MatchAllDocsQuery(), Occur.MUST)
        .add(excludeQuery, Occur.MUST_NOT)
        .build();
  }

  /**
   * Wraps the given query with a constraint ensuring only docs at exactly the {@code parentPath}
   * level are matched. For root, this excludes docs that have a {@code _nest_path_} value. For
   * non-root, this requires an exact match on {@code _nest_path_}.
   */
  protected static Query wrapWithParentPathConstraint(String parentPath, Query query) {
    final BooleanQuery.Builder builder = new BooleanQuery.Builder().add(query, Occur.MUST);
    if (parentPath.equals("/")) {
      builder.add(new FieldExistsQuery(IndexSchema.NEST_PATH_FIELD_NAME), Occur.MUST_NOT);
    } else {
      builder.add(
          new TermQuery(new Term(IndexSchema.NEST_PATH_FIELD_NAME, parentPath)), Occur.FILTER);
    }
    return builder.build();
  }

  /**
   * Constrains the child query to only match docs strictly below the given {@code parentPath}. For
   * the parent parser, the child query must match docs with a {@code _nest_path_} that is a
   * sub-path of the parent path (i.e. starts with {@code parentPath/}). For root, any doc with a
   * {@code _nest_path_} is a "child". If {@code childPath} is non-null, the constraint is an exact
   * term match on {@code parentPath/childPath} instead of a prefix query.
   */
  private static Query buildChildQueryWithPathConstraint(
      String parentPath, String childPath, Query childQuery) {
    final Query nestPathConstraint;
    if (childPath != null) {
      String effectiveChildPath =
          parentPath.equals("/") ? "/" + childPath : parentPath + "/" + childPath;
      nestPathConstraint =
          new TermQuery(new Term(IndexSchema.NEST_PATH_FIELD_NAME, effectiveChildPath));
    } else if (parentPath.equals("/")) {
      nestPathConstraint = new FieldExistsQuery(IndexSchema.NEST_PATH_FIELD_NAME);
    } else {
      nestPathConstraint =
          new PrefixQuery(new Term(IndexSchema.NEST_PATH_FIELD_NAME, parentPath + "/"));
    }
    return new BooleanQuery.Builder()
        .add(childQuery, Occur.MUST)
        .add(nestPathConstraint, Occur.FILTER)
        .build();
  }

  protected Query parseParentFilter() throws SyntaxError {
    String filter = localParams.get(getParentFilterLocalParamName());
    QParser parentParser = subQuery(filter, null);
    Query parentQ = parentParser.getQuery();
    return parentQ;
  }

  @Override
  protected Query wrapSubordinateClause(Query subordinate) throws SyntaxError {
    String scoreMode = localParams.get("score", ScoreMode.None.name());
    Query parentQ = parseParentFilter();
    return createQuery(parentQ, subordinate, scoreMode);
  }

  @Override
  protected Query noClausesQuery() throws SyntaxError {
    return new BitSetProducerQuery(getBitSetProducer(parseParentFilter()));
  }

  protected Query createQuery(final Query parentList, Query query, String scoreMode)
      throws SyntaxError {
    return new AllParentsAware(
        query, getBitSetProducer(parentList), ScoreModeParser.parse(scoreMode), parentList);
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
        public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
          BitSet bitSet = bitSetProducer.getBitSet(context);
          if (bitSet == null) {
            return null;
          }
          DocIdSetIterator disi = new BitSetIterator(bitSet, bitSet.approximateCardinality());
          return new SolrDefaultScorerSupplier(new ConstantScoreScorer(boost, scoreMode, disi));
        }

        @Override
        public boolean isCacheable(LeafReaderContext ctx) {
          return getCache();
        }
      };
    }
  }
}
