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
package org.apache.solr.search.vector;

import java.util.Optional;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.search.join.DiversifyingChildrenByteKnnVectorQuery;
import org.apache.lucene.search.join.DiversifyingChildrenFloatKnnVectorQuery;
import org.apache.lucene.search.join.ToChildBlockJoinQuery;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.DenseVectorField;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.QParser;
import org.apache.solr.search.SyntaxError;
import org.apache.solr.search.join.BlockJoinParentQParser;
import org.apache.solr.util.vector.DenseVectorParser;

public class KnnQParser extends AbstractVectorQParserBase {

  // retrieve the top K results based on the distance similarity function
  protected static final String TOP_K = "topK";
  protected static final int DEFAULT_TOP_K = 10;
  protected static final String SEED_QUERY = "seedQuery";
  protected static final String FILTERED_SEARCH_THRESHOLD = "filteredSearchThreshold";

  // parameters for PatienceKnnVectorQuery, a version of knn vector query that exits early when HNSW
  // queue saturates over a {@code #saturationThreshold} for more than {@code #patience} times.
  protected static final String EARLY_TERMINATION = "earlyTermination";
  protected static final boolean DEFAULT_EARLY_TERMINATION = false;
  protected static final String SATURATION_THRESHOLD = "saturationThreshold";
  protected static final String PATIENCE = "patience";

  public static final String PARENTS_PRE_FILTER = "parents.preFilter";
  public static final String CHILDREN_OF = "childrenOf";

  public KnnQParser(String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
    super(qstr, localParams, params, req);
  }

  public static class EarlyTerminationParams {
    private final boolean enabled;
    private final Double saturationThreshold;
    private final Integer patience;

    public EarlyTerminationParams(boolean enabled, Double saturationThreshold, Integer patience) {
      this.enabled = enabled;
      this.saturationThreshold = saturationThreshold;
      this.patience = patience;
    }

    public boolean isEnabled() {
      return enabled;
    }

    public Double getSaturationThreshold() {
      return saturationThreshold;
    }

    public Integer getPatience() {
      return patience;
    }
  }

  public EarlyTerminationParams getEarlyTerminationParams() {
    final Double saturationThreshold =
        Optional.ofNullable(localParams.get(SATURATION_THRESHOLD))
            .map(Double::parseDouble)
            .orElse(null);

    final Integer patience =
        Optional.ofNullable(localParams.get(PATIENCE)).map(Integer::parseInt).orElse(null);

    final boolean useExplicitParams = (saturationThreshold != null && patience != null);
    if ((saturationThreshold == null) != (patience == null)) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST,
          "Parameters 'saturationThreshold' and 'patience' must both be provided, or neither.");
    }

    final boolean enabled =
        localParams.getBool(EARLY_TERMINATION, DEFAULT_EARLY_TERMINATION) || useExplicitParams;
    return new EarlyTerminationParams(enabled, saturationThreshold, patience);
  }

  protected Query getSeedQuery() throws SolrException, SyntaxError {
    String seed = localParams.get(SEED_QUERY);
    if (seed == null) return null;
    if (seed.isBlank()) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST,
          "'seedQuery' parameter is present but is blank: please provide a valid query");
    }
    final QParser seedParser = subQuery(seed, null);
    return seedParser.getQuery();
  }

  @Override
  public Query parse() throws SyntaxError {
    final String vectorField = getFieldName();
    final SchemaField schemaField = req.getCore().getLatestSchema().getField(getFieldName());
    final DenseVectorField denseVectorType = getCheckedFieldType(schemaField);
    final String vectorToSearch = getVectorToSearch();
    final int topK = localParams.getInt(TOP_K, DEFAULT_TOP_K);

    final double efSearchScaleFactor = localParams.getDouble("efSearchScaleFactor", 1.0);
    if (Double.isNaN(efSearchScaleFactor) || efSearchScaleFactor < 1.0) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST,
          "efSearchScaleFactor (" + efSearchScaleFactor + ") must be >= 1.0");
    }
    final int efSearch = (int) Math.round(efSearchScaleFactor * topK);

    final Integer filteredSearchThreshold = localParams.getInt(FILTERED_SEARCH_THRESHOLD);

    // check for parent diversification logic...
    final String[] parentsFilterQueries = localParams.getParams(PARENTS_PRE_FILTER);
    final String allParentsQuery = localParams.get(CHILDREN_OF);

    boolean isDiversifyingChildrenKnnQuery =
        null != parentsFilterQueries || null != allParentsQuery;
    if (isDiversifyingChildrenKnnQuery) {
      if (null == allParentsQuery) {
        throw new SolrException(
            SolrException.ErrorCode.BAD_REQUEST,
            "When running a diversifying children KNN query, 'childrenOf' parameter is required");
      }
      final DenseVectorParser vectorBuilder =
          denseVectorType.getVectorBuilder(vectorToSearch, DenseVectorParser.BuilderPhase.QUERY);
      final VectorEncoding vectorEncoding = denseVectorType.getVectorEncoding();

      final BitSetProducer allParentsBitSet =
          BlockJoinParentQParser.getCachedBitSetProducer(
              req, subQuery(allParentsQuery, null).getQuery());
      final BooleanQuery acceptedParents = getParentsFilter(parentsFilterQueries);

      Query acceptedChildren =
          getChildrenFilter(getFilterQuery(), acceptedParents, allParentsBitSet);
      switch (vectorEncoding) {
        case FLOAT32:
          return new DiversifyingChildrenFloatKnnVectorQuery(
              vectorField,
              vectorBuilder.getFloatVector(),
              acceptedChildren,
              topK,
              allParentsBitSet);
        case BYTE:
          return new DiversifyingChildrenByteKnnVectorQuery(
              vectorField, vectorBuilder.getByteVector(), acceptedChildren, topK, allParentsBitSet);
        default:
          throw new SolrException(
              SolrException.ErrorCode.SERVER_ERROR,
              "Unexpected encoding. Vector Encoding: " + vectorEncoding);
      }
    }

    return denseVectorType.getKnnVectorQuery(
        schemaField.getName(),
        vectorToSearch,
        topK,
        efSearch,
        getFilterQuery(),
        getSeedQuery(),
        getEarlyTerminationParams(),
        filteredSearchThreshold);
  }

  private BooleanQuery getParentsFilter(String[] parentsFilterQueries) throws SyntaxError {
    BooleanQuery.Builder acceptedParentsBuilder = new BooleanQuery.Builder();
    if (parentsFilterQueries != null) {
      for (String parentsFilterQuery : parentsFilterQueries) {
        final QParser parser = subQuery(parentsFilterQuery, null);
        parser.setIsFilter(true);
        final Query parentsFilter = parser.getQuery();
        if (parentsFilter != null) {
          acceptedParentsBuilder.add(parentsFilter, BooleanClause.Occur.FILTER);
        }
      }
    }
    return acceptedParentsBuilder.build();
  }

  private Query getChildrenFilter(
      Query childrenKnnPreFilter, BooleanQuery parentsFilter, BitSetProducer allParentsBitSet) {
    Query childrenFilter = childrenKnnPreFilter;

    if (!parentsFilter.clauses().isEmpty()) {
      Query acceptedChildrenBasedOnParentsFilter =
          new ToChildBlockJoinQuery(parentsFilter, allParentsBitSet); // no scoring happens here
      BooleanQuery.Builder acceptedChildrenBuilder = new BooleanQuery.Builder();
      if (childrenFilter != null) {
        acceptedChildrenBuilder.add(childrenFilter, BooleanClause.Occur.FILTER);
      }
      acceptedChildrenBuilder.add(acceptedChildrenBasedOnParentsFilter, BooleanClause.Occur.FILTER);

      childrenFilter = acceptedChildrenBuilder.build();
    }
    return childrenFilter;
  }
}
