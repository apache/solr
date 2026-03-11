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

import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.join.ToChildBlockJoinQuery;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.SyntaxError;

/** Matches child documents based on parent doc criteria. */
public class BlockJoinChildQParser extends BlockJoinParentQParser {

  public BlockJoinChildQParser(
      String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
    super(qstr, localParams, params, req);
  }

  @Override
  protected Query createQuery(Query parentListQuery, Query query, String scoreMode) {
    return new ToChildBlockJoinQuery(query, getBitSetProducer(parentListQuery));
  }

  @Override
  protected String getParentFilterLocalParamName() {
    return "of";
  }

  @Override
  protected Query noClausesQuery() throws SyntaxError {
    final Query parents = parseParentFilter();
    final BooleanQuery notParents =
        new BooleanQuery.Builder()
            .add(new MatchAllDocsQuery(), Occur.MUST)
            .add(parents, Occur.MUST_NOT)
            .build();
    return new BitSetProducerQuery(getBitSetProducer(notParents));
  }

  /**
   * Parses the query using the {@code parentPath} localparam for the child parser.
   *
   * <p>For the {@code child} parser with {@code parentPath="/a/b/c"}:
   *
   * <pre>NEW: q={!child parentPath="/a/b/c"}p_title:dad
   *
   * OLD: q={!child of=$ff v=$vv}
   *      ff=(*:* -{!prefix f="_nest_path_" v="/a/b/c/"})
   *      vv=(+p_title:dad +{!field f="_nest_path_" v="/a/b/c"})</pre>
   *
   * <p>For {@code parentPath="/"}:
   *
   * <pre>NEW: q={!child parentPath="/"}p_title:dad
   *
   * OLD: q={!child of=$ff v=$vv}
   *      ff=(*:* -_nest_path_:*)
   *      vv=(+p_title:dad -_nest_path_:*)</pre>
   *
   * @param parentPath the normalized parent path (starts with "/", no trailing slash except for
   *     root "/")
   */
  @Override
  protected Query parseUsingParentPath(String parentPath) throws SyntaxError {
    if (localParams.get(CHILD_PATH_PARAM) != null) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST,
          CHILD_PATH_PARAM + " is not supported by the {!child} parser");
    }
    // allParents filter: (*:* -{!prefix f="_nest_path_" v="<parentPath>/"})
    // For root: (*:* -_nest_path_:*)
    final Query allParentsFilter = buildAllParentsFilterFromPath(parentPath);

    final BooleanQuery parsedParentQuery = parseImpl();

    if (parsedParentQuery.clauses().isEmpty()) {
      // no parent query: return all children of parents at this level
      final BooleanQuery notParents =
          new BooleanQuery.Builder()
              .add(new MatchAllDocsQuery(), Occur.MUST)
              .add(allParentsFilter, Occur.MUST_NOT)
              .build();
      return new BitSetProducerQuery(getBitSetProducer(notParents));
    }

    // constrain the parent query to only match docs at exactly parentPath
    // (+<original_parent> +{!field f="_nest_path_" v="<parentPath>"})
    // For root: (+<original_parent> -_nest_path_:*)
    Query constrainedParentQuery = wrapWithParentPathConstraint(parentPath, parsedParentQuery);

    return createQuery(allParentsFilter, constrainedParentQuery, null);
  }
}
