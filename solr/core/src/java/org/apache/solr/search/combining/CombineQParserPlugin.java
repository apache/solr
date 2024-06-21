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

package org.apache.solr.search.combining;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.solr.common.params.CombinerParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.handler.component.QueryComponent;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.ExtendedQueryBase;
import org.apache.solr.search.QParser;
import org.apache.solr.search.QParserPlugin;
import org.apache.solr.search.QueryCommand;
import org.apache.solr.search.QueryParsing;
import org.apache.solr.search.QueryResult;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.SyntaxError;

public class CombineQParserPlugin extends QParserPlugin {
  public static final String NAME = "combine";

  @Override
  public QParser createParser(
      String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
    return new CombineQParser(qstr, localParams, params, req);
  }

  static class CombineQParser extends QParser {

    private List<String> unparsedQueries;
    private QueriesCombiner queriesCombiningStrategy;
    private List<QParser> queryParsers;
    private List<Query> queries;

    public CombineQParser(
        String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
      super(qstr, localParams, params, req);
    }

    @Override
    public Query parse() throws SyntaxError {

      var queriesToCombineKeys = localParams.getParams("keys");
      if (queriesToCombineKeys == null) {
        queriesToCombineKeys = params.getParams(CombinerParams.COMBINER_KEYS);
      }

      unparsedQueries = new ArrayList<>(queriesToCombineKeys.length);
      queryParsers = new ArrayList<>(queriesToCombineKeys.length);
      queries = new ArrayList<>(queriesToCombineKeys.length);

      // nocommit blend localParams and params without needless suffix in local
      var blendParams = SolrParams.wrapDefaults(localParams, params);
      queriesCombiningStrategy = QueriesCombiner.getImplementation(blendParams);

      String defType = blendParams.get(QueryParsing.DEFTYPE, DEFAULT_QTYPE);

      for (String queryKey : queriesToCombineKeys) {
        final var unparsedQuery = blendParams.get(queryKey);
        final var parser = QParser.getParser(unparsedQuery, defType, req);
        var query = parser.getQuery();
        if (query == null) { // sad this can happen
          query = new MatchNoDocsQuery();
        }
        unparsedQueries.add(unparsedQuery);
        queryParsers.add(parser);
        queries.add(query);
      }

      return new CombineQuery(queriesCombiningStrategy, queries);
    }

    @Override
    public void addDebugInfo(NamedList<Object> dbg) {
      super.addDebugInfo(dbg);

      NamedList<NamedList<Object>> queriesDebug = new SimpleOrderedMap<>();
      String[] queryKeys = req.getParams().getParams(CombinerParams.COMBINER_KEYS);
      for (int i = 0; i < queries.size(); i++) {
        NamedList<Object> singleQueryDebug = new SimpleOrderedMap<>();
        singleQueryDebug.add("querystring", unparsedQueries.get(i));
        singleQueryDebug.add("queryparser", queryParsers.get(i).getClass().getSimpleName());
        singleQueryDebug.add("parsedquery", QueryParsing.toString(queries.get(i), req.getSchema()));
        singleQueryDebug.add("parsedquery_toString", queries.get(i).toString());
        queriesDebug.add(queryKeys[i], singleQueryDebug);
      }
      dbg.add("queriesToCombine", queriesDebug);
    }
  }

  static final class CombineQuery extends ExtendedQueryBase
      implements QueryComponent.SelfExecutingQuery {

    private final QueriesCombiner combiner;
    private final List<Query> queries;

    public CombineQuery(QueriesCombiner combiner, List<Query> queries) {
      this.combiner = combiner;
      this.queries = queries;
    }

    @Override
    public String toString(String field) {
      return super.toString(field) + "{!" + NAME + "}"; // TODO others
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof CombineQuery)) return false;
      CombineQuery that = (CombineQuery) o;
      return Objects.equals(combiner, that.combiner) && Objects.equals(queries, that.queries);
    }

    @Override
    public int hashCode() {
      return Objects.hash(combiner, queries);
    }

    @Override
    public void visit(QueryVisitor visitor) {
      for (Query query : queries) {
        query.visit(visitor.getSubVisitor(Occur.MUST, this));
      }
    }

    @Override
    public QueryResult search(SolrIndexSearcher searcher, QueryCommand cmd) throws IOException {
      QueryResult[] results = new QueryResult[queries.size()];
      // TODO do in multiple threads?
      for (int i = 0; i < queries.size(); i++) {
        cmd.setQuery(queries.get(i));
        QueryResult qr = new QueryResult();
        searcher.search(qr, cmd);
        results[i] = qr;
      }

      // nocommit but how is the docSet (e.g. for faceting) or maybe other things supported?

      return combiner.combine(results);
    }
  }
}
