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
package org.apache.solr.search;

import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.complexPhrase.ComplexPhraseQueryParser;
import org.apache.lucene.search.AutomatonQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.util.automaton.Operations;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.parser.QueryParser;
import org.apache.solr.request.SolrQueryRequest;

/**
 * Parse Solr's variant on the Lucene {@link
 * org.apache.lucene.queryparser.complexPhrase.ComplexPhraseQueryParser} syntax.
 *
 * <p>Modified from {@link org.apache.solr.search.LuceneQParserPlugin} and {@link
 * org.apache.solr.search.SurroundQParserPlugin}
 */
public class ComplexPhraseQParserPlugin extends QParserPlugin {

  public static final String NAME = "complexphrase";

  private boolean inOrder = true;

  @Override
  public void init(NamedList<?> args) {
    super.init(args);
    if (args != null) {
      Object val = args.get("inOrder");
      if (val != null) {
        inOrder = StrUtils.parseBool(val.toString());
      }
    }
  }

  @Override
  public QParser createParser(
      String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
    ComplexPhraseQParser qParser = new ComplexPhraseQParser(qstr, localParams, params, req);
    qParser.setInOrder(inOrder);
    return qParser;
  }

  /**
   * Modified from {@link org.apache.solr.search.LuceneQParser} and {@link
   * org.apache.solr.search.SurroundQParserPlugin.SurroundQParser}
   */
  static class ComplexPhraseQParser extends QParser {

    static final class SolrQueryParserDelegate extends SolrQueryParser {
      private SolrQueryParserDelegate(QParser parser, String defaultField) {
        super(parser, defaultField);
      }

      @Override
      protected org.apache.lucene.search.Query getWildcardQuery(String field, String termStr)
          throws SyntaxError {
        Query q = super.getWildcardQuery(field, termStr);

        // For complex phrase queries, we need to ensure wildcard queries and automaton queries
        // use SCORING_BOOLEAN_REWRITE to avoid the new constant score wrappers in Lucene 10
        if (q instanceof WildcardQuery wq) {
          return new WildcardQuery(
              wq.getTerm(),
              Operations.DEFAULT_DETERMINIZE_WORK_LIMIT,
              MultiTermQuery.SCORING_BOOLEAN_REWRITE);
        } else if (q instanceof AutomatonQuery aq) {
          // For AutomatonQuery, we need to create a new one with the desired rewrite method
          // We can't access the term directly, but for reversed wildcard queries,
          // we can use a placeholder term since the automaton contains the actual matching logic
          return new AutomatonQuery(
              new Term(field, ""),
              aq.getAutomaton(),
              aq.isAutomatonBinary(),
              MultiTermQuery.SCORING_BOOLEAN_REWRITE);
        }

        return q;
      }

      @Override
      protected Query getRangeQuery(
          String field, String part1, String part2, boolean startInclusive, boolean endInclusive)
          throws SyntaxError {
        return super.getRangeQuery(field, part1, part2, startInclusive, endInclusive);
      }

      @Override
      protected boolean isRangeShouldBeProtectedFromReverse(String field, String part1) {
        return super.isRangeShouldBeProtectedFromReverse(field, part1);
      }

      public String getLowerBoundForReverse() {
        return REVERSE_WILDCARD_LOWER_BOUND;
      }
    }

    ComplexPhraseQueryParser lparser;

    boolean inOrder = true;

    /**
     * When <code>inOrder</code> is true, the search terms must exists in the documents as the same
     * order as in query.
     *
     * @param inOrder parameter to choose between ordered or un-ordered proximity search
     */
    public void setInOrder(final boolean inOrder) {
      this.inOrder = inOrder;
    }

    public ComplexPhraseQParser(
        String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
      super(qstr, localParams, params, req);
    }

    @Override
    public Query parse() throws SyntaxError {
      String qstr = getString();

      String defaultField = getParam(CommonParams.DF);

      SolrQueryParserDelegate reverseAwareParser = new SolrQueryParserDelegate(this, defaultField);
      final var qParserReference = this;

      lparser =
          new ComplexPhraseQueryParser(defaultField, getReq().getSchema().getQueryAnalyzer()) {
            @Override
            protected Query newWildcardQuery(org.apache.lucene.index.Term t) {
              try {
                // Get the wildcard query from the reverse-aware parser
                org.apache.lucene.search.Query wildcardQuery =
                    reverseAwareParser.getWildcardQuery(t.field(), t.text());

                // In Lucene 10, we need to ensure wildcard queries use SCORING_BOOLEAN_REWRITE
                // for complex phrase queries to work properly
                if (wildcardQuery instanceof WildcardQuery wq) {
                  return new WildcardQuery(
                      wq.getTerm(),
                      Operations.DEFAULT_DETERMINIZE_WORK_LIMIT,
                      MultiTermQuery.SCORING_BOOLEAN_REWRITE);
                }

                return wildcardQuery;
              } catch (SyntaxError e) {
                throw new RuntimeException(e);
              }
            }

            @Override
            protected Query getPrefixQuery(String field, String termStr) throws ParseException {
              final var query = super.getPrefixQuery(field, termStr);
              QueryUtils.ensurePrefixQueryObeysMinimumPrefixLength(
                  qParserReference, query, termStr);
              return query;
            }

            /*  private Query setRewriteMethod(org.apache.lucene.search.Query query) {
              if (query instanceof MultiTermQuery) {
                ((MultiTermQuery) query)
                    .setRewriteMethod(
                        org.apache.lucene.search.MultiTermQuery.SCORING_BOOLEAN_REWRITE);
              }
              return query;
            }*/

            @Override
            protected Query newRangeQuery(
                String field,
                String part1,
                String part2,
                boolean startInclusive,
                boolean endInclusive) {
              boolean reverse =
                  reverseAwareParser.isRangeShouldBeProtectedFromReverse(field, part1);
              return super.newRangeQuery(
                  field,
                  reverse ? reverseAwareParser.getLowerBoundForReverse() : part1,
                  part2,
                  startInclusive || reverse,
                  endInclusive);
            }
          };

      lparser.setAllowLeadingWildcard(true);

      if (localParams != null) {
        inOrder = localParams.getBool("inOrder", inOrder);
      }

      lparser.setInOrder(inOrder);

      QueryParser.Operator defaultOperator = QueryParsing.parseOP(getParam(QueryParsing.OP));

      if (QueryParser.Operator.AND.equals(defaultOperator))
        lparser.setDefaultOperator(org.apache.lucene.queryparser.classic.QueryParser.Operator.AND);
      else
        lparser.setDefaultOperator(org.apache.lucene.queryparser.classic.QueryParser.Operator.OR);

      try {
        return lparser.parse(qstr);
      } catch (ParseException pe) {
        throw new SyntaxError(pe);
      }
    }

    @Override
    public String[] getDefaultHighlightFields() {
      return lparser == null ? new String[] {} : new String[] {lparser.getField()};
    }
  }
}
