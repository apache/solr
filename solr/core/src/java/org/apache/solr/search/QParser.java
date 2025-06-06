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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.lucene.queries.function.FunctionQuery;
import org.apache.lucene.queries.function.FunctionScoreQuery;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.valuesource.QueryValueSource;
import org.apache.lucene.search.Query;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.ValueSourceParser.LongConstValueSource;

/**
 * <b>Note: This API is experimental and may change in non backward-compatible ways in the
 * future</b>
 */
public abstract class QParser {
  /**
   * @lucene.experimental
   */
  public static final int FLAG_FILTER = 0x01;

  protected String qstr;
  protected SolrParams params;
  protected SolrParams localParams;
  protected SolrQueryRequest req;
  protected int recurseCount;

  /**
   * @lucene.experimental
   */
  protected int flags;

  protected Query query;

  protected String
      stringIncludingLocalParams; // the original query string including any local params
  protected boolean valFollowedParams; // true if the value "qstr" followed the localParams
  protected int localParamsEnd; // the position one past where the localParams ended

  /**
   * Constructor for the QParser
   *
   * @param qstr The query string to be parsed
   * @param localParams Params scoped to this query, customizing the parsing. Null if none. Most
   *     params the parser needs are resolved here first, overlaying the request. See
   *     https://solr.apache.org/guide/solr/latest/query-guide/local-params.html
   * @param params Params for the request, taken from {@link SolrQueryRequest#getParams()}; not null
   * @param req The original {@link org.apache.solr.request.SolrQueryRequest}.
   */
  public QParser(String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
    this.qstr = qstr;
    this.localParams = localParams;

    // insert tags into tagmap.
    // WARNING: the internal representation of tagged objects in the request context is
    // experimental and subject to change!
    if (localParams != null) {
      String tagStr = localParams.get(CommonParams.TAG);
      if (tagStr != null) {
        Map<Object, Object> context = req.getContext();
        @SuppressWarnings("unchecked")
        Map<Object, Collection<Object>> tagMap =
            (Map<Object, Collection<Object>>) req.getContext().get("tags");
        if (tagMap == null) {
          tagMap = new HashMap<>();
          context.put("tags", tagMap);
        }
        if (tagStr.indexOf(',') >= 0) {
          List<String> tags = StrUtils.splitSmart(tagStr, ',');
          for (String tag : tags) {
            addTag(tagMap, tag, this);
          }
        } else {
          addTag(tagMap, tagStr, this);
        }
      }
    }

    this.params = Objects.requireNonNull(params);
    this.req = req;
  }

  /**
   * @lucene.experimental
   */
  public void setFlags(int flags) {
    this.flags = flags;
  }

  /**
   * @lucene.experimental
   */
  public int getFlags() {
    return flags;
  }

  /**
   * @lucene.experimental Query is in the context of a filter, where scores don't matter
   */
  public boolean isFilter() {
    return (flags & FLAG_FILTER) != 0;
  }

  /**
   * @lucene.experimental
   */
  public void setIsFilter(boolean isFilter) {
    if (isFilter) flags |= FLAG_FILTER;
    else flags &= ~FLAG_FILTER;
  }

  private static void addTag(Map<Object, Collection<Object>> tagMap, Object key, Object val) {
    Collection<Object> lst = tagMap.get(key);
    if (lst == null) {
      lst = new ArrayList<>(2);
      tagMap.put(key, lst);
    }
    lst.add(val);
  }

  /**
   * Create and return the <code>Query</code> object represented by <code>qstr</code>. Null MAY be
   * returned to signify there was no input (e.g. no query string) to parse.
   *
   * @see #getQuery()
   */
  public abstract Query parse() throws SyntaxError;

  public SolrParams getLocalParams() {
    return localParams;
  }

  public void setLocalParams(SolrParams localParams) {
    this.localParams = localParams;
  }

  public SolrParams getParams() {
    return params;
  }

  public void setParams(SolrParams params) {
    this.params = Objects.requireNonNull(params);
  }

  public SolrQueryRequest getReq() {
    return req;
  }

  public void setReq(SolrQueryRequest req) {
    this.req = req;
  }

  public String getString() {
    return qstr;
  }

  public void setString(String s) {
    this.qstr = s;
  }

  /**
   * Returns the resulting query from this QParser, calling parse() only the first time and caching
   * the Query result. <em>A null return is possible!</em>
   */
  // TODO never return null; standardize the semantics
  public Query getQuery() throws SyntaxError {
    if (query == null) {
      query = parse();

      if (localParams != null) {
        String cacheStr = localParams.get(CommonParams.CACHE);
        if (cacheStr != null) {
          if (CommonParams.FALSE.equals(cacheStr)) {
            extendedQuery().setCache(false);
          } else if (CommonParams.TRUE.equals(cacheStr)) {
            extendedQuery().setCache(true);
          }
        }

        int cost = localParams.getInt(CommonParams.COST, Integer.MIN_VALUE);
        if (cost != Integer.MIN_VALUE) {
          extendedQuery().setCost(cost);
        }
      }
    }
    return query;
  }

  // returns an extended query (and sets "query" to a new wrapped query if necessary)
  private ExtendedQuery extendedQuery() {
    if (query instanceof ExtendedQuery) {
      return (ExtendedQuery) query;
    } else {
      WrappedQuery wq = new WrappedQuery(query);
      query = wq;
      return wq;
    }
  }

  private void checkRecurse() throws SyntaxError {
    if (recurseCount++ >= 100) {
      throw new SyntaxError("Infinite Recursion detected parsing query '" + qstr + "'");
    }
  }

  // TODO: replace with a SolrParams that defaults to checking localParams first?
  // ideas..
  //   create params that satisfy field-specific overrides
  //   overrideable syntax $x=foo  (set global for limited scope) (invariants & security?)
  //                       $x+=foo (append to global for limited scope)

  /** check both local and global params */
  public String getParam(String name) {
    String val;
    if (localParams != null) {
      val = localParams.get(name);
      if (val != null) return val;
    }
    return params.get(name);
  }

  /** Create a new QParser for parsing an embedded sub-query */
  public QParser subQuery(String q, String defaultType) throws SyntaxError {
    checkRecurse();
    if (defaultType == null && localParams != null) {
      // if not passed, try and get the defaultType from local params
      defaultType = localParams.get(QueryParsing.DEFTYPE);
    }
    QParser nestedParser = getParser(q, defaultType, getReq());
    // TODO: this would be better passed in to the constructor... change to a ParserContext object?
    nestedParser.flags = this.flags;
    nestedParser.recurseCount = recurseCount;
    recurseCount--;
    return nestedParser;
  }

  /**
   * @param useGlobalParams look up sort, start, rows in global params if not in local params
   * @return the sort specification
   */
  public SortSpec getSortSpec(boolean useGlobalParams) throws SyntaxError {
    getQuery(); // ensure query is parsed first

    String sortStr = null;
    Integer start = null;
    Integer rows = null;

    if (localParams != null) {
      sortStr = localParams.get(CommonParams.SORT);
      start = localParams.getInt(CommonParams.START);
      rows = localParams.getInt(CommonParams.ROWS);

      // if any of these parameters are present, don't go back to the global params
      if (sortStr != null || start != null || rows != null) {
        useGlobalParams = false;
      }
    }

    if (useGlobalParams) {
      if (sortStr == null) {
        sortStr = params.get(CommonParams.SORT);
      }
      if (start == null) {
        start = params.getInt(CommonParams.START);
      }
      if (rows == null) {
        rows = params.getInt(CommonParams.ROWS);
      }
    }

    start = start != null ? start : CommonParams.START_DEFAULT;
    rows = rows != null ? rows : CommonParams.ROWS_DEFAULT;

    SortSpec sort = SortSpecParsing.parseSortSpec(sortStr, req);

    sort.setOffset(start);
    sort.setCount(rows);
    return sort;
  }

  public String[] getDefaultHighlightFields() {
    return new String[] {};
  }

  public Query getHighlightQuery() throws SyntaxError {
    Query query = getQuery();
    return query instanceof WrappedQuery ? ((WrappedQuery) query).getWrappedQuery() : query;
  }

  public void addDebugInfo(NamedList<Object> debugInfo) {
    debugInfo.add("QParser", this.getClass().getSimpleName());
  }

  public int getPrefixQueryMinPrefixLength() {
    final var localLimit =
        getLocalParams() != null
            ? getLocalParams().getInt(SolrConfig.MIN_PREFIX_QUERY_TERM_LENGTH)
            : null;
    if (localLimit != null) {
      return localLimit;
    }

    return getReq().getCore().getSolrConfig().prefixQueryMinPrefixLength;
  }

  /**
   * Parse the string into a {@link ValueSource} <em>instead of a {@link Query}</em>. Solr calls
   * this in most places that "function queries" go. Overridden by {@link FunctionQParser}.
   */
  public ValueSource parseAsValueSource() throws SyntaxError {
    Query q = getQuery();
    return switch (q) {
      case null -> new LongConstValueSource(0);
      case FunctionQuery functionQuery -> functionQuery.getValueSource();
      case FunctionScoreQuery functionQuery -> ValueSource.fromDoubleValuesSource(
          functionQuery.getSource());
      default -> new QueryValueSource(q, 0.0f);
    };
  }

  /**
   * Create a {@link QParser} to parse <code>qstr</code>, using the "lucene"
   * (QParserPlugin.DEFAULT_QTYPE) query parser. The query parser may be overridden by local-params
   * in the query string itself. For example if qstr=<code>{!prefix f=myfield}foo</code> then the
   * prefix query parser will be used.
   */
  public static QParser getParser(String qstr, SolrQueryRequest req) throws SyntaxError {
    return getParser(qstr, QParserPlugin.DEFAULT_QTYPE, req);
  }

  /**
   * Create a {@link QParser} to parse <code>qstr</code> using the <code>defaultParser</code>. Note
   * that local-params is only parsed when the defaultParser is "lucene" or "func".
   */
  public static QParser getParser(String qstr, String defaultParser, SolrQueryRequest req)
      throws SyntaxError {
    boolean allowLocalParams =
        defaultParser == null
            || defaultParser.equals(QParserPlugin.DEFAULT_QTYPE)
            || defaultParser.equals(FunctionQParserPlugin.NAME);
    return getParser(qstr, defaultParser, allowLocalParams, req);
  }

  /**
   * Expert: Create a {@link QParser} to parse {@code qstr} using the {@code parserName} parser,
   * while allowing a toggle for whether local-params may be parsed. The query parser may be
   * overridden by local parameters in the query string itself (assuming {@code allowLocalParams}.
   * For example if parserName=<code>dismax</code> and qstr=<code>foo</code>, then the dismax query
   * parser will be used to parse and construct the query object. However if qstr=<code>
   * {!prefix f=myfield}foo</code> then the prefix query parser will be used.
   *
   * @param allowLocalParams Whether this query parser should parse local-params syntax. Note that
   *     the "lucene" query parser natively parses local-params regardless.
   * @lucene.internal
   */
  public static QParser getParser(
      String qstr, String parserName, boolean allowLocalParams, SolrQueryRequest req)
      throws SyntaxError {
    // SolrParams localParams = QueryParsing.getLocalParams(qstr, req.getParams());
    if (parserName == null) {
      parserName = QParserPlugin.DEFAULT_QTYPE; // "lucene"
    }
    String stringIncludingLocalParams = qstr;
    ModifiableSolrParams localParams = null;
    SolrParams globalParams = req.getParams();
    boolean valFollowedParams = true;
    int localParamsEnd = -1;

    if (allowLocalParams && qstr != null && qstr.startsWith(QueryParsing.LOCALPARAM_START)) {
      localParams = new ModifiableSolrParams();
      localParamsEnd = QueryParsing.parseLocalParams(qstr, 0, localParams, globalParams);

      String val = localParams.get(QueryParsing.V);
      if (val != null) {
        // val was directly specified in localParams via v=<something> or v=$arg
        valFollowedParams = false;
        // TODO if remainder of query string after '}' is non-empty, then what? Throw error? Fall
        // back to lucene QParser?
      } else {
        // use the remainder of the string as the value
        valFollowedParams = true;
        val = qstr.substring(localParamsEnd);
        localParams.set(QueryParsing.V, val);
      }

      parserName = localParams.get(QueryParsing.TYPE, parserName);
      qstr = localParams.get(QueryParsing.V);
    }

    QParserPlugin qplug = req.getCore().getQueryPlugin(parserName);
    if (qplug == null) {
      // there should a way to include parameter for which parsing failed
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST,
          "invalid query parser '"
              + parserName
              + (stringIncludingLocalParams == null
                  ? "'"
                  : "' for query '" + stringIncludingLocalParams + "'"));
    }
    QParser parser = qplug.createParser(qstr, localParams, req.getParams(), req);

    parser.stringIncludingLocalParams = stringIncludingLocalParams;
    parser.valFollowedParams = valFollowedParams;
    parser.localParamsEnd = localParamsEnd;
    return parser;
  }
}
