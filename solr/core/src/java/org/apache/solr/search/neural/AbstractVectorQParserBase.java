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
package org.apache.solr.search.neural;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.lucene.search.Query;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.DenseVectorField;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.QParser;
import org.apache.solr.search.QueryParsing;
import org.apache.solr.search.QueryUtils;
import org.apache.solr.search.SyntaxError;

public abstract class AbstractVectorQParserBase extends QParser {

  static final String PRE_FILTER = "preFilter";
  static final String EXCLUDE_TAGS = "excludeTags";
  static final String INCLUDE_TAGS = "includeTags";

  private final String denseVectorFieldName;
  private final String vectorToSearch;

  public AbstractVectorQParserBase(
      String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
    super(qstr, localParams, params, req);
    vectorToSearch = localParams.get(QueryParsing.V);
    denseVectorFieldName = localParams.get(QueryParsing.F);
  }

  protected String getVectorToSearch() {
    if (vectorToSearch == null || vectorToSearch.isEmpty()) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST, "the Dense Vector value 'v' to search is missing");
    }
    return vectorToSearch;
  }

  protected String getFieldName() {
    if (denseVectorFieldName == null || denseVectorFieldName.isEmpty()) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST, "the Dense Vector field 'f' is missing");
    }
    return denseVectorFieldName;
  }

  protected static DenseVectorField getCheckedFieldType(SchemaField schemaField) {
    FieldType fieldType = schemaField.getType();
    if (!(fieldType instanceof DenseVectorField)) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST,
          "only DenseVectorField is compatible with Vector Query Parsers");
    }

    return (DenseVectorField) fieldType;
  }

  protected Query getFilterQuery() throws SolrException, SyntaxError {

    // Default behavior of FQ wrapping, and suitability of some local params
    // depends on wether we are a sub-query or not
    final boolean isSubQuery = recurseCount != 0;

    // include/exclude tags for global fqs to wrap;
    // Check these up front for error handling if combined with `fq` local param.
    final List<String> includedGlobalFQTags = getLocalParamTags(INCLUDE_TAGS);
    final List<String> excludedGlobalFQTags = getLocalParamTags(EXCLUDE_TAGS);
    final boolean haveGlobalFQTags =
        !(includedGlobalFQTags.isEmpty() && excludedGlobalFQTags.isEmpty());

    if (haveGlobalFQTags) {
      // Some early error handling of incompatible options...

      if (isFilter()) { // this knn query is itself a filter query
        throw new SolrException(
            SolrException.ErrorCode.BAD_REQUEST,
            "Knn Query Parser used as a filter does not support "
                + INCLUDE_TAGS
                + " or "
                + EXCLUDE_TAGS
                + " localparams");
      }

      if (isSubQuery) { // this knn query is a sub-query of a broader query (possibly disjunction)
        throw new SolrException(
            SolrException.ErrorCode.BAD_REQUEST,
            "Knn Query Parser used as a sub-query does not support "
                + INCLUDE_TAGS
                + " or "
                + EXCLUDE_TAGS
                + " localparams");
      }
    }

    // Explicit local params specifying the filter(s) to wrap
    final String[] preFilters = getLocalParams().getParams(PRE_FILTER);
    if (null != preFilters) {

      // We don't particularly care if preFilters is empty, the usage below will still work,
      // but SolrParams API says it should be null not empty...
      assert 0 != preFilters.length
          : "SolrParams.getParams should return null, never zero len array";

      if (haveGlobalFQTags) {
        throw new SolrException(
            SolrException.ErrorCode.BAD_REQUEST,
            "Knn Query Parser does not support combining "
                + PRE_FILTER
                + " localparam with either "
                + INCLUDE_TAGS
                + " or "
                + EXCLUDE_TAGS
                + " localparams");
      }

      final List<Query> preFilterQueries = new ArrayList<>(preFilters.length);
      for (String f : preFilters) {
        final QParser parser = subQuery(f, null);
        parser.setIsFilter(true);

        // maybe null, ie: `preFilter=""`
        final Query filter = parser.getQuery();
        if (null != filter) {
          preFilterQueries.add(filter);
        }
      }
      try {
        return req.getSearcher().getProcessedFilter(null, preFilterQueries).filter;
      } catch (IOException e) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
      }
    }

    // No explicit `preFilter` localparams specifying what we should filter on.
    //
    // So now, if we're either a filter or a subquery, we have to default to
    // not wrapping anything...
    if (isFilter() || isSubQuery) {
      return null;
    }

    // At this point we now are a (regular) query and can wrap global `fq` filters...
    try {
      // Start by assuming we wrap all global filters,
      // then adjust our list based on include/exclude tag params
      List<Query> globalFQs = QueryUtils.parseFilterQueries(req);

      // Adjust our globalFQs based on any include/exclude we may have
      if (!includedGlobalFQTags.isEmpty()) {
        // NOTE: Even if no FQs match the specified tag(s) the fact that tags were specified
        // means we should replace globalFQs (even with a possibly empty list)
        globalFQs = new ArrayList<>(QueryUtils.getTaggedQueries(req, includedGlobalFQTags));
      }
      if (null != excludedGlobalFQTags) {
        globalFQs.removeAll(QueryUtils.getTaggedQueries(req, excludedGlobalFQTags));
      }

      return req.getSearcher().getProcessedFilter(null, globalFQs).filter;

    } catch (IOException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }
  }

  /**
   * @return set (possibly empty) of tags specified in the given local param
   * @see StrUtils#splitSmart
   * @see QueryUtils#getTaggedQueries
   * @see #localParams
   */
  private List<String> getLocalParamTags(final String param) {
    final String[] strVals = localParams.getParams(param);
    if (null == strVals) {
      return Collections.emptyList();
    }
    final List<String> tags = new ArrayList<>(strVals.length * 2);
    for (String val : strVals) {
      // This ensures parity w/how QParser constructor builds tagMap,
      // and that empty strings will make it into our List (for "include nothing")
      if (0 < val.indexOf(',')) {
        tags.addAll(StrUtils.splitSmart(val, ','));
      } else {
        tags.add(val);
      }
    }
    return tags;
  }
}
