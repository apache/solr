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
package org.apache.solr.client.solrj.response;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.solr.client.solrj.beans.DocumentObjectBinder;
import org.apache.solr.client.solrj.response.json.NestableJsonFacet;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.CursorMarkParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;

/**
 * @since solr 1.3
 */
@SuppressWarnings("unchecked")
public class QueryResponse extends SolrResponseBase {
  // Direct pointers to known types
  private NamedList<Object> _header = null;
  private SolrDocumentList _results = null;

  @SuppressWarnings({"rawtypes"})
  private NamedList<ArrayList> _sortvalues = null;

  private NamedList<Object> _facetInfo = null;
  private NamedList<Object> _debugInfo = null;
  private NamedList<Object> _highlightingInfo = null;
  private NamedList<Object> _spellInfo = null;
  private List<NamedList<Object>> _clusterInfo = null;
  private NamedList<Object> _jsonFacetingInfo = null;
  private NamedList<NamedList<Object>> _suggestInfo = null;
  private NamedList<Object> _statsInfo = null;
  private NamedList<NamedList<Object>> _termsInfo = null;
  private NamedList<SolrDocumentList> _moreLikeThisInfo = null;
  private NamedList<String> _tasksInfo = null;
  private String _cancellationInfo = null;
  private String _taskStatusCheckInfo = null;
  private String _cursorMarkNext = null;

  // Grouping response
  private NamedList<Object> _groupedInfo = null;
  private GroupResponse _groupResponse = null;

  private NamedList<Object> _expandedInfo = null;
  private Map<String, SolrDocumentList> _expandedResults = null;

  // Facet stuff
  private Map<String, Integer> _facetQuery = null;
  private List<FacetField> _facetFields = null;
  private List<FacetField> _limitingFacets = null;
  private List<FacetField> _facetDates = null;

  @SuppressWarnings({"rawtypes"})
  private List<RangeFacet> _facetRanges = null;

  private NamedList<List<PivotField>> _facetPivot = null;
  private List<IntervalFacet> _intervalFacets = null;

  // Highlight Info
  private Map<String, Map<String, List<String>>> _highlighting = null;

  // SpellCheck Response
  private SpellCheckResponse _spellResponse = null;

  // Clustering Response
  private ClusteringResponse _clusterResponse = null;

  // Json Faceting Response
  private NestableJsonFacet _jsonFacetingResponse = null;

  // Suggester Response
  private SuggesterResponse _suggestResponse = null;

  // Terms Response
  private TermsResponse _termsResponse = null;

  // Field stats Response
  private Map<String, FieldStatsInfo> _fieldStatsInfo = null;

  // Debug Info
  private Map<String, Object> _debugMap = null;
  private Map<String, Object> _explainMap = null;

  public QueryResponse() {}

  public QueryResponse(NamedList<Object> res) {
    this.setResponse(res);
  }

  @Override
  @SuppressWarnings({"rawtypes"})
  public void setResponse(NamedList<Object> res) {
    super.setResponse(res);

    // Look for known things
    for (Map.Entry<String, Object> resEntry : res) {
      String n = resEntry.getKey();
      Object val = resEntry.getValue();

      switch (n) {
        case "responseHeader" -> _header = (NamedList<Object>) val;
        case "response" -> _results = (SolrDocumentList) val;
        case "sort_values" -> _sortvalues = (NamedList<ArrayList>) val;
        case "facet_counts" -> _facetInfo = (NamedList<Object>) val;

          // extractFacetInfo inspects _results, so defer calling it
          // in case it hasn't been populated yet.
        case "debug" -> {
          _debugInfo = (NamedList<Object>) val;
          extractDebugInfo(_debugInfo);
        }
        case "grouped" -> {
          _groupedInfo = (NamedList<Object>) val;
          extractGroupedInfo(_groupedInfo);
        }
        case "expanded" -> {
          NamedList map = (NamedList) val;
          _expandedResults = map.asMap(1);
        }
        case "highlighting" -> {
          _highlightingInfo = (NamedList<Object>) val;
          extractHighlightingInfo(_highlightingInfo);
        }
        case "spellcheck" -> {
          _spellInfo = (NamedList<Object>) val;
          extractSpellCheckInfo(_spellInfo);
        }
        case "clusters" -> {
          _clusterInfo = (ArrayList<NamedList<Object>>) val;
          extractClusteringInfo(_clusterInfo);
        }
        case "facets" -> _jsonFacetingInfo = (NamedList<Object>) val;

          // Don't call extractJsonFacetingInfo(_jsonFacetingInfo) here in an effort to do it lazily
        case "suggest" -> {
          _suggestInfo = (NamedList<NamedList<Object>>) val;
          extractSuggesterInfo(_suggestInfo);
        }
        case "stats" -> {
          _statsInfo = (NamedList<Object>) val;
          extractStatsInfo(_statsInfo);
        }
        case "terms" -> {
          _termsInfo = (NamedList<NamedList<Object>>) val;
          extractTermsInfo(_termsInfo);
        }
        case "moreLikeThis" -> _moreLikeThisInfo = (NamedList<SolrDocumentList>) val;
        case "taskList" -> _tasksInfo = (NamedList<String>) val;
        case "cancellationResult" -> _cancellationInfo = (String) val;
        case "taskResult" -> _taskStatusCheckInfo = (String) val;
        case CursorMarkParams.CURSOR_MARK_NEXT -> _cursorMarkNext = (String) val;
      }
    }
    if (_facetInfo != null) extractFacetInfo(_facetInfo);
  }

  private void extractSpellCheckInfo(NamedList<Object> spellInfo) {
    _spellResponse = new SpellCheckResponse(spellInfo);
  }

  private void extractClusteringInfo(List<NamedList<Object>> clusterInfo) {
    _clusterResponse = new ClusteringResponse(clusterInfo);
  }

  private void extractJsonFacetingInfo(NamedList<Object> facetInfo) {
    _jsonFacetingResponse = new NestableJsonFacet(facetInfo);
  }

  private void extractSuggesterInfo(NamedList<NamedList<Object>> suggestInfo) {
    _suggestResponse = new SuggesterResponse(suggestInfo);
  }

  private void extractTermsInfo(NamedList<NamedList<Object>> termsInfo) {
    _termsResponse = new TermsResponse(termsInfo);
  }

  private void extractStatsInfo(NamedList<Object> info) {
    _fieldStatsInfo = extractFieldStatsInfo(info);
  }

  private Map<String, FieldStatsInfo> extractFieldStatsInfo(NamedList<Object> info) {
    if (info != null) {
      Map<String, FieldStatsInfo> fieldStatsInfoMap = new TreeMap<>();
      NamedList<NamedList<Object>> ff = (NamedList<NamedList<Object>>) info.get("stats_fields");
      if (ff != null) {
        for (Map.Entry<String, NamedList<Object>> entry : ff) {
          NamedList<Object> v = entry.getValue();
          if (v != null) {
            fieldStatsInfoMap.put(entry.getKey(), new FieldStatsInfo(v, entry.getKey()));
          }
        }
      }
      return fieldStatsInfoMap;
    }
    return null;
  }

  private void extractDebugInfo(NamedList<Object> debug) {
    _debugMap = new LinkedHashMap<>(); // keep the order
    for (Map.Entry<String, Object> info : debug) {
      _debugMap.put(info.getKey(), info.getValue());
    }

    // Parse out interesting bits from the debug info
    _explainMap = new HashMap<>();
    NamedList<Object> explain = (NamedList<Object>) _debugMap.get("explain");
    if (explain != null) {
      for (Map.Entry<String, Object> info : explain) {
        String key = info.getKey();
        _explainMap.put(key, info.getValue());
      }
    }
  }

  private void extractGroupedInfo(NamedList<Object> info) {
    if (info != null) {
      _groupResponse = new GroupResponse();
      for (Map.Entry<String, Object> infoEntry : info) {
        String fieldName = infoEntry.getKey();
        SimpleOrderedMap<Object> simpleOrderedMap = (SimpleOrderedMap<Object>) infoEntry.getValue();

        Object oMatches = simpleOrderedMap.get("matches");
        Object oNGroups = simpleOrderedMap.get("ngroups");
        Object oGroups = simpleOrderedMap.get("groups");
        Object queryCommand = simpleOrderedMap.get("doclist");
        if (oMatches == null) {
          continue;
        }

        if (oGroups != null) {
          Integer iMatches = (Integer) oMatches;
          ArrayList<Object> groupsArr = (ArrayList<Object>) oGroups;
          GroupCommand groupedCommand;
          if (oNGroups != null) {
            Integer iNGroups = (Integer) oNGroups;
            groupedCommand = new GroupCommand(fieldName, iMatches, iNGroups);
          } else {
            groupedCommand = new GroupCommand(fieldName, iMatches);
          }

          for (Object oGrp : groupsArr) {
            @SuppressWarnings({"rawtypes"})
            SimpleOrderedMap grpMap = (SimpleOrderedMap) oGrp;
            Object sGroupValue = grpMap.get("groupValue");
            SolrDocumentList doclist = (SolrDocumentList) grpMap.get("doclist");
            Group group = new Group(sGroupValue != null ? sGroupValue.toString() : null, doclist);
            groupedCommand.add(group);
          }

          _groupResponse.add(groupedCommand);
        } else if (queryCommand != null) {
          Integer iMatches = (Integer) oMatches;
          GroupCommand groupCommand;
          if (oNGroups != null) {
            Integer iNGroups = (Integer) oNGroups;
            groupCommand = new GroupCommand(fieldName, iMatches, iNGroups);
          } else {
            groupCommand = new GroupCommand(fieldName, iMatches);
          }
          SolrDocumentList docList = (SolrDocumentList) queryCommand;
          groupCommand.add(new Group(fieldName, docList));
          _groupResponse.add(groupCommand);
        }
      }
    }
  }

  private void extractHighlightingInfo(NamedList<Object> info) {
    _highlighting = new HashMap<>();
    for (Map.Entry<String, Object> doc : info) {
      Map<String, List<String>> fieldMap = new HashMap<>();
      _highlighting.put(doc.getKey(), fieldMap);

      NamedList<List<String>> fnl = (NamedList<List<String>>) doc.getValue();
      for (Map.Entry<String, List<String>> field : fnl) {
        fieldMap.put(field.getKey(), field.getValue());
      }
    }
  }

  @SuppressWarnings({"rawtypes"})
  private void extractFacetInfo(NamedList<Object> info) {
    // Parse the queries
    _facetQuery = new LinkedHashMap<>();
    NamedList<Integer> fq = (NamedList<Integer>) info.get("facet_queries");
    if (fq != null) {
      for (Map.Entry<String, Integer> entry : fq) {
        _facetQuery.put(entry.getKey(), entry.getValue());
      }
    }

    // Parse the facet info into fields
    // TODO?? The list could be <int> or <long>?  If always <long> then we can switch to <Long>
    NamedList<NamedList<Number>> ff = (NamedList<NamedList<Number>>) info.get("facet_fields");
    if (ff != null) {
      _facetFields = new ArrayList<>(ff.size());
      _limitingFacets = new ArrayList<>(ff.size());

      long minsize = _results == null ? Long.MAX_VALUE : _results.getNumFound();
      for (Map.Entry<String, NamedList<Number>> facet : ff) {
        FacetField f = new FacetField(facet.getKey());
        for (Map.Entry<String, Number> entry : facet.getValue()) {
          f.add(entry.getKey(), entry.getValue().longValue());
        }

        _facetFields.add(f);
        FacetField nl = f.getLimitingFields(minsize);
        if (nl.getValueCount() > 0) {
          _limitingFacets.add(nl);
        }
      }
    }

    // Parse range facets
    NamedList<NamedList<Object>> rf = (NamedList<NamedList<Object>>) info.get("facet_ranges");
    if (rf != null) {
      _facetRanges = extractRangeFacets(rf);
    }

    // Parse pivot facets
    var pf = (NamedList<List<NamedList>>) info.get("facet_pivot");
    if (pf != null) {
      _facetPivot = new NamedList<>();
      pf.forEach((name, pivot) -> _facetPivot.add(name, readPivots(pivot)));
    }

    // Parse interval facets
    NamedList<NamedList<Object>> intervalsNL =
        (NamedList<NamedList<Object>>) info.get("facet_intervals");
    if (intervalsNL != null) {
      _intervalFacets = new ArrayList<>(intervalsNL.size());
      for (Map.Entry<String, NamedList<Object>> intervalField : intervalsNL) {
        String field = intervalField.getKey();
        List<IntervalFacet.Count> counts =
            new ArrayList<IntervalFacet.Count>(intervalField.getValue().size());
        for (Map.Entry<String, Object> interval : intervalField.getValue()) {
          counts.add(new IntervalFacet.Count(interval.getKey(), (Integer) interval.getValue()));
        }
        _intervalFacets.add(new IntervalFacet(field, counts));
      }
    }
  }

  @SuppressWarnings({"rawtypes"})
  private List<RangeFacet> extractRangeFacets(NamedList<NamedList<Object>> rf) {
    List<RangeFacet> facetRanges = new ArrayList<>(rf.size());

    for (Map.Entry<String, NamedList<Object>> facet : rf) {
      NamedList<Object> values = facet.getValue();
      Object rawGap = values.get("gap");

      RangeFacet rangeFacet;
      if (rawGap instanceof Number gap) {
        Number start = (Number) values.get("start");
        Number end = (Number) values.get("end");

        Number before = (Number) values.get("before");
        Number after = (Number) values.get("after");
        Number between = (Number) values.get("between");

        rangeFacet =
            new RangeFacet.Numeric(facet.getKey(), start, end, gap, before, after, between);
      } else if (rawGap instanceof String gap && values.get("start") instanceof Date start) {
        Date end = (Date) values.get("end");

        Number before = (Number) values.get("before");
        Number after = (Number) values.get("after");
        Number between = (Number) values.get("between");

        rangeFacet = new RangeFacet.Date(facet.getKey(), start, end, gap, before, after, between);
      } else {
        String gap = (String) rawGap;
        String start = (String) values.get("start");
        String end = (String) values.get("end");

        Number before = (Number) values.get("before");
        Number after = (Number) values.get("after");
        Number between = (Number) values.get("between");

        rangeFacet =
            new RangeFacet.Currency(facet.getKey(), start, end, gap, before, after, between);
      }

      NamedList<Integer> counts = (NamedList<Integer>) values.get("counts");
      for (Map.Entry<String, Integer> entry : counts) {
        rangeFacet.addCount(entry.getKey(), entry.getValue());
      }

      facetRanges.add(rangeFacet);
    }
    return facetRanges;
  }

  @SuppressWarnings({"rawtypes"})
  protected List<PivotField> readPivots(List<NamedList> list) {
    ArrayList<PivotField> values = new ArrayList<>(list.size());
    for (NamedList nl : list) {
      // Required fields:
      String field = null;
      Object value = null;
      int count = -1;
      // Optional fields:
      List<PivotField> subPivots = null;
      Map<String, FieldStatsInfo> fieldStatsInfos = null;
      Map<String, Integer> queryCounts = null;
      List<RangeFacet> ranges = null;

      // Process all entries with a single approach
      for (Map.Entry<String, ?> entry : (Iterable<Map.Entry<String, ?>>) nl) {
        final String key = entry.getKey();
        final Object val = entry.getValue();

        switch (key) {
          case "field" -> field = (String) val;
          case "value" -> value = val;
          case "count" -> count = ((Integer) val).intValue();
          case "pivot" -> {
            assert null != val : "Server sent back 'null' for sub pivots?";
            assert val instanceof List : "Server sent non-List for sub pivots?";
            subPivots = readPivots((List<NamedList>) val);
          }
          case "stats" -> {
            assert null != val : "Server sent back 'null' for stats?";
            assert val instanceof NamedList : "Server sent non-NamedList for stats?";
            fieldStatsInfos = extractFieldStatsInfo((NamedList<Object>) val);
          }
          case "queries" -> {
            // Parse the queries
            queryCounts = new LinkedHashMap<>();
            NamedList<Integer> fq = (NamedList<Integer>) val;
            if (fq != null) {
              for (Map.Entry<String, Integer> e : fq) {
                queryCounts.put(e.getKey(), e.getValue());
              }
            }
          }
          case "ranges" -> ranges = extractRangeFacets((NamedList<NamedList<Object>>) val);
          default -> throw new RuntimeException("unknown key in pivot: " + key + " [" + val + "]");
        }
      }

      // Create and add the PivotField with all gathered information
      values.add(
          new PivotField(field, value, count, subPivots, fieldStatsInfos, queryCounts, ranges));
    }
    return values;
  }

  // ------------------------------------------------------
  // ------------------------------------------------------

  /** Remove the field facet info */
  public void removeFacets() {
    _facetFields = new ArrayList<>();
  }

  // ------------------------------------------------------
  // ------------------------------------------------------

  public NamedList<Object> getHeader() {
    return _header;
  }

  public SolrDocumentList getResults() {
    return _results;
  }

  @SuppressWarnings({"rawtypes"})
  public NamedList<ArrayList> getSortValues() {
    return _sortvalues;
  }

  public Map<String, Object> getDebugMap() {
    return _debugMap;
  }

  public Map<String, Object> getExplainMap() {
    return _explainMap;
  }

  public Map<String, Integer> getFacetQuery() {
    return _facetQuery;
  }

  /**
   * @return map with each group value as key and the expanded documents that belong to the group as
   *     value. There is no guarantee on the order of the keys obtained via an iterator.
   */
  public Map<String, SolrDocumentList> getExpandedResults() {
    return this._expandedResults;
  }

  /**
   * Returns the {@link GroupResponse} containing the group commands. A group command can be the
   * result of one of the following parameters:
   *
   * <ul>
   *   <li>group.field
   *   <li>group.func
   *   <li>group.query
   * </ul>
   *
   * @return the {@link GroupResponse} containing the group commands
   */
  public GroupResponse getGroupResponse() {
    return _groupResponse;
  }

  public Map<String, Map<String, List<String>>> getHighlighting() {
    return _highlighting;
  }

  public SpellCheckResponse getSpellCheckResponse() {
    return _spellResponse;
  }

  public ClusteringResponse getClusteringResponse() {
    return _clusterResponse;
  }

  public NestableJsonFacet getJsonFacetingResponse() {
    if (_jsonFacetingInfo != null && _jsonFacetingResponse == null)
      extractJsonFacetingInfo(_jsonFacetingInfo);
    return _jsonFacetingResponse;
  }

  public SuggesterResponse getSuggesterResponse() {
    return _suggestResponse;
  }

  public TermsResponse getTermsResponse() {
    return _termsResponse;
  }

  public NamedList<SolrDocumentList> getMoreLikeThis() {
    return _moreLikeThisInfo;
  }

  public NamedList<String> getTasksInfo() {
    return _tasksInfo;
  }

  public String getCancellationInfo() {
    return _cancellationInfo;
  }

  public String getTaskStatusCheckInfo() {
    return _taskStatusCheckInfo;
  }

  /** See also: {@link #getLimitingFacets()} */
  public List<FacetField> getFacetFields() {
    return _facetFields;
  }

  public List<FacetField> getFacetDates() {
    return _facetDates;
  }

  @SuppressWarnings({"rawtypes"})
  public List<RangeFacet> getFacetRanges() {
    return _facetRanges;
  }

  public NamedList<List<PivotField>> getFacetPivot() {
    return _facetPivot;
  }

  public List<IntervalFacet> getIntervalFacets() {
    return _intervalFacets;
  }

  /**
   * get
   *
   * @param name the name of the
   * @return the FacetField by name or null if it does not exist
   */
  public FacetField getFacetField(String name) {
    if (_facetFields == null) return null;
    for (FacetField f : _facetFields) {
      if (f.getName().equals(name)) return f;
    }
    return null;
  }

  public FacetField getFacetDate(String name) {
    if (_facetDates == null) return null;
    for (FacetField f : _facetDates) if (f.getName().equals(name)) return f;
    return null;
  }

  /**
   * @return a list of FacetFields where the count is less than the #getResults() {@link
   *     SolrDocumentList#getNumFound()}
   *     <p>If you want all results exactly as returned by solr, use: {@link #getFacetFields()}
   */
  public List<FacetField> getLimitingFacets() {
    return _limitingFacets;
  }

  public <T> List<T> getBeans(Class<T> type) {
    return DocumentObjectBinder.INSTANCE.getBeans(type, _results);
  }

  public Map<String, FieldStatsInfo> getFieldStatsInfo() {
    return _fieldStatsInfo;
  }

  public String getNextCursorMark() {
    return _cursorMarkNext;
  }
}
