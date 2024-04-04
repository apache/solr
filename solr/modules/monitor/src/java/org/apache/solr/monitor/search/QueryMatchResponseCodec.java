/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.solr.monitor.search;

import static org.apache.solr.monitor.MonitorConstants.MONITOR_DOCUMENTS_KEY;
import static org.apache.solr.monitor.MonitorConstants.MONITOR_DOCUMENT_KEY;
import static org.apache.solr.monitor.MonitorConstants.MONITOR_OUTPUT_KEY;
import static org.apache.solr.monitor.MonitorConstants.MONITOR_QUERIES_KEY;
import static org.apache.solr.monitor.MonitorConstants.MONITOR_QUERIES_RUN;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.lucene.monitor.HighlightsMatch;
import org.apache.lucene.monitor.MonitorFields;
import org.apache.lucene.monitor.MultiMatchingQueries;
import org.apache.lucene.monitor.QueryMatch;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.monitor.MonitorConstants;

@SuppressWarnings("unchecked")
class QueryMatchResponseCodec {

  static Map<String, Object> mergeResponses(
      List<NamedList<Object>> responses, QueryMatchType type) {
    Map<Integer, List<QueryMatchWrapper>> wrappedMerged = new HashMap<>();
    int queriesRun = 0;
    for (var response : responses) {
      Object smq = response.get(MONITOR_OUTPUT_KEY);
      if (!(smq instanceof Map)) {
        throw new SolrException(
            SolrException.ErrorCode.BAD_REQUEST, MONITOR_OUTPUT_KEY + " must be a map");
      }
      Map<String, Object> singleMonitorOutput = (Map<String, Object>) smq;
      decodeToWrappers(singleMonitorOutput, wrappedMerged, type);
      var delta = singleMonitorOutput.get(MONITOR_QUERIES_RUN);
      if (!(delta instanceof Integer)) {
        throw new SolrException(
            SolrException.ErrorCode.BAD_REQUEST, MONITOR_QUERIES_RUN + " must be an integer");
      }
      queriesRun += (Integer) delta;
    }

    for (var mergedValues : wrappedMerged.values()) {
      Collections.sort(mergedValues);
    }
    Map<Integer, Object> finalDocOutputs =
        wrappedMerged.entrySet().stream()
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    entry -> encodeWrappers(entry.getValue().stream(), entry.getKey())));
    Map<String, Object> output = new HashMap<>();
    output.put(MONITOR_DOCUMENTS_KEY, finalDocOutputs);
    output.put(MONITOR_QUERIES_RUN, queriesRun);
    return output;
  }

  private static void decodeToWrappers(
      Object object, Map<Integer, List<QueryMatchWrapper>> output, QueryMatchType type) {
    if (!(object instanceof Map)) {
      throw new IllegalArgumentException("input object should be a map");
    }
    Map<String, Object> docLevelMQResponse = (Map<String, Object>) object;
    Object monitorDocumentsRaw = docLevelMQResponse.get(MONITOR_DOCUMENTS_KEY);
    if (!(monitorDocumentsRaw instanceof Map)) {
      throw new IllegalArgumentException(MONITOR_DOCUMENTS_KEY + " should be a map");
    }
    var monitorDocuments = (Map<Integer, Object>) monitorDocumentsRaw;
    for (var docEntryRaw : monitorDocuments.values()) {
      if (!(docEntryRaw instanceof Map)) {
        throw new IllegalArgumentException(MONITOR_DOCUMENTS_KEY + " should contain maps");
      }
      var docEntry = (Map<String, Object>) docEntryRaw;
      Object docRaw = docEntry.get(MONITOR_DOCUMENT_KEY);
      if (!(docRaw instanceof Integer)) {
        throw new IllegalStateException(MONITOR_DOCUMENT_KEY + " needs to be an int");
      }
      Object monitorQueriesRaw = docEntry.get(MONITOR_QUERIES_KEY);
      if (!(monitorQueriesRaw instanceof List)) {
        throw new IllegalStateException(MONITOR_QUERIES_KEY + " needs to be an list");
      }
      int doc = (Integer) docRaw;
      List<QueryMatchWrapper> wrappers = output.computeIfAbsent(doc, i -> new ArrayList<>());
      List<Object> serializableFormMQs = (List<Object>) monitorQueriesRaw;
      if (type == QueryMatchType.SIMPLE) {
        serializableFormMQs.forEach(
            serializableForm ->
                wrappers.add(SimpleQueryMatchWrapper.fromSerializableForm(serializableForm)));
      } else if (type == QueryMatchType.HIGHLIGHTS) {
        serializableFormMQs.forEach(
            serializableForm ->
                wrappers.add(HighlightedQueryMatchWrapper.fromSerializableForm(serializableForm)));
      }
    }
  }

  static void simpleEncode(
      MultiMatchingQueries<QueryMatch> matchingQueries,
      Map<String, Object> output,
      int docBatchSize) {
    encodeMultiMatchingQuery(
        matchingQueries, output, docBatchSize, SimpleQueryMatchWrapper::fromQueryMatch);
  }

  static void highlightEncode(
      MultiMatchingQueries<HighlightsMatch> matchingQueries,
      Map<String, Object> output,
      int docBatchSize) {
    encodeMultiMatchingQuery(
        matchingQueries, output, docBatchSize, HighlightedQueryMatchWrapper::new);
  }

  private static <T extends QueryMatch> void encodeMultiMatchingQuery(
      MultiMatchingQueries<T> matchingQueries,
      Map<String, Object> output,
      int docBatchSize,
      Function<T, ? extends QueryMatchWrapper> wrappingFunction) {
    if (matchingQueries.getBatchSize() != docBatchSize) {
      throw new IllegalArgumentException("output size doesn't match document batch size");
    }
    Map<Integer, Object> docOutputs =
        IntStream.range(0, matchingQueries.getBatchSize())
            .mapToObj(
                doc ->
                    encodeWrappers(
                        matchingQueries.getMatches(doc).stream().map(wrappingFunction), doc))
            .collect(
                Collectors.toMap(
                    wrapper -> (Integer) wrapper.get(MONITOR_DOCUMENT_KEY), Function.identity()));
    output.put(MONITOR_DOCUMENTS_KEY, docOutputs);
    output.put(MONITOR_QUERIES_RUN, matchingQueries.getQueriesRun());
  }

  private static Map<String, Object> encodeWrappers(Stream<QueryMatchWrapper> matches, int doc) {
    Map<String, Object> map = new LinkedHashMap<>();
    map.put(MONITOR_DOCUMENT_KEY, doc);
    map.put(
        MONITOR_QUERIES_KEY,
        matches.sorted().map(QueryMatchWrapper::toSerializableForm).collect(Collectors.toList()));
    return map;
  }

  private interface QueryMatchWrapper extends Comparable<QueryMatchWrapper> {

    Object toSerializableForm();

    BytesRef queryId();

    @Override
    default int compareTo(QueryMatchWrapper o) {
      if (getClass() != o.getClass()) {
        throw new IllegalArgumentException(
            getClass().getSimpleName()
                + " can only be compared to other instances of the same concrete type");
      }
      return queryId().compareTo(o.queryId());
    }
  }

  static void decodingError(Object obj, Class<? extends QueryMatchWrapper> wrapper) {
    throw new IllegalArgumentException("Cannot decode " + obj + " to " + wrapper.getSimpleName());
  }

  private static class SimpleQueryMatchWrapper implements QueryMatchWrapper {

    private final BytesRef queryId;

    private SimpleQueryMatchWrapper(BytesRef queryId) {
      this.queryId = queryId;
    }

    private static SimpleQueryMatchWrapper fromQueryMatch(QueryMatch queryMatch) {
      return new SimpleQueryMatchWrapper(new BytesRef(queryMatch.getQueryId()));
    }

    private static SimpleQueryMatchWrapper fromSerializableForm(Object serializableForm) {
      if (!(serializableForm instanceof String)) {
        decodingError(serializableForm, SimpleQueryMatchWrapper.class);
      }
      return new SimpleQueryMatchWrapper(new BytesRef((String) serializableForm));
    }

    @Override
    public Object toSerializableForm() {
      return queryId.utf8ToString();
    }

    @Override
    public BytesRef queryId() {
      return queryId;
    }
  }

  private static class HighlightedQueryMatchWrapper implements QueryMatchWrapper {

    private final BytesRef queryId;
    private final Map<Object, Object> hits;

    private HighlightedQueryMatchWrapper(HighlightsMatch highlightsMatch) {
      this(
          new BytesRef(highlightsMatch.getQueryId()),
          highlightsMatch.getHits().entrySet().stream()
              .collect(
                  Collectors.toMap(
                      Map.Entry::getKey,
                      entry ->
                          entry.getValue().stream()
                              .map(HighlightedQueryMatchWrapper::hitMap)
                              .collect(Collectors.toList()))));
    }

    private static Map<Object, Object> hitMap(HighlightsMatch.Hit hit) {
      Map<Object, Object> map = new LinkedHashMap<>();
      map.put("startPosition", hit.startPosition);
      map.put("endPosition", hit.endPosition);
      map.put("startOffset", hit.startOffset);
      map.put("endOffset", hit.endOffset);
      return map;
    }

    private HighlightedQueryMatchWrapper(BytesRef queryId, Map<Object, Object> hits) {
      this.hits = hits;
      this.queryId = queryId;
    }

    @Override
    public Object toSerializableForm() {
      Map<Object, Object> map = new LinkedHashMap<>();
      map.put(MonitorFields.QUERY_ID, queryId.utf8ToString());
      map.put(MonitorConstants.HITS_KEY, hits);
      return map;
    }

    private static HighlightedQueryMatchWrapper fromSerializableForm(Object serializableForm) {
      if (!(serializableForm instanceof Map)) {
        decodingError(serializableForm, HighlightedQueryMatchWrapper.class);
      }
      Object queryId = ((Map<?, ?>) serializableForm).get(MonitorFields.QUERY_ID);
      Object hits = ((Map<?, ?>) serializableForm).get(MonitorConstants.HITS_KEY);
      if (!(queryId instanceof String) || !(hits instanceof Map<?, ?>)) {
        decodingError(serializableForm, HighlightedQueryMatchWrapper.class);
      }
      return new HighlightedQueryMatchWrapper(
          new BytesRef((String) queryId), (Map<Object, Object>) hits);
    }

    @Override
    public BytesRef queryId() {
      return queryId;
    }
  }
}
