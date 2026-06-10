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
package org.apache.solr.handler.component;

import com.carrotsearch.hppc.IntObjectHashMap;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.search.Matches;
import org.apache.lucene.search.NamedMatches;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.search.DocIterator;
import org.apache.solr.search.DocList;
import org.apache.solr.search.SolrDocumentFetcher;
import org.apache.solr.search.SolrIndexSearcher;

/**
 * Search component that enriches the response with named-match information for each document in the
 * top-N hits.
 *
 * <p>Activation: Add {@code matched_queries=true} (or {@code mq=true}) to the request.
 *
 * <p>Output:
 *
 * <ul>
 *   <li>{@code matched_queries_per_hit}: map of unique-key value → list of names that matched that
 *       document. Documents that matched no named query are absent.
 *   <li>{@code matched_queries_summary}: map of name → ordered list of unique-key values of
 *       documents it matched.
 * </ul>
 *
 * <p>Implementation: We use the {@link Weight#matches(LeafReaderContext, int)} API which performs a
 * separate, post-search pass over each requested document. {@link NamedMatches} become identifiable
 * through {@link NamedMatches#findNamedMatches(Matches)} on the returned Matches tree. {@link
 * org.apache.lucene.search.ScoreMode#COMPLETE_NO_SCORES} is used for the matches Weight because
 * matching does not need scoring and this lets Lucene skip score computation entirely for this
 * pass.
 */
public class MatchedQueriesComponent extends SearchComponent {

  public static final String COMPONENT_NAME = "matched_queries";
  public static final String PARAM_ENABLE = "matched_queries";
  public static final String PARAM_ENABLE_SHORT = "mq";

  @Override
  public void prepare(ResponseBuilder rb) {
    // nothing to prepare
  }

  @Override
  public void process(ResponseBuilder rb) throws IOException {
    if (!isEnabled(rb)) {
      return;
    }

    DocList docList = rb.getResults() == null ? null : rb.getResults().docList;
    if (docList == null || docList.size() == 0) {
      return;
    }

    Query query = rb.getQuery();
    if (query == null) {
      return;
    }

    SolrIndexSearcher searcher = rb.req.getSearcher();
    String idField = searcher.getSchema().getUniqueKeyField().getName();

    // Build a Weight for matching only (no scoring needed)
    Query rewritten = searcher.rewrite(query);
    Weight matchesWeight = searcher.createWeight(rewritten, ScoreMode.COMPLETE_NO_SCORES, 1.0f);

    // Collect: per global doc id → ordered set of names
    Map<Integer, Set<String>> perDocNames = new LinkedHashMap<>();
    // Collect: per name → list of global doc ids (preserves document order)
    Map<String, List<Integer>> perNameDocs = new LinkedHashMap<>();
    IntObjectHashMap<String> idCache = new IntObjectHashMap<>(docList.size());

    List<LeafReaderContext> leaves = searcher.getTopReaderContext().leaves();
    SolrDocumentFetcher docFetcher = searcher.getDocFetcher();

    DocIterator it = docList.iterator();
    while (it.hasNext()) {
      int globalDoc = it.nextDoc();

      LeafReaderContext leaf = leaves.get(ReaderUtil.subIndex(globalDoc, leaves));
      int leafDoc = globalDoc - leaf.docBase;

      Matches matches = matchesWeight.matches(leaf, leafDoc);
      if (matches == null) {
        continue;
      }
      List<NamedMatches> named = NamedMatches.findNamedMatches(matches);
      if (named.isEmpty()) {
        continue;
      }

      Set<String> names = new LinkedHashSet<>();
      for (NamedMatches nm : named) {
        names.add(nm.getName());
      }
      perDocNames.put(globalDoc, names);
      idCache.put(globalDoc, readUniqueKeyValue(docFetcher, idField, globalDoc));
      for (String name : names) {
        perNameDocs.computeIfAbsent(name, k -> new ArrayList<>()).add(globalDoc);
      }
    }

    if (perDocNames.isEmpty()) {
      return;
    }

    // Annotate each hit: we add a parallel structure (docId → matched names)
    // because mutating SolrDocument inline requires DocTransformer plumbing.
    // The hits-keyed map is keyed by the document's unique-key value (string)
    // for client convenience.
    SimpleOrderedMap<Object> perHit = new SimpleOrderedMap<>();
    for (Map.Entry<Integer, Set<String>> e : perDocNames.entrySet()) {
      perHit.add(idCache.get(e.getKey()), new ArrayList<>(e.getValue()));
    }

    // Summary: name → [id1, id2, ...]
    SimpleOrderedMap<Object> summary = new SimpleOrderedMap<>();
    for (Map.Entry<String, List<Integer>> e : perNameDocs.entrySet()) {
      List<String> ids = new ArrayList<>(e.getValue().size());
      for (Integer luceneId : e.getValue()) {
        ids.add(idCache.get(luceneId));
      }
      summary.add(e.getKey(), ids);
    }

    NamedList<Object> response = rb.rsp.getValues();
    response.add("matched_queries_per_hit", perHit);
    response.add("matched_queries_summary", summary);
  }

  private String readUniqueKeyValue(SolrDocumentFetcher docFetcher, String idField, int globalDoc)
      throws IOException {
    return docFetcher.doc(globalDoc, Set.of(idField)).get(idField);
  }

  private boolean isEnabled(ResponseBuilder rb) {
    var p = rb.req.getParams();
    return p.getBool(PARAM_ENABLE, false) || p.getBool(PARAM_ENABLE_SHORT, false);
  }

  @Override
  public String getDescription() {
    return "Adds NamedMatches information to query response";
  }
}
