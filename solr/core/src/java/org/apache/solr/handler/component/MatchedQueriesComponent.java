package org.apache.solr.handler.component;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Matches;
import org.apache.lucene.search.NamedMatches;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.search.DocIterator;
import org.apache.solr.search.DocList;
import org.apache.solr.search.SolrIndexSearcher;

/**
 * Search component that enriches the response with named-match information
 * for each document in the top-N hits.
 *
 * Activation:
 *   Add {@code matched_queries=true} (or {@code mq=true}) to the request.
 *
 * Output:
 *   - per-doc: each hit gets a "matched_queries": ["name1","name2"] field
 *   - response section: "matched_queries_summary": {
 *         "name1": {"count": 5, "docIds": ["id1","id2"]},
 *         "name2": {"count": 2, "docIds": ["id3"]}
 *     }
 *
 * Implementation:
 *   We use the {@link Weight#matches(LeafReaderContext, int)} API which performs
 *   a separate, post-search pass over each requested document. {@link NamedMatches}
 *   become identifiable through {@link NamedMatches#findNamedMatches(Matches)} on the returned Matches tree.
 *   ScoreMode.COMPLETE_NO_SCORES is used for the matches Weight because matching
 *   does not need scoring and this lets Lucene skip score computation entirely
 *   for this pass.
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
        // schema's unique key field — used to populate docIds in the summary
        String idField = rb.req.getCore().getLatestSchema().getUniqueKeyField().getName();

        // Build a Weight for matching only (no scoring needed)
        Query rewritten = searcher.rewrite(query);
        Weight matchesWeight = searcher.createWeight(rewritten,
                ScoreMode.COMPLETE_NO_SCORES, 1.0f);

        // Collect: per global doc id → ordered set of names
        Map<Integer, Set<String>> perDocNames = new LinkedHashMap<>();
        // Collect: per name → list of global doc ids (preserves document order)
        Map<String, List<Integer>> perNameDocs = new LinkedHashMap<>();
        // Cache unique-key values: each matching doc's stored id field is read exactly once here
        // and reused in both output loops below, avoiding redundant stored-field access.
        Map<Integer, String> idCache = new LinkedHashMap<>();

        List<LeafReaderContext> leaves = searcher.getTopReaderContext().leaves();

        DocIterator it = docList.iterator();
        while (it.hasNext()) {
            int globalDoc = it.nextDoc();

            LeafReaderContext leaf = leafFor(leaves, globalDoc);
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
            idCache.put(globalDoc, readUniqueKeyValue(searcher, idField, globalDoc));
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

        // Summary: name → {count, docIds}
        SimpleOrderedMap<Object> summary = new SimpleOrderedMap<>();
        for (Map.Entry<String, List<Integer>> e : perNameDocs.entrySet()) {
            List<String> ids = new ArrayList<>(e.getValue().size());
            for (Integer luceneId : e.getValue()) {
                ids.add(idCache.get(luceneId));
            }
            SimpleOrderedMap<Object> entry = new SimpleOrderedMap<>();
            entry.add("count", ids.size());
            entry.add("docIds", ids);
            summary.add(e.getKey(), entry);
        }

        NamedList<Object> response = rb.rsp.getValues();
        response.add("matched_queries_per_hit", perHit);
        response.add("matched_queries_summary", summary);
    }

    private LeafReaderContext leafFor(List<LeafReaderContext> leaves, int globalDoc) {
        // Standard binary search for the leaf owning a global docId
        int lo = 0, hi = leaves.size() - 1;
        while (lo <= hi) {
            int mid = (lo + hi) >>> 1;
            LeafReaderContext c = leaves.get(mid);
            int max = c.docBase + c.reader().maxDoc();
            if (globalDoc < c.docBase) {
                hi = mid - 1;
            } else if (globalDoc >= max) {
                lo = mid + 1;
            } else {
                return c;
            }
        }
        throw new IllegalStateException("No leaf for global doc " + globalDoc);
    }

    private String readUniqueKeyValue(IndexSearcher searcher, String idField, int globalDoc)
            throws IOException {
        var doc = searcher.storedFields().document(globalDoc, Set.of(idField));
        return doc.get(idField);
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
