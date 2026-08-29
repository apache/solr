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
package org.apache.solr.search.join.auxindexjoin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.solr.search.join.auxindexjoin.JoinIndexUtils.JoinColumnModel;

/**
 * Sibling of {@code AIJoinColumnWriter} writing the same pair columns through the plain {@link
 * Document} / {@link IndexWriter#addDocuments} API instead of {@code
 * org.apache.lucene.document.column}. The whole batch is built as one in-memory {@link List} of
 * {@code batchNumDocs} documents and handed to a single {@link IndexWriter#addDocuments} call: per
 * its block semantics that list is indexed atomically, with no flush allowed to land in the middle
 * of it, so -- exactly like {@code AIJoinColumnWriter}'s single {@code addBatch} -- the whole batch
 * is guaranteed to end up doc-for-doc (list index == doc id) in one sidecar segment, keeping doc
 * 0's edges and every from-doc id aligned the same way.
 */
final class JoinColumnDocWriter extends JoinColumWriter {

  JoinColumnDocWriter() {}

  @Override
  void writeJoinColumns(IndexWriter writer, Map<String, JoinColumnModel> mappings)
      throws IOException {
    int batchNumDocs = 0;
    for (Map.Entry<String, JoinColumnModel> entry : mappings.entrySet()) {
      batchNumDocs = Math.max(batchNumDocs, entry.getValue().maxDoc());
    }
    List<Document> docs = new ArrayList<>(batchNumDocs);
    for (int i = 0; i < batchNumDocs; i++) {
      docs.add(new Document());
    }
    for (Map.Entry<String, JoinColumnModel> entry : mappings.entrySet()) {
      addJoinColumns(docs, entry.getValue(), entry.getKey());
    }
    // a single block: IndexWriter guarantees no intermediate flush splits it across segments
    writer.addDocuments(docs);
    writer.commit();
  }

  /**
   * Adds one pair's fields to {@code docs}: the doc-map field resolving from-side doc ids to
   * to-side doc ids, spread across the batch's docs, and the edges companion fields, always added
   * to doc 0 even when the pair maps nothing, so a once-built pair is detectable in the join index
   * and never rebuilt.
   */
  private static void addJoinColumns( // TODO don't write minusones columns for tombstones!!
      List<Document> docs, JoinColumnModel mapping, String pairFieldName) throws IOException {
    addOrdMap(docs, JoinIndexUtils.TO_DOC_VAL_BY_FROM_DOCNUM + pairFieldName, mapping);
    addEdges(docs, JoinIndexUtils.FROM_EDGES_PREFIX + pairFieldName, mapping.edges().fromDocEdges());
    addEdges(docs, JoinIndexUtils.TO_EDGES_PREFIX + pairFieldName, mapping.edges().toDocEdges());
    addEdges(
        docs, JoinIndexUtils.TO_COUNT_PREFIX + pairFieldName, new int[] {mapping.edges().toCount()});
  }

  /**
   * Adds a pair's {min, max} (or count) values to doc 0, mirroring {@code AIJoinColumnWriter}'s
   * {@code edgesColumn}, which puts both values at doc 0 too.
   */
  private static void addEdges(List<Document> docs, String fieldName, int[] values) {
    Document doc0 = docs.get(0);
    for (int value : values) {
      doc0.add(new SortedNumericDocValuesField(fieldName, value));
    }
  }

  /**
   * Adds the doc-map field: batch-local doc number is the from-side doc id and the SORTED_NUMERIC
   * docvalue is the matching to-side doc id. From docs without a match get no value, hence the
   * field is sparse.
   */
  private static void addOrdMap(List<Document> docs, String fieldName, JoinColumnModel mapping)
      throws IOException {
    SortedNumericDocValues values = mapping.toDocByFromDoc();
    for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
      docs.get(doc).add(new SortedNumericDocValuesField(fieldName, values.nextValue()));
    }
  }
}
