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
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.solr.search.join.auxindexjoin.JoinIndexUtils.DocEdges;
import org.apache.solr.search.join.auxindexjoin.JoinIndexUtils.JoinColumnModel;

/**
 * Sibling of {@code AIJoinColumnWriter} writing the same pair columns through the plain {@link
 * Document} / {@link IndexWriter#addDocuments} API instead of {@code
 * org.apache.lucene.document.column}. The batch is handed over as a single {@code addDocuments}
 * call: per its block semantics it is indexed atomically, with no flush allowed to land in the
 * middle of it, so -- exactly like {@code AIJoinColumnWriter}'s single {@code addBatch} -- the
 * whole batch is guaranteed to end up doc-for-doc (batch position == doc id) in one sidecar
 * segment, keeping doc 0's edges and every from-doc id aligned the same way.
 *
 * <p><b>The batch is streamed, not materialized.</b> It spans the whole from-segment -- one
 * document per from-side doc id, since that id <em>is</em> the address a pair column is read at --
 * so building it as a {@code List<Document>} cost a {@link Document} (and its own field list) per
 * from-doc: on a 5M-doc from-segment, some 400MB of scaffolding, live for the entire {@code
 * addDocuments} call, to write a segment that lands on disk at about 1.3MB. That is what exhausted
 * a 2GB heap under load. {@link BatchDocuments} mints the documents one at a time instead, reusing
 * one {@link Document} and one field per pair column, so what the batch costs the heap no longer
 * depends on how long the from-segment is -- the sparse docvalues buffer the indexing chain fills
 * as it consumes them does, but that is a fraction of the same size.
 *
 * @lucene.experimental
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
    assert mappings.isEmpty() || batchNumDocs > 0
        : "a batch with columns to write needs a doc 0 to carry their edges: " + mappings.keySet();
    // a single block: IndexWriter guarantees no intermediate flush splits it across segments
    writer.addDocuments(new BatchDocuments(mappings, batchNumDocs));
    // seal the batch into a segment of its own, so the next batch starts again at doc 0 -- the
    // invariant every column depends on, since a sidecar doc number IS a from-doc id. This used
    // to be a commit(), which sealed the segment only incidentally and paid an fsync of every new
    // file plus a segments_N for it, on the query thread, inside AuxIndexManager's write lock.
    // flush() writes the segment and nothing else; durability and file reclamation are the
    // periodic commit's job.
    writer.flush();
  }

  /**
   * The batch as a lazy {@link Iterable}: {@code batchNumDocs} documents, each carrying whatever
   * the pair columns hold at that from-doc id, generated as {@link IndexWriter} asks for them.
   *
   * <p>Every document is the same {@link Document} instance, cleared and refilled. That is safe
   * because {@code addDocuments} consumes each document fully -- the indexing chain reads its
   * fields and buffers their values -- before pulling the next one, and it keeps no reference
   * afterwards. {@link #iterator()} builds its own cursors, so the batch can be iterated again if
   * {@code IndexWriter} ever needs to.
   */
  private static final class BatchDocuments implements Iterable<Document> {

    private final Map<String, JoinColumnModel> mappings;
    private final int numDocs;

    BatchDocuments(Map<String, JoinColumnModel> mappings, int numDocs) {
      this.mappings = mappings;
      this.numDocs = numDocs;
    }

    @Override
    public Iterator<Document> iterator() {
      List<PairColumn> columns = new ArrayList<>(mappings.size());
      for (Map.Entry<String, JoinColumnModel> entry : mappings.entrySet()) {
        columns.add(new PairColumn(entry.getKey(), entry.getValue()));
      }
      return new Iterator<>() {

        private final Document doc = new Document();
        private int nextDocNum = 0;

        @Override
        public boolean hasNext() {
          return nextDocNum < numDocs;
        }

        @Override
        public Document next() {
          if (!hasNext()) {
            throw new NoSuchElementException();
          }
          int docNum = nextDocNum++;
          doc.clear();
          for (PairColumn column : columns) {
            column.addTo(doc, docNum);
          }
          return doc;
        }
      };
    }
  }

  /**
   * One pair's contribution to the batch: the doc-map column walked in step with the batch, and the
   * edges companions that go to doc 0 -- written even when the pair maps nothing, so a once-built
   * pair is detectable in the join index and never rebuilt.
   */
  // TODO don't write minusones columns for tombstones!!
  private static final class PairColumn {

    private final String pairFieldName;
    private final DocEdges edges;
    private final SortedNumericDocValues toDocByFromDoc;

    /**
     * Reused across the batch: a from-doc carries at most one to-doc (the mapping is single-valued
     * until M:N pairs are supported), so no document ever needs two of these at once.
     */
    private final SortedNumericDocValuesField toDoc;

    /** The next from-doc this column has a value at, or {@code NO_MORE_DOCS} when spent. */
    private int atFromDoc;

    PairColumn(String pairFieldName, JoinColumnModel mapping) {
      this.pairFieldName = pairFieldName;
      this.edges = mapping.edges();
      this.toDocByFromDoc = mapping.toDocByFromDoc();
      this.toDoc =
          new SortedNumericDocValuesField(
              JoinIndexUtils.TO_DOC_VAL_BY_FROM_DOCNUM + pairFieldName, 0L);
      this.atFromDoc = nextFromDoc();
    }

    /** Adds this column's fields for one batch position, if it has any there. */
    void addTo(Document doc, int docNum) {
      if (docNum == 0) {
        addEdges(doc, JoinIndexUtils.FROM_EDGES_PREFIX + pairFieldName, edges.fromDocEdges());
        addEdges(doc, JoinIndexUtils.TO_EDGES_PREFIX + pairFieldName, edges.toDocEdges());
        addEdges(doc, JoinIndexUtils.TO_COUNT_PREFIX + pairFieldName, new int[] {edges.toCount()});
      }
      if (atFromDoc == docNum) {
        assert toDocByFromDoc.docValueCount() == 1
            : "the reused field carries one to-doc per from-doc, got "
                + toDocByFromDoc.docValueCount();
        toDoc.setLongValue(nextValue());
        doc.add(toDoc);
        atFromDoc = nextFromDoc();
      }
    }

    /**
     * Adds a pair's {min, max} (or count) values to doc 0, mirroring {@code AIJoinColumnWriter}'s
     * {@code edgesColumn}, which puts both values at doc 0 too. Not reusable the way {@link #toDoc}
     * is: doc 0 holds both edges of a pair under the same field name, so they must be two fields.
     */
    private static void addEdges(Document doc, String fieldName, int[] values) {
      for (int value : values) {
        doc.add(new SortedNumericDocValuesField(fieldName, value));
      }
    }

    // the mapping is an in-memory array (JoinIndexUtils.JoinColumnModel), so neither of these can
    // actually fail; they only inherit IOException from the docvalues read API
    private int nextFromDoc() {
      try {
        return toDocByFromDoc.nextDoc();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    private long nextValue() {
      try {
        return toDocByFromDoc.nextValue();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
  }
}
