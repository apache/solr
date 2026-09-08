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
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.FilterCodecReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.ParallelLeafReader;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SlowCodecReaderWrapper;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.index.TermVectors;
import org.apache.lucene.util.Bits;

/**
 * Compacts sidecar segments <em>without moving a single doc id</em>: doc {@code i} of the merged
 * segment carries the columns of doc {@code i} of every input segment, and the result's {@code
 * maxDoc} is the largest input's, not the sum.
 *
 * <pre>
 *   segment _a (maxDoc 2)      segment _b (maxDoc 3)      merged (maxDoc 3)
 *     doc0: A B                  doc0: D E F                doc0: A B D E F
 *     doc1: A B                  doc1: D E F                doc1: A B D E F
 *                                doc2: D E F                doc2:     D E F
 * </pre>
 *
 * <p><b>Why not an ordinary merge.</b> Lucene merges by concatenating, so segment _b's doc 0 would
 * become doc 2 of the result. A sidecar doc id is not an identity of its own -- it <em>is</em> the
 * from-side doc id the column is addressed by ({@link JoinIndexScorerSupplier} reads a pair with
 * {@code toDocsByFromDoc.advanceExact(fromDoc)}), and every pair's edges live at doc 0 -- so
 * concatenation silently shifts every mapping. That is why {@link AuxIndexJoinMergePolicy} could
 * only ever drop whole dead segments and never rewrite live ones, and why the sidecar grew by one
 * segment per written batch until the process ran out of mmap-able address space.
 *
 * <p>Aligning is well defined precisely because this merge is <em>opaque</em>: it never looks
 * inside a column. A pair field name carries both sides' segment ids ({@link
 * JoinIndexUtils#pairFieldName}), so columns of different pairs never collide, and a doc id means
 * the same thing in every input -- the from-side doc id of that column's own from-segment. Inputs
 * built from a smaller from-segment simply have no values in the tail docs.
 *
 * <p><b>How it hooks into Lucene.</b> {@link IndexWriter} merges by concatenating whatever {@link
 * #wrapForMerge} hands back, so this merge hands back nothing for all inputs but one: every input
 * except the last is wrapped as fully deleted ({@link Bits.MatchNoBits}, {@code numDocs() == 0}) so
 * it contributes no doc, and the last is replaced by a {@link ParallelLeafReader} over <em>all</em>
 * the inputs -- the reader that aligns fields by doc id. Concatenating {@code [nothing, ...,
 * nothing, union]} is the union. This relies on {@code IndexWriter#mergeMiddle} calling {@code
 * wrapForMerge} exactly once per segment, in {@link #segments} order, and only then starting the
 * merge, so that by the last call every input reader is known; the count is enforced below.
 *
 * <p>Live pairs are carried over as they are: dropping the columns of pairs queued for reaping
 * while rewriting them anyway would be nearly free, but a pair that vanishes under a query holding
 * a reference to it currently fails that query's refresh in {@code
 * JoinIndexScorerSupplier#refreshJoinTasksReferences} instead of being rebuilt, so that is left to
 * a follow-up.
 *
 * @lucene.experimental
 */
final class DocAlignedMerge extends MergePolicy.OneMerge {

  /** The readers handed to {@link #wrapForMerge}, in {@link #segments} order. */
  private final List<CodecReader> inputs;

  private final Runnable onMerged;
  private ParallelLeafReader union;

  DocAlignedMerge(List<SegmentCommitInfo> segments, Runnable onMerged) {
    super(segments);
    if (segments.size() < 2) {
      throw new IllegalArgumentException(
          "a doc-aligned merge of " + segments.size() + " segment(s) would only rewrite it");
    }
    this.inputs = new ArrayList<>(segments.size());
    this.onMerged = onMerged;
  }

  @Override
  public CodecReader wrapForMerge(CodecReader reader) throws IOException {
    if (inputs.size() >= segments.size()) {
      throw new IllegalStateException(
          "wrapForMerge called "
              + (inputs.size() + 1)
              + " times for a merge of "
              + segments.size()
              + " segments; the doc-aligned merge builds its union on the last call");
    }
    inputs.add(reader);
    if (inputs.size() < segments.size()) {
      return dropAllDocs(reader); // contributes nothing; its columns come from the union below
    }
    int alignedMaxDoc = 0;
    for (CodecReader input : inputs) {
      alignedMaxDoc = Math.max(alignedMaxDoc, input.maxDoc());
    }
    LeafReader[] aligned = new LeafReader[inputs.size()];
    for (int i = 0; i < aligned.length; i++) {
      aligned[i] = new PaddedToMaxDoc(inputs.get(i), alignedMaxDoc);
    }
    // closeSubReaders=false: the readers are IndexWriter's, this merge must not close them
    union = new ParallelLeafReader(false, aligned);
    return SlowCodecReaderWrapper.wrap(union);
  }

  @Override
  public void mergeFinished(boolean success, boolean segmentDropped) throws IOException {
    try {
      if (union != null) {
        union.close(); // only decrefs the padding views, see PaddedToMaxDoc#doClose
        union = null;
      }
      inputs.clear();
      if (success && !segmentDropped) {
        onMerged.run();
      }
    } finally {
      super.mergeFinished(success, segmentDropped);
    }
  }

  /**
   * A view of {@code reader} reporting all its docs deleted, so {@link IndexWriter} carries none of
   * them into the merged segment. Shared with {@link AuxIndexJoinMergePolicy}'s reaper, which drops
   * a whole dead segment by wrapping it this way and letting the merge produce nothing at all.
   */
  static CodecReader dropAllDocs(CodecReader reader) {
    return new FilterCodecReader(reader) {
      @Override
      public CacheHelper getCoreCacheHelper() {
        return reader.getCoreCacheHelper();
      }

      @Override
      public CacheHelper getReaderCacheHelper() {
        return null; // we are altering live docs
      }

      @Override
      public Bits getLiveDocs() {
        return new Bits.MatchNoBits(reader.maxDoc());
      }

      @Override
      public int numDocs() {
        return 0;
      }
    };
  }

  /**
   * Presents a sidecar segment as if it had {@code maxDoc} docs, the ones past its own {@code
   * maxDoc} simply carrying no value in any of its columns -- what {@link ParallelLeafReader}
   * needs, since it combines readers of equal length only.
   *
   * <p>Padding is safe here because a sidecar segment holds nothing but sparse SORTED_NUMERIC
   * docvalues: their iterators end at the last doc that has a value, well before {@code maxDoc},
   * exactly as they would for any sparse column. Stored fields and term vectors, which would
   * otherwise be asked for docs the delegate doesn't have, are answered as empty -- the sidecar
   * writes neither.
   */
  private static final class PaddedToMaxDoc extends FilterLeafReader {

    private static final StoredFields NO_STORED_FIELDS =
        new StoredFields() {
          @Override
          public void document(int docID, StoredFieldVisitor visitor) {}
        };

    private final int maxDoc;

    PaddedToMaxDoc(LeafReader in, int maxDoc) {
      super(in);
      if (in.getLiveDocs() != null) {
        // a shorter live-docs Bits would be read past its end for the padding docs; the sidecar
        // never deletes, and AuxIndexJoinMergePolicy keeps segments with deletions out of these
        // merges, so this is unreachable rather than merely unlikely
        throw new IllegalStateException("cannot doc-align a sidecar segment carrying deletions");
      }
      this.maxDoc = maxDoc;
    }

    @Override
    public int maxDoc() {
      return maxDoc;
    }

    @Override
    public int numDocs() {
      return maxDoc;
    }

    @Override
    public Bits getLiveDocs() {
      return null;
    }

    @Override
    public StoredFields storedFields() {
      return NO_STORED_FIELDS;
    }

    @Override
    public TermVectors termVectors() {
      return TermVectors.EMPTY;
    }

    @Override
    protected void doClose() {
      // the wrapped reader belongs to the merge, closing it here would pull it from under
      // IndexWriter; the union only ever decrefs these views anyway
    }

    @Override
    public CacheHelper getCoreCacheHelper() {
      return null; // maxDoc and the field set differ from the wrapped reader's
    }

    @Override
    public CacheHelper getReaderCacheHelper() {
      return null;
    }
  }
}
