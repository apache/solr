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

import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.codecs.StoredFieldsWriter;
import org.apache.lucene.codecs.lucene104.Lucene104Codec;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;

/**
 * The sidecar's codec: {@link Lucene104Codec} without stored fields. The sidecar holds doc values
 * only, yet every sidecar document would still pass through the stored-fields writer, when a batch
 * is indexed and again on every merge -- and the sidecar's own merges hand Lucene wrapped readers
 * ({@link DocAlignedMerge}, column purges), which keeps it off its bulk-copy path and makes it
 * visit each of the from-segment's documents one by one. This format writes nothing and merges by
 * counting.
 *
 * <p>The delegate is pinned rather than taken from {@code Codec.getDefault()}: segments record this
 * codec's name, not the delegate's, so a change of delegate needs a new {@link #NAME}.
 *
 * @lucene.experimental
 */
public final class AuxIndexJoinCodec extends FilterCodec {

  /** Recorded in every sidecar segment; must change whenever the delegate does. */
  public static final String NAME = "NoStoredFieldsLucene104";

  private final StoredFieldsFormat storedFieldsFormat = new NoStoredFieldsFormat();

  /** No-arg constructor, as SPI requires. */
  public AuxIndexJoinCodec() {
    super(NAME, new Lucene104Codec());
  }

  @Override
  public StoredFieldsFormat storedFieldsFormat() {
    return storedFieldsFormat;
  }

  /** How many of {@code maxDoc} documents {@code liveDocs} keeps; {@code null} keeps them all. */
  static int liveDocCount(Bits liveDocs, int maxDoc) {
    if (liveDocs == null || liveDocs instanceof Bits.MatchAllBits) {
      return maxDoc;
    }
    if (liveDocs instanceof Bits.MatchNoBits) {
      return 0;
    }
    int live = 0;
    for (int doc = 0; doc < maxDoc; doc++) {
      if (liveDocs.get(doc)) {
        live++;
      }
    }
    return live;
  }

  private static final class NoStoredFieldsFormat extends StoredFieldsFormat {
    @Override
    public StoredFieldsReader fieldsReader(
        Directory directory, SegmentInfo si, FieldInfos fn, IOContext context) {
      return new EmptyStoredFieldsReader();
    }

    @Override
    public StoredFieldsWriter fieldsWriter(Directory directory, SegmentInfo si, IOContext context) {
      return new NoStoredFieldsWriter();
    }
  }

  private static final class EmptyStoredFieldsReader extends StoredFieldsReader {
    @Override
    public void document(int docID, StoredFieldVisitor visitor) {}

    @Override
    public StoredFieldsReader clone() {
      return this;
    }

    @Override
    public void checkIntegrity() {}

    @Override
    public void close() {}
  }

  /** Accepts documents, never a field: a stored field in the sidecar is a bug. */
  private static final class NoStoredFieldsWriter extends StoredFieldsWriter {
    @Override
    public void startDocument() {}

    @Override
    public void writeField(FieldInfo info, int value) {
      throw storedField(info);
    }

    @Override
    public void writeField(FieldInfo info, long value) {
      throw storedField(info);
    }

    @Override
    public void writeField(FieldInfo info, float value) {
      throw storedField(info);
    }

    @Override
    public void writeField(FieldInfo info, double value) {
      throw storedField(info);
    }

    @Override
    public void writeField(FieldInfo info, BytesRef value) {
      throw storedField(info);
    }

    @Override
    public void writeField(FieldInfo info, String value) {
      throw storedField(info);
    }

    @Override
    public void finish(int numDocs) {}

    /**
     * Returns what the default merge would have counted -- every live document of every input --
     * since {@code SegmentMerger} checks it against the merged segment's {@code maxDoc}.
     */
    @Override
    public int merge(MergeState mergeState) {
      int docCount = 0;
      for (int i = 0; i < mergeState.maxDocs.length; i++) {
        docCount += liveDocCount(mergeState.liveDocs[i], mergeState.maxDocs[i]);
      }
      return docCount;
    }

    @Override
    public void close() {}

    @Override
    public long ramBytesUsed() {
      return 0;
    }

    private static UnsupportedOperationException storedField(FieldInfo info) {
      return new UnsupportedOperationException(
          NAME + " stores no fields, but was given stored field '" + info.name + "'");
    }
  }
}
