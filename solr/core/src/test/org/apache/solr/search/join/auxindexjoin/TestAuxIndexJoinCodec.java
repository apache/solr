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
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Bits;
import org.apache.solr.SolrTestCase;
import org.apache.solr.search.join.auxindexjoin.JoinIndexUtils.Edges;
import org.apache.solr.search.join.auxindexjoin.JoinIndexUtils.JoinColumnModel;

/** The sidecar codec keeps doc values intact while writing and merging no stored fields. */
public class TestAuxIndexJoinCodec extends SolrTestCase {

  private static final String FIELD = "value";

  public void testResolvesByNameThroughSpi() {
    assertTrue(Codec.forName(AuxIndexJoinCodec.NAME) instanceof AuxIndexJoinCodec);
  }

  public void testMergeKeepsDocValuesAndWritesNoStoredFields() throws IOException {
    int segments = 3;
    int perSegment = 50;
    long deleted = 3;
    try (Directory dir = newDirectory()) {
      TieredMergePolicy mergePolicy = new TieredMergePolicy();
      mergePolicy.setNoCFSRatio(0.0); // keep each format's files visible, not packed in a .cfs
      IndexWriterConfig config =
          new IndexWriterConfig()
              .setCodec(new AuxIndexJoinCodec())
              .setUseCompoundFile(false)
              .setMergePolicy(mergePolicy);
      try (IndexWriter writer = new IndexWriter(dir, config)) {
        for (int segment = 0; segment < segments; segment++) {
          for (int i = 0; i < perSegment; i++) {
            Document doc = new Document();
            doc.add(new NumericDocValuesField(FIELD, segment * perSegment + i));
            writer.addDocument(doc);
          }
          writer.commit();
        }
        // a deletion forces the merge to count live docs rather than take each input's maxDoc
        writer.deleteDocuments(NumericDocValuesField.newSlowExactQuery(FIELD, deleted));
        writer.forceMerge(1);
        writer.commit();
      }

      Set<Long> expected = new TreeSet<>();
      for (long value = 0; value < segments * perSegment; value++) {
        if (value != deleted) {
          expected.add(value);
        }
      }
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        assertEquals(1, reader.leaves().size());
        assertEquals(expected.size(), reader.maxDoc());
        NumericDocValues values = reader.leaves().get(0).reader().getNumericDocValues(FIELD);
        Set<Long> actual = new TreeSet<>();
        for (int doc = values.nextDoc();
            doc != DocIdSetIterator.NO_MORE_DOCS;
            doc = values.nextDoc()) {
          actual.add(values.longValue());
        }
        assertEquals(expected, actual);
        assertTrue(reader.storedFields().document(0).getFields().isEmpty());
      }
      for (String file : dir.listAll()) {
        assertFalse(
            "stored-fields file written: " + file,
            file.endsWith(".fdt") || file.endsWith(".fdx") || file.endsWith(".fdm"));
      }
      assertEverySegmentUsesTheCodec(dir);
    }
  }

  public void testLiveDocCount() {
    assertEquals(7, AuxIndexJoinCodec.liveDocCount(null, 7));
    assertEquals(7, AuxIndexJoinCodec.liveDocCount(new Bits.MatchAllBits(7), 7));
    assertEquals(0, AuxIndexJoinCodec.liveDocCount(new Bits.MatchNoBits(7), 7));
    Bits evens =
        new Bits() {
          @Override
          public boolean get(int index) {
            return index % 2 == 0;
          }

          @Override
          public int length() {
            return 7;
          }
        };
    assertEquals(4, AuxIndexJoinCodec.liveDocCount(evens, 7));
  }

  public void testSidecarSegmentsRecordTheCodec() throws IOException {
    try (Directory joinDir = newDirectory()) {
      try (AuxIndexManager joinIndex =
          new AuxIndexManager(joinDir, new AuxIndexJoinConfig().setCommitIntervalMs(0))) {
        joinIndex.writeBatch(Map.of("fromSeg:dv0_toSeg:dv0", sampleColumn()));
      }
      assertEverySegmentUsesTheCodec(joinDir);
    }
  }

  public void testDefaultCodecNameIsTheNoStoredFieldsCodec() {
    assertEquals(AuxIndexJoinCodec.NAME, new AuxIndexJoinConfig().getCodecName());
    expectThrows(IllegalArgumentException.class, () -> new AuxIndexJoinConfig().setCodecName(" "));
  }

  public void testSidecarUsesTheConfiguredCodec() throws IOException {
    String codecName = Codec.getDefault().getName();
    try (Directory joinDir = newDirectory()) {
      AuxIndexJoinConfig config =
          new AuxIndexJoinConfig().setCommitIntervalMs(0).setCodecName(codecName);
      try (AuxIndexManager joinIndex = new AuxIndexManager(joinDir, config)) {
        joinIndex.writeBatch(Map.of("fromSeg:dv0_toSeg:dv0", sampleColumn()));
      }
      assertEverySegmentUsesTheCodec(joinDir, codecName);
    }
  }

  public void testUnknownCodecNameFailsAtOpen() throws IOException {
    try (Directory joinDir = newDirectory()) {
      AuxIndexJoinConfig config = new AuxIndexJoinConfig().setCodecName("NoSuchCodec");
      IllegalArgumentException e =
          expectThrows(IllegalArgumentException.class, () -> new AuxIndexManager(joinDir, config));
      assertTrue(e.getMessage(), e.getMessage().contains("NoSuchCodec"));
    }
  }

  private static JoinColumnModel sampleColumn() {
    return JoinColumnModel.dense(
        new int[] {3, -1, 7}, new Edges(new int[] {0, 2}, new int[] {3, 7}, 2));
  }

  private static void assertEverySegmentUsesTheCodec(Directory dir) throws IOException {
    assertEverySegmentUsesTheCodec(dir, AuxIndexJoinCodec.NAME);
  }

  private static void assertEverySegmentUsesTheCodec(Directory dir, String codecName)
      throws IOException {
    SegmentInfos infos = SegmentInfos.readLatestCommit(dir);
    assertFalse("expected at least one segment", infos.asList().isEmpty());
    for (SegmentCommitInfo info : infos) {
      assertEquals(codecName, info.info.getCodec().getName());
    }
  }
}
