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
package org.apache.solr.search;

import java.io.IOException;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.analysis.MockTokenizer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.expressions.Expression;
import org.apache.lucene.expressions.js.JavascriptCompiler;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.queries.intervals.IntervalMatchesIterator;
import org.apache.lucene.queries.intervals.Intervals;
import org.apache.lucene.store.Directory;
import org.apache.solr.SolrTestCase;

/** Tests {@link ScriptFilterIntervalsSource#matches}, which highlighting and explain rely on. */
public class TestScriptFilterIntervalsSource extends SolrTestCase {

  private static final String FIELD = "f";

  private static ScriptFilterIntervalsSource filter(String script) throws Exception {
    Expression expression = JavascriptCompiler.compile(script);
    return new ScriptFilterIntervalsSource(
        Intervals.term("a"), expression, new ScriptFilterIntervalsSource.IntervalBindings());
  }

  private static IntervalMatchesIterator matches(
      Directory dir, ScriptFilterIntervalsSource source, int doc) throws IOException {
    try (DirectoryReader reader = DirectoryReader.open(dir)) {
      LeafReaderContext ctx = reader.leaves().get(0);
      return source.matches(FIELD, ctx, doc);
    }
  }

  private static Directory index(String... docs) throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig cfg =
        new IndexWriterConfig(new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false));
    cfg.setMergePolicy(newLogMergePolicy());
    try (IndexWriter w = new IndexWriter(dir, cfg)) {
      for (String text : docs) {
        Document d = new Document();
        d.add(new TextField(FIELD, text, Field.Store.NO));
        w.addDocument(d);
      }
      w.forceMerge(1);
    }
    return dir;
  }

  public void testMatchesSkipsRejectedLeadingIntervals() throws Exception {
    // "a" occurs at positions 0, 2 and 3; only the later two satisfy start >= 2
    try (Directory dir = index("a b a a")) {
      IntervalMatchesIterator mi = matches(dir, filter("interval.start >= 2"), 0);
      assertNotNull(mi);
      assertTrue(mi.next());
      assertEquals(2, mi.startPosition());
      assertEquals(2, mi.endPosition());
      assertTrue(mi.next());
      assertEquals(3, mi.startPosition());
      assertFalse(mi.next());
    }
  }

  public void testMatchesFirstAcceptedIntervalIsNotLost() throws Exception {
    // the first interval is accepted: it must be reported exactly once
    try (Directory dir = index("a b a")) {
      IntervalMatchesIterator mi = matches(dir, filter("interval.start == 0"), 0);
      assertNotNull(mi);
      assertTrue(mi.next());
      assertEquals(0, mi.startPosition());
      assertFalse(mi.next());
    }
  }

  public void testMatchesNullWhenNoIntervalAccepted() throws Exception {
    try (Directory dir = index("a b a")) {
      assertNull(matches(dir, filter("interval.start > 10"), 0));
    }
  }

  public void testMatchesNullWhenNoTermInDocument() throws Exception {
    try (Directory dir = index("a b", "c d")) {
      assertNull(matches(dir, filter("interval.start >= 0"), 1));
    }
  }
}
