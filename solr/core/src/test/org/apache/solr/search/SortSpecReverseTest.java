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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSelector;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.search.SortedSetSelector;
import org.apache.lucene.search.SortedSetSortField;
import org.apache.lucene.search.TopFieldCollectorManager;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.SolrTestCase;
import org.junit.Test;

/** Exercises inverse sorts with real collectors, including missing and multi-valued fields. */
public class SortSpecReverseTest extends SolrTestCase {
  @Test
  public void testInverseSelectionAndSearchAfter() throws Exception {
    try (Directory directory = newDirectory();
        RandomIndexWriter writer = new RandomIndexWriter(random(), directory)) {
      for (int i = 0; i < 64; i++) {
        Document doc = new Document();
        doc.add(new StoredField("id", i));
        doc.add(new NumericDocValuesField("id", i));
        if (i % 4 != 0) {
          doc.add(new SortedNumericDocValuesField("numbers", i % 5));
          doc.add(new SortedNumericDocValuesField("numbers", 100 - i));
          doc.add(new SortedSetDocValuesField("strings", new BytesRef("a" + (i % 5))));
          doc.add(new SortedSetDocValuesField("strings", new BytesRef("z" + (100 - i))));
        }
        writer.addDocument(doc);
        if (i % 11 == 0) writer.commit();
      }
      try (DirectoryReader reader = writer.getReader()) {
        IndexSearcher searcher = newSearcher(reader);
        for (boolean descending : new boolean[] {false, true}) {
          for (SortField field :
              List.of(
                  new SortedNumericSortField(
                      "numbers",
                      SortField.Type.LONG,
                      descending,
                      SortedNumericSelector.Type.MIN,
                      Long.MAX_VALUE),
                  new SortedNumericSortField(
                      "numbers",
                      SortField.Type.LONG,
                      descending,
                      SortedNumericSelector.Type.MAX,
                      Long.MIN_VALUE),
                  new SortedSetSortField(
                      "strings", descending, SortedSetSelector.Type.MIN, SortField.STRING_LAST),
                  new SortedSetSortField(
                      "strings", descending, SortedSetSelector.Type.MAX, SortField.STRING_FIRST),
                  new SortField(null, SortField.Type.SCORE, descending))) {
            SortSpec original =
                new SortSpec(
                    new Sort(field, new SortField("id", SortField.Type.LONG)),
                    Collections.nCopies(2, null));
            SortSpec inverse = original.reverse();
            assertEquals(original.getSort(), inverse.reverse().getSort());
            assertEquals(original.includesScore(), inverse.includesScore());
            TopFieldDocs forward = collect(searcher, original.getSort(), null, 64);
            List<Integer> reversed = ids(searcher, forward.scoreDocs);
            Collections.reverse(reversed);
            TopFieldDocs backward = collect(searcher, inverse.getSort(), null, 64);
            assertEquals(reversed, ids(searcher, backward.scoreDocs));

            FieldDoc boundary = (FieldDoc) forward.scoreDocs[31];
            FieldDoc before = new FieldDoc(Integer.MAX_VALUE, 0f, boundary.fields);
            TopFieldDocs previous = collect(searcher, inverse.getSort(), before, 7);
            List<Integer> expected = ids(searcher, forward.scoreDocs).subList(24, 31);
            List<Integer> actual = ids(searcher, previous.scoreDocs);
            Collections.reverse(actual);
            assertEquals(expected, actual);
          }
        }
      }
    }
  }

  private TopFieldDocs collect(IndexSearcher searcher, Sort sort, FieldDoc after, int count)
      throws Exception {
    return searcher.search(
        new MatchAllDocsQuery(), new TopFieldCollectorManager(sort, count, after, 1));
  }

  private List<Integer> ids(IndexSearcher searcher, ScoreDoc[] docs) throws Exception {
    List<Integer> ids = new ArrayList<>(docs.length);
    for (ScoreDoc doc : docs) {
      ids.add(searcher.storedFields().document(doc.doc).getField("id").numericValue().intValue());
    }
    return ids;
  }
}
