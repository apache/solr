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
import java.util.ArrayList;
import java.util.Arrays;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.solr.SolrTestCase;

public class TestFilteredDocIdSet extends SolrTestCase {
  public void testFilteredDocIdSet() throws Exception {
    final int maxdoc = 10;
    final DocIdSet innerSet =
        new DocIdSet() {

          @Override
          public long ramBytesUsed() {
            return 0L;
          }

          @Override
          public DocIdSetIterator iterator() {
            return new DocIdSetIterator() {

              int docid = -1;

              @Override
              public int docID() {
                return docid;
              }

              @Override
              public int nextDoc() {
                docid++;
                return docid < maxdoc ? docid : (docid = NO_MORE_DOCS);
              }

              @Override
              public int advance(int target) throws IOException {
                return slowAdvance(target);
              }

              @Override
              public long cost() {
                return 1;
              }
            };
          }
        };

    DocIdSet filteredSet =
        new FilteredDocIdSet(innerSet) {
          @Override
          protected boolean match(int docid) {
            return docid % 2 == 0; // validate only even docids
          }
        };

    DocIdSetIterator iter = filteredSet.iterator();
    ArrayList<Integer> list = new ArrayList<>();
    int doc = iter.advance(3);
    if (doc != DocIdSetIterator.NO_MORE_DOCS) {
      list.add(doc);
      while ((doc = iter.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
        list.add(doc);
      }
    }

    int[] docs = new int[list.size()];
    int c = 0;
    for (Integer integer : list) {
      docs[c++] = integer.intValue();
    }
    int[] answer = new int[] {4, 6, 8};
    boolean same = Arrays.equals(answer, docs);
    if (!same) {
      System.out.println("answer: " + Arrays.toString(answer));
      System.out.println("gotten: " + Arrays.toString(docs));
      fail();
    }
  }

  public void testNullDocIdSet() throws Exception {
    // (historical note) Tests that if a Query produces a null DocIdSet, which is given to
    // IndexSearcher, everything works fine. This came up in LUCENE-1754.
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    doc.add(newStringField("c", "val", Field.Store.NO));
    writer.addDocument(doc);
    IndexReader reader = writer.getReader();
    writer.close();

    // First verify the document is searchable.
    IndexSearcher searcher = newSearcher(reader);
    assertEquals(1, searcher.search(new MatchAllDocsQuery(), 10).totalHits.value);

    // Now search w/ a Query which returns a null Scorer
    DocSetQuery f = new DocSetQuery(DocSet.empty());

    Query filtered =
        new BooleanQuery.Builder()
            .add(new MatchAllDocsQuery(), Occur.MUST)
            .add(f, Occur.FILTER)
            .build();
    assertEquals(0, searcher.search(filtered, 10).totalHits.value);
    reader.close();
    dir.close();
  }

  public void testNullIteratorFilteredDocIdSet() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    doc.add(newStringField("c", "val", Field.Store.NO));
    writer.addDocument(doc);
    IndexReader reader = writer.getReader();
    writer.close();

    // First verify the document is searchable.
    IndexSearcher searcher = newSearcher(reader);
    assertEquals(1, searcher.search(new MatchAllDocsQuery(), 10).totalHits.value);

    // Now search w/ a Query which returns a null Scorer
    Query f =
        new Query() {
          @Override
          public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) {
            return new Weight(this) {

              @Override
              public Explanation explain(LeafReaderContext context, int doc) {
                return Explanation.match(0f, "No match on id " + doc);
              }

              @Override
              public Scorer scorer(LeafReaderContext leafReaderContext) {
                return null;
              }

              @Override
              public boolean isCacheable(LeafReaderContext ctx) {
                return false;
              }
            };
          }

          @Override
          public String toString(String field) {
            return "nullDocIdSetFilter";
          }

          @Override
          public void visit(QueryVisitor queryVisitor) {}

          @Override
          public boolean equals(Object other) {
            return other == this;
          }

          @Override
          public int hashCode() {
            return System.identityHashCode(this);
          }
        };

    Query filtered =
        new BooleanQuery.Builder()
            .add(new MatchAllDocsQuery(), Occur.MUST)
            .add(f, Occur.FILTER)
            .build();
    assertEquals(0, searcher.search(filtered, 10).totalHits.value);
    reader.close();
    dir.close();
  }
}
