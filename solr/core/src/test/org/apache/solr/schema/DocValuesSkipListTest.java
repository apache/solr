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
package org.apache.solr.schema;

import java.io.IOException;
import org.apache.lucene.index.DocValuesSkipIndexType;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.LeafReader;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.core.SolrCore;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.util.RefCounted;
import org.junit.BeforeClass;
import org.junit.Test;

public class DocValuesSkipListTest extends SolrTestCaseJ4 {

  private static final String[] SKIP_LIST_FIELDS = {
    "floatdv",
    "intdv",
    "doubledv",
    "longdv",
    "datedv",
    "stringdv",
    "booldv",
    "collateddv",
    "sortableTextdv",
    "enumdv",
    "trieIntdv",
    "trieLongdv",
    "trieFloatdv",
    "trieDoubledv",
    "trieDatedv",
    "floatdvs",
    "intdvs",
    "doubledvs",
    "longdvs",
    "datedvs",
    "stringdvs",
    "booldvs",
    "collateddvs",
    "sortableTextdvs",
    "enumdvs",
    "trieIntdvs",
    "trieLongdvs",
    "trieFloatdvs",
    "trieDoubledvs",
    "trieDatedvs"
  };

  @BeforeClass
  public static void beforeTests() throws Exception {
    initCore("solrconfig-basic.xml", "schema-docValuesSkipList.xml");

    final IndexSchema schema = h.getCore().getLatestSchema();
    for (String fieldName : SKIP_LIST_FIELDS) {
      final SchemaField sf = schema.getField(fieldName);
      if (fieldName.endsWith("dvs")) {
        assertTrue(fieldName + " should be multiValued", sf.multiValued());
      } else {
        assertFalse(fieldName + " should be single-valued", sf.multiValued());
      }
      assertTrue(fieldName + " should have docValues", sf.hasDocValues());
      assertTrue(fieldName + " should have docValuesSkipList", sf.hasDocValuesSkipList());
    }

    final SchemaField plain = schema.getField("long_plain");
    assertTrue(plain.hasDocValues());
    assertFalse(plain.hasDocValuesSkipList());
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    assertU(delQ("*:*"));
  }

  @Test
  public void testSkipIndexTypeOnFieldInfos() throws IOException {
    assertU(adoc("id", "1", "long_plain", "10"));
    assertU(commit());

    try (SolrCore core = h.getCoreInc()) {
      final RefCounted<SolrIndexSearcher> searcherRef = core.openNewSearcher(true, true);
      final SolrIndexSearcher searcher = searcherRef.get();
      try {
        final LeafReader reader = searcher.getSlowAtomicReader();
        assertEquals(1, reader.numDocs());
        final FieldInfos infos = reader.getFieldInfos();

        for (String f : SKIP_LIST_FIELDS) {
          assertEquals(
              f + " should have RANGE skip index",
              DocValuesSkipIndexType.RANGE,
              infos.fieldInfo(f).docValuesSkipIndexType());
        }
        assertEquals(
            "long_plain should have no skip index",
            DocValuesSkipIndexType.NONE,
            infos.fieldInfo("long_plain").docValuesSkipIndexType());
      } finally {
        searcherRef.decref();
      }
    }
  }

  @Test
  public void testRangeQueries() {
    assertU(adoc("id", "1", "longdv", "10", "stringdv", "aaa", "intdvs", "1", "intdvs", "5"));
    assertU(adoc("id", "2", "longdv", "20", "stringdv", "bbb", "intdvs", "3"));
    assertU(adoc("id", "3", "longdv", "30", "stringdv", "ccc", "intdvs", "7", "intdvs", "9"));
    assertU(commit());

    assertQ(
        req("q", "longdv:[15 TO 25]", "fl", "id", "sort", "id asc"),
        "//*[@numFound='1']",
        "//result/doc[1]/str[@name='id'][.='2']");

    assertQ(
        req("q", "stringdv:{aaa TO ccc}", "fl", "id", "sort", "id asc"),
        "//*[@numFound='1']",
        "//result/doc[1]/str[@name='id'][.='2']");

    assertQ(
        req("q", "intdvs:[4 TO 8]", "fl", "id", "sort", "id asc"),
        "//*[@numFound='2']",
        "//result/doc[1]/str[@name='id'][.='1']",
        "//result/doc[2]/str[@name='id'][.='3']");
  }
}
