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
package org.apache.solr.bench;

import static org.apache.solr.bench.Docs.docs;
import static org.apache.solr.bench.generators.SourceDSL.integers;
import static org.apache.solr.bench.generators.SourceDSL.strings;

import java.lang.invoke.MethodHandles;
import java.util.HashSet;
import java.util.Set;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.bench.generators.Distribution;
import org.apache.solr.bench.generators.SolrGen;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DockMakerTest extends SolrTestCaseJ4 {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @BeforeClass
  public static void beforeClass() {
    System.setProperty("random.counts", "true");
  }

  @After
  public void after() {
    SolrGen.countsReport().forEach(log::info);
    SolrGen.COUNTS.clear();
  }

  @Test
  public void testBasicCardinalityAlpha() throws Exception {

    Docs docs = docs();

    int maxCardinality = 2;

    docs.field(
        "AlphaCard3", strings().alpha().maxCardinality(maxCardinality).ofLengthBetween(1, 6));

    Set<String> values = new HashSet<>();
    for (int i = 0; i < 10; i++) {
      SolrInputDocument doc = docs.inputDocument();
      SolrInputField field = doc.getField("AlphaCard3");
      values.add(field.getValue().toString());
    }

    // the cardinality we see is <= maxCardinality specified, albeit almost always equal to
    assertTrue(values.toString(), values.size() <= maxCardinality);
  }

  @Test
  public void testBasicCardinalityUnicode() throws Exception {
    Docs docs = docs();
    int maxCardinality = 4;
    docs.field(
        "UnicodeCard3",
        strings()
            .basicMultilingualPlaneAlphabet()
            .maxCardinality(maxCardinality)
            .ofLengthBetween(1, 6));

    HashSet<Object> values = new HashSet<>();
    for (int i = 0; i < 30; i++) {
      SolrInputDocument doc = docs.inputDocument();
      SolrInputField field = doc.getField("UnicodeCard3");
      log.info("field={}", doc);
      values.add(field.getValue().toString());
    }

    // the cardinality we see is <= maxCardinality specified, albeit almost always equal to
    assertTrue(values.toString(), values.size() <= maxCardinality);
  }

  @Test
  public void testBasicCardinalityInteger() throws Exception {
    Docs docs = docs();
    int maxCardinality = 3;

    docs.field("IntCard2", integers().allWithMaxCardinality(maxCardinality));

    HashSet<Object> values = new HashSet<>();
    for (int i = 0; i < 30; i++) {
      SolrInputDocument doc = docs.inputDocument();
      SolrInputField field = doc.getField("IntCard2");
      values.add(field.getValue().toString());
    }

    // the cardinality we see is <= maxCardinality specified, albeit almost always equal to
    assertTrue(values.toString(), values.size() <= maxCardinality);

    if (log.isInfoEnabled()) {
      log.info(values.toString());
    }
  }

  @Test
  public void testBasicInteger() throws Exception {
    Docs docs = docs();

    docs.field("IntCard2", integers().between(10, 50).withDistribution(Distribution.GAUSSIAN));

    HashSet<Integer> values = new HashSet<>();
    for (int i = 0; i < 300; i++) {
      SolrInputDocument doc = docs.inputDocument();
      SolrInputField field = doc.getField("IntCard2");
      values.add((Integer) field.getValue());
    }

    if (log.isInfoEnabled()) {
      log.info(values.toString());
    }
  }

  @Test
  public void testBasicIntegerId() throws Exception {

    Docs docs = docs();

    docs.field("id", integers().incrementing());

    HashSet<Integer> values = new HashSet<>();
    for (int i = 0; i < 300; i++) {
      SolrInputDocument doc = docs.inputDocument();
      SolrInputField field = doc.getField("id");
      values.add((Integer) field.getValue());
    }

    Integer lastVal = null;
    for (Integer val : values) {
      if (lastVal != null) {
        assertTrue(val > lastVal);
      }
      lastVal = val;
    }
  }

  @Test
  public void testWordList() throws Exception {
    Docs docs = docs();

    docs.field("wordList", strings().wordList().multi(4));

    Set<String> values = new HashSet<>();
    for (int i = 0; i < 1; i++) {
      SolrInputDocument doc = docs.inputDocument();
      SolrInputField field = doc.getField("wordList");
      values.add((String) field.getValue());
    }

    for (String val : values) {
      assertEquals(4, val.split("\\s").length);
    }
  }

  @Test
  public void testRealisticUnicode() throws Exception {
    Docs docs = docs();

    docs.field("unicode", strings().realisticUnicode(4, 12).multi(6));

    Set<String> values = new HashSet<>();
    for (int i = 0; i < 1; i++) {
      SolrInputDocument doc = docs.inputDocument();
      SolrInputField field = doc.getField("unicode");
      values.add((String) field.getValue());
    }

    for (String val : values) {
      assertEquals(6, val.split("\\s").length);
    }
  }

  @Test
  public void testWordListZipfian() {
    Docs docs = docs();
    docs.field("wordList", strings().wordList().withDistribution(Distribution.ZIPFIAN).multi(30));

    SolrInputDocument doc = docs.inputDocument();
    SolrInputField field = doc.getField("wordList");

    assertNotNull(field.getValue().toString());
  }
}
