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
package org.apache.solr.analysis;

import java.util.Arrays;
import org.apache.lucene.analysis.commongrams.CommonGramsFilterFactory;
import org.apache.lucene.analysis.commongrams.CommonGramsQueryFilterFactory;
import org.apache.solr.SolrTestCaseJ4;
import org.junit.BeforeClass;

/**
 * Validate that using {@link CommonGramsFilterFactory} at index time with {@link
 * CommonGramsQueryFilterFactory} at query time has the expected results when doing phrase queries -
 * even if stop words are removed from the index.
 */
public class CommonGramsPhraseQueryTest extends SolrTestCaseJ4 {

  @BeforeClass
  public static void setupIndex() throws Exception {
    initCore("solrconfig.xml", "schema.xml");

    assertU(
        adoc(
            "id", "1",
            "x_commongrams", "the quick and the dead man"));
    assertU(
        adoc(
            "id", "2",
            "x_commongrams",
                "a longer field that also mentions the quick and the dead man plus extra stuff"));
    assertU(
        adoc(
            "id", "3",
            "x_commongrams", "not a dead man"));

    assertU(commit());
  }

  public void testCommonGrams() {
    testCommonQueries("x_commongrams");
    // individual stop words should also match in this field....
    for (String word : Arrays.asList("the", "and")) {
      assertQ(
          req("x_commongrams:" + word),
          "//*[@numFound='2']",
          "//str[@name='id' and .='1']",
          "//str[@name='id' and .='2']");
    }
    assertQ(
        req("x_commongrams:a"),
        "//*[@numFound='2']",
        "//str[@name='id' and .='2']",
        "//str[@name='id' and .='3']");
    assertQ(req("x_commongrams:not"), "//*[@numFound='1']", "//str[@name='id' and .='3']");
  }

  public void testCommonGramsStop() {
    testCommonQueries("x_commongrams_stop");
    // individual stop words should not match anything in this field...
    for (String word : Arrays.asList("the", "and", "not", "a")) {
      assertQ(req("x_commongrams_stop:" + word), "//*[@numFound='0']");
    }
  }

  protected void testCommonQueries(final String f) {
    // match 2...
    for (String phrase :
        Arrays.asList(
            "the quick and the dead",
            "the quick",
            "and the dead",
            "and the",
            "the dead man",
            "quick")) {
      assertQ(
          req(f + ":\"" + phrase + "\""),
          "//*[@numFound='2']",
          "//str[@name='id' and .='1']",
          "//str[@name='id' and .='2']");
    }
    assertQ(
        // just for the hell of it, let's also check this as a term and not a phrase
        req(f + ":quick"),
        "//*[@numFound='2']",
        "//str[@name='id' and .='1']",
        "//str[@name='id' and .='2']");
    // match all...
    for (String qs : Arrays.asList("dead", "man", "\"dead man\"")) {
      assertQ(
          req(f + ":" + qs),
          "//*[@numFound='3']",
          "//str[@name='id' and .='1']",
          "//str[@name='id' and .='2']",
          "//str[@name='id' and .='3']");
    }
  }
}
