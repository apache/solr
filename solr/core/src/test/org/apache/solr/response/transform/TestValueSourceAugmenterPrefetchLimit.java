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
package org.apache.solr.response.transform;

import org.apache.solr.SolrTestCaseJ4;
import org.junit.BeforeClass;

public class TestValueSourceAugmenterPrefetchLimit extends SolrTestCaseJ4 {

  private static final int MAX_PREFETCH_SIZE = 1000;

  @BeforeClass
  public static void beforeTests() throws Exception {
    initCore("solrconfig.xml", "schema.xml");

    // Index enough documents to exceed the prefetch limit
    final int numDocs = MAX_PREFETCH_SIZE + 10;
    for (int i = 0; i < numDocs; i++) {
      assertU(adoc("id", Integer.toString(i)));
    }
    assertU(commit());
  }

  public void testWithScoreBelowPrefetchLimit() {
    assertQ(
        "fl=foo:field(id),score with rows within prefetch limit",
        req(
            "q", "*:*",
            "fl", "foo:field(id),score",
            "rows", String.valueOf(MAX_PREFETCH_SIZE)),
        "//result[@numFound='" + (MAX_PREFETCH_SIZE + 10) + "']",
        "//result/doc/float[@name='score']");
  }

  public void testWithScoreAbovePrefetchLimit() {
    assertQ(
        "fl=foo:field(id),score with rows exceeding prefetch limit",
        req(
            "q", "*:*",
            "fl", "foo:field(id),score",
            "rows", String.valueOf(MAX_PREFETCH_SIZE + 10)),
        "//result[@numFound='" + (MAX_PREFETCH_SIZE + 10) + "']",
        "//result/doc/float[@name='score']");
  }

  public void testWithScoreAbovePrefetchLimitUsingPreFetchDocsParam() {
    assertQ(
        "fl=foo:field(id),score with custom preFetchDocs",
        req(
            "q", "*:*",
            "fl", "foo:field(id),score",
            "rows", String.valueOf(MAX_PREFETCH_SIZE + 10),
            "preFetchDocs", "2000"), // Set to 2000
        "//result[@numFound='" + (MAX_PREFETCH_SIZE + 10) + "']",
        "//result/doc/float[@name='score']");
  }
}
