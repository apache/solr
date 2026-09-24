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
package org.apache.solr.handler.export;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.util.Utils;
import org.apache.solr.index.LogDocMergePolicyFactory;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * SOLR-8291: {@code useFilterForSortedQuery=true} sends {@code /export} through {@code sortDocSet},
 * which never calls {@code ExportCollector.getLeafCollector} for trailing unmatched leaves. The
 * writer must treat those null bitsets as empty leaves instead of constructing a {@code
 * BitSetIterator}.
 */
public class TestExportWriterUseFilterForSortedQuery extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeClass() throws Exception {
    systemSetPropertySolrTestsMergePolicyFactory(LogDocMergePolicyFactory.class.getName());
    initCore("solrconfig-export-usefilter.xml", "schema-sortingresponse.xml");
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    assertU(delQ("*:*"));
    assertU(commit());
  }

  @Test
  public void testZeroHitsDoesNotNpe() throws Exception {
    assertU(adoc("id", "1"));
    assertU(commit());

    String resp =
        h.query(req("q", "id:does-not-exist", "qt", "/export", "fl", "id", "sort", "id asc"));
    assertJsonEquals(
        resp, "{\"responseHeader\":{\"status\":0},\"response\":{\"numFound\":0,\"docs\":[]}}");
  }

  @Test
  public void testHitsOnlyInEarlierSegment() throws Exception {
    assertU(adoc("id", "1"));
    assertU(commit());
    assertU(adoc("id", "2"));
    assertU(commit());

    String resp = h.query(req("q", "id:1", "qt", "/export", "fl", "id", "sort", "id asc"));
    assertJsonEquals(
        resp,
        "{\"responseHeader\":{\"status\":0},\"response\":{\"numFound\":1,\"docs\":[{\"id\":\"1\"}]}}");

    // Null must mean "this leaf is empty", not "skip the rest of the index".
    resp = h.query(req("q", "*:*", "qt", "/export", "fl", "id", "sort", "id asc"));
    assertJsonEquals(
        resp,
        "{\"responseHeader\":{\"status\":0},\"response\":{\"numFound\":2,\"docs\":[{\"id\":\"1\"},{\"id\":\"2\"}]}}");
  }

  private void assertJsonEquals(String actual, String expected) {
    assertEquals(
        Utils.toJSONString(Utils.fromJSONString(expected)),
        Utils.toJSONString(Utils.fromJSONString(actual)));
  }
}
