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

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.junit.BeforeClass;
import org.junit.Test;

public class FieldExistsQueryBadRequestTest extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeClass() throws Exception {
    System.setProperty(
        "solr.tests.CustomIntFieldType",
        Boolean.getBoolean(NUMERIC_POINTS_SYSPROP)
            ? "org.apache.solr.schema.IntPointPrefixActsAsRangeQueryFieldType"
            : "org.apache.solr.schema.TrieIntPrefixActsAsRangeQueryFieldType");
    initCore("solrconfig-basic.xml", "schema-customfield.xml");

    assertU(adoc("id", "1", "bad_exists", "alpha"));
    assertU(commit());
  }

  @Test
  public void qExistenceQueryReturnsBadRequest() {
    SolrException ex = expectThrows(SolrException.class, () -> h.query(req("q", "bad_exists:*")));

    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, ex.code());
    assertTrue(ex.getMessage().startsWith("FieldExistsQuery requires"));
  }

  @Test
  public void facetQueryExistenceQueryReturnsBadRequest() {
    SolrException ex =
        expectThrows(
            SolrException.class,
            () ->
                h.query(
                    req("q", "*:*", "rows", "0", "facet", "true", "facet.query", "bad_exists:*")));

    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, ex.code());
    assertTrue(ex.getMessage().startsWith("FieldExistsQuery requires"));
  }

  @Test
  public void groupedQueryExistenceQueryReturnsBadRequest() {
    SolrException ex =
        expectThrows(
            SolrException.class,
            () -> h.query(req("q", "bad_exists:*", "group", "true", "group.field", "id")));

    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, ex.code());
    assertTrue(ex.getMessage().startsWith("FieldExistsQuery requires"));
  }
}
