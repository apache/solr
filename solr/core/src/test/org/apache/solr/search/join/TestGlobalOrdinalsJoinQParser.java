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
package org.apache.solr.search.join;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestGlobalOrdinalsJoinQParser extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeTests() throws Exception {
    System.setProperty("solr.index.updatelog.enabled", "false");
    System.setProperty("solr.filterCache.async", "true");
    initCore("solrconfig-basic.xml", "schema-docValuesJoin.xml");
  }

  @Test
  public void testBasicJoin() throws Exception {
    clearIndex();

    assertU(add(doc("id", "1", "type_s", "parent", "join_s_dv", "P1", "name_s", "Parent 1")));
    assertU(add(doc("id", "2", "type_s", "parent", "join_s_dv", "P2", "name_s", "Parent 2")));
    assertU(add(doc("id", "3", "type_s", "parent", "join_s_dv", "P3", "name_s", "Parent 3")));

    assertU(
        add(
            doc(
                "id",
                "10",
                "type_s",
                "child",
                "join_s_dv",
                "P1",
                "child_name_s",
                "Child 1A",
                "skill_s",
                "java")));
    assertU(
        add(
            doc(
                "id",
                "11",
                "type_s",
                "child",
                "join_s_dv",
                "P1",
                "child_name_s",
                "Child 1B",
                "skill_s",
                "python")));
    assertU(
        add(
            doc(
                "id",
                "12",
                "type_s",
                "child",
                "join_s_dv",
                "P2",
                "child_name_s",
                "Child 2A",
                "skill_s",
                "java")));
    assertU(
        add(
            doc(
                "id",
                "13",
                "type_s",
                "child",
                "join_s_dv",
                "P3",
                "child_name_s",
                "Child 3A",
                "skill_s",
                "rust")));

    assertU(commit());

    // Query parent via child match using query body
    assertJQ(
        req(
            "q",
            "{!globalOrdinalsJoin joinField=join_s_dv which=\"type_s:parent\"}skill_s:python",
            "fl",
            "id"),
        "/response=={'numFound':1,'start':0,'numFoundExact':true,'docs':[{'id':'1'}]}");

    // Query parent via child match matching multiple parents
    assertJQ(
        req(
            "q",
            "{!globalOrdinalsJoin joinField=join_s_dv which=\"type_s:parent\"}skill_s:java",
            "fl",
            "id",
            "sort",
            "id asc"),
        "/response=={'numFound':2,'start':0,'numFoundExact':true,'docs':[{'id':'1'},{'id':'2'}]}");

    // Query parent via child match using 'v' local param
    assertJQ(
        req(
            "q",
            "{!globalOrdinalsJoin joinField=join_s_dv which=\"type_s:parent\" v=\"skill_s:rust\"}",
            "fl",
            "id"),
        "/response=={'numFound':1,'start':0,'numFoundExact':true,'docs':[{'id':'3'}]}");

    // Reverse join: Query children from parent match
    assertJQ(
        req(
            "q",
            "{!globalOrdinalsJoin joinField=join_s_dv which=\"type_s:child\"}name_s:\"Parent 1\"",
            "fl",
            "id",
            "sort",
            "id asc"),
        "/response=={'numFound':2,'start':0,'numFoundExact':true,'docs':[{'id':'10'},{'id':'11'}]}");
  }

  @Test
  public void testScoreModes() throws Exception {
    clearIndex();

    assertU(add(doc("id", "p1", "type_s", "parent", "join_s_dv", "GRP1", "title_t", "parent one")));
    assertU(add(doc("id", "p2", "type_s", "parent", "join_s_dv", "GRP2", "title_t", "parent two")));

    // Children for GRP1: c1 (score high), c2 (score low)
    assertU(
        add(
            doc(
                "id",
                "c1",
                "type_s",
                "child",
                "join_s_dv",
                "GRP1",
                "desc_t",
                "search search search search target")));
    assertU(
        add(
            doc(
                "id",
                "c2",
                "type_s",
                "child",
                "join_s_dv",
                "GRP1",
                "desc_t",
                "search random words target")));

    // Children for GRP2: c3 (score medium), c4 (score medium), c5 (score medium)
    assertU(
        add(
            doc(
                "id",
                "c3",
                "type_s",
                "child",
                "join_s_dv",
                "GRP2",
                "desc_t",
                "search other text target")));
    assertU(
        add(
            doc(
                "id",
                "c4",
                "type_s",
                "child",
                "join_s_dv",
                "GRP2",
                "desc_t",
                "search other text target")));
    assertU(
        add(
            doc(
                "id",
                "c5",
                "type_s",
                "child",
                "join_s_dv",
                "GRP2",
                "desc_t",
                "search other text target")));

    assertU(commit());

    // score=None (default)
    assertJQ(
        req(
            "q",
            "{!globalOrdinalsJoin joinField=join_s_dv which=\"type_s:parent\" score=None}desc_t:target",
            "fl",
            "id",
            "sort",
            "id asc"),
        "/response=={'numFound':2,'start':0,'numFoundExact':true,'docs':[{'id':'p1'},{'id':'p2'}]}");

    // score=Max: p1 max child is higher than p2
    assertJQ(
        req(
            "q",
            "{!globalOrdinalsJoin joinField=join_s_dv which=\"type_s:parent\" score=Max}desc_t:search",
            "fl",
            "id"),
        "/response/docs/[0]/id=='p1'");

    // score=Min: p2 min child (score > 0) vs p1 min child
    assertJQ(
        req(
            "q",
            "{!globalOrdinalsJoin joinField=join_s_dv which=\"type_s:parent\" score=Min}desc_t:search",
            "fl",
            "id,score"),
        "/response/numFound==2");

    // score=Total
    assertJQ(
        req(
            "q",
            "{!globalOrdinalsJoin joinField=join_s_dv which=\"type_s:parent\" score=Total}desc_t:target",
            "fl",
            "id,score"),
        "/response/numFound==2");
  }

  @Test
  public void testFilterAndComplexWhich() throws Exception {
    clearIndex();

    assertU(
        add(
            doc(
                "id",
                "p1",
                "type_s",
                "parent",
                "active_s",
                "yes",
                "join_s_dv",
                "GRP1",
                "name_s",
                "Parent 1")));
    assertU(
        add(
            doc(
                "id",
                "p2",
                "type_s",
                "parent",
                "active_s",
                "no",
                "join_s_dv",
                "GRP2",
                "name_s",
                "Parent 2")));
    assertU(add(doc("id", "c1", "type_s", "child", "join_s_dv", "GRP1", "skill_s", "java")));
    assertU(add(doc("id", "c2", "type_s", "child", "join_s_dv", "GRP2", "skill_s", "java")));
    assertU(commit());

    // Join as filter query (fq)
    assertJQ(
        req(
            "q",
            "*:*",
            "fq",
            "{!globalOrdinalsJoin joinField=join_s_dv which=\"type_s:parent\"}skill_s:java",
            "fl",
            "id",
            "sort",
            "id asc"),
        "/response=={'numFound':2,'start':0,'numFoundExact':true,'docs':[{'id':'p1'},{'id':'p2'}]}");

    // Complex 'which' condition (only active parents)
    assertJQ(
        req(
            "q",
            "{!globalOrdinalsJoin joinField=join_s_dv which=\"type_s:parent AND active_s:yes\"}skill_s:java",
            "fl",
            "id"),
        "/response=={'numFound':1,'start':0,'numFoundExact':true,'docs':[{'id':'p1'}]}");

    // 'which' with filter() syntax
    assertJQ(
        req(
            "q",
            "{!globalOrdinalsJoin joinField=join_s_dv which=\"filter(type_s:parent)\"}skill_s:java",
            "fl",
            "id",
            "sort",
            "id asc"),
        "/response=={'numFound':2,'start':0,'numFoundExact':true,'docs':[{'id':'p1'},{'id':'p2'}]}");
  }

  @Test
  public void testMultiSegmentAndSearcherRotation() throws Exception {
    clearIndex();

    // Segment 1
    assertU(add(doc("id", "1", "type_s", "parent", "join_s_dv", "P1")));
    assertU(add(doc("id", "10", "type_s", "child", "join_s_dv", "P1", "text_t", "common query")));
    assertU(commit());

    // Segment 2
    assertU(add(doc("id", "2", "type_s", "parent", "join_s_dv", "P2")));
    assertU(add(doc("id", "20", "type_s", "child", "join_s_dv", "P2", "text_t", "common query")));
    assertU(commit());

    // Segment 3
    assertU(add(doc("id", "3", "type_s", "parent", "join_s_dv", "P3")));
    assertU(add(doc("id", "30", "type_s", "child", "join_s_dv", "P3", "text_t", "uncommon query")));
    assertU(commit());

    // Multi-segment search
    assertJQ(
        req(
            "q",
            "{!globalOrdinalsJoin joinField=join_s_dv which=\"type_s:parent\"}text_t:common",
            "fl",
            "id",
            "sort",
            "id asc"),
        "/response=={'numFound':2,'start':0,'numFoundExact':true,'docs':[{'id':'1'},{'id':'2'}]}");

    // Add more docs and commit (searcher rotation)
    assertU(add(doc("id", "4", "type_s", "parent", "join_s_dv", "P4")));
    assertU(add(doc("id", "40", "type_s", "child", "join_s_dv", "P4", "text_t", "common query")));
    assertU(commit());

    assertJQ(
        req(
            "q",
            "{!globalOrdinalsJoin joinField=join_s_dv which=\"type_s:parent\"}text_t:common",
            "fl",
            "id",
            "sort",
            "id asc"),
        "/response=={'numFound':3,'start':0,'numFoundExact':true,'docs':[{'id':'1'},{'id':'2'},{'id':'4'}]}");

    // Optimize / forceMerge down to 1 segment (tests single segment index where OrdinalMap is null)
    assertU(optimize());

    assertJQ(
        req(
            "q",
            "{!globalOrdinalsJoin joinField=join_s_dv which=\"type_s:parent\"}text_t:common",
            "fl",
            "id",
            "sort",
            "id asc"),
        "/response=={'numFound':3,'start':0,'numFoundExact':true,'docs':[{'id':'1'},{'id':'2'},{'id':'4'}]}");
  }

  @Test
  public void testValidationErrors() {
    // Missing joinField
    SolrException ex =
        expectThrows(
            SolrException.class,
            () -> {
              h.query(req("q", "{!globalOrdinalsJoin which=\"type_s:parent\"}text_t:test"));
            });
    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, ex.code());
    assertTrue(ex.getMessage().contains("joinField"));

    // Unknown field
    ex =
        expectThrows(
            SolrException.class,
            () -> {
              h.query(
                  req(
                      "q",
                      "{!globalOrdinalsJoin joinField=non_existent_field which=\"type_s:parent\"}text_t:test"));
            });
    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, ex.code());
    assertTrue(ex.getMessage().contains("non_existent_field"));

    // Multi-valued field
    ex =
        expectThrows(
            SolrException.class,
            () -> {
              h.query(
                  req(
                      "q",
                      "{!globalOrdinalsJoin joinField=dept_ss_dv which=\"type_s:parent\"}text_t:test"));
            });
    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, ex.code());
    assertTrue(ex.getMessage().contains("single-valued"));

    // Non-docValues field
    ex =
        expectThrows(
            SolrException.class,
            () -> {
              h.query(
                  req(
                      "q",
                      "{!globalOrdinalsJoin joinField=id which=\"type_s:parent\"}text_t:test"));
            });
    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, ex.code());
    assertTrue(ex.getMessage().contains("docValues"));

    // Non-string field (e.g. numeric docValues)
    ex =
        expectThrows(
            SolrException.class,
            () -> {
              h.query(
                  req(
                      "q",
                      "{!globalOrdinalsJoin joinField=cat_i_dv which=\"type_s:parent\"}text_t:test"));
            });
    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, ex.code());
    assertTrue(ex.getMessage().contains("must be a String field"));

    // Missing which parameter
    ex =
        expectThrows(
            SolrException.class,
            () -> {
              h.query(req("q", "{!globalOrdinalsJoin joinField=join_s_dv}text_t:test"));
            });
    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, ex.code());
    assertTrue(ex.getMessage().contains("which"));

    // Invalid score mode
    ex =
        expectThrows(
            SolrException.class,
            () -> {
              h.query(
                  req(
                      "q",
                      "{!globalOrdinalsJoin joinField=join_s_dv which=\"type_s:parent\" score=UnknownScore}text_t:test"));
            });
    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, ex.code());
    assertTrue(ex.getMessage().contains("UnknownScore"));
  }

  @Test
  public void testEmptyMatchesAndUnpopulatedField() throws Exception {
    clearIndex();

    assertU(add(doc("id", "1", "type_s", "parent", "join_s_dv", "P1")));
    assertU(add(doc("id", "2", "type_s", "child", "join_s_dv", "P1", "skill_s", "java")));
    assertU(commit());

    // fromQuery matches nothing
    assertJQ(
        req(
            "q",
            "{!globalOrdinalsJoin joinField=join_s_dv which=\"type_s:parent\"}skill_s:nonexistent",
            "fl",
            "id"),
        "/response=={'numFound':0,'start':0,'numFoundExact':true,'docs':[]}");

    // toQuery matches nothing
    assertJQ(
        req(
            "q",
            "{!globalOrdinalsJoin joinField=join_s_dv which=\"type_s:nonexistent\"}skill_s:java",
            "fl",
            "id"),
        "/response=={'numFound':0,'start':0,'numFoundExact':true,'docs':[]}");

    // joinField has no doc values in entire index (e.g. dynamic field other_s_dv not populated in
    // any doc)
    assertJQ(
        req(
            "q",
            "{!globalOrdinalsJoin joinField=other_s_dv which=\"type_s:parent\"}skill_s:java",
            "fl",
            "id"),
        "/response=={'numFound':0,'start':0,'numFoundExact':true,'docs':[]}");
  }
}
