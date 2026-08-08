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
package org.apache.solr.search.function;

import org.apache.solr.SolrTestCaseJ4;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestMatchSetScaleFloatFunction extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-functionquery.xml", "schema11.xml");
  }

  @Test
  public void testLinearTransform_globalBounds() throws Exception {
    clearIndex();
    assertU(adoc("id", "1", "foo_f", "0"));
    assertU(adoc("id", "2", "foo_f", "10"));
    assertU(adoc("id", "3", "foo_f", "20"));
    assertU(adoc("id", "4", "foo_f", "30"));
    assertU(adoc("id", "5", "foo_f", "40"));
    assertU(commit());

    // No filter — bounds are min=0, max=40 across matching set (= all docs).
    // matchset_scale(v, 0, 1) = (v - 0) / 40
    assertQ(
        req("q", "{!func}matchset_scale(foo_f,0,1)", "fl", "id,score", "rows", "10"),
        "//doc[./str[@name='id']='1']/float[@name='score'][.='0.0']",
        "//doc[./str[@name='id']='3']/float[@name='score'][.='0.5']",
        "//doc[./str[@name='id']='5']/float[@name='score'][.='1.0']");
  }

  @Test
  public void testBoundsScopedToMatchingSet() throws Exception {
    clearIndex();
    // Broad value range across index, but filter will restrict.
    assertU(adoc("id", "1", "foo_f", "0", "cat_s", "A"));
    assertU(adoc("id", "2", "foo_f", "100", "cat_s", "A"));
    assertU(adoc("id", "3", "foo_f", "200", "cat_s", "A"));
    assertU(adoc("id", "4", "foo_f", "1000", "cat_s", "B"));
    assertU(adoc("id", "5", "foo_f", "2000", "cat_s", "B"));
    assertU(commit());

    // Scoped to cat_s:A → matching set values {0, 100, 200}.
    // matchset_scale(v, 0, 10) = (v - 0) * 10 / 200.
    // Critical: if bounds were global (0..2000), score for id=3 would be 1.0, not 10.0.
    assertQ(
        req(
            "q",
            "{!func}matchset_scale(foo_f,0,10)",
            "fq",
            "cat_s:A",
            "fl",
            "id,score",
            "rows",
            "10"),
        "//doc[./str[@name='id']='1']/float[@name='score'][.='0.0']",
        "//doc[./str[@name='id']='2']/float[@name='score'][.='5.0']",
        "//doc[./str[@name='id']='3']/float[@name='score'][.='10.0']");

    // And scoped to cat_s:B → matching set {1000, 2000}.
    // matchset_scale(v, 0, 10) = (v - 1000) * 10 / 1000.
    assertQ(
        req(
            "q",
            "{!func}matchset_scale(foo_f,0,10)",
            "fq",
            "cat_s:B",
            "fl",
            "id,score",
            "rows",
            "10"),
        "//doc[./str[@name='id']='4']/float[@name='score'][.='0.0']",
        "//doc[./str[@name='id']='5']/float[@name='score'][.='10.0']");
  }

  @Test
  public void testDivideByZeroGuard_allEqualValues() throws Exception {
    clearIndex();
    assertU(adoc("id", "1", "foo_f", "42"));
    assertU(adoc("id", "2", "foo_f", "42"));
    assertU(adoc("id", "3", "foo_f", "42"));
    assertU(commit());

    // min == max → avoid NaN/Inf; every matching doc gets targetMin.
    assertQ(
        req("q", "{!func}matchset_scale(foo_f,7,99)", "fl", "id,score", "rows", "10"),
        "//doc[./str[@name='id']='1']/float[@name='score'][.='7.0']",
        "//doc[./str[@name='id']='2']/float[@name='score'][.='7.0']",
        "//doc[./str[@name='id']='3']/float[@name='score'][.='7.0']");
  }

  @Test
  public void testCustomTargetRange() throws Exception {
    clearIndex();
    assertU(adoc("id", "1", "foo_f", "10"));
    assertU(adoc("id", "2", "foo_f", "20"));
    assertU(adoc("id", "3", "foo_f", "30"));
    assertU(commit());

    // Bounds: min=10, max=30. Target: [2, 8].
    // v=10 → 2, v=20 → 5, v=30 → 8.
    // Note: targetMin must be >= 0 when using matchset_scale as a top-level q, because
    // Lucene clamps negative query scores to 0. This is a Lucene constraint, not a
    // matchset_scale constraint — in a nested expression (fl / boost), negative outputs
    // pass through fine.
    assertQ(
        req("q", "{!func}matchset_scale(foo_f,2,8)", "fl", "id,score", "rows", "10"),
        "//doc[./str[@name='id']='1']/float[@name='score'][.='2.0']",
        "//doc[./str[@name='id']='2']/float[@name='score'][.='5.0']",
        "//doc[./str[@name='id']='3']/float[@name='score'][.='8.0']");
  }
}
