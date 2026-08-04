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

import java.util.List;
import java.util.Map;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Exercises {@link AIJoinQParserPlugin}, modeled on {@link TestScoreJoinQPNoScore}'s same-core
 * {@code {!join}} coverage plus {@link org.apache.solr.TestCrossCoreJoin}'s cross-core setup, since
 * cross-core joins -- not same-core -- are the reason {@link
 * org.apache.solr.search.join.aijoin.AIJoinIndex} exists.
 *
 * <p>Every join here goes from the "many" side (an employee, single-valued FK) to the "few" side
 * (that employee's department, a value unique per to-doc): {@code AIJoinUtil#computeDocMapping} is,
 * per its own javadoc, "always single-valued until M:N pairs are supported" -- it keeps exactly one
 * to-doc per from-doc. That's exact for this direction (each employee has exactly one department),
 * but would silently drop matches for the reverse, one-department-to-many-employees direction, so
 * this test doesn't exercise that one.
 *
 * <p>Join fields use the {@code *_s_dv} docValues companions that {@code schema-docValuesJoin.xml}
 * copies {@code *_s} into, rather than the plain fields: {@link
 * org.apache.solr.search.join.aijoin.AIJoinIndex} reads real per-segment {@link
 * org.apache.lucene.index.SortedSetDocValues} directly, unlike {@link ScoreJoinQParserPlugin} (via
 * {@link org.apache.lucene.search.join.JoinUtil}) which tolerates uninverted fields too.
 */
public class TestAIJoinQParserPlugin extends SolrTestCaseJ4 {

  private static SolrCore fromCore;
  private static EmbeddedSolrServer fromServer;

  @BeforeClass
  public static void beforeTests() throws Exception {
    initCore("solrconfig-aijoin.xml", "schema-docValuesJoin.xml");

    // departments: the "to"/unique side of every join below, one doc per dept_id_s value
    assertU(add(doc("id", "10", "dept_id_s", "Engineering", "text_t", "These guys develop stuff")));
    assertU(
        add(doc("id", "11", "dept_id_s", "Marketing", "text_t", "These guys make you look good")));
    assertU(add(doc("id", "12", "dept_id_s", "Sales", "text_t", "These guys sell stuff")));
    assertU(add(doc("id", "13", "dept_id_s", "Support", "text_t", "These guys help customers")));

    // employees: the "from"/many side, each with exactly one dept_s value
    assertU(add(doc("id", "1", "name_s", "john", "title_s", "Director", "dept_s", "Engineering")));
    assertU(add(doc("id", "2", "name_s", "mark", "title_s", "VP", "dept_s", "Marketing")));
    assertU(add(doc("id", "3", "name_s", "nancy", "title_s", "MTS", "dept_s", "Sales")));
    assertU(add(doc("id", "4", "name_s", "dave", "title_s", "MTS", "dept_s", "Support")));
    assertU(add(doc("id", "5", "name_s", "tina", "title_s", "VP", "dept_s", "Engineering")));

    assertU(commit());

    // aijoinFromCore holds the same employees, for the cross-core variant of the same joins
    CoreContainer coreContainer = h.getCoreContainer();
    fromCore = coreContainer.create("aijoinFromCore", Map.of("configSet", "minimal"));
    fromServer = new EmbeddedSolrServer(fromCore.getCoreContainer(), fromCore.getName());

    List<SolrInputDocument> docs =
        sdocs(
            sdoc("id", "1", "name_s", "john", "title_s", "Director", "dept_s", "Engineering"),
            sdoc("id", "2", "name_s", "mark", "title_s", "VP", "dept_s", "Marketing"),
            sdoc("id", "3", "name_s", "nancy", "title_s", "MTS", "dept_s", "Sales"),
            sdoc("id", "4", "name_s", "dave", "title_s", "MTS", "dept_s", "Support"),
            sdoc("id", "5", "name_s", "tina", "title_s", "VP", "dept_s", "Engineering"));
    fromServer.add(docs);
    fromServer.commit();
  }

  @AfterClass
  public static void nukeAll() {
    fromCore = null;
    fromServer = null;
  }

  @Test
  public void testMissingLocalParams() throws Exception {
    assertQEx(
        "aijoin requires 'from' and 'to'",
        "requires",
        req("q", "{!aijoin to=dept_id_s_dv}*:*"),
        SolrException.ErrorCode.BAD_REQUEST);
  }

  @Test
  public void testSameCoreJoin() throws Exception {
    // nancy and dave (title MTS) each belong to exactly one dept: Sales and Support
    assertJQ(
        req("q", "{!aijoin from=dept_s_dv to=dept_id_s_dv}title_s:MTS", "fl", "id"),
        "/response=={'numFound':2,'start':0,'numFoundExact':true,'docs':[{'id':'12'},{'id':'13'}]}");

    // from-side subordinate query matches nothing
    assertJQ(
        req("q", "{!aijoin from=dept_s_dv to=dept_id_s_dv}name_s:nosuchperson", "fl", "id"),
        "/response=={'numFound':0,'start':0,'numFoundExact':true,'docs':[]}");

    // to-side field has real values, but none equal to any matched from-side value
    // ("Engineering" is a dept_s_dv value, but no title_s_dv is ever "Engineering")
    assertJQ(
        req("q", "{!aijoin from=dept_s_dv to=title_s_dv}name_s:john", "fl", "id"),
        "/response=={'numFound':0,'start':0,'numFoundExact':true,'docs':[]}");

    // a single from-doc resolves to exactly its one department
    assertJQ(
        req("q", "{!aijoin from=dept_s_dv to=dept_id_s_dv}name_s:john", "fl", "id"),
        "/response=={'numFound':1,'start':0,'numFoundExact':true,'docs':[{'id':'10'}]}");

    // variable deref for sub-query parsing, plus defType local param
    assertJQ(
        req(
            "q", "{!aijoin from=dept_s_dv to=dept_id_s_dv v=$qq}",
            "qq", "{!dismax qf=name_s}dave",
            "fl", "id"),
        "/response=={'numFound':1,'start':0,'numFoundExact':true,'docs':[{'id':'13'}]}");

    // fq on the to-side (department) is pushed down alongside the join, mirroring SOLR-3062 for
    // {!join}: john and tina (title VP) join to Engineering and Marketing, fq narrows to just one
    assertJQ(
        req(
            "q", "{!aijoin from=dept_s_dv to=dept_id_s_dv}title_s:VP",
            "fl", "id",
            "fq", "dept_id_s:Engineering"),
        "/response=={'numFound':1,'start':0,'numFoundExact':true,'docs':[{'id':'10'}]}");
  }

  @Test
  public void testCrossCoreJoin() throws Exception {
    // nancy and dave (title MTS) live in aijoinFromCore; their departments live in this core
    assertJQ(
        req(
            "q",
            "{!aijoin from=dept_s to=dept_id_s_dv fromIndex=aijoinFromCore}title_s:MTS",
            "fl",
            "id"),
        "/response=={'numFound':2,'start':0,'numFoundExact':true,'docs':[{'id':'12'},{'id':'13'}]}");

    // fq on the querying (to) core is still pushed down alongside the cross-core join
    assertJQ(
        req(
            "q",
            "{!aijoin from=dept_s to=dept_id_s_dv fromIndex=aijoinFromCore}title_s:VP",
            "fl",
            "id",
            "fq",
            "dept_id_s:Engineering"),
        "/response=={'numFound':1,'start':0,'numFoundExact':true,'docs':[{'id':'10'}]}");

    assertFalse(fromCore.isClosed());
    assertFalse(h.getCore().isClosed());
  }
}
