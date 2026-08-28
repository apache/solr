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

import static org.apache.solr.SolrTestCaseJ4.DEFAULT_TEST_COLLECTION_NAME;
import static org.apache.solr.SolrTestCaseJ4.assertFieldValues;
import static org.apache.solr.SolrTestCaseJ4.configset;
import static org.apache.solr.SolrTestCaseJ4.params;

import java.nio.file.Path;
import java.util.List;
import org.apache.commons.io.file.PathUtils;
import org.apache.solr.SolrTestCase;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.search.join.aijoin.AuxIndexManager;
import org.apache.solr.util.EmbeddedSolrServerTestRule;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

/**
 * Exercises {@link AuxIndexJoinQParserPlugin}, modeled on {@link TestScoreJoinQPNoScore}'s
 * same-core {@code {!join}} coverage plus {@link org.apache.solr.TestCrossCoreJoin}'s cross-core
 * setup, since cross-core joins -- not same-core -- are the reason {@link AuxIndexManager} exists.
 *
 * <p>Every join here goes from the "many" side (an employee, single-valued FK) to the "few" side
 * (that employee's department, a value unique per to-doc): {@code AIJoinUtil#computeDocMapping} is,
 * per its own javadoc, "always single-valued until M:N pairs are supported" -- it keeps exactly one
 * to-doc per from-doc. That's exact for this direction (each employee has exactly one department),
 * but would silently drop matches for the reverse, one-department-to-many-employees direction, so
 * this test doesn't exercise that one.
 *
 * <p>Both cores use version 1.7 schemas ({@code configsets/aijoin}, shared with {@link
 * org.apache.solr.cloud.DistribAIJoinFromCollectionTest}, and {@code configsets/minimal}), where
 * docValues default to true: {@link AuxIndexManager} reads real per-segment {@link
 * org.apache.lucene.index.SortedSetDocValues} directly, unlike {@link ScoreJoinQParserPlugin} (via
 * {@link org.apache.lucene.search.join.JoinUtil}) which tolerates uninverted fields too.
 */
public class TestAuxIndexJoinQParserPlugin extends SolrTestCase {

  private static final String FROM_CORE = "aijoinFromCore";

  @ClassRule
  public static final EmbeddedSolrServerTestRule solrRule = new EmbeddedSolrServerTestRule();

  @BeforeClass
  public static void beforeTests() throws Exception {
    solrRule.startSolr();

    // the aijoin configset declares no schemaFactory, so loading it file-based would let the
    // default managed-schema factory rewrite the source-tree conf dir; work on a temp copy
    Path aijoinConfigSet = createTempDir("aijoin");
    PathUtils.copyDirectory(configset("aijoin"), aijoinConfigSet.resolve("conf"));
    solrRule.newCollection().withConfigSet(aijoinConfigSet).create();

    // "minimal" uses ClassicIndexSchemaFactory, which never writes, so it's safe to use in place
    solrRule.newCollection(FROM_CORE).withConfigSet(configset("minimal")).create();

    SolrClient toSide = solrRule.getSolrClient();

    // departments: the "to"/unique side of every join below, one doc per dept_id_s value
    toSide.add(
        List.of(
            doc("id", "10", "dept_id_s", "Engineering", "text_s", "These guys develop stuff"),
            doc("id", "11", "dept_id_s", "Marketing", "text_s", "These guys make you look good"),
            doc("id", "12", "dept_id_s", "Sales", "text_s", "These guys sell stuff"),
            doc("id", "13", "dept_id_s", "Support", "text_s", "These guys help customers")));

    // employees: the "from"/many side, each with exactly one dept_s value
    toSide.add(employees());
    toSide.commit();

    // aijoinFromCore holds the same employees, for the cross-core variant of the same joins
    SolrClient fromSide = solrRule.getSolrClient(FROM_CORE);
    fromSide.add(employees());
    fromSide.commit();
  }

  private static List<SolrInputDocument> employees() {
    return List.of(
        doc("id", "1", "name_s", "john", "title_s", "Director", "dept_s", "Engineering"),
        doc("id", "2", "name_s", "mark", "title_s", "VP", "dept_s", "Marketing"),
        doc("id", "3", "name_s", "nancy", "title_s", "MTS", "dept_s", "Sales"),
        doc("id", "4", "name_s", "dave", "title_s", "MTS", "dept_s", "Support"),
        doc("id", "5", "name_s", "tina", "title_s", "VP", "dept_s", "Engineering"));
  }

  private static SolrInputDocument doc(String... fieldsAndValues) {
    return new SolrInputDocument(fieldsAndValues);
  }

  /** Queries the main core and asserts the matches' ids, in index (docid) order. */
  private static void assertJoin(SolrParams query, String... expectedIds) throws Exception {
    SolrDocumentList results = solrRule.getSolrClient().query(query).getResults();
    assertEquals(results.toString(), expectedIds.length, results.getNumFound());
    assertFieldValues(results, "id", (Object[]) expectedIds);
  }

  @Test
  public void testMissingLocalParams() {
    SolrException e =
        expectThrows(
            SolrException.class,
            () -> solrRule.getSolrClient().query(params("q", "{!auxIndexJoin to=dept_id_s}*:*")));
    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, e.code());
    assertTrue(e.getMessage(), e.getMessage().contains("requires"));
  }

  @Test
  public void testSameCoreJoin() throws Exception {
    // nancy and dave (title MTS) each belong to exactly one dept: Sales and Support
    assertJoin(
        params("q", "{!auxIndexJoin from=dept_s to=dept_id_s}title_s:MTS", "fl", "id"), "12", "13");

    // from-side subordinate query matches nothing
    assertJoin(
        params("q", "{!auxIndexJoin from=dept_s to=dept_id_s}name_s:nosuchperson", "fl", "id"));

    // to-side field has real values, but none equal to any matched from-side value
    // ("Engineering" is a dept_s value, but no title_s is ever "Engineering")
    assertJoin(params("q", "{!auxIndexJoin from=dept_s to=title_s}name_s:john", "fl", "id"));

    // a single from-doc resolves to exactly its one department
    assertJoin(
        params("q", "{!auxIndexJoin from=dept_s to=dept_id_s}name_s:john", "fl", "id"), "10");

    // variable deref for sub-query parsing, plus defType local param
    assertJoin(
        params(
            "q", "{!auxIndexJoin from=dept_s to=dept_id_s v=$qq}",
            "qq", "{!dismax qf=name_s}dave",
            "fl", "id"),
        "13");

    // fq on the to-side (department) is pushed down alongside the join, mirroring SOLR-3062 for
    // {!join}: john and tina (title VP) join to Engineering and Marketing, fq narrows to just one
    assertJoin(
        params(
            "q", "{!auxIndexJoin from=dept_s to=dept_id_s}title_s:VP",
            "fl", "id",
            "fq", "dept_id_s:Engineering"),
        "10");
  }

  @Test
  public void testCrossCoreJoin() throws Exception {
    // nancy and dave (title MTS) live in aijoinFromCore; their departments live in this core
    assertJoin(
        params(
            "q",
            "{!auxIndexJoin from=dept_s to=dept_id_s fromIndex=" + FROM_CORE + "}title_s:MTS",
            "fl",
            "id"),
        "12",
        "13");

    // fq on the querying (to) core is still pushed down alongside the cross-core join
    assertJoin(
        params(
            "q",
            "{!auxIndexJoin from=dept_s to=dept_id_s fromIndex=" + FROM_CORE + "}title_s:VP",
            "fl",
            "id",
            "fq",
            "dept_id_s:Engineering"),
        "10");

    // the cross-core join must not leak a close on either core
    CoreContainer coreContainer = solrRule.getCoreContainer();
    try (SolrCore fromCore = coreContainer.getCore(FROM_CORE);
        SolrCore toCore = coreContainer.getCore(DEFAULT_TEST_COLLECTION_NAME)) {
      assertFalse(fromCore.isClosed());
      assertFalse(toCore.isClosed());
    }
  }
}
