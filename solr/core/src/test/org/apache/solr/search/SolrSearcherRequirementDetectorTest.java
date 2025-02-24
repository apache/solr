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

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.solr.SolrTestCase;
import org.apache.solr.search.join.GraphQuery;
import org.junit.Test;

/** Unit tests for {@link SolrSearcherRequirementDetector} */
public class SolrSearcherRequirementDetectorTest extends SolrTestCase {

  @Test
  public void testDetectsWhenQueriesDontRequireSolrSearcher() {
    final var termQuery = new TermQuery(new Term("someField", "someFieldValue"));
    final var termQuery2 = new TermQuery(new Term("someField", "someOtherFieldValue"));

    var needsSearcherDetector = new SolrSearcherRequirementDetector();
    termQuery.visit(needsSearcherDetector);
    assertFalse(needsSearcherDetector.getRequiresSolrSearcher());

    final var boolQuery =
        new BooleanQuery.Builder()
            .add(termQuery, BooleanClause.Occur.MUST)
            .add(termQuery2, BooleanClause.Occur.SHOULD)
            .build();
    needsSearcherDetector = new SolrSearcherRequirementDetector();
    boolQuery.visit(needsSearcherDetector);
    assertFalse(needsSearcherDetector.getRequiresSolrSearcher());
  }

  @Test
  public void testDetectsWhenQueriesDoRequireSolrSearcher_TopLevel() {
    final var termQuery = new TermQuery(new Term("someField", "someFieldValue"));

    var needsSearcherDetector = new SolrSearcherRequirementDetector();
    final var joinQuery = new JoinQuery("fromField", "toField", "someCoreName", termQuery);
    assertThat(
        joinQuery,
        instanceOf(SolrSearcherRequirer.class)); // Ensure that JoinQuery still requires SIS
    joinQuery.visit(needsSearcherDetector);
    assertTrue(needsSearcherDetector.getRequiresSolrSearcher());

    needsSearcherDetector = new SolrSearcherRequirementDetector();
    final var graphQuery = new GraphQuery(termQuery, "fromField", "toField");
    assertThat(
        graphQuery,
        instanceOf(SolrSearcherRequirer.class)); // Ensure that GraphQuery still requires SIS
    graphQuery.visit(needsSearcherDetector);
    assertTrue(needsSearcherDetector.getRequiresSolrSearcher());
  }

  @Test
  public void testDeteectsWhenQueriesDoRequireSolrSearcher_Nested() {
    final var termQuery = new TermQuery(new Term("someField", "someFieldValue"));
    final var termQuery2 = new TermQuery(new Term("someField", "someOtherFieldValue"));
    final var joinQuery = new JoinQuery("fromField", "toField", "someCoreName", termQuery);
    final var boolQuery =
        new BooleanQuery.Builder()
            .add(new BooleanClause(termQuery, BooleanClause.Occur.MUST))
            .add(new BooleanClause(termQuery2, BooleanClause.Occur.SHOULD))
            .add(new BooleanClause(joinQuery, BooleanClause.Occur.SHOULD))
            .build();

    final var needsSearcherDetector = new SolrSearcherRequirementDetector();
    // Top level query and some leaves don't require SIS, but JoinQuery does.
    assertThat(boolQuery, not(instanceOf(SolrSearcherRequirer.class)));
    assertThat(termQuery, not(instanceOf(SolrSearcherRequirer.class)));
    assertThat(joinQuery, instanceOf(SolrSearcherRequirer.class));
    boolQuery.visit(needsSearcherDetector);
    assertTrue(needsSearcherDetector.getRequiresSolrSearcher());
  }
}
