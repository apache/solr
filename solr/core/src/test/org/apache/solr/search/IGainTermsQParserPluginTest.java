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

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.solr.SolrTestCase;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.request.SolrQuery;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.util.EmbeddedSolrServerTestRule;
import org.apache.solr.util.SolrClientTestRule;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

/** Tests the {@code {!igain}} query parser's response sections. */
public class IGainTermsQParserPluginTest extends SolrTestCase {

  @ClassRule
  public static final SolrClientTestRule solrTestRule =
      new EmbeddedSolrServerTestRule() {
        @Override
        protected void before() {
          startSolr();
        }
      };

  /** Documents with their outcome (1 = positive); the text field is whitespace tokenized. */
  private static final Map<String, Integer> DOCS =
      Map.of(
          "red red blue", 1,
          "red green", 1,
          "red", 1,
          "blue green", 0,
          "blue", 0,
          "green yellow", 0);

  @BeforeClass
  public static void setUpCollection() throws Exception {
    // the minimal solrconfig reads the randomized index settings from system properties
    SolrTestCaseJ4.newRandomConfig();
    Path configSet = createTempDir();
    SolrTestCaseJ4.copyMinConf(configSet);
    Path schemaXml = configSet.resolve("conf/schema.xml");
    Files.writeString(
        schemaXml,
        Files.readString(schemaXml)
            .replace(
                "</schema>",
                "<fieldType name=\"pint\" class=\"solr.IntPointField\" docValues=\"true\"/>\n"
                    + "<field name=\"outcome\" type=\"pint\" indexed=\"true\" stored=\"true\"/>\n"
                    + "</schema>"));
    solrTestRule.newCollection().withConfigSet(configSet).create();

    SolrClient client = solrTestRule.getSolrClient();
    int id = 0;
    for (Map.Entry<String, Integer> doc : DOCS.entrySet()) {
      client.add(
          SolrTestCaseJ4.sdoc(
              "id", Integer.toString(id++), "text", doc.getKey(), "outcome", doc.getValue()));
    }
    client.commit();
  }

  @Test
  public void testDocFreqHasOneEntryPerTerm() throws Exception {
    SolrQuery query =
        new SolrQuery("*:*")
            .setRows(0)
            .addFilterQuery("{!igain field=text positiveLabel=1 outcome=outcome numTerms=2}");
    NamedList<Object> response = solrTestRule.getSolrClient().query(query).getResponse();

    @SuppressWarnings("unchecked")
    NamedList<Double> featuredTerms = (NamedList<Double>) response.get("featuredTerms");
    @SuppressWarnings("unchecked")
    NamedList<Integer> docFreq = (NamedList<Integer>) response.get("docFreq");
    assertEquals(DOCS.size(), response.get("numDocs"));
    assertEquals(2, featuredTerms.size());

    Set<String> names = new HashSet<>();
    for (Map.Entry<String, Integer> entry : docFreq) {
      assertTrue("docFreq repeats " + entry.getKey(), names.add(entry.getKey()));
      assertNotNull("docFreq has no value for " + entry.getKey(), entry.getValue());
    }
    for (Map.Entry<String, Double> featured : featuredTerms) {
      String term = featured.getKey();
      long expected = DOCS.keySet().stream().filter(text -> text.contains(term)).count();
      assertEquals("docFreq of featured term " + term, (int) expected, (int) docFreq.get(term));
    }
  }
}
