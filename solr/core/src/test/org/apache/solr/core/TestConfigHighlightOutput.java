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
package org.apache.solr.core;

import java.util.List;
import java.util.Map;
import org.apache.solr.SolrTestCase;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.util.Utils;
import org.apache.solr.util.EmbeddedSolrServerTestRule;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

/** Tests that the serialized config, as returned by the Config API, includes the highlighter. */
public class TestConfigHighlightOutput extends SolrTestCase {

  @ClassRule
  public static final EmbeddedSolrServerTestRule solrTestRule = new EmbeddedSolrServerTestRule();

  @BeforeClass
  public static void beforeClass() throws Exception {
    solrTestRule.startSolr(SolrTestCaseJ4.TEST_HOME());
    // Sets the randomized solr.tests.* properties the collection1 solrconfig.xml requires.
    SolrTestCaseJ4.newRandomConfig();
    solrTestRule
        .newCollection()
        .withConfigSet(SolrTestCaseJ4.TEST_COLL1_CONF())
        .withSchemaFile("schema.xml")
        .create();
  }

  @Test
  public void testHighlightComponentIsWrittenWithItsChildren() throws Exception {
    Object config;
    try (SolrCore core =
        solrTestRule.getCoreContainer().getCore(SolrTestCaseJ4.DEFAULT_TEST_CORENAME)) {
      config = Utils.fromJSONString(core.getSolrConfig().jsonStr());
    }
    Object highlighting =
        Utils.getObjectByPath(
            config, false, List.of("searchComponent", "highlight", "highlighting"));
    assertTrue("highlighting must be written as an object", highlighting instanceof Map);

    // the child types are keys, even though several children share the name "simple"
    for (String type :
        List.of(
            "fragmenter", "formatter", "fragListBuilder", "fragmentsBuilder", "boundaryScanner")) {
      assertNotNull("missing child type " + type, ((Map<?, ?>) highlighting).get(type));
    }
    assertFalse(((Map<?, ?>) highlighting).containsKey("simple"));

    assertEquals(2, ((List<?>) ((Map<?, ?>) highlighting).get("fragmenter")).size());
    assertEquals(2, ((List<?>) ((Map<?, ?>) highlighting).get("fragmentsBuilder")).size());
    assertEquals(2, ((List<?>) ((Map<?, ?>) highlighting).get("boundaryScanner")).size());
    assertEquals("html", ((Map<?, ?>) ((Map<?, ?>) highlighting).get("formatter")).get("name"));
    assertEquals(
        "simple", ((Map<?, ?>) ((Map<?, ?>) highlighting).get("fragListBuilder")).get("name"));
  }
}
