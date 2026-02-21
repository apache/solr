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
package org.apache.solr.mappings.search;

import com.carrotsearch.randomizedtesting.generators.RandomStrings;
import java.lang.invoke.MethodHandles;
import java.util.List;
import org.apache.lucene.search.Query;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.mappings.MappingsTestUtils;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrQueryRequestBase;
import org.apache.solr.search.QParser;
import org.apache.solr.search.QueryParsing;
import org.apache.solr.search.SyntaxError;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestMappingsQParserPlugin extends SolrTestCaseJ4 {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-mappings.xml", "schema-mappings.xml");
  }

  @Test
  public void testQParserKeySearch() {
    String queryStr = """
        q={!mappings f=multi_mapping key="key_1" value=*}
        """;
    ModifiableSolrParams localParams = new ModifiableSolrParams();
    localParams.add(MappingsQParserPlugin.FIELD_PARAM, "multi_mapping");
    localParams.add(QueryParsing.TYPE, "mappings");
    localParams.add(MappingsQParserPlugin.KEY_PARAM, "key_1");
    localParams.add(MappingsQParserPlugin.VALUE_PARAM, "*");

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add(CommonParams.Q, queryStr);

    MappingsQParserPlugin parserPlugin = new MappingsQParserPlugin();

    SolrQueryRequest req = new SolrQueryRequestBase(h.getCore(), params) {};

    QParser parser = parserPlugin.createParser(queryStr, localParams, params, req);

    try {
      Query query = parser.parse();
      if (log.isInfoEnabled()) {
        log.info(query.toString());
      }
      Assert.assertEquals(
          "+FieldExistsQuery [field=multi_mapping] +multi_mapping_key___string:key_1 +multi_mapping_value___string:{* TO *}",
          query.toString());
    } catch (SyntaxError e) {
      Assert.fail("Should not throw SyntaxError");
    }
  }

  @Test
  public void testQParserValueSearch() {
    String queryStr = """
        q={!mappings f=float_mapping value=12.34}
        """;
    ModifiableSolrParams localParams = new ModifiableSolrParams();
    localParams.add(MappingsQParserPlugin.FIELD_PARAM, "float_mapping");
    localParams.add(QueryParsing.TYPE, "mappings");
    localParams.add(MappingsQParserPlugin.VALUE_PARAM, "12.34");

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add(CommonParams.Q, queryStr);

    MappingsQParserPlugin parserPlugin = new MappingsQParserPlugin();

    SolrQueryRequest req = new SolrQueryRequestBase(h.getCore(), params) {};

    QParser parser = parserPlugin.createParser(queryStr, localParams, params, req);

    try {
      Query query = parser.parse();
      if (log.isInfoEnabled()) {
        log.info(query.toString());
      }
      Assert.assertEquals(
          "+FieldExistsQuery [field=float_mapping] +float_mapping_value___float:[12.34 TO 12.34]",
          query.toString());
    } catch (SyntaxError e) {
      Assert.fail("Should not throw SyntaxError");
    }
  }

  @Test
  public void testQParserKeySearchAnyMapping() {
    String queryStr = """
        q={!mappings key="key_1"}
        """;
    ModifiableSolrParams localParams = new ModifiableSolrParams();
    localParams.add(QueryParsing.TYPE, "mappings");
    // use a date as key, to have a parseable key for "date_str_mapping"
    localParams.add(MappingsQParserPlugin.KEY_PARAM, "2025-12-08T00:00:00Z");

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add(CommonParams.Q, queryStr);

    MappingsQParserPlugin parserPlugin = new MappingsQParserPlugin();

    SolrQueryRequest req = new SolrQueryRequestBase(h.getCore(), params) {};

    QParser parser = parserPlugin.createParser(queryStr, localParams, params, req);

    try {
      Query query = parser.parse();
      if (log.isInfoEnabled()) {
        log.info(query.toString());
      }
      Assert.assertEquals(
          "multi_mapping_key___string:2025-12-08T00:00:00Z single_mapping_key___string:2025-12-08T00:00:00Z float_mapping_key___string:2025-12-08T00:00:00Z date_str_mapping_key___date:[1765152000000 TO 1765152000000]",
          query.toString());
    } catch (SyntaxError e) {
      Assert.fail("Should not throw SyntaxError");
    }
  }

  @Test
  public void testSearchWithParsedQuery() {
    String queryStr = """
        {!mappings f=single_mapping key="key_1"}
        """;
    int requiredDocs = 5;
    List<SolrInputDocument> docs =
        MappingsTestUtils.generateDocs(random(), "single_mapping", requiredDocs, false, true);
    for (SolrInputDocument doc : docs) {
      assertU(adoc(doc));
    }
    assertU(commit());

    String response =
        assertXmlQ(
            req("q", queryStr.trim(), "indent", "true"),
            "//doc/mapping[@name=\"single_mapping\"]/str[@name=\"key\"][text()='key_1']",
            "//result[@name=\"response\"][@numFound=\"1\"]");
    if (log.isInfoEnabled()) {
      log.info("Parsed query response: {}", response);
    }
  }

  @Test
  public void testSearchWithValueRangeQuery() throws Exception {
    int requiredDocs = 5;
    for (int i = 0; i <= requiredDocs; i++) {
      SolrInputDocument sdoc = new SolrInputDocument();
      sdoc.addField("id", "" + i);
      String key = RandomStrings.randomAsciiAlphanumOfLengthBetween(random(), 5, 10);
      float val = (float) (i * 10);
      sdoc.addField("float_mapping", "\"" + key + "\",\"" + val + "\"");
      assertU(adoc(sdoc));
    }
    assertU(commit());

    // URL-encoded range query
    String queryStr1 = """
        {!mappings f=float_mapping value=[30.0+TO+*]}
        """;
    String response1 =
        assertJQ(
            req("q", queryStr1.trim(), "indent", "true", "wt", "json"),
            "/response/numFound==3",
            "/response/docs/[0]/float_mapping/value==\"30.0\"",
            "/response/docs/[1]/float_mapping/value==\"40.0\"",
            "/response/docs/[2]/float_mapping/value==\"50.0\"");
    if (log.isInfoEnabled()) {
      log.info("Value range query response: {}", response1);
    }

    // wrap the range query in quotes
    String queryStr2 = """
        {!mappings f=float_mapping value="[30.0 TO *]"}
        """;
    assertJQ(
        req("q", queryStr2.trim(), "indent", "true", "wt", "json"),
        "/response/numFound==3",
        "/response/docs/[0]/float_mapping/value==\"30.0\"",
        "/response/docs/[1]/float_mapping/value==\"40.0\"",
        "/response/docs/[2]/float_mapping/value==\"50.0\"");
  }

  @Test
  public void testSearchWithWildcardQuery() throws Exception {
    // wildcard query without quotes
    String queryStr1 = """
        {!mappings f=single_mapping key=key*}
        """;
    int requiredDocs = 5;
    List<SolrInputDocument> docs1 =
        MappingsTestUtils.generateDocs(random(), "single_mapping", requiredDocs, false, true);
    for (SolrInputDocument doc : docs1) {
      assertU(adoc(doc));
    }
    assertU(commit());

    String response1 =
        assertXmlQ(
            req("q", queryStr1.trim(), "indent", "true"),
            "//result[@name=\"response\"][@numFound=\"5\"]");
    if (log.isInfoEnabled()) {
      log.info("Wildcard query response 1: {}", response1);
    }

    // wildcard query in quotes
    String queryStr2 = """
        {!mappings f=single_mapping key="key?1"}
        """;
    List<SolrInputDocument> docs2 =
        MappingsTestUtils.generateDocs(random(), "single_mapping", requiredDocs, false, true);
    for (SolrInputDocument doc : docs2) {
      assertU(adoc(doc));
    }
    assertU(commit());

    String response2 =
        assertXmlQ(
            req("q", queryStr2.trim(), "indent", "true"),
            "//result[@name=\"response\"][@numFound=\"1\"]");
    if (log.isInfoEnabled()) {
      log.info("Wildcard query response 2: {}", response2);
    }
  }
}
