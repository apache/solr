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
package org.apache.solr.search.json;

import static org.hamcrest.core.StringContains.containsString;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.solr.JSONTestUtil;
import org.apache.solr.SolrTestCaseHS;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.CaffeineCache;
import org.apache.solr.search.DocSet;
import org.hamcrest.MatcherAssert;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;

@LuceneTestCase.SuppressCodecs({
  "Lucene3x",
  "Lucene40",
  "Lucene41",
  "Lucene42",
  "Lucene45",
  "Appending"
})
public class TestJsonRequest extends SolrTestCaseHS {

  private static SolrInstances servers; // for distributed testing

  @SuppressWarnings("deprecation")
  @BeforeClass
  public static void beforeTests() throws Exception {
    systemSetPropertySolrDisableUrlAllowList("true");
    System.setProperty("solr.enableStreamBody", "true");
    JSONTestUtil.failRepeatedKeys = true;
    initCore("solrconfig-tlog.xml", "schema_latest.xml");
  }

  public static void initServers() throws Exception {
    if (servers == null) {
      servers = new SolrInstances(3, "solrconfig-tlog.xml", "schema_latest.xml");
    }
  }

  @SuppressWarnings("deprecation")
  @AfterClass
  public static void afterTests() throws Exception {
    JSONTestUtil.failRepeatedKeys = false;
    if (servers != null) {
      servers.stop();
      servers = null;
    }
    systemClearPropertySolrDisableUrlAllowList();
  }

  @Test
  public void testLocalJsonRequest() throws Exception {
    doJsonRequest(Client.localClient, false);
  }

  @Test
  public void testLocalJsonRequestWithTags() throws Exception {
    doJsonRequestWithTag(Client.localClient);
  }

  @Test
  public void testDistribJsonRequest() throws Exception {
    initServers();
    Client client = servers.getClient(random().nextInt());
    client.queryDefaults().set("shards", servers.getShards());
    doJsonRequest(client, true);
  }

  public static void doJsonRequest(Client client, boolean isDistrib) throws Exception {
    addDocs(client);

    ignoreException("Expected JSON");

    client.testJQ(
            params("json", "{ \"queries\": { \"lexical\": { \"lucene\": { \"query\": \"id:(1 OR 2 OR 3 OR 4)\" } }, \"neural\": { \"knn\": { \"f\": \"vector\", \"topK\": 10, \"query\": \"[1.5,2.5,3.5,4.5]\" } }}, \"limit\": 10, \"fields\": [id,score], \"params\":{\"combiner.upTo\": 5}}"),
            "response/numFound==5");
  }

  private static void doParamRefDslTest(Client client) throws Exception {
    // referencing in dsl                //nestedqp
    client.testJQ(
        params("json", "{query: {query:  {param:'ref1'}}}", "ref1", "{!field f=cat_s}A"),
        "response/numFound==2");
    // referencing json string param
    client.testJQ(
        params(
            "json",
            random().nextBoolean()
                ? "{query:{query:{param:'ref1'}}}" // nestedqp
                : "{query: {query: {query:{param:'ref1'}}}}", // nestedqp, v local param
            "json",
            random().nextBoolean()
                ? "{params:{ref1:'{!field f=cat_s}A'}}" // string param
                : "{queries:{ref1:{field:{f:cat_s,query:A}}}}"), // qdsl
        "response/numFound==2");
    { // shortest top level ref
      final ModifiableSolrParams params = params("json", "{query:{param:'ref1'}}");
      if (random().nextBoolean()) {
        params.add("ref1", "cat_s:A"); // either to plain string
      } else {
        params.add("json", "{queries:{ref1:{field:{f:cat_s,query:A}}}}"); // or to qdsl
      }
      client.testJQ(params, "response/numFound==2");
    } // ref in bool must
    client.testJQ(
        params(
            "json",
            "{query:{bool: {must:[{param:fq1},{param:fq2}]}}}",
            "json",
            "{params:{fq1:'cat_s:A', fq2:'where_s:NY'}}",
            "json.fields",
            "id"),
        "response/docs==[{id:'1'}]"); // referencing dsl&strings from filters objs&array
    client.testJQ(
        params(
            "json.filter",
            "{param:fq1}",
            "json.filter",
            "{param:fq2}",
            "json",
            random().nextBoolean()
                ? "{queries:{fq1:{lucene:{query:'cat_s:A'}}, fq2:{lucene:{query:'where_s:NY'}}}}"
                : "{params:{fq1:'cat_s:A', fq2:'where_s:NY'}}",
            "json.fields",
            "id",
            "q",
            "*:*"),
        "response/docs==[{id:'1'}]");
  }

  private static void testFilterCachingLocally(Client client) throws Exception {
    if (client.getClientProvider() == null) {
      final SolrQueryRequest request = req();
      try {
        final CaffeineCache<Query, DocSet> filterCache =
            (CaffeineCache<Query, DocSet>) request.getSearcher().getFilterCache();
        filterCache.clear();
        final TermQuery catA = new TermQuery(new Term("cat_s", "A"));
        assertNull("cache is empty", filterCache.get(catA));

        if (random().nextBoolean()) {
          if (random().nextBoolean()) {
            if (random().nextBoolean()) {
              assertCatANot1(client, "must");
            } else {
              assertCatANot1(client, "must", "cat_s:A");
            }
          } else {
            assertCatANot1(
                client,
                "must",
                "{!lucene q.op=AND df=cat_s " + "cache=" + random().nextBoolean() + "}A");
          }
        } else {
          assertCatANot1(client, "filter", "{!lucene q.op=AND df=cat_s cache=false}A");
        }
        assertNull("no cache still", filterCache.get(catA));

        if (random().nextBoolean()) {
          if (random().nextBoolean()) {
            assertCatANot1(client, "filter", "cat_s:A");
          } else {
            assertCatANot1(client, "filter");
          }
        } else {
          assertCatANot1(client, "filter", "{!lucene q.op=AND df=cat_s cache=true}A");
        }
        assertNotNull("got cached ", filterCache.get(catA));

      } finally {
        request.close();
      }
    }
  }

  private static void assertCatANot1(Client client, final String occur) throws Exception {
    assertCatANot1(client, occur, "{!lucene q.op=AND df=cat_s}A");
  }

  private static void assertCatANot1(Client client, final String occur, String catAclause)
      throws Exception {
    client.testJQ(
        params(
            "json",
            "{ "
                + " query : {"
                + "  bool : {"
                + "   "
                + occur
                + " : '"
                + catAclause
                + "'"
                + "   must_not : '{!lucene v=\\'id:1\\'}'"
                + "  }"
                + " }"
                + "}"),
        "response/numFound==1");
  }

  public static void doJsonRequestWithTag(Client client) throws Exception {
    addDocs(client);

    try {
      client.testJQ(
          params(
              "json",
              "{"
                  + " query : '*:*',"
                  + " filter : { \"RCAT\" : \"cat_s:A OR ignore_exception\" }"
                  + // without the pound, the tag would be interpreted as a query type
                  "}",
              "json.facet",
              "{"
                  + "categories:{ type:terms, field:cat_s, domain:{excludeTags:\"RCAT\"} }  "
                  + "}"),
          "facets=={ count:2, "
              + " categories:{ buckets:[ {val:B, count:3}, {val:A, count:2} ]  }"
              + "}");
      fail("no # no tag");
    } catch (Exception e) {
      // This is just the current mode of failure.  It's fine if it fails a different way (with a
      // 400 error) in the future.
      assertTrue(e.getMessage().contains("expect a json object"));
    }

    final String taggedQ =
        "{"
            + " \"#RCAT\" : "
            + (random().nextBoolean()
                ? "{" + "     term : {" + "       f : cat_s," + "       v : A" + "     } " + "   } "
                : "\"cat_s:A\"")
            + " } ";
    boolean queryAndFilter = random().nextBoolean();
    client.testJQ(
        params(
            "json",
            "{"
                + " query :"
                + (queryAndFilter ? " '*:*', filter : " : "")
                + (!queryAndFilter || random().nextBoolean() ? taggedQ : "[" + taggedQ + "]")
                + "}",
            "json.facet",
            "{" + "categories:{ type:terms, field:cat_s, domain:{excludeTags:\"RCAT\"} }  " + "}"),
        "facets=={ count:2, "
            + " categories:{ buckets:[ {val:B, count:3}, {val:A, count:2} ]  }"
            + "}");

    client.testJQ(
        params(
            "json",
            "{"
                + " query : '*:*',"
                + " filter : {"
                + "  term : {"
                + "   f : cat_s,"
                + "   v : A"
                + "  } "
                + " } "
                + "}",
            "json.facet",
            "{"
                + "categories:{ type:terms, field:cat_s"
                + (random().nextBoolean() ? ", domain:{excludeTags:\"RCAT\"} " : " ")
                + "}  "
                + "}"),
        "facets=={ count:2, " + " categories:{ buckets:[ {val:A, count:2} ] }" + "}");

    client.testJQ(
        params(
            "json",
            "{"
                + " query : '*:*',"
                + " filter : {"
                + "   \"#RCAT\" : {"
                + "     term : {"
                + "       f : cat_s,"
                + "       v : A"
                + "     } "
                + "   } "
                + " } "
                + "}",
            "json.facet",
            "{" + "categories:{ type:terms, field:cat_s }  " + "}"),
        "facets=={ count:2, " + " categories:{ buckets:[ {val:A, count:2} ] }" + "}");

    boolean multiTag = random().nextBoolean();
    client.testJQ(
        params(
            "json",
            "{"
                + " query : '*:*',"
                + " filter : ["
                + "{ \"#RCAT"
                + (multiTag ? ",RCATSECONDTAG" : "")
                + "\" :  \"cat_s:A\" },"
                + "{ \"#RWHERE\" : {"
                + "     term : {"
                + "       f : where_s,"
                + "       v : NY"
                + "     } "
                + "   }"
                + "}]}",
            "json.facet",
            "{"
                + "categories:{ type:terms, field:cat_s, domain:{excludeTags:\"RCAT\"} }  "
                + "countries:{ type:terms, field:where_s, domain:{excludeTags:\"RWHERE\"} }  "
                + "ids:{ type:terms, field:id, domain:{excludeTags:[\""
                + (multiTag ? "RCATSECONDTAG" : "RCAT")
                + "\", \"RWHERE\"]} }  "
                + "}"),
        "facets=="
            + "{\n"
            + "    \"count\":1,\n"
            + "    \"categories\":{\n"
            + "      \"buckets\":[{\n"
            + "          \"val\":\"A\",\n"
            + "          \"count\":1},\n"
            + "        {\n"
            + "          \"val\":\"B\",\n"
            + "          \"count\":1}]},\n"
            + "    \"countries\":{\n"
            + "      \"buckets\":[{\n"
            + "          \"val\":\"NJ\",\n"
            + "          \"count\":1},\n"
            + "        {\n"
            + "          \"val\":\"NY\",\n"
            + "          \"count\":1}]},\n"
            + "    \"ids\":{\n"
            + "      \"buckets\":[{\n"
            + "          \"val\":\"1\",\n"
            + "          \"count\":1},\n"
            + "        {\n"
            + "          \"val\":\"2\",\n"
            + "          \"count\":1},\n"
            + "        {\n"
            + "          \"val\":\"3\",\n"
            + "          \"count\":1},\n"
            + "        {\n"
            + "          \"val\":\"4\",\n"
            + "          \"count\":1},\n"
            + "        {\n"
            + "          \"val\":\"5\",\n"
            + "          \"count\":1},\n"
            + "        {\n"
            + "          \"val\":\"6\",\n"
            + "          \"count\":1}]}}}");
  }

  private static void addDocs(Client client) throws Exception {
    client.deleteByQuery("*:*", null);
    client.add(sdoc("id", "1", "cat_s", "A", "where_s", "NY", "vector", Arrays.asList(1f, 2f, 3f, 4f)), null);
    client.add(sdoc("id", "2", "cat_s", "B", "where_s", "NJ", "vector",Arrays.asList(1.2f, 2.2f, 3.2f, 4.2f)), null);
    client.add(sdoc("id", "3"), null);
    client.commit();
    client.add(sdoc("id", "4", "cat_s", "A", "where_s", "NJ", "vector",Arrays.asList(1.4f, 2.4f, 3.4f, 4.4f)), null);
    client.add(sdoc("id", "5", "cat_s", "B", "where_s", "NJ", "vector",Arrays.asList(1.5f, 2.5f, 3.5f, 4.5f)), null);
    client.commit();
    client.add(sdoc("id", "6", "cat_s", "B", "where_s", "NY", "vector",Arrays.asList(1.6f, 2.6f, 3.6f, 4.6f)), null);
    client.commit();
  }
}
