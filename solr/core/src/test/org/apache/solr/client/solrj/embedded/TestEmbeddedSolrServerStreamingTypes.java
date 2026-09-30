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
package org.apache.solr.client.solrj.embedded;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.lucene.index.IndexableField;
import org.apache.solr.SolrTestCase;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.request.SolrQuery;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.StreamingResponseCallback;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.util.EmbeddedSolrServerTestRule;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

/**
 * EmbeddedSolrServer {@code queryAndStreamResponse} must return the same native field types as
 * {@code query} / HttpSolrClient, not Lucene {@link IndexableField} instances.
 */
public class TestEmbeddedSolrServerStreamingTypes extends SolrTestCase {

  @ClassRule
  public static final EmbeddedSolrServerTestRule solrTestRule = new EmbeddedSolrServerTestRule();

  @BeforeClass
  public static void beforeClass() throws Exception {
    solrTestRule.startSolr(SolrTestCaseJ4.TEST_HOME());
    SolrTestCaseJ4.newRandomConfig();
    solrTestRule.newCollection().withConfigSet(SolrTestCaseJ4.TEST_COLL1_CONF()).create();
  }

  @Test
  public void testQueryAndStreamResponseReturnsNativeFieldTypes() throws Exception {
    EmbeddedSolrServer server = solrTestRule.getSolrClient("collection1");
    server.deleteByQuery("*:*");
    // city_s1 is *_s1: stored string, multiValued=false. schema.xml is version 1.0, so
    // fields without multiValued="false" (including name) default to multiValued and
    // query() returns a one-element list. foo_i_p is pint so the native type is Integer.
    // name is multiValued: streaming must convert each element (Collection branch).
    SolrInputDocument doc = new SolrInputDocument();
    doc.addField("id", "1");
    doc.addField("foo_i_p", 42);
    doc.addField("city_s1", "Boston");
    doc.addField("name", "Alice");
    doc.addField("name", "Bob");
    server.add(doc);
    server.commit();

    SolrQuery q = new SolrQuery("*:*");
    q.setFields("id", "foo_i_p", "city_s1", "name");

    QueryResponse rsp = server.query(q);
    SolrDocument queried = rsp.getResults().get(0);
    Object queriedId = queried.getFieldValue("id");
    Object queriedInt = queried.getFieldValue("foo_i_p");
    Object queriedName = queried.getFieldValue("city_s1");
    assertEquals("1", queriedId);
    assertEquals(Integer.valueOf(42), queriedInt);
    assertEquals("Boston", queriedName);
    assertFalse("query() must not leak Lucene stored fields", queriedInt instanceof IndexableField);

    AtomicReference<SolrDocument> streamed = new AtomicReference<>();
    server.queryAndStreamResponse(
        q,
        new StreamingResponseCallback() {
          @Override
          public void streamSolrDocument(SolrDocument doc) {
            streamed.set(doc);
          }

          @Override
          public void streamDocListInfo(long numFound, long start, Float maxScore) {
            assertEquals(1, numFound);
          }
        });

    SolrDocument streamedDoc = streamed.get();
    assertNotNull(streamedDoc);
    Object streamedId = streamedDoc.getFieldValue("id");
    Object streamedInt = streamedDoc.getFieldValue("foo_i_p");
    Object streamedName = streamedDoc.getFieldValue("city_s1");
    assertFalse(
        "queryAndStreamResponse must not leak Lucene stored fields",
        streamedInt instanceof IndexableField);
    assertEquals(queriedId.getClass(), streamedId.getClass());
    assertEquals(queriedInt.getClass(), streamedInt.getClass());
    assertEquals(queriedName.getClass(), streamedName.getClass());
    assertEquals(queriedId, streamedId);
    assertEquals(queriedInt, streamedInt);
    assertEquals(queriedName, streamedName);
    assertEquals(Integer.class, streamedInt.getClass());
    assertEquals(String.class, streamedName.getClass());
    // multi-valued field: both paths return a list of native Strings
    Object queriedNames = queried.getFieldValue("name");
    Object streamedNames = streamedDoc.getFieldValue("name");
    assertTrue(queriedNames instanceof List);
    assertTrue(streamedNames instanceof List);
    assertEquals(queriedNames, streamedNames);
    assertEquals(Arrays.asList("Alice", "Bob"), streamedNames);
  }
}
