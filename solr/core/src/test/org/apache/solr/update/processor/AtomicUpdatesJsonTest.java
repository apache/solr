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
package org.apache.solr.update.processor;

import java.util.HashMap;
import java.util.Map;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.util.RandomNoReverseMergePolicyFactory;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TestRule;

// Tests atomic updates using JSON loader, since the existing
// tests all use XML format, and there have been some atomic update
// issues that were specific to the JSONformat.
public class AtomicUpdatesJsonTest extends SolrTestCaseJ4 {

  @ClassRule
  public static final TestRule noReverseMerge = RandomNoReverseMergePolicyFactory.createRule();

  @BeforeClass
  public static void beforeTests() throws Exception {
    System.setProperty("enable.update.log", "true");
    initCore("solrconfig.xml", "atomic-update-json-test.xml");
  }

  @Before
  public void before() {
    h.update("<delete><query>*:*</query></delete>");
    assertU(commit());
  }

  @Test
  public void testSchemaIsNotUsableForChildDocs() throws Exception {
    // the schema we loaded shouldn't be usable for child docs,
    // since we're testing JSON loader functionality that only
    // works in that case and is ambiguous if nested docs are supported.
    assert !h.getCore().getLatestSchema().isUsableForChildDocs();
  }

  @Test
  public void testAddOne() throws Exception {
    SolrInputDocument doc = new SolrInputDocument();
    doc.setField("id", "1");
    doc.setField("name", new String[] {"aaa"});
    updateJ(jsonAdd(doc), null);
    assertU(commit());
    assertQ(req("q", "name:bbb", "indent", "true"), "//result[@numFound = '0']");

    doc = new SolrInputDocument();
    doc.setField("id", "1");
    doc.setField("name", Map.of("add", "bbb"));
    updateJ(jsonAdd(doc), null);
    assertU(commit());
    assertQ(req("q", "name:bbb", "indent", "true"), "//result[@numFound = '1']");
  }

  @Test
  public void testRemoveOne() throws Exception {
    SolrInputDocument doc = new SolrInputDocument();
    doc.setField("id", "1");
    doc.setField("name", new String[] {"aaa", "bbb"});
    updateJ(jsonAdd(doc), null);
    assertU(commit());
    assertQ(req("q", "name:bbb", "indent", "true"), "//result[@numFound = '1']");

    doc = new SolrInputDocument();
    doc.setField("id", "1");
    doc.setField("name", Map.of("remove", "bbb"));
    updateJ(jsonAdd(doc), null);
    assertU(commit());
    assertQ(req("q", "name:bbb", "indent", "true"), "//result[@numFound = '0']");
    assertQ(req("q", "name:aaa", "indent", "true"), "//result[@numFound = '1']");
  }

  @Test
  public void testRemoveMultiple() throws Exception {
    SolrInputDocument doc = new SolrInputDocument();
    doc.setField("id", "1");
    doc.setField("name", new String[] {"aaa", "bbb", "ccc"});
    updateJ(jsonAdd(doc), null);
    assertU(commit());
    assertQ(req("q", "name:bbb", "indent", "true"), "//result[@numFound = '1']");

    doc = new SolrInputDocument();
    doc.setField("id", "1");
    doc.setField(
        "name",
        new HashMap<String, Object>() {
          {
            put("add", new String[] {"ddd", "eee"});
            put("remove", new String[] {"aaa", "ccc"});
          }
        });
    updateJ(jsonAdd(doc), null);
    assertU(commit());
    assertQ(req("q", "name:aaa", "indent", "true"), "//result[@numFound = '0']");
    assertQ(req("q", "name:ccc", "indent", "true"), "//result[@numFound = '0']");
    assertQ(req("q", "name:bbb", "indent", "true"), "//result[@numFound = '1']");
    assertQ(req("q", "name:ddd", "indent", "true"), "//result[@numFound = '1']");
    assertQ(req("q", "name:eee", "indent", "true"), "//result[@numFound = '1']");
  }

  @Test
  public void testAddAndRemove() throws Exception {
    SolrInputDocument doc = new SolrInputDocument();
    doc.setField("id", "1");
    doc.setField("name", new String[] {"aaa", "bbb", "ccc"});
    updateJ(jsonAdd(doc), null);
    assertU(commit());
    assertQ(req("q", "name:aaa", "indent", "true"), "//result[@numFound = '1']");
    assertQ(req("q", "name:bbb", "indent", "true"), "//result[@numFound = '1']");
    assertQ(req("q", "name:ccc", "indent", "true"), "//result[@numFound = '1']");

    doc = new SolrInputDocument();
    doc.setField("id", "1");
    doc.setField("name", Map.of("add", "ddd", "remove", "bbb"));
    updateJ(jsonAdd(doc), null);
    assertU(commit());
    assertQ(req("q", "name:ddd", "indent", "true"), "//result[@numFound = '1']");
    assertQ(req("q", "name:bbb", "indent", "true"), "//result[@numFound = '0']");
    assertQ(req("q", "name:ccc", "indent", "true"), "//result[@numFound = '1']");
    assertQ(req("q", "name:aaa", "indent", "true"), "//result[@numFound = '1']");
  }

  @Test
  public void testAtomicUpdateModifierNameSingleValued() throws Exception {
    // Testing atomic update with a single-valued field named 'set'
    SolrInputDocument doc = new SolrInputDocument();
    doc.setField("id", "1");
    doc.setField("set", "setval");
    doc.setField("name", new String[] {"aaa"});
    updateJ(jsonAdd(doc), null);
    assertU(commit());
    assertQ(req("q", "set:setval", "indent", "true"), "//result[@numFound = '1']");

    doc = new SolrInputDocument();
    doc.setField("id", "1");
    doc.setField("set", Map.of("set", "modval"));
    updateJ(jsonAdd(doc), null);
    assertU(commit());
    assertQ(req("q", "set:modval", "indent", "true"), "//result[@numFound = '1']");
    assertQ(req("q", "set:setval", "indent", "true"), "//result[@numFound = '0']");

    doc = new SolrInputDocument();
    doc.setField("id", "1");
    doc.setField(
        "set",
        new HashMap<String, String>() {
          {
            put("set", null);
          }
        });
    updateJ(jsonAdd(doc), null);
    assertU(commit());
    assertQ(req("q", "set:modval", "indent", "true"), "//result[@numFound = '0']");
    assertQ(req("q", "name:aaa", "indent", "true"), "//result[@numFound = '1']");
  }

  @Test
  public void testAtomicUpdateModifierNameMultiValued() throws Exception {
    // Testing atomic update with a multi-valued field named 'add'
    SolrInputDocument doc = new SolrInputDocument();
    doc.setField("id", "1");
    doc.setField("name", new String[] {"aaa"});
    doc.setField("add", new String[] {"aaa", "bbb", "ccc"});
    updateJ(jsonAdd(doc), null);
    assertU(commit());
    assertQ(req("q", "add:bbb", "indent", "true"), "//result[@numFound = '1']");

    doc = new SolrInputDocument();
    doc.setField("id", "1");
    doc.setField(
        "add",
        new HashMap<String, Object>() {
          {
            put("add", new String[] {"ddd", "eee"});
            put("remove", new String[] {"bbb", "ccc"});
          }
        });
    updateJ(jsonAdd(doc), null);
    assertU(commit());
    assertQ(req("q", "add:ddd", "indent", "true"), "//result[@numFound = '1']");
    assertQ(req("q", "add:eee", "indent", "true"), "//result[@numFound = '1']");
    assertQ(req("q", "add:aaa", "indent", "true"), "//result[@numFound = '1']");
    assertQ(req("q", "add:bbb", "indent", "true"), "//result[@numFound = '0']");
    assertQ(req("q", "add:ccc", "indent", "true"), "//result[@numFound = '0']");
  }
}
