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
package org.apache.solr.common.util;

import static org.apache.solr.common.util.URLUtil.getBaseUrlForNodeName;
import static org.apache.solr.common.util.URLUtil.getNodeNameForBaseUrl;

import java.net.MalformedURLException;
import java.net.URISyntaxException;
import org.apache.solr.SolrTestCase;
import org.junit.Test;

public class URLUtilTest extends SolrTestCase {

  @Test
  public void test() {
    assertTrue(URLUtil.hasScheme("http://host:1234/"));
    assertTrue(URLUtil.hasScheme("https://host/"));
    assertFalse(URLUtil.hasScheme("host/"));
    assertFalse(URLUtil.hasScheme("host:8989"));
    assertEquals("foo/", URLUtil.removeScheme("https://foo/"));
    assertEquals("foo:8989/", URLUtil.removeScheme("https://foo:8989/"));
    assertEquals("http://", URLUtil.getScheme("http://host:1928"));
    assertEquals("https://", URLUtil.getScheme("https://host:1928"));
  }

  @Test
  public void testCanExtractBaseUrl() {
    assertEquals(
        "http://localhost:8983/solr",
        URLUtil.extractBaseUrl("http://localhost:8983/solr/techproducts"));
    assertEquals(
        "http://localhost:8983/solr",
        URLUtil.extractBaseUrl("http://localhost:8983/solr/techproducts/"));

    assertEquals(
        "http://localhost/solr", URLUtil.extractBaseUrl("http://localhost/solr/techproducts"));
    assertEquals(
        "http://localhost/solr", URLUtil.extractBaseUrl("http://localhost/solr/techproducts/"));

    assertEquals(
        "http://localhost:8983/root/solr",
        URLUtil.extractBaseUrl("http://localhost:8983/root/solr/techproducts"));
    assertEquals(
        "http://localhost:8983/root/solr",
        URLUtil.extractBaseUrl("http://localhost:8983/root/solr/techproducts/"));
  }

  @Test
  public void testCanExtractCoreNameFromCoreUrl() {
    assertEquals(
        "techproducts", URLUtil.extractCoreFromCoreUrl("http://localhost:8983/solr/techproducts"));
    assertEquals(
        "techproducts", URLUtil.extractCoreFromCoreUrl("http://localhost:8983/solr/techproducts/"));

    assertEquals(
        "techproducts", URLUtil.extractCoreFromCoreUrl("http://localhost/solr/techproducts"));
    assertEquals(
        "techproducts", URLUtil.extractCoreFromCoreUrl("http://localhost/solr/techproducts/"));

    assertEquals(
        "techproducts",
        URLUtil.extractCoreFromCoreUrl("http://localhost:8983/root/solr/techproducts"));
    assertEquals(
        "techproducts",
        URLUtil.extractCoreFromCoreUrl("http://localhost:8983/root/solr/techproducts/"));

    // Exercises most of the edge cases that SolrIdentifierValidator allows
    assertEquals(
        "sTrAnGe-name.for_core",
        URLUtil.extractCoreFromCoreUrl("http://localhost:8983/solr/sTrAnGe-name.for_core"));
    assertEquals(
        "sTrAnGe-name.for_core",
        URLUtil.extractCoreFromCoreUrl("http://localhost:8983/solr/sTrAnGe-name.for_core/"));
  }

  @Test
  public void testCanBuildCoreUrl() {
    assertEquals(
        "http://localhost:8983/solr/techproducts",
        URLUtil.buildCoreUrl("http://localhost:8983/solr", "techproducts"));
    assertEquals(
        "http://localhost:8983/solr/techproducts",
        URLUtil.buildCoreUrl("http://localhost:8983/solr/", "techproducts"));
    assertEquals(
        "http://localhost:8983/solr/sTrAnGe-name.for_core",
        URLUtil.buildCoreUrl("http://localhost:8983/solr", "sTrAnGe-name.for_core"));
  }

  @Test
  public void testGetNodeNameForBaseUrl() throws MalformedURLException, URISyntaxException {
    assertEquals("node-1-url:8983_solr", getNodeNameForBaseUrl("https://node-1-url:8983/solr"));
    assertEquals("node-1-url:8983_solr", getNodeNameForBaseUrl("http://node-1-url:8983/solr"));
    assertEquals("node-1-url:8983_api", getNodeNameForBaseUrl("http://node-1-url:8983/api"));
    assertThrows(MalformedURLException.class, () -> getNodeNameForBaseUrl("node-1-url:8983/solr"));
    assertThrows(
        URISyntaxException.class, () -> getNodeNameForBaseUrl("http://node-1-url:8983/solr^"));
  }

  @Test
  public void testGetBaseUrlForNodeName() {
    assertEquals(
        "http://app-node-1:8983/solr",
        getBaseUrlForNodeName("app-node-1:8983_solr", "http", false));
    assertEquals(
        "https://app-node-1:8983/solr",
        getBaseUrlForNodeName("app-node-1:8983_solr", "https", false));
    assertEquals(
        "http://app-node-1:8983/api", getBaseUrlForNodeName("app-node-1:8983_solr", "http", true));
    assertEquals(
        "https://app-node-1:8983/api",
        getBaseUrlForNodeName("app-node-1:8983_solr", "https", true));
  }
}
