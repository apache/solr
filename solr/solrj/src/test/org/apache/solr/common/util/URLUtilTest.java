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
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.solr.SolrTestCase;
import org.apache.solr.common.params.ModifiableSolrParams;
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

  @Test
  public void testBuildURIBasic() {
    URI baseUri = URI.create("http://localhost:8983/solr");
    // Test with and without trailing/leading slashes
    URI uri1 = URLUtil.buildURI(baseUri, "/config/overlay");
    assertEquals("http://localhost:8983/solr/config/overlay", uri1.toString());

    URI uri2 = URLUtil.buildURI(URI.create("http://localhost:8983/solr/"), "config/overlay");
    assertEquals("http://localhost:8983/solr/config/overlay", uri2.toString());

    URI uri3 = URLUtil.buildURI(baseUri, "admin/cores");
    assertEquals("http://localhost:8983/solr/admin/cores", uri3.toString());

    URI uri4 = URLUtil.buildURI(URI.create("http://localhost:8983/solr/"), "/admin/cores");
    assertEquals("http://localhost:8983/solr/admin/cores", uri4.toString());
  }

  @Test
  public void testBuildURIWithSpecialCharactersInPath() {
    URI baseUri = URI.create("http://localhost:8983/solr");
    // Test with umlaut in path
    URI uri1 = URLUtil.buildURI(baseUri, "config/überlay");
    assertEquals("http://localhost:8983/solr/config/%C3%BCberlay", uri1.toASCIIString());

    // Test with multiple special characters
    URI uri2 = URLUtil.buildURI(baseUri, "cöllection/döc");
    assertEquals("http://localhost:8983/solr/c%C3%B6llection/d%C3%B6c", uri2.toASCIIString());

    // Test with spaces (should be encoded)
    URI uri3 = URLUtil.buildURI(baseUri, "admin/my core");
    assertEquals("http://localhost:8983/solr/admin/my%20core", uri3.toASCIIString());
  }

  @Test
  public void testBuildURIWithQueryParams() {
    URI baseUri = URI.create("http://localhost:8983/solr");
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("action", "STATUS");
    params.add("wt", "json");

    URI uri = URLUtil.buildURI(baseUri, "admin/cores", params);
    String uriString = uri.toString();
    assertTrue(uriString.startsWith("http://localhost:8983/solr/admin/cores?"));
    assertTrue(uriString.contains("action=STATUS"));
    assertTrue(uriString.contains("wt=json"));
    assertFalse(uriString.contains("??")); // Should NOT have double '?'

    // Verify exactly one '?' character
    int questionMarkCount = 0;
    for (int i = 0; i < uriString.length(); i++) {
      if (uriString.charAt(i) == '?') questionMarkCount++;
    }
    assertEquals("Should have exactly one '?' in the URI", 1, questionMarkCount);
  }

  @Test
  public void testBuildURIWithSpecialCharactersInQueryParams() {
    URI baseUri = URI.create("http://localhost:8983/solr");
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "naïve");
    params.add("fq", "field:übung");

    URI uri = URLUtil.buildURI(baseUri, "select", params);
    String uriString = uri.toASCIIString();
    assertTrue(uriString.startsWith("http://localhost:8983/solr/select?"));
    // Query params should be percent-encoded by SolrParams.toQueryString()
    assertTrue(uriString.contains("na%C3%AFve"));
    assertTrue(uriString.contains("%C3%BCbung"));
  }

  @Test
  public void testBuildURIWithPathAndQueryParamsWithSpecialChars() {
    URI baseUri = URI.create("http://localhost:8983/solr");
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("name", "Müller");
    params.add("city", "Zürich");

    URI uri = URLUtil.buildURI(baseUri, "cöre/select", params);
    String uriString = uri.toASCIIString();
    assertTrue(uriString.startsWith("http://localhost:8983/solr/c%C3%B6re/select?"));
    // Check that both path and query params are encoded
    assertTrue(uriString.contains("M%C3%BCller"));
    assertTrue(uriString.contains("Z%C3%BCrich"));
  }

  @Test
  public void testBuildURIWithSpecialCharsInParamValues() {
    URI baseUri = URI.create("http://localhost:8983/solr");
    ModifiableSolrParams params = new ModifiableSolrParams();
    // Test ampersand and equals sign as literal values in parameters
    params.add("q", "foo=bar&baz");
    params.add("fq", "field:a=b");

    URI uri = URLUtil.buildURI(baseUri, "select", params);
    String uriString = uri.toASCIIString();

    // These characters should be percent-encoded in values
    // URLEncoder also encodes ':' to %3A
    assertTrue(
        "Expected encoded = and & in value, got: " + uriString,
        uriString.contains("foo%3Dbar%26baz"));
    assertTrue(
        "Expected encoded = and : in value, got: " + uriString,
        uriString.contains("field%3Aa%3Db"));
  }

  @Test
  public void testBuildURIValidation() {
    URI baseUri = URI.create("http://localhost:8983/solr");
    // Test null baseUri
    assertThrows(IllegalArgumentException.class, () -> URLUtil.buildURI(null, "path"));

    // Test null/empty path
    assertThrows(IllegalArgumentException.class, () -> URLUtil.buildURI(baseUri, null));
    assertThrows(IllegalArgumentException.class, () -> URLUtil.buildURI(baseUri, ""));
  }

  @Test
  public void testBuildURIWithQueryStringInPath() {
    URI baseUri = URI.create("http://localhost:8983/solr");
    // Test with query string in path parameter
    URI uri1 = URLUtil.buildURI(baseUri, "/select?q=test");
    assertEquals("http://localhost:8983/solr/select?q=test", uri1.toString());

    // Test with query string with special characters
    URI uri2 = URLUtil.buildURI(baseUri, "/select?q=naïve");
    String uriString = uri2.toASCIIString();
    assertTrue(uriString.startsWith("http://localhost:8983/solr/select?"));
    assertTrue(uriString.contains("na%C3%AFve"));
  }

  @Test
  public void testBuildURIWithQueryStringInPathAndParams() {
    URI baseUri = URI.create("http://localhost:8983/solr");
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("wt", "json");

    // Test combining query string from path with additional params
    URI uri = URLUtil.buildURI(baseUri, "/select?q=test", params);
    String uriString = uri.toString();
    assertTrue(uriString.startsWith("http://localhost:8983/solr/select?"));
    assertTrue(uriString.contains("q=test"));
    assertTrue(uriString.contains("wt=json"));
    assertTrue(uriString.contains("&")); // Should have & separator
    assertFalse(uriString.contains("??")); // Should NOT have double '?'

    // Verify exactly one '?' character
    int questionMarkCount = 0;
    for (int i = 0; i < uriString.length(); i++) {
      if (uriString.charAt(i) == '?') questionMarkCount++;
    }
    assertEquals("Should have exactly one '?' in the URI", 1, questionMarkCount);
  }
}
