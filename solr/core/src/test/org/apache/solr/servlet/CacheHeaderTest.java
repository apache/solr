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
package org.apache.solr.servlet;

import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;
import org.apache.solr.common.util.SuppressForbidden;
import org.apache.solr.embedded.JettyConfig;
import org.eclipse.jetty.client.ContentResponse;
import org.eclipse.jetty.http.HttpHeader;
import org.junit.BeforeClass;
import org.junit.Test;

/** A test case for the several HTTP cache headers emitted by Solr */
public class CacheHeaderTest extends CacheHeaderTestBase {

  @BeforeClass
  public static void beforeTest() throws Exception {
    // System properties are required by collection1 solrconfig.xml propTest configuration
    // These cannot be removed as they are used for property substitution in the config
    System.setProperty("solr.test.sys.prop1", "propone");
    System.setProperty("solr.test.sys.prop2", "proptwo");

    Path solrHomeDirectory = createTempDir();
    copySolrHomeToTemp(solrHomeDirectory, "collection1");

    Path coresDir = createTempDir().resolve("cores");
    Properties props = new Properties();
    props.setProperty("name", DEFAULT_TEST_CORENAME);
    props.setProperty("configSet", "collection1");

    writeCoreProperties(coresDir.resolve("core"), props, "CacheHeaderTest");

    Properties nodeProps = new Properties();
    nodeProps.setProperty("coreRootDirectory", coresDir.toString());
    nodeProps.setProperty("configSetBaseDir", solrHomeDirectory.toString());

    solrTestRule.startSolr(solrHomeDirectory, nodeProps, JettyConfig.builder().build());
  }

  @Test
  public void testCacheVetoException() throws Exception {
    String url = getSelectUrl("q", "xyz_ignore_exception:solr", "qt", "standard");
    // We force an exception from Solr. This should emit "no-cache" HTTP headers
    ContentResponse response = getHttpClient().GET(url);
    assertNotEquals(200, response.getStatus());
    checkVetoHeaders(response);
  }

  @SuppressForbidden(reason = "Needs currentTimeMillis to check against expiry headers from Solr")
  protected void checkVetoHeaders(ContentResponse response) {
    String head = response.getHeaders().get(HttpHeader.CACHE_CONTROL);
    assertNotNull("We got no Cache-Control header", head);
    assertTrue(
        "We got no no-cache in the Cache-Control header [" + head + "]", head.contains("no-cache"));
    assertTrue(
        "We got no no-store in the Cache-Control header [" + head + "]", head.contains("no-store"));

    head = response.getHeaders().get(HttpHeader.PRAGMA);
    assertNotNull("We got no Pragma header", head);
    assertEquals("no-cache", head);
  }

  @Override
  protected void doLastModified(String method) throws Exception {
    // We do a first request to get the last modified
    // This must result in a 200 OK response
    String url = getSelectUrl();
    ContentResponse response = getHttpClient().newRequest(url).method(method).send();
    checkResponseBody(method, response);

    assertEquals("Got no response code 200 in initial request", 200, response.getStatus());

    String head = response.getHeaders().get(HttpHeader.LAST_MODIFIED);
    assertNotNull("We got no Last-Modified header", head);

    ZonedDateTime lastModified = ZonedDateTime.parse(head, DateTimeFormatter.RFC_1123_DATE_TIME);

    // If-Modified-Since tests
    response =
        getHttpClient()
            .newRequest(url)
            .method(method)
            .headers(
                h ->
                    h.add(
                        HttpHeader.IF_MODIFIED_SINCE,
                        DateTimeFormatter.RFC_1123_DATE_TIME.format(
                            Instant.now().atZone(ZoneOffset.UTC))))
            .send();
    checkResponseBody(method, response);
    assertEquals("Expected 304 NotModified response with current date", 304, response.getStatus());

    response =
        getHttpClient()
            .newRequest(url)
            .method(method)
            .headers(
                h ->
                    h.add(
                        HttpHeader.IF_MODIFIED_SINCE,
                        DateTimeFormatter.RFC_1123_DATE_TIME.format(lastModified.minusSeconds(10))))
            .send();
    checkResponseBody(method, response);
    assertEquals(
        "Expected 200 OK response with If-Modified-Since in the past", 200, response.getStatus());

    // If-Unmodified-Since tests
    response =
        getHttpClient()
            .newRequest(url)
            .method(method)
            .headers(
                h ->
                    h.add(
                        HttpHeader.IF_UNMODIFIED_SINCE,
                        DateTimeFormatter.RFC_1123_DATE_TIME.format(lastModified.minusSeconds(10))))
            .send();
    checkResponseBody(method, response);
    assertEquals(
        "Expected 412 Precondition failed with If-Unmodified-Since in the past",
        412,
        response.getStatus());

    response =
        getHttpClient()
            .newRequest(url)
            .method(method)
            .headers(
                h ->
                    h.add(
                        HttpHeader.IF_UNMODIFIED_SINCE,
                        DateTimeFormatter.RFC_1123_DATE_TIME.format(
                            Instant.now().atZone(ZoneOffset.UTC))))
            .send();
    checkResponseBody(method, response);
    assertEquals(
        "Expected 200 OK response with If-Unmodified-Since and current date",
        200,
        response.getStatus());
  }

  // test ETag
  @Override
  protected void doETag(String method) throws Exception {
    String url = getSelectUrl();
    ContentResponse response = getHttpClient().newRequest(url).method(method).send();
    checkResponseBody(method, response);

    assertEquals("Got no response code 200 in initial request", 200, response.getStatus());

    String head = response.getHeaders().get(HttpHeader.ETAG);
    assertNotNull("We got no ETag in the response", head);
    assertTrue("Not a valid ETag", head.startsWith("\"") && head.endsWith("\""));

    String etag = head;

    // If-None-Match tests
    // we set a non-matching ETag
    response =
        getHttpClient()
            .newRequest(url)
            .method(method)
            .headers(h -> h.add(HttpHeader.IF_NONE_MATCH, "\"xyz123456\""))
            .send();
    checkResponseBody(method, response);
    assertEquals(
        "If-None-Match: Got no response code 200 in response to non matching ETag",
        200,
        response.getStatus());

    // now we set matching ETags
    response =
        getHttpClient()
            .newRequest(url)
            .method(method)
            .headers(
                h -> {
                  h.add(HttpHeader.IF_NONE_MATCH, "\"xyz1223\"");
                  h.add(HttpHeader.IF_NONE_MATCH, "\"1231323423\", \"1211211\",   " + etag);
                })
            .send();
    checkResponseBody(method, response);
    assertEquals("If-None-Match: Got no response 304 to matching ETag", 304, response.getStatus());

    // we now set the special star ETag
    response =
        getHttpClient()
            .newRequest(url)
            .method(method)
            .headers(h -> h.add(HttpHeader.IF_NONE_MATCH, "*"))
            .send();
    checkResponseBody(method, response);
    assertEquals("If-None-Match: Got no response 304 for star ETag", 304, response.getStatus());

    // If-Match tests
    // we set a non-matching ETag
    response =
        getHttpClient()
            .newRequest(url)
            .method(method)
            .headers(h -> h.add(HttpHeader.IF_MATCH, "\"xyz123456\""))
            .send();
    checkResponseBody(method, response);
    assertEquals(
        "If-Match: Got no response code 412 in response to non matching ETag",
        412,
        response.getStatus());

    // now we set matching ETags
    response =
        getHttpClient()
            .newRequest(url)
            .method(method)
            .headers(
                h -> {
                  h.add(HttpHeader.IF_MATCH, "\"xyz1223\"");
                  h.add(HttpHeader.IF_MATCH, "\"1231323423\", \"1211211\",   " + etag);
                })
            .send();
    checkResponseBody(method, response);
    assertEquals("If-Match: Got no response 200 to matching ETag", 200, response.getStatus());

    // now we set the special star ETag
    response =
        getHttpClient()
            .newRequest(url)
            .method(method)
            .headers(h -> h.add(HttpHeader.IF_MATCH, "*"))
            .send();
    checkResponseBody(method, response);
    assertEquals("If-Match: Got no response 200 to star ETag", 200, response.getStatus());
  }

  @Override
  protected void doCacheControl(String method) throws Exception {
    String url = getSelectUrl();
    if ("POST".equals(method)) {
      ContentResponse response = getHttpClient().POST(url).send();
      checkResponseBody(method, response);

      String head = response.getHeaders().get(HttpHeader.CACHE_CONTROL);
      assertNull("We got a cache-control header in response to POST", head);

      head = response.getHeaders().get(HttpHeader.EXPIRES);
      assertNull("We got an Expires  header in response to POST", head);
    } else {
      ContentResponse response = getHttpClient().newRequest(url).method(method).send();
      checkResponseBody(method, response);

      String head = response.getHeaders().get(HttpHeader.CACHE_CONTROL);
      assertNotNull("We got no cache-control header", head);

      head = response.getHeaders().get(HttpHeader.EXPIRES);
      assertNotNull("We got no Expires header in response", head);
    }
  }
}
