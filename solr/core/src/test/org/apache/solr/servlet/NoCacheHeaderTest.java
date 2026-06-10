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
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import org.apache.solr.common.util.SuppressForbidden;
import org.eclipse.jetty.client.ContentResponse;
import org.eclipse.jetty.http.HttpHeader;
import org.junit.BeforeClass;
import org.junit.Test;

/** A test case for the several HTTP cache headers emitted by Solr */
public class NoCacheHeaderTest extends CacheHeaderTestBase {

  @BeforeClass
  public static void beforeTest() throws Exception {
    Path solrHome = createTempDir();
    Path collectionDirectory = solrHome.resolve(DEFAULT_TEST_COLLECTION_NAME);

    // Create minimal config with custom nocache solrconfig
    copyMinConf(
        collectionDirectory,
        "name=" + DEFAULT_TEST_COLLECTION_NAME + "\n",
        "solrconfig-nocache.xml");

    solrTestRule.startSolr(solrHome);
  }

  // The tests
  @Override
  @Test
  public void testLastModified() throws Exception {
    doLastModified("GET");
    doLastModified("HEAD");
  }

  @Override
  @Test
  public void testEtag() throws Exception {
    doETag("GET");
    doETag("HEAD");
  }

  @Override
  @Test
  public void testCacheControl() throws Exception {
    doCacheControl("GET");
    doCacheControl("HEAD");
    doCacheControl("POST");
  }

  @SuppressForbidden(reason = "Needs currentTimeMillis for testing caching headers")
  @Override
  protected void doLastModified(String method) throws Exception {
    // We do a first request to get the last modified
    // This must result in a 200 OK response
    String url = getSelectUrl();
    ContentResponse response = getHttpClient().newRequest(url).method(method).send();
    checkResponseBody(method, response);

    assertEquals("Got no response code 200 in initial request", 200, response.getStatus());

    String head = response.getHeaders().get(HttpHeader.LAST_MODIFIED);
    assertNull("We got a Last-Modified header", head);

    // If-Modified-Since tests
    response =
        getHttpClient()
            .newRequest(url)
            .method(method)
            .headers(
                h ->
                    h.add(
                        HttpHeader.IF_MODIFIED_SINCE,
                        DateTimeFormatter.RFC_1123_DATE_TIME.format(ZonedDateTime.now())))
            .send();
    checkResponseBody(method, response);
    assertEquals(
        "Expected 200 with If-Modified-Since header. We should never get a 304 here",
        200,
        response.getStatus());

    response =
        getHttpClient()
            .newRequest(url)
            .method(method)
            .headers(
                h ->
                    h.add(
                        HttpHeader.IF_MODIFIED_SINCE,
                        DateTimeFormatter.RFC_1123_DATE_TIME.format(
                            ZonedDateTime.now().minusSeconds(10))))
            .send();
    checkResponseBody(method, response);
    assertEquals(
        "Expected 200 with If-Modified-Since header. We should never get a 304 here",
        200,
        response.getStatus());

    // If-Unmodified-Since tests
    response =
        getHttpClient()
            .newRequest(url)
            .method(method)
            .headers(
                h ->
                    h.add(
                        HttpHeader.IF_UNMODIFIED_SINCE,
                        DateTimeFormatter.RFC_1123_DATE_TIME.format(
                            ZonedDateTime.now().minusSeconds(10))))
            .send();
    checkResponseBody(method, response);
    assertEquals(
        "Expected 200 with If-Unmodified-Since header. We should never get a 304 here",
        200,
        response.getStatus());

    response =
        getHttpClient()
            .newRequest(url)
            .method(method)
            .headers(
                h ->
                    h.add(
                        HttpHeader.IF_UNMODIFIED_SINCE,
                        DateTimeFormatter.RFC_1123_DATE_TIME.format(ZonedDateTime.now())))
            .send();
    checkResponseBody(method, response);
    assertEquals(
        "Expected 200 with If-Unmodified-Since header. We should never get a 304 here",
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
    assertNull("We got an ETag in the response", head);

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

    // we now set the special star ETag
    response =
        getHttpClient()
            .newRequest(url)
            .method(method)
            .headers(h -> h.add(HttpHeader.IF_NONE_MATCH, "*"))
            .send();
    checkResponseBody(method, response);
    assertEquals("If-None-Match: Got no response 200 for star ETag", 200, response.getStatus());

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
        "If-Match: Got no response code 200 in response to non matching ETag",
        200,
        response.getStatus());

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
    ContentResponse response = getHttpClient().newRequest(url).method(method).send();
    checkResponseBody(method, response);

    String head = response.getHeaders().get(HttpHeader.CACHE_CONTROL);
    assertNull("We got a cache-control header in response", head);

    head = response.getHeaders().get(HttpHeader.EXPIRES);
    assertNull("We got an Expires header in response", head);
  }
}
