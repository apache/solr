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
package org.apache.solr.util;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import org.apache.solr.common.util.URLUtil;
import org.eclipse.jetty.client.ContentResponse;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.Request;
import org.eclipse.jetty.client.StringRequestContent;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.http.HttpMethod;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Facilitates testing Solr's REST API via Jetty HttpClient. Immutable and cheap to construct. */
public class RestTestHarness extends BaseTestHarness {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final HttpClient httpClient;
  private final URI baseUri;

  /**
   * Creates a RestTestHarness for the given base URL using the provided HttpClient.
   *
   * @param httpClient The HttpClient to use for requests (not closed by this harness)
   * @param baseUri The base URI (e.g., "http://localhost:8983/solr/collection1")
   */
  public RestTestHarness(HttpClient httpClient, URI baseUri) {
    if (httpClient == null) {
      throw new IllegalArgumentException("httpClient cannot be null");
    }
    if (baseUri == null || !baseUri.isAbsolute()) {
      throw new IllegalArgumentException("baseUrl cannot be null or non-absolute");
    }
    this.httpClient = httpClient;
    this.baseUri = baseUri;
  }

  /** Creates a new instance for the specified URL, sharing the underlying HTTP client. */
  public RestTestHarness newWithUrl(String baseUrl) {
    return new RestTestHarness(httpClient, URI.create(baseUrl));
  }

  @Override
  public String toString() {
    return getBaseURL();
  }

  public String getBaseURL() {
    return getBaseURI().toASCIIString();
  }

  public URI getBaseURI() {
    return baseUri;
  }

  public String getAdminURL() {
    return getBaseURL().replace("/collection1", "");
  }

  private URI getAdminURI() {
    return URI.create(getAdminURL());
  }

  /**
   * Processes a "query" using a URL path (with no context path) + optional query params, e.g.
   * "/schema/fields?indent=off"
   *
   * @param request the URL path and optional query params
   * @return The response to the query
   * @exception IOException any exception in the response.
   */
  public String query(String request) throws IOException {
    return sendAndReturnString(
        getHttpClient().newRequest(buildUri(request)).method(HttpMethod.GET));
  }

  public String adminQuery(String request) throws IOException {
    return sendAndReturnString(
        getHttpClient()
            .newRequest(URLUtil.buildURI(getAdminURI(), request))
            .method(HttpMethod.GET));
  }

  /**
   * Processes a HEAD request using a URL path (with no context path) + optional query params and
   * returns the status code.
   *
   * @param request The URL path and optional query params
   * @return The HTTP status code
   */
  public int head(String request) throws IOException {
    ContentResponse rsp =
        send(getHttpClient().newRequest(buildUri(request)).method(HttpMethod.HEAD));
    assert rsp.getHeaders().getLongField(HttpHeader.CONTENT_LENGTH) <= 0 : "Expected no content";
    return rsp.getStatus();
  }

  /**
   * Processes a PUT request using a URL path (with no context path) + optional query params, e.g.
   * "/schema/fields/newfield", PUTs the given content, and returns the response content.
   *
   * @param request The URL path and optional query params
   * @param json The content to include with the PUT request
   * @return The response to the PUT request
   */
  public String put(String request, String json) throws IOException {
    return sendAndReturnString(
        getHttpClient()
            .newRequest(buildUri(request))
            .method(HttpMethod.PUT)
            .body(new StringRequestContent("application/json", json, StandardCharsets.UTF_8)));
  }

  /**
   * Processes a DELETE request using a URL path (with no context path) + optional query params,
   * e.g. "/schema/analysis/protwords/english", and returns the response content.
   *
   * @param request the URL path and optional query params
   * @return The response to the DELETE request
   */
  public String delete(String request) throws IOException {
    return sendAndReturnString(
        getHttpClient().newRequest(buildUri(request)).method(HttpMethod.DELETE));
  }

  /**
   * Processes a POST request using a URL path (with no context path) + optional query params, e.g.
   * "/schema/fields/newfield", PUTs the given content, and returns the response content.
   *
   * @param request The URL path and optional query params
   * @param json The content to include with the POST request
   * @return The response to the POST request
   */
  public String post(String request, String json) throws IOException {
    return sendAndReturnString(
        getHttpClient()
            .newRequest(buildUri(request))
            .method(HttpMethod.POST)
            .body(new StringRequestContent("application/json", json, StandardCharsets.UTF_8)));
  }

  public String checkAdminResponseStatus(String request, int code) throws Exception {
    try {
      String response = adminQuery(request);
      String valid = validateXPath(response, "//int[@name='status']=" + code);
      return (null == valid) ? null : response;
    } catch (XPathExpressionException e) {
      throw new RuntimeException("?!? static xpath has bug?", e);
    }
  }

  /** Reloads the first core listed in the response to the core admin handler STATUS command */
  @Override
  public void reload() throws Exception {
    String coreName =
        (String)
            evaluateXPath(
                adminQuery("/admin/cores?wt=xml&action=STATUS"),
                "//lst[@name='status']/lst[1]/str[@name='name']",
                XPathConstants.STRING);
    String xml = checkAdminResponseStatus("/admin/cores?wt=xml&action=RELOAD&core=" + coreName, 0);
    if (null != xml) {
      throw new RuntimeException("RELOAD failed:\n" + xml);
    }
  }

  /**
   * Processes an "update" (add, commit or optimize) and returns the response as a String.
   *
   * @param xml The XML of the update
   * @return The XML response to the update
   */
  @Override
  public String update(String xml) {
    try {
      return sendAndReturnString(
          getHttpClient()
              .POST(buildUri("/update"))
              .body(new StringRequestContent("application/xml", xml, StandardCharsets.UTF_8)));
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private URI buildUri(String request) {
    return request == null ? getBaseURI() : URLUtil.buildURI(getBaseURI(), request);
  }

  /** Executes the given request and returns the response as a String. */
  private String sendAndReturnString(Request request) throws IOException {
    ContentResponse rsp = send(request);
    return rsp.getContentAsString();
  }

  private static ContentResponse send(Request request) throws IOException {
    ContentResponse rsp;
    try {
      rsp = request.send();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt(); // reset
      throw new RuntimeException(e);
    } catch (TimeoutException e) {
      throw new RuntimeException(e);
    } catch (ExecutionException e) {
      log.warn("Propagating cause of executionException");
      try {
        throw e.getCause();
      } catch (RuntimeException | IOException | Error cause) {
        throw cause;
      } catch (Exception cause) {
        throw new IOException(cause);
      } catch (Throwable ex) {
        throw new RuntimeException(ex);
      }
    }
    return rsp;
  }

  public HttpClient getHttpClient() {
    return httpClient;
  }
}
