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

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import org.apache.solr.client.solrj.jetty.HttpJettySolrClient;
import org.apache.solr.common.util.IOUtils;
import org.eclipse.jetty.client.ContentResponse;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.Request;
import org.eclipse.jetty.client.StringRequestContent;

/** Facilitates testing Solr's REST API */
public class RestTestHarness extends BaseTestHarness implements Closeable {
  private RESTfulServerProvider serverProvider;
  private HttpJettySolrClient solrClient; // lazy instantiated given serverProvider

  public RestTestHarness(RESTfulServerProvider serverProvider) {
    this.serverProvider = serverProvider;
  }

  public String getBaseURL() {
    return serverProvider.getBaseURL();
  }

  public synchronized void setServerProvider(RESTfulServerProvider serverProvider) {
    this.serverProvider = serverProvider;
    IOUtils.closeQuietly(solrClient);
    solrClient = null;
  }

  public RESTfulServerProvider getServerProvider() {
    return this.serverProvider;
  }

  public String getAdminURL() {
    return getBaseURL().replace("/collection1", "");
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
    return getResponse(getHttpClient().newRequest(buildURI(getBaseURL(), request)).method("GET"));
  }

  public String adminQuery(String request) throws IOException {
    return getResponse(getHttpClient().newRequest(buildURI(getAdminURL(), request)).method("GET"));
  }

  /**
   * Processes a PUT request using a URL path (with no context path) + optional query params, e.g.
   * "/schema/fields/newfield", PUTs the given content, and returns the response content.
   *
   * @param request The URL path and optional query params
   * @param content The content to include with the PUT request
   * @return The response to the PUT request
   */
  public String put(String request, String content) throws IOException {
    return getResponse(
        getHttpClient()
            .newRequest(buildURI(getBaseURL(), request))
            .method("PUT")
            .body(new StringRequestContent("application/json", content, StandardCharsets.UTF_8)));
  }

  /**
   * Processes a DELETE request using a URL path (with no context path) + optional query params,
   * e.g. "/schema/analysis/protwords/english", and returns the response content.
   *
   * @param request the URL path and optional query params
   * @return The response to the DELETE request
   */
  public String delete(String request) throws IOException {
    return getResponse(
        getHttpClient().newRequest(buildURI(getBaseURL(), request)).method("DELETE"));
  }

  /**
   * Processes a POST request using a URL path (with no context path) + optional query params, e.g.
   * "/schema/fields/newfield", PUTs the given content, and returns the response content.
   *
   * @param request The URL path and optional query params
   * @param content The content to include with the POST request
   * @return The response to the POST request
   */
  public String post(String request, String content) throws IOException {
    return getResponse(
        getHttpClient()
            .newRequest(buildURI(getBaseURL(), request))
            .method("POST")
            .body(new StringRequestContent("application/json", content, StandardCharsets.UTF_8)));
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
      return getResponse(
          getHttpClient()
              .newRequest(buildURI(getBaseURL(), "/update"))
              .method("POST")
              .body(new StringRequestContent("application/xml", xml, StandardCharsets.UTF_8)));
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Constructs a properly encoded URI by combining the base URL with the request path. This ensures
   * special characters (like umlauts) are properly percent-encoded in {@code request}.
   */
  private URI buildURI(String baseURL, String request) {
    // append '/' so that URI.resolve appends instead of replaces the last component
    assert !baseURL.endsWith("/");
    baseURL += "/";
    // skip leading / so that this is relative (i.e. appends not replaces the base path)
    assert request.startsWith("/");
    request = request.substring(1);

    // toASCIIString ensures it's encoded.  create(str) doesn't do encoding.
    String encodedRemainingRequest = URI.create(request).toASCIIString();

    return URI.create(baseURL).resolve(encodedRemainingRequest);
  }

  /** Executes the given request and returns the response. */
  private String getResponse(Request request) throws IOException {
    try {
      ContentResponse rsp = request.send();
      //      if (rsp.getStatus() >= 300 && rsp.getStatus()!=) {
      //        throw new IOException("Unexpected response " + rsp);
      //      }
      return rsp.getContentAsString();
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  public synchronized HttpClient getHttpClient() {
    if (solrClient == null) {
      solrClient = new HttpJettySolrClient.Builder(getBaseURL()).build();
    }
    return solrClient.getHttpClient();
  }

  @Override
  public void close() {
    IOUtils.closeQuietly(solrClient);
  }
}
