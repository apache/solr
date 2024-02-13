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

package org.apache.solr.client.solrj.impl;

import java.net.CookieStore;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.solr.client.solrj.ResponseParser;
import org.apache.solr.client.solrj.request.RequestWriter;

public abstract class HttpSolrClientBuilderBase {
  protected Long idleTimeoutMillis;
  protected Long connectionTimeoutMillis;
  protected Long requestTimeoutMillis;
  protected String basicAuthAuthorizationStr;
  protected Boolean followRedirects;
  protected String baseSolrUrl;
  protected RequestWriter requestWriter;
  protected ResponseParser responseParser;
  protected String defaultCollection;
  protected Set<String> urlParamNames;
  protected Integer maxConnectionsPerHost;
  protected ExecutorService executor;
  protected CookieStore cookieStore;
  protected String proxyHost;
  protected int proxyPort;
  protected boolean proxyIsSocks4;
  protected boolean proxyIsSecure;

  public HttpSolrClientBuilderBase() {}

  protected abstract <B extends Http2SolrClientBase> B build(Class<B> type);

  public HttpSolrClientBuilderBase(String baseSolrUrl) {
    this.baseSolrUrl = baseSolrUrl;
  }

  /** Provides a {@link RequestWriter} for created clients to use when handing requests. */
  public HttpSolrClientBuilderBase withRequestWriter(RequestWriter requestWriter) {
    this.requestWriter = requestWriter;
    return this;
  }

  /** Provides a {@link ResponseParser} for created clients to use when handling requests. */
  public HttpSolrClientBuilderBase withResponseParser(ResponseParser responseParser) {
    this.responseParser = responseParser;
    return this;
  }

  /** Sets a default for core or collection based requests. */
  public HttpSolrClientBuilderBase withDefaultCollection(String defaultCoreOrCollection) {
    this.defaultCollection = defaultCoreOrCollection;
    return this;
  }

  public HttpSolrClientBuilderBase withFollowRedirects(boolean followRedirects) {
    this.followRedirects = followRedirects;
    return this;
  }

  public HttpSolrClientBuilderBase withExecutor(ExecutorService executor) {
    this.executor = executor;
    return this;
  }

  public HttpSolrClientBuilderBase withBasicAuthCredentials(String user, String pass) {
    if (user != null || pass != null) {
      if (user == null || pass == null) {
        throw new IllegalStateException(
            "Invalid Authentication credentials. Either both username and password or none must be provided");
      }
    }
    this.basicAuthAuthorizationStr =
        Http2SolrClient.basicAuthCredentialsToAuthorizationString(user, pass);
    return this;
  }

  /**
   * Expert Method
   *
   * @param urlParamNames set of param keys that are only sent via the query string. Note that the
   *     param will be sent as a query string if the key is part of this Set or the SolrRequest's
   *     query params.
   * @see org.apache.solr.client.solrj.SolrRequest#getQueryParams
   */
  public HttpSolrClientBuilderBase withTheseParamNamesInTheUrl(Set<String> urlParamNames) {
    this.urlParamNames = urlParamNames;
    return this;
  }

  /**
   * Set maxConnectionsPerHost for http1 connections, maximum number http2 connections is limited to
   * 4
   */
  public HttpSolrClientBuilderBase withMaxConnectionsPerHost(int max) {
    this.maxConnectionsPerHost = max;
    return this;
  }

  public HttpSolrClientBuilderBase withIdleTimeout(long idleConnectionTimeout, TimeUnit unit) {
    this.idleTimeoutMillis = TimeUnit.MILLISECONDS.convert(idleConnectionTimeout, unit);
    return this;
  }

  public Long getIdleTimeoutMillis() {
    return idleTimeoutMillis;
  }

  public HttpSolrClientBuilderBase withConnectionTimeout(long connectionTimeout, TimeUnit unit) {
    this.connectionTimeoutMillis = TimeUnit.MILLISECONDS.convert(connectionTimeout, unit);
    return this;
  }

  public Long getConnectionTimeout() {
    return connectionTimeoutMillis;
  }

  /**
   * Set a timeout in milliseconds for requests issued by this client.
   *
   * @param requestTimeout The timeout in milliseconds
   * @return this Builder.
   */
  public HttpSolrClientBuilderBase withRequestTimeout(long requestTimeout, TimeUnit unit) {
    this.requestTimeoutMillis = TimeUnit.MILLISECONDS.convert(requestTimeout, unit);
    return this;
  }

  /**
   * Set a cookieStore other than the default ({@code java.net.InMemoryCookieStore})
   *
   * @param cookieStore The CookieStore to set. {@code null} will set the default.
   * @return this Builder
   */
  public HttpSolrClientBuilderBase withCookieStore(CookieStore cookieStore) {
    this.cookieStore = cookieStore;
    return this;
  }

  /**
   * Setup a proxy
   *
   * @param host The proxy host
   * @param port The proxy port
   * @param isSocks4 If true creates an SOCKS 4 proxy, otherwise creates an HTTP proxy
   * @param isSecure If true enables the secure flag on the proxy
   * @return this Builder
   */
  public HttpSolrClientBuilderBase withProxyConfiguration(
      String host, int port, boolean isSocks4, boolean isSecure) {
    this.proxyHost = host;
    this.proxyPort = port;
    this.proxyIsSocks4 = isSocks4;
    this.proxyIsSecure = isSecure;
    return this;
  }

  /**
   * Setup basic authentication from a string formatted as username:password. If the string is Null
   * then it doesn't do anything.
   *
   * @param credentials The username and password formatted as username:password
   * @return this Builder
   */
  public HttpSolrClientBuilderBase withOptionalBasicAuthCredentials(String credentials) {
    if (credentials != null) {
      if (credentials.indexOf(':') == -1) {
        throw new IllegalStateException(
            "Invalid Authentication credential formatting. Provide username and password in the 'username:password' format.");
      }
      String username = credentials.substring(0, credentials.indexOf(':'));
      String password = credentials.substring(credentials.indexOf(':') + 1, credentials.length());
      withBasicAuthCredentials(username, password);
    }
    return this;
  }
}
