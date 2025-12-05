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

import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.solr.client.solrj.request.RequestWriter;
import org.apache.solr.client.solrj.response.ResponseParser;

public abstract class HttpSolrClientBuilderBase<
    B extends HttpSolrClientBuilderBase<?, ?>, C extends HttpSolrClientBase> {

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
  protected final boolean defaultUseHttp1_1 = Boolean.getBoolean("solr.http1");
  protected Boolean useHttp1_1;
  protected String proxyHost;
  protected int proxyPort;
  protected boolean proxyIsSocks4;
  protected boolean proxyIsSecure;

  public abstract C build();

  /**
   * Provide a seed HttpSolrClient for the builder values, values can still be overridden by using
   * builder methods
   */
  @SuppressWarnings("unchecked")
  public B withHttpClient(C httpSolrClient) {
    if (this.basicAuthAuthorizationStr == null) {
      this.basicAuthAuthorizationStr = httpSolrClient.basicAuthAuthorizationStr;
    }
    if (this.requestTimeoutMillis == null) {
      this.requestTimeoutMillis = httpSolrClient.requestTimeoutMillis;
    }
    if (this.requestWriter == null) {
      this.requestWriter = httpSolrClient.requestWriter;
    }
    if (this.responseParser == null) {
      this.responseParser = httpSolrClient.parser;
    }
    if (this.urlParamNames == null) {
      this.urlParamNames = httpSolrClient.urlParamNames;
    }
    return (B) (this);
  }

  /** Provides the Base Solr Url. */
  @SuppressWarnings("unchecked")
  public B withBaseSolrUrl(String baseSolrUrl) {
    this.baseSolrUrl = baseSolrUrl;
    return (B) this;
  }

  /** Provides a {@link RequestWriter} for created clients to use when handing requests. */
  @SuppressWarnings("unchecked")
  public B withRequestWriter(RequestWriter requestWriter) {
    this.requestWriter = requestWriter;
    return (B) this;
  }

  /** Provides a {@link ResponseParser} for created clients to use when handling requests. */
  @SuppressWarnings("unchecked")
  public B withResponseParser(ResponseParser responseParser) {
    this.responseParser = responseParser;
    return (B) this;
  }

  /** Sets a default for core or collection based requests. */
  @SuppressWarnings("unchecked")
  public B withDefaultCollection(String defaultCoreOrCollection) {
    this.defaultCollection = defaultCoreOrCollection;
    return (B) this;
  }

  @SuppressWarnings("unchecked")
  public B withFollowRedirects(boolean followRedirects) {
    this.followRedirects = followRedirects;
    return (B) this;
  }

  @SuppressWarnings("unchecked")
  public B withExecutor(ExecutorService executor) {
    this.executor = executor;
    return (B) this;
  }

  @SuppressWarnings("unchecked")
  public B withBasicAuthCredentials(String user, String pass) {
    if (user != null || pass != null) {
      if (user == null || pass == null) {
        throw new IllegalStateException(
            "Invalid Authentication credentials. Either both username and password or none must be provided");
      }
    }
    this.basicAuthAuthorizationStr =
        HttpSolrClientBase.basicAuthCredentialsToAuthorizationString(user, pass);
    return (B) this;
  }

  /**
   * Expert Method
   *
   * @param urlParamNames set of param keys that are only sent via the query string. Note that the
   *     param will be sent as a query string if the key is part of this Set or the SolrRequest's
   *     query params.
   * @see org.apache.solr.client.solrj.SolrRequest#getQueryParams
   */
  @SuppressWarnings("unchecked")
  public B withTheseParamNamesInTheUrl(Set<String> urlParamNames) {
    this.urlParamNames = urlParamNames;
    return (B) this;
  }

  /**
   * Set maxConnectionsPerHost for http1 connections, maximum number http2 connections is limited to
   * 4
   */
  @SuppressWarnings("unchecked")
  public B withMaxConnectionsPerHost(int max) {
    this.maxConnectionsPerHost = max;
    return (B) this;
  }

  /**
   * The max time a connection can be idle (that is, without traffic of bytes in either direction).
   * Sometimes called a "socket timeout". Note: not applicable to the JDK HttpClient.
   */
  @SuppressWarnings("unchecked")
  public B withIdleTimeout(long idleConnectionTimeout, TimeUnit unit) {
    this.idleTimeoutMillis = TimeUnit.MILLISECONDS.convert(idleConnectionTimeout, unit);
    return (B) this;
  }

  public long getIdleTimeoutMillis() {
    return idleTimeoutMillis != null && idleTimeoutMillis > 0
        ? idleTimeoutMillis
        : SolrHttpConstants.DEFAULT_SO_TIMEOUT;
  }

  /** The max time a connection can take to connect to destinations. */
  @SuppressWarnings("unchecked")
  public B withConnectionTimeout(long connectionTimeout, TimeUnit unit) {
    this.connectionTimeoutMillis = TimeUnit.MILLISECONDS.convert(connectionTimeout, unit);
    return (B) this;
  }

  public long getConnectionTimeoutMillis() {
    return connectionTimeoutMillis != null && connectionTimeoutMillis > 0
        ? connectionTimeoutMillis
        : SolrHttpConstants.DEFAULT_CONNECT_TIMEOUT;
  }

  /** Set a timeout for requests to receive a response. */
  @SuppressWarnings("unchecked")
  public B withRequestTimeout(long requestTimeout, TimeUnit unit) {
    this.requestTimeoutMillis = TimeUnit.MILLISECONDS.convert(requestTimeout, unit);
    return (B) this;
  }

  public long getRequestTimeoutMillis() {
    return requestTimeoutMillis != null && requestTimeoutMillis > 0
        ? requestTimeoutMillis
        : getIdleTimeoutMillis();
  }

  /**
   * If true, prefer http1.1 over http2. If not set, the default is determined by system property
   * 'solr.http1'. Otherwise, false.
   *
   * @param useHttp1_1 prefer http1.1?
   * @return this Builder
   */
  @SuppressWarnings("unchecked")
  public B useHttp1_1(boolean useHttp1_1) {
    this.useHttp1_1 = useHttp1_1;
    return (B) this;
  }

  /**
   * Return whether the HttpSolrClient built will prefer http1.1 over http2.
   *
   * @return whether to prefer http1.1
   */
  public boolean shouldUseHttp1_1() {
    return useHttp1_1 != null ? useHttp1_1 : defaultUseHttp1_1;
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
  @SuppressWarnings("unchecked")
  public B withProxyConfiguration(String host, int port, boolean isSocks4, boolean isSecure) {
    this.proxyHost = host;
    this.proxyPort = port;
    this.proxyIsSocks4 = isSocks4;
    this.proxyIsSecure = isSecure;
    return (B) this;
  }

  /**
   * Setup basic authentication from a string formatted as username:password. If the string is Null
   * then it doesn't do anything.
   *
   * @param credentials The username and password formatted as username:password
   * @return this Builder
   */
  @SuppressWarnings("unchecked")
  public B withOptionalBasicAuthCredentials(String credentials) {
    if (credentials != null) {
      if (credentials.indexOf(':') == -1) {
        throw new IllegalStateException(
            "Invalid Authentication credential formatting. Provide username and password in the 'username:password' format.");
      }
      String username = credentials.substring(0, credentials.indexOf(':'));
      String password = credentials.substring(credentials.indexOf(':') + 1, credentials.length());
      withBasicAuthCredentials(username, password);
    }
    return (B) this;
  }

  public Integer getMaxConnectionsPerHost() {
    return maxConnectionsPerHost;
  }

  public Boolean getFollowRedirects() {
    return followRedirects;
  }

  public String getProxyHost() {
    return proxyHost;
  }

  public int getProxyPort() {
    return proxyPort;
  }

  public boolean isProxyIsSocks4() {
    return proxyIsSocks4;
  }

  public boolean isProxyIsSecure() {
    return proxyIsSecure;
  }

  public ExecutorService getExecutor() {
    return executor;
  }
}
