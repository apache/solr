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
package org.apache.solr.security;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongCounter;
import jakarta.servlet.FilterChain;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletRequestWrapper;
import jakarta.servlet.http.HttpServletResponse;
import java.security.Principal;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.apache.solr.core.SolrInfoBean;
import org.apache.solr.metrics.SolrMetricsContext;
import org.apache.solr.metrics.otel.OtelUnit;
import org.apache.solr.metrics.otel.instruments.AttributedLongCounter;
import org.apache.solr.metrics.otel.instruments.AttributedLongTimer;
import org.eclipse.jetty.client.Request;

/**
 * @lucene.experimental
 */
public abstract class AuthenticationPlugin implements SolrInfoBean {

  public static final String AUTHENTICATION_PLUGIN_PROP = "solr.security.auth.plugin";
  public static final String HTTP_HEADER_X_SOLR_AUTHDATA = "X-Solr-AuthData";

  // Metrics
  private Set<String> metricNames = ConcurrentHashMap.newKeySet();
  protected SolrMetricsContext solrMetricsContext;

  protected AttributedLongCounter numErrors;
  protected AttributedLongCounter requests;
  protected AttributedLongTimer requestTimes;
  protected AttributedLongCounter numAuthenticated;
  protected AttributedLongCounter numPassThrough;
  protected AttributedLongCounter numWrongCredentials;
  protected AttributedLongCounter numMissingCredentials;

  /**
   * This is called upon loading up of a plugin, used for setting it up.
   *
   * @param pluginConfig Config parameters, possibly from a ZK source
   */
  public abstract void init(Map<String, Object> pluginConfig);

  /**
   * This method attempts to authenticate the request. Upon a successful authentication, this must
   * call the next filter in the filter chain and set the user principal of the request, or else,
   * upon an error or an authentication failure, throw an exception.
   *
   * @param request the http request
   * @param response the http response
   * @param filterChain the servlet filter chain
   * @return false if the request not be processed by Solr (not continue), i.e. the response and
   *     status code have already been sent.
   * @throws Exception any exception thrown during the authentication, e.g.
   *     PrivilegedActionException
   */
  public abstract boolean doAuthenticate(
      HttpServletRequest request, HttpServletResponse response, FilterChain filterChain)
      throws Exception;

  /**
   * This method is called by SolrDispatchFilter in order to initiate authentication. It does some
   * standard metrics counting.
   */
  public final boolean authenticate(
      HttpServletRequest request, HttpServletResponse response, FilterChain filterChain)
      throws Exception {
    AttributedLongTimer.MetricTimer timer = requestTimes.start(TimeUnit.NANOSECONDS);
    requests.inc();
    try {
      return doAuthenticate(request, response, filterChain);
    } catch (Exception e) {
      numErrors.inc();
      throw e;
    } finally {
      // Record the timing metric
      timer.stop();
    }
  }

  protected HttpServletRequest wrapWithPrincipal(HttpServletRequest request, Principal principal) {
    return wrapWithPrincipal(request, principal, principal.getName());
  }

  protected HttpServletRequest wrapWithPrincipal(
      HttpServletRequest request, Principal principal, String username) {
    return new HttpServletRequestWrapper(request) {
      @Override
      public Principal getUserPrincipal() {
        return principal;
      }

      @Override
      public String getRemoteUser() {
        return username;
      }
    };
  }

  /**
   * Override this method to intercept internode requests. This allows your authentication plugin to
   * decide on per-request basis whether it should handle inter-node requests or delegate to {@link
   * PKIAuthenticationPlugin}. Return true to indicate that your plugin did handle the request, or
   * false to signal that PKI plugin should handle it. This method will be called by {@link
   * PKIAuthenticationPlugin}'s interceptor.
   *
   * <p>If not overridden, this method will return true for plugins implementing {@link
   * HttpClientBuilderPlugin}. This method can be overridden by subclasses e.g. to set HTTP headers,
   * even if you don't use a clientBuilder.
   *
   * @param request the httpRequest that is about to be sent to another internal Solr node
   * @return true if this plugin handled authentication for the request, else false
   */
  protected boolean interceptInternodeRequest(Request request) {
    return this instanceof HttpClientBuilderPlugin;
  }

  /** Cleanup any per request data */
  public void closeRequest() {}

  @Override
  public SolrMetricsContext getSolrMetricsContext() {
    return solrMetricsContext;
  }

  @Override
  public void initializeMetrics(SolrMetricsContext parentContext, Attributes attributes) {
    this.solrMetricsContext = parentContext.getChildContext(this);
    Attributes attrsWithCategory =
        Attributes.builder()
            .putAll(attributes)
            .put(CATEGORY_ATTR, getCategory().toString())
            .put(PLUGIN_NAME_ATTR, this.getClass().getSimpleName())
            .build();
    // Metrics
    numErrors =
        new AttributedLongCounter(
            this.solrMetricsContext.longCounter(
                "solr_authentication_errors", "Count of errors during authentication"),
            attrsWithCategory);
    requests =
        new AttributedLongCounter(
            this.solrMetricsContext.longCounter(
                "solr_authentication_requests", "Count of requests for authentication"),
            attrsWithCategory);
    numAuthenticated =
        new AttributedLongCounter(
            this.solrMetricsContext.longCounter(
                "solr_authentication_num_authenticated",
                "Count of successful requests for authentication"),
            attrsWithCategory);
    numPassThrough =
        new AttributedLongCounter(
            this.solrMetricsContext.longCounter(
                "solr_authentication_num_pass_through",
                "Count of requests allowed to pass through without authentication credentials (as enabled with configuration \"blockUnknown\": false)"),
            attrsWithCategory);
    LongCounter solrAuthenticationPluginFail =
        this.solrMetricsContext.longCounter(
            "solr_authentication_failures",
            "Count of authentication failures (unsuccessful, but processed correctly)");
    numWrongCredentials =
        new AttributedLongCounter(
            solrAuthenticationPluginFail,
            attrsWithCategory.toBuilder().put(TYPE_ATTR, "wrong_credentials").build());
    numMissingCredentials =
        new AttributedLongCounter(
            solrAuthenticationPluginFail,
            attrsWithCategory.toBuilder().put(TYPE_ATTR, "missing_credentials").build());
    requestTimes =
        new AttributedLongTimer(
            this.solrMetricsContext.longHistogram(
                "solr_authentication_request_times",
                "Distribution of authentication request durations",
                OtelUnit.NANOSECONDS),
            attrsWithCategory);
  }

  @Override
  public String getName() {
    return this.getClass().getName();
  }

  @Override
  public String getDescription() {
    return "Authentication Plugin " + this.getClass().getName();
  }

  @Override
  public Category getCategory() {
    return Category.SECURITY;
  }
}
