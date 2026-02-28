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

import static org.apache.solr.common.SolrException.ErrorCode.SERVER_ERROR;
import static org.apache.solr.security.AuditEvent.EventType.ANONYMOUS;
import static org.apache.solr.security.AuditEvent.EventType.AUTHENTICATED;
import static org.apache.solr.security.AuditEvent.EventType.REJECTED;

import io.opentelemetry.api.trace.Span;
import jakarta.servlet.FilterChain;
import jakarta.servlet.FilterConfig;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import org.apache.solr.common.SolrException;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.security.AuthenticationPlugin;
import org.apache.solr.security.PKIAuthenticationPlugin;
import org.apache.solr.security.PublicKeyHandler;
import org.apache.solr.util.tracing.TraceUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AuthenticationFilter extends CoreContainerAwareHttpFilter {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  public static final String PLUGIN_DID_NOT_SET_ERROR_STATUS =
      "{} threw SolrAuthenticationException without setting request status >= 400";

  @Override
  public void init(FilterConfig config) throws ServletException {
    super.init(config);
  }

  @Override
  protected void doFilter(HttpServletRequest req, HttpServletResponse res, FilterChain chain)
      throws IOException, ServletException {

    CoreContainer cc = getCores();
    AuthenticationPlugin authenticationPlugin = cc.getAuthenticationPlugin();
    String requestPath = ServletUtils.getPathAfterContext(req);

    /////////////////////////////////////////////////////////
    //////// Check cases where auth is not required. ////////
    /////////////////////////////////////////////////////////

    // Is authentication configured?
    if (authenticationPlugin == null) {
      cc.audit(ANONYMOUS, req);
      chain.doFilter(req, res);
      return;
    }

    // /admin/info/key must be always open. see SOLR-9188
    if (PublicKeyHandler.PATH.equals(requestPath)) {
      log.debug("Pass through PKI authentication endpoint");
      chain.doFilter(req, res);
      return;
    }

    if (isAdminUI(requestPath)) {
      log.debug("Pass through Admin UI entry point");
      chain.doFilter(req, res);
      return;
    }

    /////////////////////////////////////////////////////////////////
    //////// if we make it here, authentication is required. ////////
    /////////////////////////////////////////////////////////////////

    // internode (internal) requests have their own PKI auth plugin
    if (isInternodePKI(req, cc)) {
      authenticationPlugin = cc.getPkiAuthenticationSecurityBuilder();
    }

    boolean authSuccess = false;
    try {
      authSuccess =
          authenticate(req, res, new AuditSuccessChainWrapper(cc, chain), authenticationPlugin);
    } finally {
      if (!authSuccess) {
        cc.audit(REJECTED, req);
        res.flushBuffer();
        if (res.getStatus() < 400) {
          log.error(PLUGIN_DID_NOT_SET_ERROR_STATUS, authenticationPlugin.getClass());
          res.sendError(401, "Authentication Plugin rejected credentials.");
        }
      }
    }
  }

  /**
   * The plugin called by this method will call doFilter(req, res) for us (which is why we pass in
   * the filter chain object as a parameter).
   */
  private boolean authenticate(
      HttpServletRequest req,
      HttpServletResponse res,
      FilterChain chain,
      AuthenticationPlugin authenticationPlugin) {
    try {

      // It is imperative that authentication plugins obey the contract in the javadoc for
      // org.apache.solr.security.AuthenticationPlugin.doAuthenticate, and either return
      // false or throw an exception if they cannot validate the user's credentials.
      // Any plugin that doesn't do this is broken and should be fixed.

      logAuthAttempt(req);
      return authenticationPlugin.authenticate(req, res, chain);
    } catch (Exception e) {
      log.info("Error authenticating", e);
      throw new SolrException(SERVER_ERROR, "Error during request authentication, ", e);
    }
  }

  private static boolean isAdminUI(String requestPath) {
    return "/solr/".equals(requestPath) || "/".equals(requestPath);
  }

  private boolean isInternodePKI(HttpServletRequest req, CoreContainer cores) {
    String header = req.getHeader(PKIAuthenticationPlugin.HEADER);
    String headerV2 = req.getHeader(PKIAuthenticationPlugin.HEADER_V2);
    return (header != null || headerV2 != null)
        && cores.getPkiAuthenticationSecurityBuilder() != null;
  }

  private void logAuthAttempt(HttpServletRequest req) {
    // moved this to a method because spotless formatting is so horrible, and makes the log
    // message look like a big deal... but it's just taking up space
    if (log.isDebugEnabled()) {
      log.debug(
          "Request to authenticate: {}, domain: {}, port: {}",
          req,
          req.getLocalName(),
          req.getLocalPort());
    }
  }



  private record AuditSuccessChainWrapper(CoreContainer cc, FilterChain chain)
      implements FilterChain {

    @Override
    public void doFilter(ServletRequest rq, ServletResponse rsp)
        throws IOException, ServletException {
      // this is a hack. The authentication plugin should accept a callback
      // to be executed before doFilter is called if authentication succeeds
      cc.audit(AUTHENTICATED, (HttpServletRequest) rq);
      Span span = TraceUtils.getSpan((HttpServletRequest) rq);
      setPrincipalForTracing((HttpServletRequest) rq, span);
      chain.doFilter(rq, rsp);
    }

    private void setPrincipalForTracing(HttpServletRequest request, Span span) {
      if (log.isDebugEnabled()) {
        log.debug("User principal: {}", request.getUserPrincipal());
      }
      final String principalName;
      if (request.getUserPrincipal() != null) {
        principalName = request.getUserPrincipal().getName();
      } else {
        principalName = null;
      }
      TraceUtils.setUser(span, String.valueOf(principalName));
    }
  }
}
