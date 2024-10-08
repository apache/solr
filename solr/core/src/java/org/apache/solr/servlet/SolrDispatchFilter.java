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

import static org.apache.solr.security.AuditEvent.EventType;
import static org.apache.solr.servlet.ServletUtils.closeShield;
import static org.apache.solr.servlet.ServletUtils.configExcludes;
import static org.apache.solr.servlet.ServletUtils.excludedPath;

import com.google.common.annotations.VisibleForTesting;
import io.opentracing.Span;
import io.opentracing.Tracer;
import io.opentracing.tag.Tags;
import io.opentracing.util.GlobalTracer;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.UnavailableException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.http.client.HttpClient;
import org.apache.solr.api.V2HttpCall;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.SuppressForbidden;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.NodeRoles;
import org.apache.solr.handler.api.V2ApiUtils;
import org.apache.solr.logging.MDCLoggingContext;
import org.apache.solr.logging.MDCSnapshot;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.security.AuditEvent;
import org.apache.solr.security.AuthenticationPlugin;
import org.apache.solr.security.PKIAuthenticationPlugin;
import org.apache.solr.security.PublicKeyHandler;
import org.apache.solr.servlet.CoreContainerProvider.ServiceHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This filter looks at the incoming URL maps them to handlers defined in solrconfig.xml
 *
 * @since solr 1.2
 */
// todo: get rid of this class entirely! Request dispatch is the container's responsibility. Much of
// what we have here should be several separate but composable servlet Filters, wrapping multiple
// servlets that are more focused in scope. This should become possible now that we have a
// ServletContextListener for startup/shutdown of CoreContainer that sets up a service from which
// things like CoreContainer can be requested. (or better yet injected)
public class SolrDispatchFilter extends BaseSolrFilter implements PathExcluder {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  public static final String ATTR_TRACING_SPAN = Span.class.getName();
  public static final String ATTR_TRACING_TRACER = Tracer.class.getName();

  // TODO: see if we can get rid of the holder here (Servlet spec actually guarantees
  // ContextListeners run before filter init, but JettySolrRunner that we use for tests is
  // complicated)
  private ServiceHolder coreService;

  protected final CountDownLatch init = new CountDownLatch(1);

  protected String abortErrorMessage = null;

  private HttpSolrCallFactory solrCallFactory;

  @Override
  public void setExcludePatterns(List<Pattern> excludePatterns) {
    this.excludePatterns = excludePatterns;
  }

  private List<Pattern> excludePatterns;

  public final boolean isV2Enabled = V2ApiUtils.isEnabled();

  public HttpClient getHttpClient() {
    try {
      return coreService.getService().getHttpClient();
    } catch (UnavailableException e) {
      throw new SolrException(
          ErrorCode.SERVER_ERROR, "Internal Http Client Unavailable, startup may have failed");
    }
  }

  /**
   * Enum to define action that needs to be processed. PASSTHROUGH: Pass through to another filter
   * via webapp. FORWARD: Forward rewritten URI (without path prefix and core/collection name) to
   * another filter in the chain RETURN: Returns the control, and no further specific processing is
   * needed. This is generally when an error is set and returned. RETRY:Retry the request. In cases
   * when a core isn't found to work with, this is set.
   */
  public enum Action {
    PASSTHROUGH,
    FORWARD,
    RETURN,
    RETRY,
    ADMIN,
    REMOTEQUERY,
    PROCESS,
    ADMIN_OR_REMOTEQUERY
  }

  public SolrDispatchFilter() {}

  public static final String PROPERTIES_ATTRIBUTE = "solr.properties";

  public static final String SOLRHOME_ATTRIBUTE = "solr.solr.home";

  public static final String SOLR_INSTALL_DIR_ATTRIBUTE = "solr.install.dir";

  public static final String SOLR_DEFAULT_CONFDIR_ATTRIBUTE = "solr.default.confdir";

  public static final String SOLR_LOG_MUTECONSOLE = "solr.log.muteconsole";

  public static final String SOLR_LOG_LEVEL = "solr.log.level";

  @Override
  public void init(FilterConfig config) throws ServletException {
    try {
      coreService = CoreContainerProvider.serviceForContext(config.getServletContext());
      boolean isCoordinator =
          NodeRoles.MODE_ON.equals(
              coreService
                  .getService()
                  .getCoreContainer()
                  .nodeRoles
                  .getRoleMode(NodeRoles.Role.COORDINATOR));
      solrCallFactory =
          isCoordinator ? new CoordinatorHttpSolrCall.Factory() : new HttpSolrCallFactory() {};
      if (log.isTraceEnabled()) {
        log.trace("SolrDispatchFilter.init(): {}", this.getClass().getClassLoader());
      }

      configExcludes(this, config.getInitParameter("excludePatterns"));
    } catch (InterruptedException e) {
      throw new ServletException("Interrupted while fetching core service");

    } catch (Throwable t) {
      // catch this so our filter still works
      log.error("Could not start Dispatch Filter.", t);
      if (t instanceof Error) {
        throw (Error) t;
      }
    } finally {
      log.trace("SolrDispatchFilter.init() done");
      init.countDown();
    }
  }

  public CoreContainer getCores() throws UnavailableException {
    return coreService.getService().getCoreContainer();
  }

  @Override
  public void destroy() {
    // CoreService shuts itself down as a ContextListener. The filter does not own anything with a
    // lifecycle anymore! Yay!
  }

  @Override
  @SuppressForbidden(
      reason =
          "Set the thread contextClassLoader for all 3rd party dependencies that we cannot control")
  public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
      throws IOException, ServletException {
    try (var mdcSnapshot = MDCSnapshot.create()) {
      assert null != mdcSnapshot; // prevent compiler warning
      MDCLoggingContext.reset();
      MDCLoggingContext.setNode(getCores());
      Thread.currentThread().setContextClassLoader(getCores().getResourceLoader().getClassLoader());

      doFilter(request, response, chain, false);
    }
  }

  public void doFilter(
      ServletRequest _request, ServletResponse _response, FilterChain chain, boolean retry)
      throws IOException, ServletException {
    if (!(_request instanceof HttpServletRequest)) return;
    HttpServletRequest request = closeShield((HttpServletRequest) _request, retry);
    HttpServletResponse response = closeShield((HttpServletResponse) _response, retry);

    if (excludedPath(excludePatterns, request, response, chain)) {
      return;
    }
    Tracer t = getCores() == null ? GlobalTracer.get() : getCores().getTracer();
    request.setAttribute(ATTR_TRACING_TRACER, t);
    RateLimitManager rateLimitManager = coreService.getService().getRateLimitManager();
    try {
      ServletUtils.rateLimitRequest(
          rateLimitManager,
          request,
          response,
          () -> {
            try {
              dispatch(chain, request, response, retry);
            } catch (IOException | ServletException | SolrAuthenticationException e) {
              throw new ExceptionWhileTracing(e);
            }
          });
    } finally {
      ServletUtils.consumeInputFully(request, response);
      SolrRequestInfo.reset();
      SolrRequestParsers.cleanupMultipartFiles(request);
    }
  }

  private static Span getSpan(HttpServletRequest req) {
    return (Span) req.getAttribute(ATTR_TRACING_SPAN);
  }

  private void dispatch(
      FilterChain chain, HttpServletRequest request, HttpServletResponse response, boolean retry)
      throws IOException, ServletException, SolrAuthenticationException {

    AtomicReference<HttpServletRequest> wrappedRequest = new AtomicReference<>();
    authenticateRequest(request, response, wrappedRequest);
    if (wrappedRequest.get() != null) {
      request = wrappedRequest.get();
    }

    if (getCores().getAuthenticationPlugin() != null) {
      if (log.isDebugEnabled()) {
        log.debug("User principal: {}", request.getUserPrincipal());
      }

      final String principalName;
      if (request.getUserPrincipal() != null) {
        principalName = request.getUserPrincipal().getName();
      } else {
        principalName = null;
      }
      getSpan(request).setTag(Tags.DB_USER, String.valueOf(principalName));
    }

    HttpSolrCall call = getHttpSolrCall(request, response, retry);
    ExecutorUtil.setServerThreadFlag(Boolean.TRUE);
    try {
      Action result = call.call();
      switch (result) {
        case PASSTHROUGH:
          getSpan(request).log("SolrDispatchFilter PASSTHROUGH");
          chain.doFilter(request, response);
          break;
        case RETRY:
          getSpan(request).log("SolrDispatchFilter RETRY");
          doFilter(request, response, chain, true); // RECURSION
          break;
        case FORWARD:
          getSpan(request).log("SolrDispatchFilter FORWARD");
          request.getRequestDispatcher(call.getPath()).forward(request, response);
          break;
        case ADMIN:
        case PROCESS:
        case REMOTEQUERY:
        case ADMIN_OR_REMOTEQUERY:
        case RETURN:
          break;
      }
    } finally {
      call.destroy();
      ExecutorUtil.setServerThreadFlag(null);
    }
  }

  /**
   * Allow a subclass to modify the HttpSolrCall. In particular, subclasses may want to add
   * attributes to the request and send errors differently
   */
  protected HttpSolrCall getHttpSolrCall(
      HttpServletRequest request, HttpServletResponse response, boolean retry) {
    String path = ServletUtils.getPathAfterContext(request);

    CoreContainer cores;
    try {
      cores = getCores();
    } catch (UnavailableException e) {
      throw new SolrException(ErrorCode.SERVER_ERROR, "Core Container Unavailable");
    }
    return solrCallFactory.createInstance(this, path, cores, request, response, retry);
  }

  // TODO: make this a servlet filter
  private void authenticateRequest(
      HttpServletRequest request,
      HttpServletResponse response,
      final AtomicReference<HttpServletRequest> wrappedRequest)
      throws IOException, SolrAuthenticationException {
    boolean requestContinues;
    final AtomicBoolean isAuthenticated = new AtomicBoolean(false);
    CoreContainer cores;
    try {
      cores = getCores();
    } catch (UnavailableException e) {
      throw new SolrException(ErrorCode.SERVER_ERROR, "Core Container Unavailable");
    }
    AuthenticationPlugin authenticationPlugin = cores.getAuthenticationPlugin();
    if (authenticationPlugin == null) {
      if (shouldAudit(EventType.ANONYMOUS)) {
        cores.getAuditLoggerPlugin().doAudit(new AuditEvent(EventType.ANONYMOUS, request));
      }
      return;
    } else {
      // /admin/info/key must be always open. see SOLR-9188
      String requestPath = ServletUtils.getPathAfterContext(request);
      if (PublicKeyHandler.PATH.equals(requestPath)) {
        log.debug("Pass through PKI authentication endpoint");
        return;
      }
      // /solr/ (Admin UI) must be always open to allow displaying Admin UI with login page
      if ("/solr/".equals(requestPath) || "/".equals(requestPath)) {
        log.debug("Pass through Admin UI entry point");
        return;
      }
      String header = request.getHeader(PKIAuthenticationPlugin.HEADER);
      String headerV2 = request.getHeader(PKIAuthenticationPlugin.HEADER_V2);
      if ((header != null || headerV2 != null)
          && cores.getPkiAuthenticationSecurityBuilder() != null)
        authenticationPlugin = cores.getPkiAuthenticationSecurityBuilder();
      try {
        if (log.isDebugEnabled()) {
          log.debug(
              "Request to authenticate: {}, domain: {}, port: {}",
              request,
              request.getLocalName(),
              request.getLocalPort());
        }
        // For legacy reasons, upon successful authentication this wants to call the chain's next
        // filter, which obfuscates the layout of the code since one usually expects to be able to
        // find the call to doFilter() in the implementation of javax.servlet.Filter. Supplying a
        // trivial impl here to keep existing code happy while making the flow clearer. Chain will
        // be called after this method completes. Eventually auth all moves to its own filter
        // (hopefully). Most auth plugins simply return true after calling this anyway, so they
        // obviously don't care. Kerberos plugins seem to mostly use it to satisfy the api of a
        // wrapped instance of javax.servlet.Filter and neither of those seem to be doing anything
        // fancy with the filter chain, so this would seem to be a hack brought on by the fact that
        // our auth code has been forced to be code within dispatch filter, rather than being a
        // filter itself. The HadoopAuthPlugin has a suspicious amount of code after the call to
        // doFilter() which seems to imply that anything in this chain can get executed before
        // authentication completes, and I can't figure out how that's a good idea in the first
        // place.
        requestContinues =
            authenticationPlugin.authenticate(
                request,
                response,
                (req, rsp) -> {
                  isAuthenticated.set(true);
                  wrappedRequest.set((HttpServletRequest) req);
                });
      } catch (Exception e) {
        log.info("Error authenticating", e);
        throw new SolrException(ErrorCode.SERVER_ERROR, "Error during request authentication, ", e);
      }
    }
    // requestContinues is an optional short circuit, thus we still need to check isAuthenticated.
    // This is because the AuthenticationPlugin doesn't always have enough information to determine
    // if it should short circuit, e.g. the Kerberos Authentication Filter will send an error and
    // not call later filters in chain, but doesn't throw an exception.  We could force each Plugin
    // to implement isAuthenticated to simplify the check here, but that just moves the complexity
    // to multiple code paths.
    if (!requestContinues || !isAuthenticated.get()) {
      response.flushBuffer();
      if (shouldAudit(EventType.REJECTED)) {
        cores.getAuditLoggerPlugin().doAudit(new AuditEvent(EventType.REJECTED, request));
      }
      throw new SolrAuthenticationException();
    }
    if (shouldAudit(EventType.AUTHENTICATED)) {
      cores.getAuditLoggerPlugin().doAudit(new AuditEvent(EventType.AUTHENTICATED, request));
    }
    // Auth Success
  }

  /**
   * Check if audit logging is enabled and should happen for given event type
   *
   * @param eventType the audit event
   */
  private boolean shouldAudit(AuditEvent.EventType eventType) {
    CoreContainer cores;
    try {
      cores = getCores();
    } catch (UnavailableException e) {
      throw new SolrException(ErrorCode.SERVER_ERROR, "Core Container Unavailable");
    }
    return cores.getAuditLoggerPlugin() != null
        && cores.getAuditLoggerPlugin().shouldLog(eventType);
  }

  @VisibleForTesting
  void replaceRateLimitManager(RateLimitManager rateLimitManager) {
    coreService.getService().setRateLimitManager(rateLimitManager);
  }

  /** internal API */
  public interface HttpSolrCallFactory {
    default HttpSolrCall createInstance(
        SolrDispatchFilter filter,
        String path,
        CoreContainer cores,
        HttpServletRequest request,
        HttpServletResponse response,
        boolean retry) {
      if (filter.isV2Enabled && (path.startsWith("/____v2/") || path.equals("/____v2"))) {
        return new V2HttpCall(filter, cores, request, response, retry);
      } else {
        return new HttpSolrCall(filter, cores, request, response, retry);
      }
    }
  }
}
