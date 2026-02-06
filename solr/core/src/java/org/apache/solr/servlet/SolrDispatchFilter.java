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

import static org.apache.solr.util.tracing.TraceUtils.getSpan;

import io.opentelemetry.api.trace.Span;
import jakarta.servlet.FilterChain;
import jakarta.servlet.FilterConfig;
import jakarta.servlet.ServletException;
import jakarta.servlet.UnavailableException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import org.apache.solr.api.V2HttpCall;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.NodeRoles;
import org.apache.solr.handler.api.V2ApiUtils;
import org.apache.solr.util.tracing.TraceUtils;
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
public class SolrDispatchFilter extends CoreContainerAwareHttpFilter {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  protected String abortErrorMessage = null;

  private HttpSolrCallFactory solrCallFactory;

  public final boolean isV2Enabled = V2ApiUtils.isEnabled();

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
    REMOTEPROXY,
    PROCESS,
    ADMIN_OR_REMOTEPROXY
  }

  public SolrDispatchFilter() {}

  public static final String PROPERTIES_ATTRIBUTE = "solr.properties";

  public static final String SOLRHOME_ATTRIBUTE = "solr.solr.home";

  public static final String SOLR_INSTALL_DIR_ATTRIBUTE = "solr.install.dir";

  public static final String SOLR_CONFIGSET_DEFAULT_CONFDIR_ATTRIBUTE =
      "solr.configset.default.confdir";

  public static final String SOLR_LOG_MUTECONSOLE = "solr.log.muteconsole";

  public static final String SOLR_LOG_LEVEL = "solr.log.level";

  @Override
  public void init(FilterConfig config) throws ServletException {
    try {
      super.init(config);
      boolean isCoordinator =
          NodeRoles.MODE_ON.equals(getCores().nodeRoles.getRoleMode(NodeRoles.Role.COORDINATOR));
      solrCallFactory =
          isCoordinator ? new CoordinatorHttpSolrCall.Factory() : new HttpSolrCallFactory() {};
      if (log.isTraceEnabled()) {
        log.trace("SolrDispatchFilter.init(): {}", this.getClass().getClassLoader());
      }

    } catch (Throwable t) {
      // catch this so our filter still works
      log.error("Could not start Dispatch Filter.", t);
      if (t instanceof Error) {
        throw (Error) t;
      }
    } finally {
      log.trace("SolrDispatchFilter.init() done");
    }
  }

  @Override
  public void doFilter(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
      throws IOException, ServletException {
    // internal version of doFilter that tracks if we are in a retry

    dispatch(chain, request, response, false);
  }

  /*
  Wait? Where did X go??? (I hear you ask).

  For over a decade this class did anything and everything
  In late 2021 SOLR-15590 moved container startup to CoreContainerProvider
  In late 2025 SOLR-18040 moved request wrappers to independent ServletFilters
    such as PathExclusionFilter see web.xml for a full, up-to-date list

  This class is moving toward only handling dispatch, please think twice
  before adding anything else to it.
   */

  private void dispatch(
      FilterChain chain, HttpServletRequest request, HttpServletResponse response, boolean retry)
      throws IOException, ServletException {
    var span = getSpan(request);
    HttpSolrCall call = getHttpSolrCall(request, response, retry);

    // this flag LOOKS like it should be in RequiredSolrRequestFilter, but
    // the value set here is drives PKIAuthenticationPlugin.isSolrThread
    // which gets used BEFORE this is set for some reason.
    // BUG? Important timing?
    ExecutorUtil.setServerThreadFlag(Boolean.TRUE);
    try {
      Action result = call.call();
      switch (result) {
        case PASSTHROUGH:
          span.addEvent("SolrDispatchFilter PASSTHROUGH");
          chain.doFilter(request, response);
          break;
        case RETRY:
          span.addEvent("SolrDispatchFilter RETRY");
          // RECURSION
          dispatch(chain, request, response, true);
          break;
        case FORWARD:
          span.addEvent("SolrDispatchFilter FORWARD");
          request.getRequestDispatcher(call.getPath()).forward(request, response);
          break;
        case ADMIN:
        case PROCESS:
        case REMOTEPROXY:
        case ADMIN_OR_REMOTEPROXY:
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
