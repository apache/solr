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

import jakarta.servlet.ServletConfig;
import jakarta.servlet.ServletException;
import jakarta.servlet.UnavailableException;
import jakarta.servlet.http.HttpServlet;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Solr's interface to Jetty.
 *
 * @since solr 1.2
 */
public class SolrDispatchFilter extends HttpServlet { // TODO rename to SolrServlet
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private HttpSolrCallFactory solrCallFactory;
  private CoreContainerProvider containerProvider;

  public final boolean isV2Enabled = V2ApiUtils.isEnabled();

  public SolrDispatchFilter() {}

  @Override
  public void init(ServletConfig config) throws ServletException {
    try {
      super.init(config);
      containerProvider = CoreContainerProvider.serviceForContext(config.getServletContext());
      boolean isCoordinator =
          NodeRoles.MODE_ON.equals(getCores().nodeRoles.getRoleMode(NodeRoles.Role.COORDINATOR));
      solrCallFactory =
          isCoordinator ? new CoordinatorHttpSolrCall.Factory() : new HttpSolrCallFactory() {};
      if (log.isTraceEnabled()) {
        log.trace("SolrDispatchFilter.init(): {}", this.getClass().getClassLoader());
      }
    } catch (Throwable t) {
      // catch this so our servlet still works
      log.error("Could not start Servlet.", t);
      if (t instanceof Error) {
        throw (Error) t;
      }
    } finally {
      log.trace("SolrDispatchFilter.init() done");
    }
  }

  /**
   * The CoreContainer. It's guaranteed to be constructed before this servlet is initialized, but
   * could have been shut down. Never null.
   */
  public CoreContainer getCores() throws UnavailableException {
    return containerProvider.getCoreContainer();
  }

  @Override
  protected void service(HttpServletRequest request, HttpServletResponse response)
      throws IOException, ServletException {
    // internal version that tracks if we are in a retry
    dispatch(request, response, false);
  }

  /*
  Wait? Where did X go??? (I hear you ask).

  For over a decade this class did anything and everything
  In late 2021 SOLR-15590 moved container startup to CoreContainerProvider
  In late 2025 SOLR-18040 moved request wrappers to independent ServletFilters
    such as PathExclusionFilter see web.xml for a full, up-to-date list

  This class is only handling dispatch, please think twice before adding anything else to it.
   */

  private void dispatch(HttpServletRequest request, HttpServletResponse response, boolean retry)
      throws IOException, ServletException {
    HttpSolrCall call = getHttpSolrCall(request, response, retry);

    // this flag LOOKS like it should be in RequiredSolrRequestFilter, but
    // the value set here is drives PKIAuthenticationPlugin.isSolrThread
    // which gets used BEFORE this is set for some reason.
    // BUG? Important timing?
    ExecutorUtil.setServerThreadFlag(Boolean.TRUE);
    try {
      switch (call.call()) {
        case RETRY -> {
          getSpan(request).addEvent("Action RETRY");
          // RECURSION
          dispatch(request, response, true);
        }
        case FORWARD -> {
          getSpan(request).addEvent("Action FORWARD");
          request.getRequestDispatcher(call.getPath()).forward(request, response);
        }
        default -> {}
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
