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

import static org.apache.solr.common.cloud.ZkStateReader.COLLECTION_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.CORE_NAME_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.NODE_NAME_PROP;
import static org.apache.solr.servlet.SolrDispatchFilter.Action.ADMIN;
import static org.apache.solr.servlet.SolrDispatchFilter.Action.FORWARD;
import static org.apache.solr.servlet.SolrDispatchFilter.Action.PASSTHROUGH;
import static org.apache.solr.servlet.SolrDispatchFilter.Action.PROCESS;
import static org.apache.solr.servlet.SolrDispatchFilter.Action.REMOTEPROXY;
import static org.apache.solr.servlet.SolrDispatchFilter.Action.RETRY;
import static org.apache.solr.servlet.SolrDispatchFilter.Action.RETURN;

import io.opentelemetry.api.trace.Span;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.EOFException;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.function.Supplier;
import net.jcip.annotations.ThreadSafe;
import org.apache.solr.api.ApiBag;
import org.apache.solr.api.V2HttpCall;
import org.apache.solr.client.api.util.SolrVersion;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.Aliases;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.CommandOperation;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.JsonSchemaValidator;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.util.SuppressForbidden;
import org.apache.solr.common.util.Utils;
import org.apache.solr.common.util.ValidatingJsonMap;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrQueryRequestBase;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.response.QueryResponseWriter;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.security.AuditEvent;
import org.apache.solr.security.AuditEvent.EventType;
import org.apache.solr.security.AuthenticationPlugin;
import org.apache.solr.security.AuthorizationContext;
import org.apache.solr.security.AuthorizationContext.CollectionRequest;
import org.apache.solr.security.AuthorizationContext.RequestType;
import org.apache.solr.security.AuthorizationUtils;
import org.apache.solr.security.HttpServletAuthorizationContext;
import org.apache.solr.security.PublicKeyHandler;
import org.apache.solr.servlet.SolrDispatchFilter.Action;
import org.apache.solr.servlet.cache.HttpCacheHeaderUtil;
import org.apache.solr.servlet.cache.Method;
import org.apache.solr.update.processor.DistributingUpdateProcessorFactory;
import org.apache.solr.util.RTimerTree;
import org.apache.solr.util.tracing.TraceUtils;
import org.apache.zookeeper.KeeperException;
import org.eclipse.jetty.client.HttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MarkerFactory;

/** This class represents a call made to Solr */
@ThreadSafe
public class HttpSolrCall {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final String INTERNAL_REQUEST_COUNT = "_forwardedCount";

  protected final SolrDispatchFilter solrDispatchFilter;
  protected final CoreContainer cores;
  protected final HttpServletRequest req;
  protected final HttpServletResponse response;
  protected final boolean retry;
  private final SolrVersion userAgentSolrVersion; // of the client
  private final Span span;
  protected SolrCore core = null;
  protected SolrQueryRequest solrReq = null;
  private boolean mustClearSolrRequestInfo = false;
  protected SolrRequestHandler handler = null;
  protected SolrParams queryParams;
  protected String path;
  protected Action action;
  protected String coreUrl;
  protected SolrConfig config;
  protected Map<String, Integer> invalidStates;

  // The states of client that is invalid in this request
  // What's in the URL path; might reference a collection/alias or a Solr core name
  protected String origCorename;
  // The list of SolrCloud collections if in SolrCloud (usually 1)
  protected List<String> collectionsList;

  protected RequestType requestType;

  public HttpSolrCall(
      SolrDispatchFilter solrDispatchFilter,
      CoreContainer cores,
      HttpServletRequest request,
      HttpServletResponse response,
      boolean retry) {
    this.solrDispatchFilter = solrDispatchFilter;
    this.cores = cores;
    this.req = request;
    this.response = response;
    this.retry = retry;
    this.requestType = RequestType.UNKNOWN;
    this.userAgentSolrVersion = parseUserAgentSolrVersion();
    this.span = Optional.ofNullable(TraceUtils.getSpan(req)).orElse(Span.getInvalid());
    this.path = ServletUtils.getPathAfterContext(req);

    req.setAttribute(HttpSolrCall.class.getName(), this);
    // set a request timer which can be reused by requests if needed
    req.setAttribute(SolrRequestParsers.REQUEST_TIMER_SERVLET_ATTRIBUTE, new RTimerTree());
    // put the core container in request attribute
    req.setAttribute("org.apache.solr.CoreContainer", cores);
  }

  public String getPath() {
    return path;
  }

  /**
   * WARNING: This method returns a non-null {@link HttpServletRequest}, but calling certain methods
   * on it — such as {@link HttpServletRequest#getAttribute(String)} — may throw {@link
   * NullPointerException} if accessed outside the original servlet thread (e.g., in asynchronous
   * tasks).
   *
   * <p>Always cache required request data early during request handling if it needs to be accessed
   * later.
   */
  public HttpServletRequest getReq() {
    return req;
  }

  public SolrCore getCore() {
    return core;
  }

  public SolrParams getQueryParams() {
    return queryParams;
  }

  /** The collection(s) referenced in this request. Populated in {@link #init()}. Not null. */
  public List<String> getCollectionsList() {
    return collectionsList != null ? collectionsList : Collections.emptyList();
  }

  @SuppressForbidden(
      reason =
          "Set the thread contextClassLoader for all 3rd party dependencies that we cannot control")
  protected void init() throws Exception {
    // check for management path
    String alternate = cores.getManagementPath();
    if (alternate != null && path.startsWith(alternate)) {
      path = path.substring(0, alternate.length());
    }

    queryParams = SolrRequestParsers.parseQueryString(req.getQueryString());

    // Check for container handlers
    handler = cores.getRequestHandler(path);
    if (handler != null) {
      solrReq = SolrRequestParsers.DEFAULT.parse(null, path, req);
      solrReq.getContext().put(CoreContainer.class.getName(), cores);
      requestType = RequestType.ADMIN;
      action = ADMIN;
      return;
    }

    // Parse a core or collection name from the path and attempt to see if it's a core name
    int idx = path.indexOf('/', 1);
    if (idx > 1) {
      origCorename = path.substring(1, idx);

      // Try to resolve a Solr core name
      core = cores.getCore(origCorename);
      if (core != null) {
        path = path.substring(idx);
      } else {
        // extra mem barriers, so don't look at this before trying to get core
        if (cores.isCoreLoading(origCorename)) {
          throw new SolrException(ErrorCode.SERVICE_UNAVAILABLE, "SolrCore is loading");
        }
        // the core may have just finished loading
        core = cores.getCore(origCorename);
        if (core != null) {
          path = path.substring(idx);
        } else {
          if (!cores.isZooKeeperAware()) {
            core = cores.getCore("");
          }
        }
      }
    }

    if (cores.isZooKeeperAware()) {
      // init collectionList (usually one name but not when there are aliases)
      String def = core != null ? core.getCoreDescriptor().getCollectionName() : origCorename;
      collectionsList =
          resolveCollectionListOrAlias(
              queryParams.get(COLLECTION_PROP, def)); // &collection= takes precedence

      if (core == null) {
        // lookup core from collection, or route away if need to
        // route to 1st
        String collectionName = collectionsList.isEmpty() ? null : collectionsList.get(0);
        // TODO try the other collections if can't find a local replica of the first?   (and do to
        // V2HttpSolrCall)

        boolean isPreferLeader = (path.endsWith("/update") || path.contains("/update/"));

        // find a local replica/core for the collection
        core = getCoreByCollection(collectionName, isPreferLeader);
        if (core != null) {
          if (idx > 0) {
            path = path.substring(idx);
          }
        } else {
          // if we couldn't find it locally, look on other nodes
          if (idx > 0) {
            extractRemotePath(collectionName);
            if (action == REMOTEPROXY) {
              path = path.substring(idx);
              return;
            }
          }
          if (action != null) return;
        }
      }
    }

    // With a valid core...
    if (core != null) {
      Thread.currentThread().setContextClassLoader(core.getResourceLoader().getClassLoader());

      config = core.getSolrConfig();
      // get or create/cache the parser for the core
      SolrRequestParsers parser = config.getRequestParsers();

      // Determine the handler from the url path if not set
      // (we might already have selected the cores handler)
      extractHandlerFromURLPath(parser);
      if (action != null) return;

      // With a valid handler and a valid core...
      if (handler != null) {
        // if not a /select, create the request
        if (solrReq == null) {
          solrReq = parser.parse(core, path, req);
        }

        invalidStates =
            checkStateVersionsAreValid(solrReq.getParams().get(CloudSolrClient.STATE_VERSION));

        addCollectionParamIfNeeded(getCollectionsList());

        action = PROCESS;
        return; // we are done with a valid handler
      }
    }
    log.debug("no handler or core retrieved for {}, follow through...", path);

    action = PASSTHROUGH;
  }

  /**
   * Resolves the specified collection name to a {@link DocCollection} object. If Solr is not in
   * cloud mode, a {@link SolrException} is thrown. Returns null if the collection name is null or
   * empty. Retrieves the {@link DocCollection} using the {@link ZkStateReader} from {@link
   * CoreContainer}. If the collection is null, updates the state by refreshing aliases and forcing
   * a collection update.
   */
  protected DocCollection resolveDocCollection(String collectionName) {
    if (!cores.isZooKeeperAware()) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST, "Solr not running in cloud mode ");
    }
    if (collectionName == null || collectionName.trim().isEmpty()) {
      return null;
    }
    ZkStateReader zkStateReader = cores.getZkController().getZkStateReader();
    Supplier<DocCollection> logic =
        () -> zkStateReader.getClusterState().getCollectionOrNull(collectionName);

    DocCollection docCollection = logic.get();
    if (docCollection != null) {
      return docCollection;
    }
    // ensure our view is up to date before trying again
    try {
      zkStateReader.aliasesManager.update();
      zkStateReader.forceUpdateCollection(collectionName);
    } catch (Exception e) {
      log.error("Error trying to update state while resolving collection.", e);
      // don't propagate exception on purpose
    }
    return logic.get();
  }

  /**
   * Resolves the parameter as a potential comma delimited list of collections, and resolves aliases
   * too. One level of aliases pointing to another alias is supported. De-duplicates and retains the
   * order. {@link #getCollectionsList()}
   */
  protected List<String> resolveCollectionListOrAlias(String collectionStr) {
    if (collectionStr == null || collectionStr.trim().isEmpty()) {
      return Collections.emptyList();
    }
    List<String> result = null;
    LinkedHashSet<String> uniqueList = null;
    Aliases aliases = cores.getAliases();
    List<String> inputCollections = StrUtils.splitSmart(collectionStr, ",", true);
    if (inputCollections.size() > 1) {
      uniqueList = new LinkedHashSet<>();
    }
    for (String inputCollection : inputCollections) {
      List<String> resolvedCollections = aliases.resolveAliases(inputCollection);
      if (uniqueList != null) {
        uniqueList.addAll(resolvedCollections);
      } else {
        result = resolvedCollections;
      }
    }
    if (uniqueList != null) {
      return new ArrayList<>(uniqueList);
    } else {
      return result;
    }
  }

  /** Extract handler from the URL path if not set. */
  protected void extractHandlerFromURLPath(SolrRequestParsers parser) throws Exception {
    if (handler == null && path.length() > 1) { // don't match "" or "/" as valid path
      handler = core.getRequestHandler(path);
    }
  }

  protected void extractRemotePath(String collectionName)
      throws KeeperException, InterruptedException, SolrException {
    assert core == null;
    coreUrl = getRemoteCoreUrl(collectionName);
    // don't proxy for internal update requests
    invalidStates = checkStateVersionsAreValid(queryParams.get(CloudSolrClient.STATE_VERSION));
    if (coreUrl != null
        && queryParams.get(DistributingUpdateProcessorFactory.DISTRIB_UPDATE_PARAM) == null) {
      if (invalidStates != null) {
        // it does not make sense to send the request to a remote node
        throw new SolrException(
            SolrException.ErrorCode.INVALID_STATE,
            new String(Utils.toJSON(invalidStates), StandardCharsets.UTF_8));
      }
      action = REMOTEPROXY;
    } else {
      if (!retry) {
        // we couldn't find a core to work with, try reloading aliases & this collection
        if (!cores.getZkController().getZkStateReader().aliasesManager.update()
            && !cores
                .getZkController()
                .zkStateReader
                .getZkClient()
                .exists(DocCollection.getCollectionPath(collectionName), true)) {
          // no change and such a collection does not exist. go back
          return;
        }
        cores.getZkController().zkStateReader.forceUpdateCollection(collectionName);
        action = RETRY;
      }
    }
  }

  /** This method processes the request. */
  public Action call() throws IOException {

    if (cores == null) {
      sendError(503, "Server is shutting down or failed to initialize");
      return RETURN;
    }

    if (solrDispatchFilter.abortErrorMessage != null) {
      sendError(500, solrDispatchFilter.abortErrorMessage);
      if (shouldAudit(EventType.ERROR)) {
        cores.getAuditLoggerPlugin().doAudit(new AuditEvent(EventType.ERROR, getReq()));
      }
      return RETURN;
    }

    try {
      init();

      TraceUtils.ifNotNoop(getSpan(), this::populateTracingSpan);

      // Perform authorization here, if:
      //    (a) Authorization is enabled, and
      //    (b) The requested resource is not a known static file
      //    (c) And this request should be handled by this node (see NOTE below)
      // NOTE: If the query is to be handled by another node, then let that node do the
      // authorization.
      // In case of authentication using BasicAuthPlugin, for example, the internode request
      // is secured using PKI authentication and the internode request here will contain the
      // original user principal as a payload/header, using which the receiving node should be
      // able to perform the authorization.
      if (cores.getAuthorizationPlugin() != null
          && shouldAuthorize()
          && !(action == REMOTEPROXY || action == FORWARD)) {
        final AuthorizationContext authzContext = getAuthCtx();
        AuthorizationUtils.AuthorizationFailure authzFailure =
            AuthorizationUtils.authorize(req, response, cores, authzContext);
        if (authzFailure != null) {
          sendError(authzFailure.getStatusCode(), authzFailure.getMessage());
          return RETURN;
        }
      }

      HttpServletResponse resp = response;
      switch (action) {
        case ADMIN_OR_REMOTEPROXY:
          handleAdminOrRemoteRequest();
          return RETURN;
        case ADMIN:
          handleAdminRequest();
          return RETURN;
        case REMOTEPROXY:
          sendRemoteProxy();
          return RETURN;
        case PROCESS:
          final Method reqMethod = Method.getMethod(req.getMethod());
          HttpCacheHeaderUtil.setCacheControlHeader(config, resp, reqMethod);
          // unless we have been explicitly told not to, do cache validation
          // if we fail cache validation, execute the query
          if (config.getHttpCachingConfig().isNever304()
              || !HttpCacheHeaderUtil.doCacheHeaderValidation(solrReq, req, reqMethod, resp)) {
            SolrQueryResponse solrRsp = new SolrQueryResponse();
            /* even for HEAD requests, we need to execute the handler to
             * ensure we don't get an error (and to make sure the correct
             * QueryResponseWriter is selected and we get the correct
             * Content-Type)
             */
            SolrRequestInfo.setRequestInfo(new SolrRequestInfo(solrReq, solrRsp, action));
            mustClearSolrRequestInfo = true;
            executeCoreRequest(solrRsp);
            if (shouldAudit(cores)) {
              EventType eventType =
                  solrRsp.getException() == null ? EventType.COMPLETED : EventType.ERROR;
              if (shouldAudit(cores, eventType)) {
                cores
                    .getAuditLoggerPlugin()
                    .doAudit(
                        new AuditEvent(
                            eventType,
                            req,
                            getAuthCtx(),
                            solrReq.getRequestTimer().getTime(),
                            solrRsp.getException()));
              }
            }
            HttpCacheHeaderUtil.checkHttpCachingVeto(solrRsp, resp, reqMethod);
            Iterator<Map.Entry<String, String>> headers = solrRsp.httpHeaders();
            while (headers.hasNext()) {
              Map.Entry<String, String> entry = headers.next();
              resp.addHeader(entry.getKey(), entry.getValue());
            }
            QueryResponseWriter responseWriter = getResponseWriter();
            if (invalidStates != null)
              solrReq.getContext().put(CloudSolrClient.STATE_VERSION, invalidStates);
            writeResponse(solrRsp, responseWriter, reqMethod);
          }
          return RETURN;
        default:
          return action;
      }
    } catch (Throwable ex) {
      if (shouldAudit(EventType.ERROR)) {
        cores.getAuditLoggerPlugin().doAudit(new AuditEvent(EventType.ERROR, ex, req));
      }
      sendError(ex);
      // walk the entire cause chain to search for an Error
      Throwable t = ex;
      while (t != null) {
        if (t instanceof Error) {
          if (t != ex) {
            log.error(
                "An Error was wrapped in another exception - please report complete stacktrace on SOLR-6161",
                ex);
          }
          throw (Error) t;
        }
        t = t.getCause();
      }
      return RETURN;
    }
  }

  /**
   * Handle a request whose "type" could not be discerned in advance and may be either "admin" or
   * "remoteproxy".
   *
   * <p>Some implementations (such as {@link V2HttpCall}) may find it difficult to differentiate all
   * request types in advance. This method serves as a hook; allowing those implementations to
   * handle these cases gracefully.
   *
   * @see V2HttpCall
   */
  protected void handleAdminOrRemoteRequest() throws IOException {
    throw new IllegalStateException(
        "handleOrForwardRequest should not be invoked when serving v1 requests.");
  }

  /** Get the Span for this request. Not null. */
  public Span getSpan() {
    return span;
  }

  // called after init().
  protected void populateTracingSpan(Span span) {
    // Set db.instance
    String coreOrColName = getCoreOrColName();
    TraceUtils.setDbInstance(span, coreOrColName);

    // Set operation name.
    String path = getPath();
    if (coreOrColName != null) {
      // prefix path by core or collection name
      if (getCore() != null && getCore().getName().equals(coreOrColName)) {
        path = "/{core}" + path;
      } else {
        path = "/{collection}" + path;
      }
    }
    String verb =
        getQueryParams().get(CoreAdminParams.ACTION, req.getMethod()).toLowerCase(Locale.ROOT);
    span.updateName(verb + ":" + path);
  }

  protected String getCoreOrColName() {
    String coreOrColName = HttpSolrCall.this.origCorename;
    if (coreOrColName == null && getCore() != null) {
      coreOrColName = getCore().getName();
    }
    return coreOrColName;
  }

  public boolean shouldAudit() {
    return shouldAudit(cores);
  }

  public boolean shouldAudit(AuditEvent.EventType eventType) {
    return shouldAudit(cores, eventType);
  }

  public static boolean shouldAudit(CoreContainer cores) {
    return cores.getAuditLoggerPlugin() != null;
  }

  public static boolean shouldAudit(CoreContainer cores, AuditEvent.EventType eventType) {
    return shouldAudit(cores) && cores.getAuditLoggerPlugin().shouldLog(eventType);
  }

  private boolean shouldAuthorize() {
    if (PublicKeyHandler.PATH.equals(path)) return false;
    // admin/info/key is the path where public key is exposed . it is always unsecured
    if ("/".equals(path) || "/solr/".equals(path))
      return false; // Static Admin UI files must always be served
    if (cores.getPkiAuthenticationSecurityBuilder() != null && req.getUserPrincipal() != null) {
      boolean b = cores.getPkiAuthenticationSecurityBuilder().needsAuthorization(req);
      log.debug("PkiAuthenticationPlugin says authorization required : {} ", b);
      return b;
    }
    return true;
  }

  void destroy() {
    try {
      if (solrReq != null) {
        log.debug("Closing out SolrRequest: {}", solrReq);
        solrReq.close();
      }
    } finally {
      try {
        if (core != null) core.close();
      } finally {
        if (mustClearSolrRequestInfo) {
          SolrRequestInfo.clearRequestInfo();
        }
      }
      AuthenticationPlugin authcPlugin = cores.getAuthenticationPlugin();
      if (authcPlugin != null) authcPlugin.closeRequest();
    }
  }

  protected void sendRemoteProxy() throws IOException {
    ModifiableSolrParams updatedQueryParams = new ModifiableSolrParams(queryParams);
    int forwardCount = queryParams.getInt(INTERNAL_REQUEST_COUNT, 0) + 1;
    updatedQueryParams.set(INTERNAL_REQUEST_COUNT, forwardCount);
    String queryStr = updatedQueryParams.toQueryString();

    String coreUrlAndPath = coreUrl + path;
    log.info("Proxying request to: {}", coreUrlAndPath);
    try {
      response.reset(); // clear all headers and status
      HttpClient httpClient = cores.getDefaultHttpSolrClient().getHttpClient();
      HttpSolrProxy.doHttpProxy(httpClient, req, response, coreUrlAndPath + queryStr);
    } catch (Exception e) {
      // note: don't handle interruption differently; we are stopping
      sendError(
          new SolrException(
              SolrException.ErrorCode.SERVER_ERROR,
              "Error trying to proxy request for url: "
                  + coreUrl
                  + " with _forwardCount: "
                  + forwardCount,
              e));
    } catch (Error e) {
      throw e;
    } catch (Throwable e) {
      throw new RuntimeException(e);
    }
  }

  protected void sendError(Throwable ex) throws IOException {
    Exception exp = null;
    SolrCore localCore = null;
    try {
      SolrQueryResponse solrResp = new SolrQueryResponse();
      if (ex instanceof Exception) {
        solrResp.setException((Exception) ex);
      } else {
        solrResp.setException(new RuntimeException(ex));
      }
      localCore = core;
      if (solrReq == null) {
        final SolrParams solrParams;
        if (req != null) {
          // use GET parameters if available:
          solrParams = SolrRequestParsers.parseQueryString(req.getQueryString());
        } else {
          // we have no params at all, use empty ones:
          solrParams = new MapSolrParams(Collections.emptyMap());
        }
        solrReq = new SolrQueryRequestBase(core, solrParams) {};
      }
      QueryResponseWriter writer = getResponseWriter();
      writeResponse(solrResp, writer, Method.GET);
    } catch (Exception e) { // This error really does not matter
      exp = e;
    } finally {
      try {
        if (exp != null) {
          SimpleOrderedMap<Object> info = new SimpleOrderedMap<>();
          int code =
              ResponseUtils.getErrorInfo(
                  ex,
                  info,
                  log,
                  localCore != null && localCore.getCoreContainer().hideStackTrace());
          sendError(code, info.toString());
        }
      } finally {
        if (core == null && localCore != null) {
          localCore.close();
        }
      }
    }
  }

  protected void sendError(int code, String message) throws IOException {
    try {
      response.sendError(code, message);
    } catch (EOFException e) {
      log.info(
          "Unable to write error response, client closed connection or we are shutting down", e);
    }
  }

  protected void executeCoreRequest(SolrQueryResponse rsp) {
    // a custom filter could add more stuff to the request before passing it on.
    // for example: sreq.getContext().put( "HttpServletRequest", req );
    // used for logging query stats in SolrCore.execute()
    solrReq.getContext().put("webapp", req.getContextPath());
    solrReq.getCore().execute(handler, solrReq, rsp);
  }

  private void handleAdminRequest() throws IOException {
    SolrQueryResponse solrResp = new SolrQueryResponse();
    handleAdmin(solrResp);
    logAndFlushAdminRequest(solrResp);
  }

  protected void logAndFlushAdminRequest(SolrQueryResponse solrResp) throws IOException {
    if (solrResp.getToLog().size() > 0) {
      // has to come second and in it's own if to keep ./gradlew check happy.
      if (log.isInfoEnabled()) {
        log.info(
            handler != null
                ? MarkerFactory.getMarker(handler.getClass().getName())
                : MarkerFactory.getMarker(HttpSolrCall.class.getName()),
            solrResp.getToLogAsString("[admin]"));
      }
    }
    QueryResponseWriter respWriter =
        SolrCore.DEFAULT_RESPONSE_WRITERS.get(solrReq.getParams().get(CommonParams.WT));
    if (respWriter == null) respWriter = getResponseWriter();
    writeResponse(solrResp, respWriter, Method.getMethod(req.getMethod()));
    if (shouldAudit()) {
      EventType eventType = solrResp.getException() == null ? EventType.COMPLETED : EventType.ERROR;
      if (shouldAudit(eventType)) {
        cores
            .getAuditLoggerPlugin()
            .doAudit(
                new AuditEvent(
                    eventType,
                    req,
                    getAuthCtx(),
                    solrReq.getRequestTimer().getTime(),
                    solrResp.getException()));
      }
    }
  }

  /**
   * Returns {@link QueryResponseWriter} to be used. When {@link CommonParams#WT} not specified in
   * the request or specified value doesn't have corresponding {@link QueryResponseWriter} then,
   * returns the default query response writer Note: This method must not return null
   */
  protected QueryResponseWriter getResponseWriter() {
    return solrReq.getResponseWriter();
  }

  protected void handleAdmin(SolrQueryResponse solrResp) {
    SolrCore.preDecorateResponse(solrReq, solrResp);
    handler.handleRequest(solrReq, solrResp);
    SolrCore.postDecorateResponse(handler, solrReq, solrResp);
  }

  /**
   * Sets the "collection" parameter on the request to the list of alias-resolved collections for
   * this request. It can be avoided sometimes. Note: {@link
   * org.apache.solr.handler.component.HttpShardHandler} processes this param.
   *
   * @see #getCollectionsList()
   */
  protected void addCollectionParamIfNeeded(List<String> collections) {
    if (collections.isEmpty()) {
      return;
    }
    assert cores.isZooKeeperAware();
    String collectionParam = queryParams.get(COLLECTION_PROP);
    // if there is no existing collection param and the core we go to is for the expected
    // collection, then we needn't add a collection param
    if (collectionParam == null
        && // if collection param already exists, we may need to over-write it
        core != null
        && collections.equals(
            Collections.singletonList(core.getCoreDescriptor().getCollectionName()))) {
      return;
    }
    String newCollectionParam = StrUtils.join(collections, ',');
    if (newCollectionParam.equals(collectionParam)) {
      return;
    }
    // TODO add a SolrRequest.getModifiableParams ?
    ModifiableSolrParams params = new ModifiableSolrParams(solrReq.getParams());
    params.set(COLLECTION_PROP, newCollectionParam);
    solrReq.setParams(params);
  }

  protected void writeResponse(
      SolrQueryResponse solrRsp, QueryResponseWriter responseWriter, Method reqMethod)
      throws IOException {
    try {
      Object invalidStates = solrReq.getContext().get(CloudSolrClient.STATE_VERSION);
      // This is the last item added to the response and the client would expect it that way.
      // If that assumption is changed , it would fail. This is done to avoid an O(n) scan on
      // the response for each request
      if (invalidStates != null) solrRsp.add(CloudSolrClient.STATE_VERSION, invalidStates);
      // Now write it out
      final String ct = responseWriter.getContentType(solrReq, solrRsp);
      // don't call setContentType on null
      if (null != ct) response.setContentType(ct);

      if (solrRsp.getException() != null) {
        NamedList<Object> info = new SimpleOrderedMap<>();
        int code =
            ResponseUtils.getErrorInfo(
                solrRsp.getException(),
                info,
                log,
                core != null && core.getCoreContainer().hideStackTrace());
        solrRsp.add("error", info);
        response.setStatus(code);
      }

      if (Method.HEAD != reqMethod) {
        responseWriter.write(response.getOutputStream(), solrReq, solrRsp, ct);
      }
      // else http HEAD request, nothing to write out, waited this long just to get ContentType
    } catch (EOFException e) {
      log.info("Unable to write response, client closed connection or we are shutting down", e);
    }
  }

  /**
   * Returns null if the state ({@link CloudSolrClient#STATE_VERSION}) is good; otherwise returns
   * state problems.
   */
  private Map<String, Integer> checkStateVersionsAreValid(String stateVer) {
    Map<String, Integer> result = null;
    String[] pairs;
    if (stateVer != null && !stateVer.isEmpty() && cores.isZooKeeperAware()) {
      // many have multiple collections separated by |
      pairs = stateVer.split("\\|");
      for (String pair : pairs) {
        String[] pcs = pair.split(":");
        if (pcs.length == 2 && !pcs[0].isEmpty() && !pcs[1].isEmpty()) {
          Integer status =
              cores
                  .getZkController()
                  .getZkStateReader()
                  .compareStateVersions(pcs[0], Integer.parseInt(pcs[1]));
          if (status != null) {
            if (result == null) result = new HashMap<>();
            result.put(pcs[0], status);
          }
        }
      }
    }
    return result;
  }

  /**
   * Retrieves a SolrCore instance associated with the specified collection name, with an option to
   * prefer leader replicas. Makes a call to {@link #resolveDocCollection} which make an attempt to
   * force update collection if it is not found in local cluster state
   */
  protected SolrCore getCoreByCollection(String collectionName, boolean isPreferLeader) {
    ZkStateReader zkStateReader = cores.getZkController().getZkStateReader();
    ClusterState clusterState = zkStateReader.getClusterState();
    DocCollection collection = resolveDocCollection(collectionName);
    // the usage of getCoreByCollection assumes that if null is returned, collection is found, but
    // replicas might not
    // have been created. Hence returning null here would be misleading...
    if (collection == null) {
      return null;
    }
    Set<String> liveNodes = clusterState.getLiveNodes();
    List<Replica> replicas = collection.getReplicasOnNode(cores.getZkController().getNodeName());

    if (isPreferLeader) {
      SolrCore core = null;
      if (replicas != null && !replicas.isEmpty()) {
        List<Replica> leaderReplicas = replicas.stream().filter(Replica::isLeader).toList();
        core = randomlyGetSolrCore(liveNodes, leaderReplicas);
        if (core != null) return core;
      }
    }

    return randomlyGetSolrCore(liveNodes, replicas);
  }

  private SolrCore randomlyGetSolrCore(Set<String> liveNodes, List<Replica> replicas) {
    if (replicas != null) {
      RandomIterator<Replica> it = new RandomIterator<>(Utils.RANDOM, replicas);
      while (it.hasNext()) {
        Replica replica = it.next();
        if (liveNodes.contains(replica.getNodeName())
            && replica.getState() == Replica.State.ACTIVE) {
          SolrCore core = checkProps(replica);
          if (core != null) return core;
        }
      }
    }
    return null;
  }

  private SolrCore checkProps(ZkNodeProps zkProps) {
    String corename;
    SolrCore core = null;
    if (cores.getZkController().getNodeName().equals(zkProps.getStr(NODE_NAME_PROP))) {
      corename = zkProps.getStr(CORE_NAME_PROP);
      core = cores.getCore(corename);
    }
    return core;
  }

  protected String getRemoteCoreUrl(String collectionName) throws SolrException {
    ClusterState clusterState = cores.getZkController().getClusterState();
    final DocCollection docCollection = clusterState.getCollectionOrNull(collectionName);
    if (docCollection == null) {
      return null;
    }
    Collection<Slice> activeSlices = docCollection.getActiveSlices();

    int totalReplicas = 0;

    for (Slice s : activeSlices) {
      totalReplicas += s.getReplicas().size();
    }
    if (activeSlices.isEmpty()) {
      return null;
    }

    // XXX (ab) most likely this is not needed? it seems all code paths
    // XXX already make sure the collectionName is on the list
    if (!collectionsList.contains(collectionName)) {
      collectionsList = new ArrayList<>(collectionsList);
      collectionsList.add(collectionName);
    }

    // Avoid getting into a recursive loop of requests being forwarded by
    // stopping forwarding and erroring out after (totalReplicas) forwards
    if (queryParams.getInt(INTERNAL_REQUEST_COUNT, 0) > totalReplicas) {
      throw new SolrException(
          SolrException.ErrorCode.INVALID_STATE,
          "No active replicas found for collection: " + collectionName);
    }

    String coreUrl = getCoreUrl(activeSlices, true, clusterState.getLiveNodes());

    if (coreUrl == null) {
      coreUrl = getCoreUrl(activeSlices, false, clusterState.getLiveNodes());
    }

    return coreUrl;
  }

  private String getCoreUrl(
      Collection<Slice> slices, boolean activeReplicas, Set<String> liveNodes) {

    Iterator<Slice> shuffledSlices = new RandomIterator<>(Utils.RANDOM, slices);
    while (shuffledSlices.hasNext()) {
      Slice slice = shuffledSlices.next();

      Iterator<Replica> shuffledReplicas = new RandomIterator<>(Utils.RANDOM, slice.getReplicas());
      while (shuffledReplicas.hasNext()) {
        Replica replica = shuffledReplicas.next();

        if (!activeReplicas
            || (liveNodes.contains(replica.getNodeName())
                && replica.getState() == Replica.State.ACTIVE)) {

          if (Objects.equals(replica.getBaseUrl(), cores.getZkController().getBaseUrl())) {
            // don't count a local core
            continue;
          }

          if (origCorename != null) {
            coreUrl = replica.getBaseUrl() + "/" + origCorename;
          } else {
            coreUrl = replica.getCoreUrl();
            if (coreUrl.endsWith("/")) {
              coreUrl = coreUrl.substring(0, coreUrl.length() - 1);
            }
          }

          return coreUrl;
        }
      }
    }
    return null;
  }

  protected Object _getHandler() {
    return handler;
  }

  /**
   * Gets the client (user-agent) SolrJ version, or null if isn't SolrJ. Note that older SolrJ
   * clients prior to 9.9 present themselves as 1.0 or 2.0.
   */
  public SolrVersion getUserAgentSolrVersion() {
    return userAgentSolrVersion;
  }

  private SolrVersion parseUserAgentSolrVersion() {
    String header = req.getHeader("User-Agent");
    if (header == null || !header.startsWith("Solr")) {
      return null;
    }
    try {
      String userAgent = header.substring(header.lastIndexOf(' ') + 1);
      if ("1.0".equals(userAgent)) {
        userAgent = "1.0.0";
      } else if ("2.0".equals(userAgent)) {
        userAgent = "2.0.0";
      }
      return SolrVersion.valueOf(userAgent);
    } catch (Exception e) {
      // unexpected but let's not freak out
      assert false : e.toString();
      return null;
    }
  }

  private AuthorizationContext getAuthCtx() {

    String resource = getPath();

    final List<CollectionRequest> collectionRequests =
        AuthorizationUtils.getCollectionRequests(getPath(), getCollectionsList(), getQueryParams());

    // Populate the request type if the request is select or update
    if (requestType == RequestType.UNKNOWN) {
      if (resource.startsWith("/select") || resource.startsWith("/get"))
        requestType = RequestType.READ;
      if (resource.startsWith("/update")) requestType = RequestType.WRITE;
    }

    return new HttpServletAuthorizationContext(getReq()) {
      @Override
      public SolrParams getParams() {
        return null == solrReq ? null : solrReq.getParams();
      }

      @Override
      public List<CollectionRequest> getCollectionRequests() {
        return collectionRequests;
      }

      @Override
      public String getResource() {
        return path;
      }

      @Override
      public RequestType getRequestType() {
        return requestType;
      }

      @Override
      public Object getHandler() {
        return _getHandler();
      }

      @Override
      public String toString() {
        StringBuilder response =
            new StringBuilder("userPrincipal: [")
                .append(getUserPrincipal())
                .append("]")
                .append(" type: [")
                .append(requestType.toString())
                .append("], collections: [");
        for (CollectionRequest collectionRequest : collectionRequests) {
          response.append(collectionRequest.collectionName).append(", ");
        }
        if (collectionRequests.size() > 0)
          response.delete(response.length() - 1, response.length());

        response.append("], Path: [").append(resource).append("]");
        response.append(" path : ").append(path).append(" params :").append(getParams());
        return response.toString();
      }
    };
  }

  List<CommandOperation> parsedCommands;

  public List<CommandOperation> getCommands(boolean validateInput) {
    if (parsedCommands == null) {
      Iterable<ContentStream> contentStreams = solrReq.getContentStreams();
      if (contentStreams == null) parsedCommands = Collections.emptyList();
      else {
        parsedCommands =
            ApiBag.getCommandOperations(
                contentStreams.iterator().next(), getValidators(), validateInput);
      }
    }
    return CommandOperation.clone(parsedCommands);
  }

  protected ValidatingJsonMap getSpec() {
    return null;
  }

  protected Map<String, JsonSchemaValidator> getValidators() {
    return Collections.emptyMap();
  }

  /** A faster method for randomly picking items when you do not need to consume all items. */
  private static class RandomIterator<E> implements Iterator<E> {
    private Random rand;
    private ArrayList<E> elements;
    private int size;

    public RandomIterator(Random rand, Collection<E> elements) {
      this.rand = rand;
      this.elements = new ArrayList<>(elements);
      this.size = elements.size();
    }

    @Override
    public boolean hasNext() {
      return size > 0;
    }

    @Override
    public E next() {
      int idx = rand.nextInt(size);
      E e1 = elements.get(idx);
      E e2 = elements.get(size - 1);
      elements.set(idx, e2);
      size--;
      return e1;
    }
  }
}
