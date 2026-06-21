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

package org.apache.solr.api;

import com.google.common.collect.ImmutableSet;
import io.opentracing.Span;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.impl.BaseCloudSolrClient;
import org.apache.solr.common.ParWork;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.MultiMapSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.common.util.JsonSchemaValidator;
import org.apache.solr.common.util.PathTrie;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.common.util.ValidatingJsonMap;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.PluginBag;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.RequestHandlerUtils;
import org.apache.solr.handler.admin.PrepRecoveryOp;
import org.apache.solr.logging.MDCLoggingContext;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.response.QueryResponseWriter;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.security.AuditEvent;
import org.apache.solr.security.AuthenticationPlugin;
import org.apache.solr.security.AuthorizationContext;
import org.apache.solr.servlet.HttpSolrCall;
import org.apache.solr.servlet.ResponseUtils;
import org.apache.solr.servlet.SolrCall;
import org.apache.solr.servlet.SolrDispatchFilter;
import org.apache.solr.servlet.SolrRequestParsers;
import org.apache.solr.servlet.cache.HttpCacheHeaderUtil;
import org.apache.solr.servlet.cache.Method;
import org.apache.solr.util.RTimerTree;
import org.apache.solr.util.tracing.GlobalTracer;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.EOFException;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import static org.apache.solr.common.cloud.ZkStateReader.COLLECTION_PROP;
import static org.apache.solr.common.util.PathTrie.getPathSegments;
import static org.apache.solr.servlet.SolrDispatchFilter.Action.ADMIN;
import static org.apache.solr.servlet.SolrDispatchFilter.Action.FORWARD;
import static org.apache.solr.servlet.SolrDispatchFilter.Action.PROCESS;
import static org.apache.solr.servlet.SolrDispatchFilter.Action.REMOTEQUERY;
import static org.apache.solr.servlet.SolrDispatchFilter.Action.RETURN;

// class that handle the '/v2' path
public class V2HttpCall extends SolrCall {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private final Api api;
  static final Set<String> knownPrefixes = ImmutableSet.of("cluster", "node", "collections", "cores", "c");
  private final List<String> pieces;
  private final Map<String,String> parts;
  private final CoreContainer cores;
  private final SolrDispatchFilter.Action action;
  private final HttpServletRequest req;
  private final HttpServletResponse response;

  protected final SolrParams solrParams;

  protected final SolrQueryRequest solrReq;

  private final String coreUrl;

  private final String path;
  private final AuthorizationContext.RequestType requestType;
  private final SolrDispatchFilter solrDispatchFilter;
  private volatile SolrCore solrCore;
  protected String origCorename; // What's in the URL path; might reference a collection/alias or a Solr core name
  private List<String> collectionsList;

  protected volatile Map<String,String> invalidStates;

  public V2HttpCall(SolrDispatchFilter solrDispatchFilter, String path, HttpServletRequest request, HttpServletResponse response) throws Exception {
    this.solrDispatchFilter = solrDispatchFilter;
    this.cores = solrDispatchFilter.getCores();
    this.req = request;
    this.response = response;
    AuthorizationContext.RequestType requestType = AuthorizationContext.RequestType.UNKNOWN;
    SolrQueryRequest solrRequest = null;
    SolrDispatchFilter.Action callAction = null;
    request.setAttribute(HttpSolrCall.class.getName(), this);
    Map<String, String[]> copy = new Object2ObjectOpenHashMap<>(request.getParameterMap());
    solrParams = new MultiMapSolrParams(copy);//SolrRequestParsers.getDefaultInstance().parseQueryString(request.getQueryString());
    // set a request timer which can be reused by requests if needed
    request.setAttribute(SolrRequestParsers.REQUEST_TIMER_SERVLET_ATTRIBUTE, new RTimerTree());
    // put the core container in request attribute
    request.setAttribute("org.apache.solr.CoreContainer", cores);
    String reqPath = path;
    if (log.isTraceEnabled()) log.trace("Path is parsed as {}", reqPath);

    String solrCoreUrl = null;
    Api solrApi = null;

    String fullPath = reqPath.substring(7);//strip off '/____v2'
    reqPath = fullPath;
    HashMap<String, String> parts = new HashMap<>();
    List<String> pieces = getPathSegments(fullPath);
    try {

      String prefix;
      log.info("request path={} pieces={}", reqPath, pieces);
      if (pieces.size() == 0 || (pieces.size() == 1 && reqPath.endsWith(CommonParams.INTROSPECT))) {
        api = new MyApi();
        solrRequest = SolrRequestParsers.getDefaultInstance().parse(null, reqPath, req, solrParams);
        solrRequest.getContext().put(CoreContainer.class.getName(), cores);
        solrRequest.getContext().put(CommonParams.PATH, reqPath);
        this.solrReq = solrRequest;
        this.action = ADMIN;
        this.requestType = AuthorizationContext.RequestType.ADMIN;
        this.pieces = pieces;
        this.parts = Collections.emptyMap();
        this.path = reqPath;
        this.coreUrl = null;
        return;
      } else {
        prefix = pieces.get(0);
      }
      log.info("prefix={}", prefix);
      boolean isCompositeApi = false;
      if (knownPrefixes.contains(prefix)) {

        solrApi = getApiInfo(cores.getRequestHandlers(), reqPath, request.getMethod(), fullPath, parts);
        log.info("getAPIInfo {} path={} fullPath={} parts={}", solrApi, reqPath, fullPath, pieces);
        if (solrApi != null) {
          isCompositeApi = solrApi instanceof CompositeApi;
          if (!isCompositeApi) {
            this.solrReq = SolrRequestParsers.getDefaultInstance().parse(null, reqPath, req, solrParams);
            solrReq.getContext().put(CoreContainer.class.getName(), cores);
            solrReq.getContext().put(CommonParams.PATH, reqPath);
            this.action = ADMIN;
            this.requestType = AuthorizationContext.RequestType.ADMIN;
            api = solrApi;
            this.pieces = pieces;
            this.path = reqPath;
            this.coreUrl = null;
            this.parts = Collections.unmodifiableMap(parts);
            return;
          }
        }
      }

      String origCorename = null;

      if (("c".equals(prefix) || "collections".equals(prefix))) {

        origCorename = pieces.get(1);

        DocCollection collection = resolveDocCollection(solrParams.get(COLLECTION_PROP, origCorename));

        if (collection == null) {
          if ( ! reqPath.endsWith(CommonParams.INTROSPECT)) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "no such collection or alias");
          }
        } else {
          boolean isPreferLeader = (reqPath.endsWith("/update") || reqPath.contains("/update/"));
          solrCore = getCoreByCollection(cores, collection.getName(), isPreferLeader, request);
          if (solrCore == null) {
            // This collection exists but this node has no ACTIVE local replica for it, so proxy the
            // whole request to a node that does (the same thing v1 HttpSolrCall does). extractRemotePath
            // returns <baseUrl>/<coreName> for an active remote replica; v2 requests are not
            // core-addressed, so we proxy to that node's v2 endpoint (<baseUrl>/____v2) and the
            // REMOTEQUERY dispatch in call() appends the v2 request path (this.path). The
            // X-Forwarded-For header set on proxied requests prevents proxy loops. If no active remote
            // replica exists we fall through to the coreless introspect / 404 handling below.
            if (log.isDebugEnabled()) log.debug("check remote path extraction {} {}", collection.getName(), origCorename);
            solrCoreUrl = extractRemotePath(cores, collection.getName(), solrParams, request);
            if (solrCoreUrl != null) {
              String remoteBaseUrl = solrCoreUrl.substring(0, solrCoreUrl.lastIndexOf('/'));
              this.coreUrl = remoteBaseUrl + "/____v2";
              this.path = reqPath;
              this.solrReq = null;
              this.api = null;
              this.pieces = null;
              this.parts = Collections.unmodifiableMap(parts);
              this.requestType = requestType;
              this.action = REMOTEQUERY;
              if (log.isDebugEnabled()) log.debug("v2 proxy collection={} to {}{}", collection.getName(), this.coreUrl, reqPath);
              return;
            }
          }
        }
      } else if ("cores".equals(prefix)) {
        origCorename = pieces.get(1);
        solrCore = cores.getCore(origCorename);

        if (solrCore == null) {
          while (true) {
            final boolean coreLoading = cores.isCoreLoading(origCorename);
            if (!coreLoading) break;
            Thread.sleep(150); // nocommit - make efficient
          }
          solrCore = cores.getCore(origCorename);
        }
      }
      if (solrCore == null) {
        if (reqPath.endsWith(CommonParams.INTROSPECT)) {
          this.solrReq = SolrRequestParsers.getDefaultInstance().parse(null, reqPath, req, solrParams);
          solrReq.getContext().put(CoreContainer.class.getName(), cores);
          solrReq.getContext().put(CommonParams.PATH, reqPath);
          this.action = ADMIN;
          this.requestType = AuthorizationContext.RequestType.ADMIN;
          api = solrApi;
          this.pieces = pieces;
          this.parts = Collections.unmodifiableMap(parts);
          this.path = reqPath;
          this.coreUrl = null;
          return;
        } else {
         throw new SolrException(SolrException.ErrorCode.NOT_FOUND, "no core retrieved for " + origCorename);
        }
      }

      reqPath = reqPath.substring(prefix.length() + pieces.get(1).length() + 2);
      Api apiInfo = getApiInfo(solrCore.getRequestHandlers(), reqPath, request.getMethod(), fullPath, parts);
      if (isCompositeApi && apiInfo instanceof CompositeApi) {
        ((CompositeApi) solrApi).add(apiInfo);
      } else {
        solrApi = apiInfo == null ? solrApi : apiInfo;
      }
      this.solrReq = parseRequest(solrCore, reqPath);

      addCollectionParamIfNeeded(cores, solrReq, solrParams, getCollectionsList());

      api = solrApi;
      this.pieces = pieces;
      this.parts = Collections.unmodifiableMap(parts);
      this.action = PROCESS;
      this.path = reqPath;
      this.coreUrl = null;
      this.requestType = requestType;
      if (solrReq != null) solrReq.getContext().put(CommonParams.PATH, this.path);
      // we are done with a valid handler
    } catch (RuntimeException rte) {
      log.error("Error in init()", rte);
      // Preserve the original error code if this is already a SolrException; wrapping it
      // as SERVER_ERROR (500) would mask a genuine 404/400 and prevent audit events from
      // recording the correct HTTP status.
      if (rte instanceof SolrException) {
        throw rte;
      }
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, rte);
    }
  }

  protected SolrQueryRequest parseRequest(SolrCore core, String reqPath) throws Exception {
    // get or create/cache the parser for the core
    SolrRequestParsers parser = core.getSolrConfig().getRequestParsers();

    // With a valid handler and a valid core...
    log.debug("  reqPath={}", reqPath);
    SolrQueryRequest solrQueryRequest = parser.parse(core, reqPath, req, solrParams);
    solrQueryRequest.getContext().put(CommonParams.PATH, reqPath);
    return solrQueryRequest;
  }

  public AuthorizationContext.RequestType getRequestType() {
    return requestType;
  }

  public String getPath() {
    return path;
  }

  public HttpServletRequest getReq() {
    return req;
  }

  @Override
  public SolrQueryRequest getSolrReq() {
    return solrReq;
  }

  public SolrParams getSolrParams() {
    return solrParams;
  }

  /** The collection(s) referenced in this request. */
  @Override
  public List<String> getCollectionsList() {
    return collectionsList != null ? collectionsList : Collections.emptyList();
  }

  /**
   * This method processes the request.
   */
  @Override
  public
  SolrDispatchFilter.Action call() throws IOException {
    MDCLoggingContext.reset();
    Span activeSpan = GlobalTracer.getTracer().activeSpan();
    if (activeSpan != null) {
      MDCLoggingContext.setTracerId(activeSpan.context().toTraceId());
    }
    if (cores.isZooKeeperAware()) {
      MDCLoggingContext.setNode(cores.getZkController().getNodeName());
    }

    if (solrDispatchFilter.getAbortErrorMessage() != null) {
      sendError(500, solrDispatchFilter.getAbortErrorMessage(), response);
      if (shouldAudit(cores, AuditEvent.EventType.ERROR)) {
        cores.getAuditLoggerPlugin().doAudit(new AuditEvent(AuditEvent.EventType.ERROR, req));
      }
      return RETURN;
    }

    try {

      // Perform authorization here, if:
      //    (a) Authorization is enabled, and
      //    (b) The requested resource is not a known static file
      //    (c) And this request should be handled by this node (see NOTE below)
      // NOTE: If the query is to be handled by another node, then let that node do the authorization.
      // In case of authentication using BasicAuthPlugin, for example, the internode request
      // is secured using PKI authentication and the internode request here will contain the
      // original user principal as a payload/header, using which the receiving node should be
      // able to perform the authorization.
      if (cores.getAuthorizationPlugin() != null && shouldAuthorize(cores, path, req)
          && !(action == REMOTEQUERY || action == FORWARD)) {
        SolrDispatchFilter.Action authorizationAction = authorize(cores, solrReq, req, response);
        if (authorizationAction == RETURN) {
          return authorizationAction;
        }
      }

      switch (action) {
        case ADMIN:
          handleAdminRequest(cores, req, response);
          return RETURN;
        case REMOTEQUERY:
          SolrRequestInfo.setRequestInfo(new SolrRequestInfo(req, new SolrQueryResponse(), action));
          SolrDispatchFilter.Action a = remoteQuery(req, response, coreUrl + path, solrDispatchFilter.getCores().getUpdateShardHandler().getTheSharedHttpClient().getHttpClient(), solrParams);
          return a;
        case PROCESS:
          final Method reqMethod = Method.getMethod(req.getMethod());
          SolrConfig config = solrReq.getCore().getSolrConfig();
          HttpCacheHeaderUtil.setCacheControlHeader(config, response, reqMethod);
          // unless we have been explicitly told not to, do cache validation
          // if we fail cache validation, execute the query
          if (config.getHttpCachingConfig().isNever304() ||
              !HttpCacheHeaderUtil.doCacheHeaderValidation(solrReq, req, reqMethod, response)) {
            SolrQueryResponse solrRsp = new SolrQueryResponse();
            /* even for HEAD requests, we need to execute the handler to
             * ensure we don't get an error (and to make sure the correct
             * QueryResponseWriter is selected and we get the correct
             * Content-Type)
             */
            SolrRequestInfo.setRequestInfo(new SolrRequestInfo(solrReq, solrRsp, action));
            execute(solrRsp);
            if (shouldAudit(cores)) {
              AuditEvent.EventType eventType = solrRsp.getException() == null ? AuditEvent.EventType.COMPLETED : AuditEvent.EventType.ERROR;
              if (shouldAudit(cores, eventType)) {
                cores.getAuditLoggerPlugin().doAudit(
                    new AuditEvent(eventType, req, getAuthCtx(solrReq, req, path, requestType), solrReq.getRequestTimer().getTime(), solrRsp.getException()));
              }
            }
            HttpCacheHeaderUtil.checkHttpCachingVeto(solrRsp, response, reqMethod);
            Iterator<Map.Entry<String, String>> headers = solrRsp.httpHeaders();
            while (headers.hasNext()) {
              Map.Entry<String, String> entry = headers.next();
              response.addHeader(entry.getKey(), entry.getValue());
            }
            QueryResponseWriter responseWriter = getResponseWriter(solrReq);
            if (invalidStates != null) solrReq.getContext().put(BaseCloudSolrClient.STATE_VERSION, invalidStates);

            writeResponse(solrReq, solrRsp, req, response, responseWriter, reqMethod);
          }
          return RETURN;
        default: return action;
      }
    } catch (Throwable ex) {
      log.error("ERROR", ex);
      if (!(ex instanceof PrepRecoveryOp.NotValidLeader) && shouldAudit(cores, AuditEvent.EventType.ERROR)) {
        cores.getAuditLoggerPlugin().doAudit(new AuditEvent(AuditEvent.EventType.ERROR, ex, req));
      }
      // Serialize the error through the Solr response writer (octet-stream/javabin) rather than
      // response.sendError(), which renders Jetty's HTML error page — the client's javabin parser then
      // can't read it ("Expected mime type application/octet-stream but got text/html") and the real
      // SolrException message/metadata is lost. This mirrors the non-V2 path (HttpSolrCall's throw is
      // caught by SolrDispatchFilter which calls the same sendException).
      SolrDispatchFilter.sendException(ex, this, req, response);
      // walk the the entire cause chain to search for an Error
      Throwable t = ex;
      while (t != null) {
        if (t instanceof Error) {
          if (t != ex) {
            log.error("An Error was wrapped in another exception - please report complete stacktrace on SOLR-6161", ex);
          }
          throw (Error) t;
        }
        t = t.getCause();
      }
      return RETURN;
    }
  }

  public void destroy() {
    try {
      if (solrReq != null) {
        if (log.isTraceEnabled()) {
          log.trace("Closing out SolrRequest: {}", solrReq);
        }

        IOUtils.closeQuietly(solrCore);

        IOUtils.closeQuietly(solrReq);
      }
    } finally {
      try {
        AuthenticationPlugin authcPlugin = cores.getAuthenticationPlugin();
        if (authcPlugin != null) authcPlugin.closeRequest();
      } finally {
        SolrRequestInfo.clearRequestInfo();
      }
    }
  }

  /**
   * Lookup the collection from the collection string (maybe comma delimited).
   * Also sets {@link SolrCall#getCollectionsList()} by side-effect.
   * if {@code secondTry} is false then we'll potentially recursively try this all one more time while ensuring
   * the alias and collection info is sync'ed from ZK.
   */
  protected DocCollection resolveDocCollection(String collectionStr) {
    if (!cores.isZooKeeperAware()) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Solr not running in cloud mode ");
    }
    ZkStateReader zkStateReader = cores.getZkController().getZkStateReader();

    Supplier<DocCollection> logic = () -> {
      List<String> collectionsList = resolveCollectionListOrAlias(cores, collectionStr); // side-effect
      if (collectionsList.size() > 1) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Request must be sent to a single collection " +
            "or an alias that points to a single collection," +
            " but '" + collectionStr + "' resolves to " + collectionsList);
      }
      String collectionName = collectionsList.get(0); // first
      //TODO an option to choose another collection in the list if can't find a local replica of the first?

      return zkStateReader.getClusterState().getCollectionOrNull(collectionName);
    };

    DocCollection docCollection = logic.get();
    if (docCollection != null) {
      return docCollection;
    }
    // ensure our view is up to date before trying again
    try {
      zkStateReader.aliasesManager.update();
    } catch (Exception e) {
      ParWork.propagateInterrupt("Error trying to update state while resolving collection.", e);
      if (e instanceof KeeperException.SessionExpiredException) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
      }
      //don't propagate exception on purpose
    }
    return logic.get();
  }

  public static Api getApiInfo(PluginBag<SolrRequestHandler> requestHandlers,
                               String path, String method,
                               String fullPath,
                               Map<String, String> parts) {
    fullPath = fullPath == null ? path : fullPath;
    Api api = requestHandlers.v2lookup(path, method, parts);
    if (api == null && path.endsWith(CommonParams.INTROSPECT)) {
      // the particular http method does not have any ,
      // just try if any other method has this path
      api = requestHandlers.v2lookup(path, null, parts);
    }

    if (api == null) {
      return getSubPathApi(requestHandlers, path, fullPath, new CompositeApi(null));
    }

    if (api instanceof ApiBag.IntrospectApi) {
      final Map<String, Api> apis = new LinkedHashMap<>();
      for (String m : SolrRequest.SUPPORTED_METHODS) {
        Api x = requestHandlers.v2lookup(path, m, parts);
        if (x != null) apis.put(m, x);
      }
      api = new CompositeApi(new Api(ApiBag.EMPTY_SPEC) {
        @Override
        public void call(SolrQueryRequest req, SolrQueryResponse rsp) {
          String method = req.getParams().get("method");
          Set<Api> added = new HashSet<>();
          apis.forEach((key, value) -> {
            if (method == null || key.equals(method)) {
              if (!added.contains(value)) {
                value.call(req, rsp);
                added.add(value);
              }
            }
          });
          RequestHandlerUtils.addExperimentalFormatWarning(rsp);
        }
      });
      getSubPathApi(requestHandlers,path, fullPath, (CompositeApi) api);
    }

    return api;
  }

  private static CompositeApi getSubPathApi(PluginBag<SolrRequestHandler> requestHandlers, String path, String fullPath, CompositeApi compositeApi) {

    String newPath = path.endsWith(CommonParams.INTROSPECT) ? path.substring(0, path.length() - CommonParams.INTROSPECT.length()) : path;
    Map<String, Set<String>> subpaths = new LinkedHashMap<>();

    getSubPaths(newPath, requestHandlers.getApiBag(), subpaths);
    final Map<String, Set<String>> subPaths = subpaths;
    if (subPaths.isEmpty()) return null;
    return compositeApi.add(new Api(() -> ValidatingJsonMap.EMPTY) {
      @Override
      public void call(SolrQueryRequest req1, SolrQueryResponse rsp) {
        String prefix;
        prefix = fullPath.endsWith(CommonParams.INTROSPECT) ?
            fullPath.substring(0, fullPath.length() - CommonParams.INTROSPECT.length()) :
            fullPath;
        LinkedHashMap<String, Set<String>> result = new LinkedHashMap<>(subPaths.size());
        for (Map.Entry<String, Set<String>> e : subPaths.entrySet()) {
          if (e.getKey().endsWith(CommonParams.INTROSPECT)) continue;
          result.put(prefix + e.getKey(), e.getValue());
        }

        Map m = (Map) rsp.getValues().get("availableSubPaths");
        if(m != null){
          m.putAll(result);
        } else {
          rsp.add("availableSubPaths", result);
        }
      }
    });
  }

  private static void getSubPaths(String path, ApiBag bag, Map<String, Set<String>> pathsVsMethod) {
    for (SolrRequest.METHOD m : SolrRequest.METHOD.values()) {
      PathTrie<Api> registry = bag.getRegistry(m.toString());
      if (registry != null) {
        HashSet<String> subPaths = new HashSet<>();
        registry.lookup(path, new HashMap<>(), subPaths);
        for (String subPath : subPaths) {
          Set<String> supportedMethods = pathsVsMethod.computeIfAbsent(subPath, k -> new HashSet<>());
          supportedMethods.add(m.toString());
        }
      }
    }
  }


  protected void sendError(Throwable ex) throws IOException {
    SimpleOrderedMap info = new SimpleOrderedMap();
    int code = ResponseUtils.getErrorInfo(ex, info, log);
    sendError(code, info.toString());
  }

  protected void sendError(int code, String message) throws IOException {
    try {
      response.sendError(code, message);
    } catch (EOFException e) {
      log.info("Unable to write error response, client closed connection or we are shutting down", e);
    }
  }

  public List<String> getPieces() {
    return pieces;
  }

  public static class CompositeApi extends Api {
    private final LinkedList<Api> apis = new LinkedList<>();

    public CompositeApi(Api api) {
      super(ApiBag.EMPTY_SPEC);
      if (api != null) apis.add(api);
    }

    @Override
    public void call(SolrQueryRequest req, SolrQueryResponse rsp) {
      for (Api api : apis) {
        api.call(req, rsp);
      }
    }

    public CompositeApi add(Api api) {
      apis.add(api);
      return this;
    }
  }

  protected void handleAdmin(SolrQueryResponse solrResp) {
    if (api == null) {
      // Defense in depth (mirrors execute()): a null api here means no handler/spec could be
      // resolved for the path on this node. Return a clean NOT_FOUND instead of NPE'ing on
      // api.call, which would surface to the client as an opaque 500.
      solrResp.setException(new SolrException(SolrException.ErrorCode.NOT_FOUND,
          "Cannot find corresponding api for the path : "
              + (solrReq == null ? path : solrReq.getContext().get(CommonParams.PATH))));
      return;
    }
    api.call(this.solrReq, solrResp);
  }

  protected void execute(SolrQueryResponse rsp) {
    SolrCore.preDecorateResponse(solrReq, rsp);
    if (api == null) {
      rsp.setException(new SolrException(SolrException.ErrorCode.NOT_FOUND,
          "Cannot find correspond api for the path : " + solrReq.getContext().get(CommonParams.PATH)));
    } else {
      try {
        api.call(solrReq, rsp);
      } catch (Exception e) {
        ParWork.propagateInterrupt(e);
        rsp.setException(e);
      }
    }

    SolrCore.postDecorateResponse(null, solrReq, rsp);
  }

  protected SolrRequestHandler _getHandler() {
    return null;
  }

  public Map<String,String> getUrlParts(){
    return parts;
  }

  protected ValidatingJsonMap getSpec() {
    return api == null ? null : api.getSpec();
  }

  @Override
  protected Map<Object, JsonSchemaValidator> getValidators() {
    return api == null ? null : api.getCommandSchema();
  }

  private static class MyApi extends Api {
    public MyApi() {
      super(null);
    }

    @Override
    public void call(SolrQueryRequest req, SolrQueryResponse rsp) {
      rsp.add("documentation", "https://lucene.apache.org/solr/guide/v2-api.html");
      rsp.add("description", "V2 API root path");
    }
  }
}
