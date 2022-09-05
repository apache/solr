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

import io.opentracing.Span;
import io.opentracing.tag.Tags;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.annotation.SolrThreadSafe;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.util.CommandOperation;
import org.apache.solr.common.util.JsonSchemaValidator;
import org.apache.solr.common.util.PathTrie;
import org.apache.solr.common.util.SuppressForbidden;
import org.apache.solr.common.util.ValidatingJsonMap;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.PluginBag;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.RequestHandlerUtils;
import org.apache.solr.jersey.RequestContextConstants;
import org.apache.solr.jersey.container.ContainerRequestUtils;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.response.QueryResponseWriter;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.security.AuthorizationContext;
import org.apache.solr.servlet.HttpSolrCall;
import org.apache.solr.servlet.SolrDispatchFilter;
import org.apache.solr.servlet.SolrRequestParsers;
import org.apache.solr.servlet.cache.Method;
import org.glassfish.jersey.server.ApplicationHandler;
import org.glassfish.jersey.server.ContainerRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import static org.apache.solr.common.cloud.ZkStateReader.COLLECTION_PROP;
import static org.apache.solr.servlet.SolrDispatchFilter.Action.ADMIN;
import static org.apache.solr.servlet.SolrDispatchFilter.Action.PROCESS;
import static org.apache.solr.servlet.SolrDispatchFilter.Action.REMOTEQUERY;

// class that handle the '/v2' path
@SolrThreadSafe
public class V2HttpCall extends HttpSolrCall {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private Api api;
  private List<String> pathSegments;
  private String prefix;
  private boolean servedByJaxRs = false; // A flag indicating whether the request was served by JAX-RS or the native framework
  HashMap<String, String> parts = new HashMap<>();
  static final Set<String> knownPrefixes = Set.of("cluster", "node", "collections", "cores", "c");

  public V2HttpCall(
      SolrDispatchFilter solrDispatchFilter,
      CoreContainer cc,
      HttpServletRequest request,
      HttpServletResponse response,
      boolean retry) {
    super(solrDispatchFilter, cc, request, response, retry);
  }

  @SuppressForbidden(
      reason =
          "Set the thread contextClassLoader for all 3rd party dependencies that we cannot control")
  protected void init() throws Exception {
    queryParams = SolrRequestParsers.parseQueryString(req.getQueryString());
    String path = this.path;
    final String fullPath = path = path.substring(7); // strip off '/____v2'

    try {
      pathSegments = PathTrie.getPathSegments(path);
      if (pathSegments.size() == 0
          || (pathSegments.size() == 1 && path.endsWith(CommonParams.INTROSPECT))) {
        api =
            new Api(null) {
              @Override
              public void call(SolrQueryRequest req, SolrQueryResponse rsp) {
                rsp.add(
                    "documentation",
                    "https://solr.apache.org/guide/solr/latest/configuration-guide/v2-api.html");
                rsp.add("description", "V2 API root path");
              }
            };
        initAdminRequest(path);
        return;
      } else {
        prefix = pathSegments.get(0);
      }

      boolean isCompositeApi = false;
      api = getApiInfo(cores.getRequestHandlers(), path, req.getMethod(), fullPath, parts);
      if (knownPrefixes.contains(prefix)) {
        if (api != null) {
          isCompositeApi = api instanceof CompositeApi;
          if (!isCompositeApi) {
            initAdminRequest(path);
            return;
          }
        }
      } else { // custom plugin
        if (api != null) {
          initAdminRequest(path);
          return;
        }
        assert core == null;
      }

      if ("c".equals(prefix) || "collections".equals(prefix)) {
        origCorename = pathSegments.get(1);

        DocCollection collection =
            resolveDocCollection(queryParams.get(COLLECTION_PROP, origCorename));

        if (collection == null) {
          if (!path.endsWith(CommonParams.INTROSPECT)) {
            throw new SolrException(
                SolrException.ErrorCode.BAD_REQUEST, "no such collection or alias");
          }
        } else {
          boolean isPreferLeader = (path.endsWith("/update") || path.contains("/update/"));
          core = getCoreByCollection(collection.getName(), isPreferLeader);
          if (core == null) {
            // this collection exists , but this node does not have a replica for that collection
            extractRemotePath(collection.getName(), collection.getName());
            if (action == REMOTEQUERY) {
              coreUrl = coreUrl.replace("/solr/", "/solr/____v2/c/");
              this.path =
                  path = path.substring(prefix.length() + collection.getName().length() + 2);
              return;
            }
          }
        }
      } else if ("cores".equals(prefix)) {
        origCorename = pathSegments.get(1);
        core = cores.getCore(origCorename);
      }

      // We didn't find a core, so we're either an error or a Jersey 'ADMIN' api
      if (core == null) {
        // initAdminRequest handles "custom plugin" Jersey APIs, as well as in-built ones
        initAdminRequest(path);
        return;
      }

      Thread.currentThread().setContextClassLoader(core.getResourceLoader().getClassLoader());
      this.path = path = path.substring(prefix.length() + pathSegments.get(1).length() + 2);
      Api apiInfo = getApiInfo(core.getRequestHandlers(), path, req.getMethod(), fullPath, parts);
      if (isCompositeApi && apiInfo instanceof CompositeApi) {
        ((CompositeApi) this.api).add(apiInfo);
      } else {
        api = apiInfo == null ? api : apiInfo;
      }
      parseRequest();
      addCollectionParamIfNeeded(getCollectionsList());

      action = PROCESS;
      // we are done with a valid handler
    } catch (RuntimeException rte) {
      log.error("Error in init()", rte);
      throw rte;
    } finally {
      if (action == null && api == null) action = PROCESS;
      if (solrReq != null) solrReq.getContext().put(CommonParams.PATH, path);
    }
  }

  private void initAdminRequest(String path) throws Exception {
    solrReq = SolrRequestParsers.DEFAULT.parse(null, path, req);
    solrReq.getContext().put(CoreContainer.class.getName(), cores);
    requestType = AuthorizationContext.RequestType.ADMIN;
    action = ADMIN;
  }

  protected void parseRequest() throws Exception {
    config = core.getSolrConfig();
    // get or create/cache the parser for the core
    SolrRequestParsers parser = config.getRequestParsers();

    // With a valid handler and a valid core...

    if (solrReq == null) solrReq = parser.parse(core, path, req);
  }

  /**
   * Lookup the collection from the collection string (maybe comma delimited). Also sets {@link
   * #collectionsList} by side-effect. if {@code secondTry} is false then we'll potentially
   * recursively try this all one more time while ensuring the alias and collection info is sync'ed
   * from ZK.
   */
  protected DocCollection resolveDocCollection(String collectionStr) {
    if (!cores.isZooKeeperAware()) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST, "Solr not running in cloud mode ");
    }
    ZkStateReader zkStateReader = cores.getZkController().getZkStateReader();

    Supplier<DocCollection> logic =
        () -> {
          this.collectionsList = resolveCollectionListOrAlias(collectionStr); // side-effect
          if (collectionsList.size() > 1) {
            throw new SolrException(
                SolrException.ErrorCode.BAD_REQUEST,
                "Request must be sent to a single collection "
                    + "or an alias that points to a single collection,"
                    + " but '"
                    + collectionStr
                    + "' resolves to "
                    + this.collectionsList);
          }
          String collectionName = collectionsList.get(0); // first
          // TODO an option to choose another collection in the list if can't find a local replica
          // of the first?

          return zkStateReader.getClusterState().getCollectionOrNull(collectionName);
        };

    DocCollection docCollection = logic.get();
    if (docCollection != null) {
      return docCollection;
    }
    // ensure our view is up to date before trying again
    try {
      zkStateReader.aliasesManager.update();
      zkStateReader.forceUpdateCollection(collectionsList.get(0));
    } catch (Exception e) {
      log.error("Error trying to update state while resolving collection.", e);
      // don't propagate exception on purpose
    }
    return logic.get();
  }

  public static Api getApiInfo(
      PluginBag<SolrRequestHandler> requestHandlers,
      String path,
      String method,
      String fullPath,
      Map<String, String> parts) {
    fullPath = fullPath == null ? path : fullPath;
    Api api = requestHandlers.v2lookup(path, method, parts);
    if (api == null && path.endsWith(CommonParams.INTROSPECT)) {
      // the particular http method does not have any,
      // just try if any other method has this path
      api = requestHandlers.v2lookup(path, null, parts);
    }

    if (api == null) {
      return null;
    }

    if (api instanceof ApiBag.IntrospectApi) {
      final Map<String, Api> apis = new LinkedHashMap<>();
      for (String m : SolrRequest.SUPPORTED_METHODS) {
        Api x = requestHandlers.v2lookup(path, m, parts);
        if (x != null) apis.put(m, x);
      }
      api =
          new CompositeApi(
              new Api(ApiBag.EMPTY_SPEC) {
                @Override
                public void call(SolrQueryRequest req, SolrQueryResponse rsp) {
                  String method = req.getParams().get("method");
                  Set<Api> added = new HashSet<>();
                  for (Map.Entry<String, Api> e : apis.entrySet()) {
                    if (method == null || e.getKey().equals(method)) {
                      if (!added.contains(e.getValue())) {
                        e.getValue().call(req, rsp);
                        added.add(e.getValue());
                      }
                    }
                  }
                  RequestHandlerUtils.addExperimentalFormatWarning(rsp);
                }
              });
    }

    return api;
  }

  public List<String> getPathSegments() {
    return pathSegments;
  }

  public static class CompositeApi extends Api {
    private LinkedList<Api> apis = new LinkedList<>();

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

  private void invokeJerseyRequest(
      CoreContainer cores, SolrCore core, ApplicationHandler jerseyHandler, SolrQueryResponse rsp) {
    try {
      final ContainerRequest containerRequest =
          ContainerRequestUtils.createContainerRequest(
              req, response, jerseyHandler.getConfiguration());

      // Set properties that may be used by Jersey filters downstream
      containerRequest.setProperty(RequestContextConstants.SOLR_QUERY_REQUEST_KEY, solrReq);
      containerRequest.setProperty(RequestContextConstants.SOLR_QUERY_RESPONSE_KEY, rsp);
      containerRequest.setProperty(RequestContextConstants.CORE_CONTAINER_KEY, cores);
      containerRequest.setProperty(RequestContextConstants.HTTP_SERVLET_REQ_KEY, req);
      containerRequest.setProperty(RequestContextConstants.REQUEST_TYPE_KEY, requestType);
      containerRequest.setProperty(RequestContextConstants.SOLR_PARAMS_KEY, queryParams);
      containerRequest.setProperty(RequestContextConstants.COLLECTION_LIST_KEY, collectionsList);
      containerRequest.setProperty(RequestContextConstants.HTTP_SERVLET_RSP_KEY, response);
      if (core != null) {
        containerRequest.setProperty(RequestContextConstants.SOLR_CORE_KEY, core);
      }
      servedByJaxRs = true;
      jerseyHandler.handle(containerRequest);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected void handleAdmin(SolrQueryResponse solrResp) {
      if (api == null) {
        invokeJerseyRequest(cores, null, cores.getJerseyApplicationHandler(), solrResp);
      } else {
        SolrCore.preDecorateResponse(solrReq, solrResp);
        try {
          api.call(this.solrReq, solrResp);
        } catch (Exception e) {
          solrResp.setException(e);
        } finally {
          SolrCore.postDecorateResponse(handler, solrReq, solrResp);
        }
      }
  }

  @Override
  protected void executeCoreRequest(SolrQueryResponse rsp) {
    if (api == null) {
      invokeJerseyRequest(cores, core, core.getJerseyApplicationHandler(), rsp);
    } else {
      SolrCore.preDecorateResponse(solrReq, rsp);
      try {
        api.call(solrReq, rsp);
      } catch (Exception e) {
        rsp.setException(e);
      } finally {
        SolrCore.postDecorateResponse(handler, solrReq, rsp);
      }
    }
  }

  @Override
  protected void populateTracingSpan(Span span) {
    // Set db.instance
    String coreOrColName = this.origCorename;
    if (coreOrColName == null) {
      Map<String, String> pathTemplateValues = getUrlParts(); // == solrReq.getPathTemplateValues()
      coreOrColName = pathTemplateValues.get("collection");
      if (coreOrColName == null) {
        coreOrColName = pathTemplateValues.get("core");
      }
    }
    if (coreOrColName != null) {
      span.setTag(Tags.DB_INSTANCE, coreOrColName);
    }

    // Get the templatize-ed path, ex: "/c/{collection}"
    final String path = computeEndpointPath();

    String verb = null;
    // if this api has commands ...
    final Map<String, JsonSchemaValidator> validators = getValidators(); // should be cached
    if (validators != null && validators.isEmpty() == false && solrReq != null) {
      boolean validateInput = true; // because getCommands caches it; and we want it validated later
      // does this request have one command?
      List<CommandOperation> cmds = solrReq.getCommands(validateInput);
      if (cmds.size() == 1) {
        verb = cmds.get(0).name;
      }
    }
    if (verb == null) {
      verb = req.getMethod().toLowerCase(Locale.ROOT);
    }

    span.setOperationName(verb + ":" + path);
  }

  /** Example: /c/collection1/ and template map collection->collection1 produces /c/{collection}. */
  private String computeEndpointPath() {
    // It's not ideal to compute this; let's try to transition away from hitting this code path
    //  by using Annotation APIs
    // collection -> myColName
    final Map<String, String> pathTemplateKeyVal = getUrlParts();
    // myColName -> collection
    final Map<String, String> pathTemplateValKey;
    if (pathTemplateKeyVal.isEmpty()) { // typical
      pathTemplateValKey = Collections.emptyMap();
    } else if (pathTemplateKeyVal.size() == 1) { // typical
      Map.Entry<String, String> entry = pathTemplateKeyVal.entrySet().iterator().next();
      pathTemplateValKey = Map.of(entry.getValue(), entry.getKey());
    } else { // uncommon
      pathTemplateValKey = new HashMap<>();
      for (Map.Entry<String, String> entry : pathTemplateKeyVal.entrySet()) {
        pathTemplateValKey.put(entry.getValue(), entry.getKey());
      }
    }
    final StringBuilder builder = new StringBuilder();
    for (String segment : pathSegments) {
      builder.append('/');
      String key = pathTemplateValKey.get(segment);
      if (key == null) {
        builder.append(segment);
      } else {
        builder.append('{').append(key).append('}');
      }
    }
    return builder.toString();
  }

  @Override
  protected void writeResponse(
      SolrQueryResponse solrRsp, QueryResponseWriter responseWriter, Method reqMethod)
      throws IOException {
    // JAX-RS has its own code that flushes out the Response to the relevant output stream, so we no-op here if the
    // request was already handled via JAX-RS
    if (! servedByJaxRs) {
      super.writeResponse(solrRsp, responseWriter, reqMethod);
    }
  }

  @Override
  protected Object _getHandler() {
    return api;
  }

  public Map<String, String> getUrlParts() {
    return parts;
  }

  @Override
  protected ValidatingJsonMap getSpec() {
    return api == null ? null : api.getSpec();
  }

  @Override
  protected Map<String, JsonSchemaValidator> getValidators() {
    return api == null ? null : api.getCommandSchema();
  }
}
