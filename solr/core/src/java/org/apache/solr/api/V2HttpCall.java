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

import static org.apache.solr.common.cloud.ZkStateReader.COLLECTION_PROP;
import static org.apache.solr.servlet.SolrDispatchFilter.Action.ADMIN;
import static org.apache.solr.servlet.SolrDispatchFilter.Action.PROCESS;
import static org.apache.solr.servlet.SolrDispatchFilter.Action.REMOTEQUERY;

import com.google.common.collect.ImmutableSet;
import io.opentracing.Span;
import io.opentracing.tag.Tags;
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
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
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
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.security.AuthorizationContext;
import org.apache.solr.servlet.HttpSolrCall;
import org.apache.solr.servlet.SolrDispatchFilter;
import org.apache.solr.servlet.SolrRequestParsers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// class that handle the '/v2' path
@SolrThreadSafe
public class V2HttpCall extends HttpSolrCall {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private Api api;
  private List<String> pathSegments;
  private String prefix;
  HashMap<String, String> parts = new HashMap<>();
  static final Set<String> knownPrefixes =
      ImmutableSet.of("cluster", "node", "collections", "cores", "c");

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
        throw new SolrException(
            SolrException.ErrorCode.NOT_FOUND, "Could not load plugin at " + path);
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
      if (core == null) {
        log.error(">> path: '{}'", path);
        if (path.endsWith(CommonParams.INTROSPECT)) {
          initAdminRequest(path);
          return;
        } else {
          throw new SolrException(
              SolrException.ErrorCode.NOT_FOUND,
              "no core retrieved for core name: " + origCorename + ". Path: " + path);
        }
      } else {
        Thread.currentThread().setContextClassLoader(core.getResourceLoader().getClassLoader());
      }

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
      return getSubPathApi(requestHandlers, path, fullPath, new CompositeApi(null));
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
      getSubPathApi(requestHandlers, path, fullPath, (CompositeApi) api);
    }

    return api;
  }

  private static CompositeApi getSubPathApi(
      PluginBag<SolrRequestHandler> requestHandlers,
      String path,
      String fullPath,
      CompositeApi compositeApi) {

    String newPath =
        path.endsWith(CommonParams.INTROSPECT)
            ? path.substring(0, path.length() - CommonParams.INTROSPECT.length())
            : path;
    Map<String, Set<String>> subpaths = new LinkedHashMap<>();

    getSubPaths(newPath, requestHandlers.getApiBag(), subpaths);
    final Map<String, Set<String>> subPaths = subpaths;
    if (subPaths.isEmpty()) return null;
    return compositeApi.add(
        new Api(() -> ValidatingJsonMap.EMPTY) {
          @Override
          public void call(SolrQueryRequest req1, SolrQueryResponse rsp) {
            String prefix = null;
            prefix =
                fullPath.endsWith(CommonParams.INTROSPECT)
                    ? fullPath.substring(0, fullPath.length() - CommonParams.INTROSPECT.length())
                    : fullPath;
            LinkedHashMap<String, Set<String>> result = new LinkedHashMap<>(subPaths.size());
            for (Map.Entry<String, Set<String>> e : subPaths.entrySet()) {
              if (e.getKey().endsWith(CommonParams.INTROSPECT)) continue;
              result.put(prefix + e.getKey(), e.getValue());
            }

            @SuppressWarnings({"unchecked"})
            Map<Object, Object> m = (Map<Object, Object>) rsp.getValues().get("availableSubPaths");
            if (m != null) {
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
          Set<String> supportedMethods = pathsVsMethod.get(subPath);
          if (supportedMethods == null)
            pathsVsMethod.put(subPath, supportedMethods = new HashSet<>());
          supportedMethods.add(m.toString());
        }
      }
    }
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

  @Override
  protected void handleAdmin(SolrQueryResponse solrResp) {
    try {
      api.call(this.solrReq, solrResp);
    } catch (Exception e) {
      solrResp.setException(e);
    }
  }

  @Override
  protected void execute(SolrQueryResponse rsp) {
    SolrCore.preDecorateResponse(solrReq, rsp);
    if (api == null) {
      rsp.setException(
          new SolrException(
              SolrException.ErrorCode.NOT_FOUND,
              "Cannot find correspond api for the path : "
                  + solrReq.getContext().get(CommonParams.PATH)));
    } else {
      try {
        api.call(solrReq, rsp);
      } catch (Exception e) {
        rsp.setException(e);
      }
    }

    SolrCore.postDecorateResponse(handler, solrReq, rsp);
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
