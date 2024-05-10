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
package org.apache.solr.handler;

import static java.util.Collections.singletonMap;
import static org.apache.solr.common.params.CommonParams.JSON;
import static org.apache.solr.schema.IndexSchema.SchemaProps.Handler.COPY_FIELDS;
import static org.apache.solr.schema.IndexSchema.SchemaProps.Handler.DYNAMIC_FIELDS;
import static org.apache.solr.schema.IndexSchema.SchemaProps.Handler.FIELDS;
import static org.apache.solr.schema.IndexSchema.SchemaProps.Handler.FIELD_TYPES;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.solr.api.AnnotatedApi;
import org.apache.solr.api.Api;
import org.apache.solr.api.ApiBag;
import org.apache.solr.api.JerseyResource;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.admin.api.GetSchema;
import org.apache.solr.handler.admin.api.GetSchemaFieldAPI;
import org.apache.solr.handler.admin.api.SchemaBulkModifyAPI;
import org.apache.solr.handler.api.V2ApiUtils;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.rest.RestManager;
import org.apache.solr.schema.SchemaManager;
import org.apache.solr.security.AuthorizationContext;
import org.apache.solr.security.PermissionNameProvider;
import org.apache.solr.util.plugin.SolrCoreAware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings({"unchecked"})
public class SchemaHandler extends RequestHandlerBase
    implements SolrCoreAware, PermissionNameProvider {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private boolean isImmutableConfigSet = false;
  private SolrRequestHandler managedResourceRequestHandler;

  // java.util factory collections do not accept null values, so we roll our own
  private static final Map<String, String> level2 = new HashMap<>();

  static {
    level2.put(FIELD_TYPES.nameLower, null);
    level2.put(FIELDS.nameLower, "fl");
    level2.put(DYNAMIC_FIELDS.nameLower, "fl");
    level2.put(COPY_FIELDS.nameLower, null);
  }
  ;

  @Override
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    RequestHandlerUtils.setWt(req, JSON);
    String httpMethod = (String) req.getContext().get("httpMethod");
    if ("POST".equals(httpMethod)) {
      if (isImmutableConfigSet) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "ConfigSet is immutable");
      }
      if (req.getContentStreams() == null) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "no stream");
      }

      try {
        List<Map<String, Object>> errs = new SchemaManager(req).performOperations();
        if (!errs.isEmpty())
          throw new ApiBag.ExceptionWithErrObject(
              SolrException.ErrorCode.BAD_REQUEST, "error processing commands", errs);
      } catch (IOException e) {
        throw new SolrException(
            SolrException.ErrorCode.BAD_REQUEST, "Error reading input String " + e.getMessage(), e);
      }
    } else {
      handleGET(req, rsp);
    }
  }

  @Override
  public PermissionNameProvider.Name getPermissionName(AuthorizationContext ctx) {
    switch (ctx.getHttpMethod()) {
      case "GET":
        return PermissionNameProvider.Name.SCHEMA_READ_PERM;
      case "PUT":
      case "DELETE":
      case "POST":
        return PermissionNameProvider.Name.SCHEMA_EDIT_PERM;
      default:
        return null;
    }
  }

  private void handleGET(SolrQueryRequest req, SolrQueryResponse rsp) {
    try {
      String path = (String) req.getContext().get("path");
      switch (path) {
        case "/schema":
          {
            V2ApiUtils.squashIntoSolrResponseWithoutHeader(
                rsp, new GetSchema(req.getCore(), req.getCore().getLatestSchema()).getSchemaInfo());
            break;
          }
        case "/schema/version":
          {
            V2ApiUtils.squashIntoSolrResponseWithoutHeader(
                rsp,
                new GetSchema(req.getCore(), req.getCore().getLatestSchema()).getSchemaVersion());
            break;
          }
        case "/schema/uniquekey":
          {
            V2ApiUtils.squashIntoSolrResponseWithoutHeader(
                rsp,
                new GetSchema(req.getCore(), req.getCore().getLatestSchema()).getSchemaUniqueKey());
            break;
          }
        case "/schema/similarity":
          {
            V2ApiUtils.squashIntoSolrResponseWithoutHeader(
                rsp,
                new GetSchema(req.getCore(), req.getCore().getLatestSchema())
                    .getSchemaSimilarity());
            break;
          }
        case "/schema/name":
          {
            V2ApiUtils.squashIntoSolrResponseWithoutHeader(
                rsp, new GetSchema(req.getCore(), req.getCore().getLatestSchema()).getSchemaName());
            break;
          }
        case "/schema/zkversion":
          {
            V2ApiUtils.squashIntoSolrResponseWithoutHeader(
                rsp,
                new GetSchema(req.getCore(), req.getCore().getLatestSchema())
                    .getSchemaZkVersion(req.getParams().getInt("refreshIfBelowVersion", -1)));
            break;
          }
        default:
          {
            List<String> parts = StrUtils.splitSmart(path, '/', true);
            if (parts.size() > 1 && level2.containsKey(parts.get(1))) {
              String realName = parts.get(1);

              String pathParam = level2.get(realName); // Might be null
              if (parts.size() > 2) {
                req.setParams(
                    SolrParams.wrapDefaults(
                        new MapSolrParams(singletonMap(pathParam, parts.get(2))), req.getParams()));
              }
              switch (realName) {
                case "fields":
                  {
                    if (parts.size() > 2) {
                      V2ApiUtils.squashIntoSolrResponseWithoutHeader(
                          rsp,
                          new GetSchemaFieldAPI(req.getCore().getLatestSchema(), req.getParams())
                              .getFieldInfo(parts.get(2)));
                    } else {
                      V2ApiUtils.squashIntoSolrResponseWithoutHeader(
                          rsp,
                          new GetSchemaFieldAPI(req.getCore().getLatestSchema(), req.getParams())
                              .listSchemaFields());
                    }
                    return;
                  }
                case "copyfields":
                  {
                    V2ApiUtils.squashIntoSolrResponseWithoutHeader(
                        rsp,
                        new GetSchemaFieldAPI(req.getCore().getLatestSchema(), req.getParams())
                            .listCopyFields());
                    return;
                  }
                case "dynamicfields":
                  {
                    if (parts.size() > 2) {
                      V2ApiUtils.squashIntoSolrResponseWithoutHeader(
                          rsp,
                          new GetSchemaFieldAPI(req.getCore().getLatestSchema(), req.getParams())
                              .getDynamicFieldInfo(parts.get(2)));
                    } else {
                      V2ApiUtils.squashIntoSolrResponseWithoutHeader(
                          rsp,
                          new GetSchemaFieldAPI(req.getCore().getLatestSchema(), req.getParams())
                              .listDynamicFields());
                    }
                    return;
                  }
                case "fieldtypes":
                  {
                    if (parts.size() > 2) {
                      V2ApiUtils.squashIntoSolrResponseWithoutHeader(
                          rsp,
                          new GetSchemaFieldAPI(req.getCore().getLatestSchema(), req.getParams())
                              .getFieldTypeInfo(parts.get(2)));
                    } else {
                      V2ApiUtils.squashIntoSolrResponseWithoutHeader(
                          rsp,
                          new GetSchemaFieldAPI(req.getCore().getLatestSchema(), req.getParams())
                              .listSchemaFieldTypes());
                    }
                    return;
                  }
                default:
                  {
                    break;
                  }
              }
            }
            throw new SolrException(SolrException.ErrorCode.NOT_FOUND, "No such path " + path);
          }
      }

    } catch (Exception e) {
      rsp.setException(e);
    }
  }

  private static final Set<String> subPaths =
      new HashSet<>(
          Set.of(
              "version",
              "uniquekey",
              "name",
              "similarity",
              "defaultsearchfield",
              "solrqueryparser",
              "zkversion"));

  static {
    subPaths.addAll(level2.keySet());
  }

  @Override
  public SolrRequestHandler getSubHandler(String subPath) {
    List<String> parts = StrUtils.splitSmart(subPath, '/', true);
    String prefix = parts.get(0);
    if (subPaths.contains(prefix)) return this;

    if (managedResourceRequestHandler != null) return managedResourceRequestHandler;

    return null;
  }

  @Override
  public String getDescription() {
    return "CRUD operations over the Solr schema";
  }

  @Override
  public Category getCategory() {
    return Category.ADMIN;
  }

  @Override
  public void inform(SolrCore core) {
    isImmutableConfigSet = SolrConfigHandler.getImmutable(core);
    this.managedResourceRequestHandler = new ManagedResourceRequestHandler(core.getRestManager());
  }

  @Override
  public Collection<Api> getApis() {

    final List<Api> apis = new ArrayList<>();
    apis.addAll(AnnotatedApi.getApis(new SchemaBulkModifyAPI(this)));

    return apis;
  }

  @Override
  public Collection<Class<? extends JerseyResource>> getJerseyResources() {
    return List.of(GetSchema.class, GetSchemaFieldAPI.class);
  }

  @Override
  public Boolean registerV2() {
    return Boolean.TRUE;
  }

  private class ManagedResourceRequestHandler extends RequestHandlerBase
      implements PermissionNameProvider {

    private final RestManager restManager;

    private ManagedResourceRequestHandler(RestManager restManager) {
      this.restManager = restManager;
    }

    @Override
    public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) {
      RestManager.ManagedEndpoint me = new RestManager.ManagedEndpoint(restManager);
      me.doInit(req, rsp);
      me.delegateRequestToManagedResource();
    }

    @Override
    public Name getPermissionName(AuthorizationContext ctx) {
      return SchemaHandler.this.getPermissionName(ctx);
    }

    @Override
    public String getName() {
      return null;
    }

    @Override
    public String getDescription() {
      return null;
    }
  }
}
