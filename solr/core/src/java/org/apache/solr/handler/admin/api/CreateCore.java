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
package org.apache.solr.handler.admin.api;

import static org.apache.solr.common.params.CollectionAdminParams.PROPERTY_PREFIX;
import static org.apache.solr.handler.api.V2ApiUtils.flattenMapWithPrefix;
import static org.apache.solr.handler.api.V2ApiUtils.flattenToCommaDelimitedString;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.inject.Inject;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.solr.client.api.endpoint.CreateCoreApi;
import org.apache.solr.client.api.model.CreateCoreRequestBody;
import org.apache.solr.client.api.model.CreateCoreResponseBody;
import org.apache.solr.client.api.model.SolrJerseyResponse;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.util.PropertiesUtil;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.admin.CoreAdminHandler;
import org.apache.solr.jersey.PermissionName;
import org.apache.solr.jersey.SolrJacksonMapper;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.security.PermissionNameProvider;

/**
 * Implementation of V2 api {@link CreateCoreApi} for creating core.
 *
 * @see CreateCoreRequestBody
 * @see CoreAdminAPIBase
 */
public class CreateCore extends CoreAdminAPIBase implements CreateCoreApi {
  private final ObjectMapper objectMapper;

  @Inject
  public CreateCore(
      CoreContainer coreContainer,
      CoreAdminHandler.CoreAdminAsyncTracker coreAdminAsyncTracker,
      SolrQueryRequest req,
      SolrQueryResponse rsp) {
    super(coreContainer, coreAdminAsyncTracker, req, rsp);
    this.objectMapper = SolrJacksonMapper.getObjectMapper();
  }

  @Override
  @PermissionName(PermissionNameProvider.Name.CORE_EDIT_PERM)
  public SolrJerseyResponse createCore(
      CreateCoreRequestBody createCoreRequestBody, String coreName) {
    final Map<String, Object> requestBodyMap =
        objectMapper.convertValue(createCoreRequestBody, new TypeReference<>() {});
    if (createCoreRequestBody.isTransient != null) {
      requestBodyMap.put("transient", requestBodyMap.remove("isTransient"));
    }
    if (createCoreRequestBody.properties != null) {
      requestBodyMap.remove("properties");
      flattenMapWithPrefix(createCoreRequestBody.properties, requestBodyMap, PROPERTY_PREFIX);
    }
    if (createCoreRequestBody.roles != null && !createCoreRequestBody.roles.isEmpty()) {
      requestBodyMap.remove("roles");
      flattenToCommaDelimitedString(requestBodyMap, createCoreRequestBody.roles, "roles");
    }
    return createCore(requestBodyMap, coreName);
  }

  public SolrJerseyResponse createCore(Map<String, Object> params, String coreName) {
    var response = instantiateJerseyResponse(CreateCoreResponseBody.class);
    Map<String, String> coreParams = buildCoreParams(params);
    Path instancePath;

    // TODO: Should we nuke setting odd instance paths?  They break core discovery, generally
    String instanceDir =
        params.get(CoreAdminParams.INSTANCE_DIR) != null
            ? params.get(CoreAdminParams.INSTANCE_DIR).toString()
            : null;
    if (instanceDir == null)
      instanceDir =
          params.get("property.instanceDir") != null
              ? params.get("property.instanceDir").toString()
              : null;
    if (instanceDir != null) {
      instanceDir =
          PropertiesUtil.substituteProperty(instanceDir, coreContainer.getContainerProperties());
      instancePath = coreContainer.getCoreRootDirectory().resolve(instanceDir).normalize();
    } else {
      instancePath = coreContainer.getCoreRootDirectory().resolve(coreName);
    }

    boolean newCollection =
        params.get(CoreAdminParams.NEW_COLLECTION) != null
            ? StrUtils.parseBool(params.get(CoreAdminParams.NEW_COLLECTION).toString())
            : false;

    coreContainer.create(coreName, instancePath, coreParams, newCollection);

    response.core = coreName;
    return response;
  }

  public static Map<String, String> buildCoreParams(Map<String, Object> params) {
    Map<String, String> coreParams = new HashMap<>();

    // standard core create parameters
    for (Map.Entry<String, String> entry : CoreAdminHandler.paramToProp.entrySet()) {
      var propVal =
          Optional.ofNullable(params.get(entry.getKey())).map(Object::toString).orElse("");
      if (!propVal.isEmpty()) {
        coreParams.put(entry.getValue(), propVal);
      }
    }

    // extra properties
    params.forEach(
        (param, propValue) -> {
          var propValueStr = Optional.ofNullable(propValue).map(Object::toString).orElse("");
          if (param.startsWith(CoreAdminParams.PROPERTY_PREFIX)) {
            String propName = param.substring(CoreAdminParams.PROPERTY_PREFIX.length());
            coreParams.put(propName, propValueStr);
          }
          if (param.startsWith(ZkController.COLLECTION_PARAM_PREFIX)) {
            coreParams.put(param, propValueStr);
          }
        });

    return coreParams;
  }
}
