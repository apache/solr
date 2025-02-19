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

import static org.apache.solr.cloud.ZkController.COLLECTION_PARAM_PREFIX;
import static org.apache.solr.common.params.CollectionAdminParams.PROPERTY_PREFIX;
import static org.apache.solr.common.params.CommonAdminParams.ASYNC;
import static org.apache.solr.common.params.CoreAdminParams.ACTION;
import static org.apache.solr.common.params.CoreAdminParams.ROLES;
import static org.apache.solr.handler.admin.CoreAdminHandler.paramToProp;
import static org.apache.solr.security.PermissionNameProvider.Name.CORE_EDIT_PERM;

import com.fasterxml.jackson.core.type.TypeReference;
import jakarta.inject.Inject;
import java.lang.invoke.MethodHandles;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.solr.client.api.endpoint.CoreApis;
import org.apache.solr.client.api.model.CreateCoreParams;
import org.apache.solr.client.api.model.CreateCoreResponse;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.CollectionUtil;
import org.apache.solr.common.util.PropertiesUtil;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.admin.CoreAdminHandler;
import org.apache.solr.jersey.PermissionName;
import org.apache.solr.jersey.SolrJacksonMapper;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.util.TestInjection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * V2 API for creating a new core on the receiving node.
 *
 * <p>This API (POST /api/cores {...}) is analogous to the v1 /admin/cores?action=CREATE command.
 */
public class CreateCore extends CoreAdminAPIBase implements CoreApis.Create {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @Inject
  public CreateCore(
      CoreContainer coreContainer,
      CoreAdminHandler.CoreAdminAsyncTracker coreAdminAsyncTracker,
      SolrQueryRequest solrQueryRequest,
      SolrQueryResponse solrQueryResponse) {
    super(coreContainer, coreAdminAsyncTracker, solrQueryRequest, solrQueryResponse);
  }

  @Override
  @PermissionName(CORE_EDIT_PERM)
  public CreateCoreResponse createCore(CreateCoreParams requestBody) throws Exception {
    final var response = instantiateJerseyResponse(CreateCoreResponse.class);

    ensureRequiredRequestBodyProvided(requestBody);
    ensureRequiredParameterProvided("name", requestBody.name);

    assert TestInjection.injectRandomDelayInCoreCreation();
    ensureRequiredRequestBodyProvided(requestBody);
    ensureRequiredParameterProvided("name", requestBody.name);

    return handlePotentiallyAsynchronousTask(
        response,
        requestBody.name,
        requestBody.async,
        "create",
        () -> {
          return doCreate(requestBody, response);
        });
  }

  private CreateCoreResponse doCreate(CreateCoreParams requestBody, CreateCoreResponse response) {
    assert TestInjection.injectRandomDelayInCoreCreation();

    if (log.isInfoEnabled()) {
      log.info("core create command {}", Utils.toJSONString(requestBody));
    }
    Map<String, String> coreParams = buildCoreParams(requestBody);
    final var instancePath = determineInstancePath(requestBody);

    boolean newCollection = Boolean.TRUE.equals(requestBody.newCollection);

    coreContainer.create(requestBody.name, instancePath, coreParams, newCollection);

    response.core = requestBody.name;
    return response;
  }

  // TODO: Should we nuke setting odd instance paths?  They break core discovery, generally
  private Path determineInstancePath(CreateCoreParams createParams) {
    String instanceDir = createParams.instanceDir;
    if (instanceDir == null && CollectionUtil.isNotEmpty(createParams.properties))
      instanceDir = createParams.properties.get("instanceDir");

    if (instanceDir != null) {
      instanceDir =
          PropertiesUtil.substituteProperty(instanceDir, coreContainer.getContainerProperties());
      return coreContainer.getCoreRootDirectory().resolve(instanceDir).normalize();
    } else {
      return coreContainer.getCoreRootDirectory().resolve(createParams.name);
    }
  }

  // TODO create-core API accepts a strongly-typed object, but CoreContainer, CoreDescriptor, et al.
  // still consume a Map<String,String> of properties, persist this to disk as a properties file.
  // We should think through how far to push the strong-typing down into things, if/how it impacts
  // on-disk representations (e.g. core.properties), etc.
  private Map<String, String> buildCoreParams(CreateCoreParams createParams) {
    final var createParamMap =
        SolrJacksonMapper.getObjectMapper()
            .convertValue(createParams, new TypeReference<Map<String, Object>>() {});
    final var coreParams = new HashMap<String, String>();

    // Copy standard core create parameters from paramToProp allowlist
    for (Map.Entry<String, String> entry : paramToProp.entrySet()) {
      if (createParamMap.containsKey(entry.getKey())) {
        Object value = createParamMap.get(entry.getKey());
        if (value != null) {
          if (entry.getKey().equals(ROLES) && value instanceof List) {
            @SuppressWarnings("unchecked")
            final var values = (List<String>) value;
            value = StrUtils.join(values, ',');
          }
          coreParams.put(entry.getValue(), value.toString());
        }
      }
    }

    // User-properties are added without any prefix
    if (CollectionUtil.isNotEmpty(createParams.properties)) {
      coreParams.putAll(createParams.properties);
    }
    // "Collection"-specific properties are given a "collection." prefix in the flattened map
    if (CollectionUtil.isNotEmpty(createParams.collectionProperties)) {
      createParams.collectionProperties.entrySet().stream()
          .forEach(
              entry -> {
                coreParams.put(
                    ZkController.COLLECTION_PARAM_PREFIX + entry.getKey(), entry.getValue());
              });
    }

    return coreParams;
  }

  public static CreateCoreParams createRequestBodyFromV1Params(SolrParams solrParams) {
    final var v1ParamMap = solrParams.toMap(new HashMap<>());
    v1ParamMap.remove(ACTION);
    v1ParamMap.remove(ASYNC);

    final var coreProperties = new HashMap<String, String>();
    final var collectionProperties = new HashMap<String, String>();

    Iterator<String> paramIterator = solrParams.getParameterNamesIterator();
    while (paramIterator.hasNext()) {
      final String key = paramIterator.next();
      final Object value = v1ParamMap.get(key);
      if (key.startsWith(PROPERTY_PREFIX)) {
        v1ParamMap.remove(key);
        coreProperties.put(key.substring(PROPERTY_PREFIX.length()), value.toString());
      }
      if (key.startsWith(COLLECTION_PARAM_PREFIX)) {
        v1ParamMap.remove(key);
        collectionProperties.put(key.substring(COLLECTION_PARAM_PREFIX.length()), value.toString());
      }
      if (key.equals(ROLES)) {
        v1ParamMap.put(ROLES, value.toString().split(","));
      }
    }
    if (CollectionUtil.isNotEmpty(coreProperties)) {
      v1ParamMap.put("properties", coreProperties);
    }
    if (CollectionUtil.isNotEmpty(collectionProperties)) {
      v1ParamMap.put("collectionProperties", collectionProperties);
    }

    return SolrJacksonMapper.getObjectMapper().convertValue(v1ParamMap, CreateCoreParams.class);
  }
}
