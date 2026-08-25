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
package org.apache.solr.handler.admin;

import static org.apache.solr.client.api.model.NodePropertiesResponse.SYSTEM_PROPERTIES;
import static org.apache.solr.common.params.CommonParams.NAME;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.solr.api.Api;
import org.apache.solr.api.JerseyResource;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.handler.admin.api.GetNodeProperties;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.security.AuthorizationContext;

/**
 * v1 implementation of {@code GET /admin/info/properties}. Business logic lives in {@link
 * GetNodeProperties}.
 *
 * @since solr 1.2
 */
public class PropertiesRequestHandler extends RequestHandlerBase {

  private CoreContainer cc;

  public PropertiesRequestHandler() {
    this(null);
  }

  public PropertiesRequestHandler(CoreContainer cc) {
    super();
    this.cc = cc;
  }

  @Override
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws IOException {
    rsp.setHttpCaching(false);
    String name = req.getParams().get(NAME);
    Map<String, String> props =
        new GetNodeProperties(getCoreContainer(req)).collectProperties(name);
    NamedList<String> values = new SimpleOrderedMap<>();
    props.forEach(values::add);
    rsp.add(SYSTEM_PROPERTIES, values);
  }

  //////////////////////// SolrInfoMBeans methods //////////////////////

  @Override
  public String getDescription() {
    return "Get System Properties";
  }

  @Override
  public Category getCategory() {
    return Category.ADMIN;
  }

  @Override
  public Collection<Api> getApis() {
    return List.of();
  }

  @Override
  public Collection<Class<? extends JerseyResource>> getJerseyResources() {
    return List.of(GetNodeProperties.class);
  }

  @Override
  public Boolean registerV2() {
    return Boolean.TRUE;
  }

  @Override
  public Name getPermissionName(AuthorizationContext request) {
    return Name.CONFIG_READ_PERM;
  }

  private CoreContainer getCoreContainer(SolrQueryRequest req) {
    CoreContainer coreContainer = req.getCoreContainer();
    return coreContainer == null ? cc : coreContainer;
  }
}
