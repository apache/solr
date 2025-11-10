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

import static org.apache.solr.common.params.CommonParams.NAME;

import java.io.IOException;
import java.util.Collection;
import java.util.Enumeration;
import org.apache.solr.api.AnnotatedApi;
import org.apache.solr.api.Api;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.NodeConfig;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.handler.admin.api.NodePropertiesAPI;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.security.AuthorizationContext;

/**
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
    NamedList<String> props = new SimpleOrderedMap<>();
    String name = req.getParams().get(NAME);
    NodeConfig nodeConfig = getCoreContainer(req).getNodeConfig();
    if (name != null) {
      String property = nodeConfig.getRedactedSysPropValue(name);
      props.add(name, property);
    } else {
      Enumeration<?> enumeration = System.getProperties().propertyNames();
      while (enumeration.hasMoreElements()) {
        name = (String) enumeration.nextElement();
        props.add(name, nodeConfig.getRedactedSysPropValue(name));
      }
    }
    rsp.add("system.properties", props);
    rsp.setHttpCaching(false);
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
    return AnnotatedApi.getApis(new NodePropertiesAPI(this));
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
