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

import java.lang.invoke.MethodHandles;
import java.util.Collection;
import java.util.Set;
import org.apache.solr.api.AnnotatedApi;
import org.apache.solr.api.Api;
import org.apache.solr.api.JerseyResource;
import org.apache.solr.client.api.model.NodeSystemInfoResponse;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.handler.admin.api.GetNodeSystemInfo;
import org.apache.solr.handler.admin.api.NodeSystemInfoAPI;
import org.apache.solr.handler.api.V2ApiUtils;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.security.AuthorizationContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This handler returns system info
 *
 * @since solr 1.2
 */
public class SystemInfoHandler extends RequestHandlerBase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private CoreContainer cc;

  public SystemInfoHandler() {
    this(null);
  }

  public SystemInfoHandler(CoreContainer cc) {
    super();
    this.cc = cc;
  }

  @Override
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    rsp.setHttpCaching(false);

    if (AdminHandlersProxy.maybeProxyToNodes(req, rsp, getCoreContainer(req))) {
      return; // Request was proxied to other node
    }

    NodeSystemInfoProvider provider = new NodeSystemInfoProvider(req);
    NodeSystemInfoResponse response = provider.getNodeSystemInfo(new NodeSystemInfoResponse());
    V2ApiUtils.squashIntoSolrResponseWithoutHeader(rsp, response);

    return;
  }

  private CoreContainer getCoreContainer(SolrQueryRequest req) {
    CoreContainer coreContainer = req.getCoreContainer();
    return coreContainer == null ? cc : coreContainer;
  }

  //////////////////////// SolrInfoMBeans methods //////////////////////

  @Override
  public String getDescription() {
    return "Get System Info";
  }

  @Override
  public Category getCategory() {
    return Category.ADMIN;
  }

  @Override
  public Collection<Api> getApis() {
    return AnnotatedApi.getApis(new NodeSystemInfoAPI(this));
  }

  @Override
  public Collection<Class<? extends JerseyResource>> getJerseyResources() {
    return Set.of(GetNodeSystemInfo.class);
  }

  @Override
  public Boolean registerV2() {
    return Boolean.TRUE;
  }

  @Override
  public Name getPermissionName(AuthorizationContext request) {
    return Name.CONFIG_READ_PERM;
  }
}
