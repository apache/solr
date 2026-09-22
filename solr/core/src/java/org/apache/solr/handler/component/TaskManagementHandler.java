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
package org.apache.solr.handler.component;

import static org.apache.solr.common.params.CommonParams.DISTRIB;

import java.util.List;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.security.PermissionNameProvider;
import org.apache.solr.util.plugin.SolrCoreAware;

/** Abstract class which serves as the root of all task managing handlers */
public abstract class TaskManagementHandler extends RequestHandlerBase
    implements SolrCoreAware, PermissionNameProvider {
  private ShardHandlerFactory shardHandlerFactory;

  @Override
  public void inform(SolrCore core) {
    this.shardHandlerFactory = core.getCoreContainer().getShardHandlerFactory();
  }

  public static ResponseBuilder buildResponseBuilder(
      SolrQueryRequest req, SolrQueryResponse rsp, List<SearchComponent> components) {
    CoreContainer cc = req.getCoreContainer();
    boolean isZkAware = cc.isZooKeeperAware();

    ResponseBuilder rb = new ResponseBuilder(req, rsp, components);

    rb.isDistrib = req.getParams().getBool(DISTRIB, isZkAware);

    return rb;
  }
}
