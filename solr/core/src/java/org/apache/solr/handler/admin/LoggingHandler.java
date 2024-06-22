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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.solr.api.Api;
import org.apache.solr.api.JerseyResource;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.handler.admin.api.NodeLoggingAPI;
import org.apache.solr.handler.api.V2ApiUtils;
import org.apache.solr.logging.LogWatcher;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.security.AuthorizationContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A request handler to show which loggers are registered and allows you to set them
 *
 * @since 4.0
 */
public class LoggingHandler extends RequestHandlerBase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final LogWatcher<?> watcher;
  private final CoreContainer cc;

  public LoggingHandler(CoreContainer cc) {
    this.cc = cc;
    this.watcher = cc.getLogging();
  }

  @Override
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    final NodeLoggingAPI loggingApi = new NodeLoggingAPI(cc);

    SolrParams params = req.getParams();
    if (params.get("threshold") != null) {
      squashV2Response(
          rsp,
          loggingApi.setMessageThreshold(
              new NodeLoggingAPI.SetThresholdRequestBody(params.get("threshold"))));
    }

    // Write something at each level
    if (params.get("test") != null) {
      NodeLoggingAPI.writeLogsForTesting();
    }

    String[] set = params.getParams("set");
    if (set != null) {
      final List<NodeLoggingAPI.LogLevelChange> changes =
          NodeLoggingAPI.LogLevelChange.createRequestBodyFromV1Params(set);
      squashV2Response(rsp, loggingApi.modifyLocalLogLevel(changes));
    }

    String since = req.getParams().get("since");
    if (since != null) {
      long time = -1;
      try {
        time = Long.parseLong(since);
      } catch (Exception ex) {
        throw new SolrException(ErrorCode.BAD_REQUEST, "invalid timestamp: " + since);
      }
      squashV2Response(rsp, loggingApi.fetchLocalLogMessages(time));
    } else {
      squashV2Response(rsp, loggingApi.listAllLoggersAndLevels());
    }

    rsp.setHttpCaching(false);
    if (cc != null && AdminHandlersProxy.maybeProxyToNodes(req, rsp, cc)) {
      return; // Request was proxied to other node
    }
  }

  private void squashV2Response(SolrQueryResponse rsp, NodeLoggingAPI.LoggingResponse response) {
    V2ApiUtils.squashIntoSolrResponseWithoutHeader(rsp, response);
  }

  // ////////////////////// SolrInfoMBeans methods //////////////////////

  @Override
  public String getDescription() {
    return "Logging Handler";
  }

  @Override
  public Category getCategory() {
    return Category.ADMIN;
  }

  @Override
  public Collection<Api> getApis() {
    return new ArrayList<>();
  }

  @Override
  public Collection<Class<? extends JerseyResource>> getJerseyResources() {
    return List.of(NodeLoggingAPI.class);
  }

  @Override
  public Boolean registerV2() {
    return Boolean.TRUE;
  }

  @Override
  public Name getPermissionName(AuthorizationContext request) {
    if (request.getParams().get("set") != null) {
      return Name.CONFIG_EDIT_PERM; // Change log level
    } else {
      return Name.CONFIG_READ_PERM;
    }
  }
}
