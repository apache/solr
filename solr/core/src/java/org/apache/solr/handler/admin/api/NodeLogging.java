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

import static org.apache.solr.common.SolrException.ErrorCode.BAD_REQUEST;
import static org.apache.solr.security.PermissionNameProvider.Name.CONFIG_EDIT_PERM;
import static org.apache.solr.security.PermissionNameProvider.Name.CONFIG_READ_PERM;

import jakarta.inject.Inject;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.apache.solr.api.JerseyResource;
import org.apache.solr.client.api.endpoint.NodeLoggingApis;
import org.apache.solr.client.api.model.ListLevelsResponse;
import org.apache.solr.client.api.model.LogLevelChange;
import org.apache.solr.client.api.model.LogLevelInfo;
import org.apache.solr.client.api.model.LogMessageInfo;
import org.apache.solr.client.api.model.LogMessagesResponse;
import org.apache.solr.client.api.model.LoggingResponse;
import org.apache.solr.client.api.model.SetThresholdRequestBody;
import org.apache.solr.client.solrj.request.LoggingApi;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrException;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.admin.proxy.RemoteRequestProxy;
import org.apache.solr.handler.admin.proxy.V2SolrRequestBasedProxy;
import org.apache.solr.jersey.PermissionName;
import org.apache.solr.logging.LogWatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * V2 APIs for getting or setting log levels on an individual node, or for broadcasting log level
 * changes across nodes.
 *
 * <p>These APIs ('/api/node/logging' and descendants) are analogous to the v1 /admin/info/logging.
 * GET endpoints accept an optional {@code nodes} parameter to proxy to other live nodes via {@link
 * V2SolrRequestBasedProxy}.
 */
public class NodeLogging extends JerseyResource implements NodeLoggingApis {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final CoreContainer coreContainer;
  private final LogWatcher<?> watcher;

  @Inject
  public NodeLogging(CoreContainer coreContainer) {
    this.coreContainer = coreContainer;
    this.watcher = coreContainer.getLogging();
  }

  @Override
  @PermissionName(CONFIG_READ_PERM)
  public ListLevelsResponse listAllLoggersAndLevels(String nodes) {
    final ListLevelsResponse response = instantiateJerseyResponse(ListLevelsResponse.class);
    final var req = new LoggingApi.ListAllLoggersAndLevels();
    req.setNodes(nodes);
    if (V2SolrRequestBasedProxy.proxyToNodes(coreContainer, req, response)) {
      return response;
    }
    ensureLogWatcherEnabled();
    response.watcherName = watcher.getName();

    response.levels = watcher.getAllLevels();

    final List<LogLevelInfo> loggerInfo =
        watcher.getAllLoggers().stream()
            .sorted()
            .map(li -> new LogLevelInfo(li.getName(), li.getLevel(), li.isSet()))
            .collect(Collectors.toList());

    response.loggers = loggerInfo;

    return response;
  }

  @Override
  @PermissionName(CONFIG_EDIT_PERM)
  public LoggingResponse modifyLocalLogLevel(String nodes, List<LogLevelChange> requestBody) {
    ensureLogWatcherEnabled();
    final LoggingResponse response = instantiateLoggingResponse(LoggingResponse.class);

    if (requestBody == null) {
      throw new SolrException(BAD_REQUEST, "Missing request body");
    }

    if (nodes != null && !nodes.isEmpty()) {
      proxyLevelChangesToNodes(response, nodes, requestBody);
      return response;
    }

    for (LogLevelChange change : requestBody) {
      watcher.setLogLevel(change.logger, change.level);
    }
    return response;
  }

  /**
   * Applies the given log level changes on other nodes, mirroring how {@link GetNodeSystemInfo}
   * fans its request out. The receiving node does not apply the changes locally; it is covered only
   * if the resolved node set includes it, in which case it calls itself over HTTP.
   *
   * <p>{@link RemoteRequestProxy#validateNodeNames} rejects an unknown node name before any request
   * goes out, so a broadcast either reaches every named node or none of them; a node that stops
   * responding after that point (timeout, error, or leaving the live set mid-request) is instead
   * reported via {@link RemoteRequestProxy#getFailedNodes()}.
   */
  private void proxyLevelChangesToNodes(
      LoggingResponse response, String nodes, List<LogLevelChange> requestBody) {
    if (coreContainer == null || coreContainer.getZkController() == null) {
      throw new SolrException(
          BAD_REQUEST, "The 'nodes' parameter is only supported in SolrCloud mode");
    }
    try {
      final var req = new LoggingApi.ModifyLocalLogLevel();
      requestBody.forEach(req::addLogLevelChange);
      req.setNodes(nodes);
      final var reqProxy =
          new V2SolrRequestBasedProxy<LoggingResponse>(coreContainer, req) {
            @Override
            public void processTypedProxiedResponse(
                String nodeName, LoggingResponse proxiedResponse) {
              response.remoteNodeData.put(nodeName, proxiedResponse);
            }
          };
      reqProxy.proxyRequest();
      response.failedNodes = reqProxy.getFailedNodes();
    } catch (SolrException e) {
      throw e;
    } catch (Exception e) {
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR, "Error occurred while proxying to other nodes", e);
    }
  }

  @Override
  @PermissionName(CONFIG_READ_PERM)
  public LogMessagesResponse fetchLocalLogMessages(Long boundingTimeMillis, String nodes) {
    final LogMessagesResponse response = instantiateJerseyResponse(LogMessagesResponse.class);
    final var req = new LoggingApi.FetchLocalLogMessages();
    req.setSince(boundingTimeMillis);
    req.setNodes(nodes);
    if (V2SolrRequestBasedProxy.proxyToNodes(coreContainer, req, response)) {
      return response;
    }
    ensureLogWatcherEnabled();
    response.watcherName = watcher.getName();
    if (boundingTimeMillis == null) {
      throw new SolrException(BAD_REQUEST, "Missing required parameter, 'since'.");
    }

    AtomicBoolean found = new AtomicBoolean(false);
    SolrDocumentList docs = watcher.getHistory(boundingTimeMillis, found);
    if (docs == null) {
      throw new SolrException(BAD_REQUEST, "History not enabled");
    }

    final LogMessageInfo info = new LogMessageInfo();
    if (boundingTimeMillis > 0) {
      info.boundingTimeMillis = boundingTimeMillis;
      info.found = found.get();
    } else {
      info.levels = watcher.getAllLevels(); // show for the first request
    }
    info.lastRecordTimestampMillis = watcher.getLastEvent();
    info.buffer = watcher.getHistorySize();

    response.info = info;
    response.docs = docs;

    return response;
  }

  @Override
  @PermissionName(CONFIG_EDIT_PERM)
  public LoggingResponse setMessageThreshold(SetThresholdRequestBody requestBody) {
    ensureLogWatcherEnabled();
    final LoggingResponse response = instantiateLoggingResponse(LoggingResponse.class);

    if (requestBody == null || requestBody.level == null) {
      throw new SolrException(BAD_REQUEST, "Required parameter 'level' missing");
    }
    watcher.setThreshold(requestBody.level);

    return response;
  }

  // A hacky testing-only parameter used to test the v1 LoggingHandler
  public static void writeLogsForTesting() {
    log.trace("trace message");
    log.debug("debug message");
    RuntimeException exc = new RuntimeException("test");
    log.info("info (with exception) INFO", exc);
    log.warn("warn (with exception) WARN", exc);
    log.error("error (with exception) ERROR", exc);
  }

  private void ensureLogWatcherEnabled() {
    if (watcher == null) {
      throw new SolrException(BAD_REQUEST, "Logging Not Initialized");
    }
  }

  private <T extends LoggingResponse> T instantiateLoggingResponse(Class<T> clazz) {
    final T response = instantiateJerseyResponse(clazz);
    response.watcherName = watcher.getName();
    return response;
  }

  public static List<LogLevelChange> parseLogLevelChanges(String[] rawChangeValues) {
    final List<LogLevelChange> changes = new ArrayList<>();

    for (String rawChange : rawChangeValues) {
      String[] split = rawChange.split(":");
      if (split.length != 2) {
        throw new SolrException(
            SolrException.ErrorCode.SERVER_ERROR,
            "Invalid format, expected level:value, got " + rawChange);
      }
      changes.add(new LogLevelChange(split[0], split[1]));
    }

    return changes;
  }
}
