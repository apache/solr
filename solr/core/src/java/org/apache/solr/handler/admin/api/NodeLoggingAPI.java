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

import static org.apache.solr.client.solrj.impl.BinaryResponseParser.BINARY_CONTENT_TYPE_V2;
import static org.apache.solr.common.SolrException.ErrorCode.BAD_REQUEST;
import static org.apache.solr.security.PermissionNameProvider.Name.CONFIG_EDIT_PERM;
import static org.apache.solr.security.PermissionNameProvider.Name.CONFIG_READ_PERM;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import org.apache.solr.api.JerseyResource;
import org.apache.solr.client.api.model.SolrJerseyResponse;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrException;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.jersey.JacksonReflectMapWriter;
import org.apache.solr.jersey.PermissionName;
import org.apache.solr.logging.LogWatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO Add support for 'nodes' param once SOLR-16738 is completed.
/**
 * V2 APIs for getting or setting log levels on an individual node.
 *
 * <p>These APIs ('/api/node/logging' and descendants) are analogous to the v1 /admin/info/logging.
 */
@Path("/node/logging")
public class NodeLoggingAPI extends JerseyResource {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final CoreContainer coreContainer;
  private final LogWatcher<?> watcher;

  @Inject
  public NodeLoggingAPI(CoreContainer coreContainer) {
    this.coreContainer = coreContainer;
    this.watcher = coreContainer.getLogging();
  }

  @Path("/levels")
  @GET
  @PermissionName(CONFIG_READ_PERM)
  @Produces({"application/json", "application/xml", BINARY_CONTENT_TYPE_V2})
  public ListLevelsResponse listAllLoggersAndLevels() {
    ensureLogWatcherEnabled();
    final ListLevelsResponse response = instantiateLoggingResponse(ListLevelsResponse.class);

    response.levels = watcher.getAllLevels();

    final List<LogLevelInfo> loggerInfo =
        watcher.getAllLoggers().stream()
            .sorted()
            .map(li -> new LogLevelInfo(li.getName(), li.getLevel(), li.isSet()))
            .collect(Collectors.toList());

    response.loggers = loggerInfo;

    return response;
  }

  @Path("/levels")
  @PUT
  @PermissionName(CONFIG_EDIT_PERM)
  @Produces({"application/json", "application/xml", BINARY_CONTENT_TYPE_V2})
  public LoggingResponse modifyLocalLogLevel(List<LogLevelChange> requestBody) {
    ensureLogWatcherEnabled();
    final LoggingResponse response = instantiateLoggingResponse(LoggingResponse.class);

    if (requestBody == null) {
      throw new SolrException(BAD_REQUEST, "Missing request body");
    }

    for (LogLevelChange change : requestBody) {
      watcher.setLogLevel(change.logger, change.level);
    }
    return response;
  }

  @Path("/messages")
  @GET
  @PermissionName(CONFIG_READ_PERM)
  @Produces({"application/json", "application/xml", BINARY_CONTENT_TYPE_V2})
  public LogMessagesResponse fetchLocalLogMessages(@QueryParam("since") Long boundingTimeMillis) {
    ensureLogWatcherEnabled();
    final LogMessagesResponse response = instantiateLoggingResponse(LogMessagesResponse.class);
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

  @Path("/messages/threshold")
  @PUT
  @PermissionName(CONFIG_EDIT_PERM)
  @Produces({"application/json", "application/xml", BINARY_CONTENT_TYPE_V2})
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

  /** Generic logging response that includes the name of the log watcher (e.g. "Log4j2") */
  public static class LoggingResponse extends SolrJerseyResponse {
    @JsonProperty("watcher")
    public String watcherName;
  }

  /** A user-requested modification in the level that a specified logger reports at. */
  public static class LogLevelChange implements JacksonReflectMapWriter {
    public LogLevelChange() {}

    public LogLevelChange(String logger, String level) {
      this.logger = logger;
      this.level = level;
    }

    @JsonProperty public String logger;
    @JsonProperty public String level;

    public static List<LogLevelChange> createRequestBodyFromV1Params(String[] rawChangeValues) {
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

  /** The request body for the 'PUT /api/node/logging/messages/threshold' API. */
  public static class SetThresholdRequestBody implements JacksonReflectMapWriter {
    public SetThresholdRequestBody() {}

    public SetThresholdRequestBody(String level) {
      this.level = level;
    }

    @JsonProperty(required = true)
    public String level;
  }

  /** Response format for the 'GET /api/node/logging/messages' API. */
  public static class LogMessagesResponse extends LoggingResponse {
    @JsonProperty public LogMessageInfo info;

    @JsonProperty("history")
    public SolrDocumentList docs;
  }

  /** Metadata about the log messages returned by the 'GET /api/node/logging/messages' API */
  public static class LogMessageInfo implements JacksonReflectMapWriter {
    @JsonProperty("since")
    public Long boundingTimeMillis;

    @JsonProperty public Boolean found;
    @JsonProperty public List<String> levels;

    @JsonProperty("last")
    public long lastRecordTimestampMillis;

    @JsonProperty public int buffer;
    @JsonProperty public String threshold;
  }

  /** Response format for the 'GET /api/node/logging/levels' API. */
  public static class ListLevelsResponse extends LoggingResponse {
    @JsonProperty public List<String> levels;
    @JsonProperty public List<LogLevelInfo> loggers;
  }

  /** Representation of a single logger and its current state. */
  public static class LogLevelInfo implements JacksonReflectMapWriter {
    public LogLevelInfo() {}

    public LogLevelInfo(String name, String level, boolean set) {
      this.name = name;
      this.level = level;
      this.set = set;
    }

    @JsonProperty("name")
    public String name;

    @JsonProperty("level")
    public String level;

    @JsonProperty("set")
    public boolean set;
  }
}
