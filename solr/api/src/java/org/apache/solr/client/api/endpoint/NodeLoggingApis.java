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
package org.apache.solr.client.api.endpoint;

import io.swagger.v3.oas.annotations.Operation;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.QueryParam;
import java.util.List;
import org.apache.solr.client.api.model.ListLevelsResponse;
import org.apache.solr.client.api.model.LogLevelChange;
import org.apache.solr.client.api.model.LogMessagesResponse;
import org.apache.solr.client.api.model.LoggingResponse;
import org.apache.solr.client.api.model.SetThresholdRequestBody;

@Path("/node/logging")
public interface NodeLoggingApis {

  @GET
  @Path("/levels")
  @Operation(
      summary = "List all log-levels for the target node.",
      tags = {"logging"})
  ListLevelsResponse listAllLoggersAndLevels();

  @PUT
  @Path("/levels")
  @Operation(
      summary = "Set one or more logger levels on the target node.",
      tags = {"logging"})
  LoggingResponse modifyLocalLogLevel(List<LogLevelChange> requestBody);

  @GET
  @Path("/messages")
  @Operation(
      summary = "Fetch recent log messages on the targeted node.",
      tags = {"logging"})
  LogMessagesResponse fetchLocalLogMessages(@QueryParam("since") Long boundingTimeMillis);

  @PUT
  @Path("/messages/threshold")
  @Operation(
      summary = "Set a threshold level for the targeted node's log message watcher.",
      tags = {"logging"})
  LoggingResponse setMessageThreshold(SetThresholdRequestBody requestBody);
}
