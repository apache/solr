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
      description =
          "If the 'nodes' parameter is provided, the listing is instead collected from "
              + "each of the named nodes (or from every live node, if 'nodes' is 'all'), and "
              + "the response reports the per-node results.",
      tags = {"logging"})
  ListLevelsResponse listAllLoggersAndLevels(@QueryParam("nodes") String nodes);

  @PUT
  @Path("/levels")
  @Operation(
      summary = "Set one or more logger levels on the target node.",
      description =
          "If the 'nodes' parameter is provided, the level changes are instead applied to "
              + "each of the named nodes (or to every live node, if 'nodes' is 'all'), and the "
              + "response reports the per-node results.",
      tags = {"logging"})
  LoggingResponse modifyLocalLogLevel(
      @QueryParam("nodes") String nodes, List<LogLevelChange> requestBody);

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
