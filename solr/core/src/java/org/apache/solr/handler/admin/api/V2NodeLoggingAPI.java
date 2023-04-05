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

import static org.apache.solr.security.PermissionNameProvider.Name.CONFIG_EDIT_PERM;

import java.util.List;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import org.apache.solr.jersey.PermissionName;
import org.apache.solr.logging.LogWatcher;

/**
 * V2 API for getting log levels on an individual node.
 *
 * <p>The endpoint is defined as api/node/logging/levels
 */
@Path("/api/node/logging")
public class V2NodeLoggingAPI {
  private final LogWatcher<?> watcher;

  @Inject
  public V2NodeLoggingAPI(LogWatcher<?> watcher) {
    this.watcher = watcher;
  }

  @GET
  @Path("/levels")
  @PermissionName(CONFIG_EDIT_PERM)
  @Produces(MediaType.APPLICATION_JSON)
  public List<String> getAllLevels() {
    return this.watcher.getAllLevels();
  }
}
