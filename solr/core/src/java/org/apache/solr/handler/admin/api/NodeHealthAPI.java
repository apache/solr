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

import static org.apache.solr.security.PermissionNameProvider.Name.HEALTH_PERM;

import jakarta.inject.Inject;
import org.apache.solr.api.JerseyResource;
import org.apache.solr.client.api.endpoint.NodeHealthApi;
import org.apache.solr.client.api.model.NodeHealthResponse;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.admin.HealthCheckHandler;
import org.apache.solr.jersey.PermissionName;

/**
 * V2 API for checking the health of the receiving node.
 *
 * <p>This API (GET /v2/node/health) is analogous to the v1 /admin/info/health.
 *
 * <p>This is a thin JAX-RS wrapper; the health-check logic lives in {@link HealthCheckHandler}.
 */
public class NodeHealthAPI extends JerseyResource implements NodeHealthApi {

  private final HealthCheckHandler handler;

  @Inject
  public NodeHealthAPI(CoreContainer coreContainer) {
    this.handler = new HealthCheckHandler(coreContainer);
  }

  @Override
  @PermissionName(HEALTH_PERM)
  public NodeHealthResponse checkNodeHealth(Boolean requireHealthyCores) {
    return handler.checkNodeHealth(requireHealthyCores, null);
  }

  /**
   * Convenience overload used by tests and the v1 handler to pass both health-check parameters.
   *
   * @see HealthCheckHandler#checkNodeHealth(Boolean, Integer)
   */
  public NodeHealthResponse checkNodeHealth(Boolean requireHealthyCores, Integer maxGenerationLag) {
    return handler.checkNodeHealth(requireHealthyCores, maxGenerationLag);
  }
}
