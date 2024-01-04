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

import org.apache.solr.client.api.model.CreateCoreRequestBody;
import org.apache.solr.client.api.model.SolrJerseyResponse;

import javax.ws.rs.POST;
import javax.ws.rs.Path;

/**
 * V2 API for creating a new core on the receiving node.
 *
 * <p>This API (POST /api/cores {...}) is analogous to the v1 /admin/cores?action=CREATE
 * command.
 *
 */
@Path("/cores")
public interface CreateCoreApi {
    @POST
    public SolrJerseyResponse createCore(CreateCoreRequestBody createCoreRequestBody);
}
