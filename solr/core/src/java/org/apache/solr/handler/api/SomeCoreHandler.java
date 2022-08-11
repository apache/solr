/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.solr.handler.api;

import org.apache.solr.api.JerseyResource;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.security.AuthorizationContext;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class SomeCoreHandler extends RequestHandlerBase {
    @Override
    public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
        rsp.add("status", "Hello v1 world");
    }

    @Override
    public String getDescription() {
        return "Stub to expose a core-level Jersey endpoint";
    }

    @Override
    public Name getPermissionName(AuthorizationContext request) {
        return Name.READ_PERM;
    }

    @Override
    public Boolean registerV2() {
        return Boolean.TRUE;
    }

    @Override
    public Collection<Class<? extends JerseyResource>> getJerseyResources() {
        final List<Class<? extends JerseyResource>> resources = new ArrayList<>();
        resources.add(SomeCoreResource.class);

        return resources;
    }
}
