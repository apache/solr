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

import org.apache.solr.api.EndPoint;
import org.apache.solr.handler.UpdateRequestHandler;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

import static org.apache.solr.client.solrj.SolrRequest.METHOD.POST;
import static org.apache.solr.common.params.CommonParams.PATH;
import static org.apache.solr.security.PermissionNameProvider.Name.UPDATE_PERM;

/**
 * All v2 APIs that share a prefix of /update
 *
 * Most of these v2 APIs are implemented as pure "pass-throughs" to the v1 code paths, but there are a few exceptions:
 * /update and /update/json are both rewritten to /update/json/docs, and /update/json/commands has its path changed to
 * /update/json.
 */
public class UpdateAPI {
    private final UpdateRequestHandler updateRequestHandler;

    public UpdateAPI(UpdateRequestHandler updateRequestHandler) {
        this.updateRequestHandler = updateRequestHandler;
    }

    @EndPoint(method = POST,
            path = "/update",
            permission = UPDATE_PERM)
    public void update(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
        req.getContext().put(PATH, "/update/json/docs");
        updateRequestHandler.handleRequestBody(req, rsp);
    }

    @EndPoint(method = POST,
            path = "/update/xml",
            permission = UPDATE_PERM)
    public void updateXml(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
        updateRequestHandler.handleRequestBody(req, rsp);
    }

    @EndPoint(method = POST,
            path = "/update/csv",
            permission = UPDATE_PERM)
    public void updateCsv(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
        updateRequestHandler.handleRequestBody(req, rsp);
    }

    @EndPoint(method = POST,
            path = "/update/json",
            permission = UPDATE_PERM)
    public void updateJson(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
        req.getContext().put(PATH, "/update/json/docs");
        updateRequestHandler.handleRequestBody(req, rsp);
    }

    @EndPoint(method = POST,
            path = "/update/bin",
            permission = UPDATE_PERM)
    public void updateJavabin(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
        updateRequestHandler.handleRequestBody(req, rsp);
    }

    // TODO Is this API really needed/wanted?  It's not documented anywhere and seemingly only duplicates an existing
    //  API
    @EndPoint(method = POST,
            path = "/update/json/commands",
            permission = UPDATE_PERM)
    public void updateCommands(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
        req.getContext().put(PATH, "/update/json");
        updateRequestHandler.handleRequestBody(req, rsp);
    }
}
