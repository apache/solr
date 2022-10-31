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

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.solr.api.EndPoint;
import org.apache.solr.client.solrj.request.beans.RenameCollectionPayload;
import org.apache.solr.common.params.CollectionAdminParams;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.handler.admin.CollectionsHandler;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.util.SolrJacksonAnnotationInspector;

import java.util.Locale;

import static org.apache.solr.client.solrj.SolrRequest.METHOD.POST;
import static org.apache.solr.common.cloud.ZkStateReader.ALIASES;
import static org.apache.solr.common.params.CollectionAdminParams.TARGET;
import static org.apache.solr.common.params.CommonAdminParams.ASYNC;
import static org.apache.solr.common.params.CommonParams.ACTION;
import static org.apache.solr.common.params.CommonParams.NAME;
import static org.apache.solr.handler.ClusterAPI.wrapParams;
import static org.apache.solr.security.PermissionNameProvider.Name.COLL_EDIT_PERM;


public class RenameCollectionAPI {



    private final CollectionsHandler collectionsHandler;
    public RenameCollectionAPI(CollectionsHandler collectionsHandler) {
        this.collectionsHandler = collectionsHandler;
    }

    public static final String RENAME_COLLECTION_CMD = "rename";

    private static final ObjectMapper mapper = SolrJacksonAnnotationInspector.createObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
            .disable(MapperFeature.AUTO_DETECT_FIELDS);

    @EndPoint(
            path = {"/collections/{collection}/command/rename"},
            method = POST,
            permission = COLL_EDIT_PERM)
    public void renameCollection(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception{

        final ContentStream cs = req.getContentStreams().iterator().next();
        RenameCollectionPayload v2Body = mapper.readValue(cs.getStream(), RenameCollectionPayload.class);

        req =
                wrapParams(
                        req,
                        ACTION,
                        CollectionParams.CollectionAction.RENAME.name().toLowerCase(Locale.ROOT),
                        NAME,
                        req.getPathTemplateValues().get(CollectionAdminParams.COLLECTION),
                        TARGET,
                        v2Body.to,
                        ASYNC,
                        v2Body.async,
                        ALIASES,
                        v2Body.followAliases);
        collectionsHandler.handleRequestBody(req, rsp);
    }
}
