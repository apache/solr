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

import jakarta.inject.Inject;
import org.apache.solr.api.JerseyResource;
import org.apache.solr.client.api.endpoint.SegmentsApi;
import org.apache.solr.client.api.model.ListSegmentsResponse;
import org.apache.solr.core.SolrCore;
import org.apache.solr.jersey.PermissionName;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.security.PermissionNameProvider;

import java.io.IOException;

/**
 * V2 API implementation for {@link SegmentsApi}
 *
 * Equivalent to the v1 /solr/coreName/admin/segments endpoint.
 */
public class ListSegments extends JerseyResource implements SegmentsApi {

    protected final SolrCore solrCore;
    protected final SolrQueryRequest solrQueryRequest;
    protected final SolrQueryResponse solrQueryResponse;

    @Inject
    public ListSegments(SolrCore solrCore, SolrQueryRequest req, SolrQueryResponse rsp) {
        this.solrCore = solrCore;
        this.solrQueryRequest = req;
        this.solrQueryResponse = rsp;
    }

    @Override
    @PermissionName(PermissionNameProvider.Name.METRICS_READ_PERM)
    public ListSegmentsResponse listSegments(Boolean coreInfo, Boolean fieldInfo, Boolean rawSize, Boolean rawSizeSummary, Boolean rawSizeDetails, Float rawSizeSamplingPercent, Boolean sizeInfo) throws IOException {
        // TODO - reimplement SegmentsInfoRequestHandler in strongly-type terms here!
        return null;
    }
}
