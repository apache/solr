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

package org.apache.solr.jersey;

import org.apache.solr.handler.api.V2ApiUtils;
import org.apache.solr.request.SolrQueryRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;
import java.io.IOException;
import java.lang.invoke.MethodHandles;

import static javax.ws.rs.core.HttpHeaders.CONTENT_TYPE;
import static org.apache.solr.jersey.RequestContextKeys.SOLR_QUERY_REQUEST;

// TODO Deprecate or remove support for the 'wt' parameter in the v2 APIs in favor of the more
//  HTTP-compliant 'Accept' header
/**
 * Overrides the content-type of the response based on an optional user-provided 'wt' parameter
 */
public class MediaTypeOverridingFilter implements ContainerResponseFilter {
    @Override
    public void filter(ContainerRequestContext requestContext, ContainerResponseContext responseContext) throws IOException {
        final SolrQueryRequest solrQueryRequest =
                (SolrQueryRequest) requestContext.getProperty(SOLR_QUERY_REQUEST);
        final String mediaType = V2ApiUtils.getMediaTypeFromWtParam(solrQueryRequest, null);
        if (mediaType != null) {
            responseContext.getHeaders().putSingle(CONTENT_TYPE, mediaType);
        }
    }
}
