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

package org.apache.solr.jersey;

import org.apache.solr.common.params.SolrParams;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.security.AuthorizationContext;
import org.apache.solr.security.AuthorizationUtils;
import org.apache.solr.security.HttpServletAuthorizationContext;
import org.apache.solr.security.PermissionNameProvider;
import org.apache.solr.servlet.ServletUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.ResourceInfo;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.Provider;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.List;

@Provider
public class SolrRequestAuthorizer implements ContainerRequestFilter {

    public static final String CORE_CONTAINER_PROP_NAME = CoreContainer.class.getName();
    public static final String HTTP_SERVLET_REQ_PROP_NAME = HttpServletRequest.class.getName();
    public static final String HTTP_SERVLET_RSP_PROP_NAME = HttpServletResponse.class.getName();
    public static final String SOLR_CORE_PROP_NAME = SolrCore.class.getName();
    public static final String REQUEST_TYPE_PROP_NAME = AuthorizationContext.RequestType.class.getName();
    public static final String SOLR_PARAMS_PROP_NAME = SolrParams.class.getName();
    public static final String COLLECTION_LIST_PROP_NAME = "collection_name_list";

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    @Context
    private ResourceInfo resourceInfo;

    public SolrRequestAuthorizer() {
        log.info("Creating a new SolrRequestAuthorizer");
    }

    @SuppressWarnings("unchecked")
    @Override
    public void filter(ContainerRequestContext requestContext) throws IOException {
        final CoreContainer coreContainer = (CoreContainer) requestContext.getProperty(CORE_CONTAINER_PROP_NAME);
        final SolrCore solrCore = (SolrCore) requestContext.getProperty(SOLR_CORE_PROP_NAME); // May be null
        final HttpServletRequest servletRequest = (HttpServletRequest) requestContext.getProperty(HTTP_SERVLET_REQ_PROP_NAME);
        final HttpServletResponse servletResponse = (HttpServletResponse) requestContext.getProperty(HTTP_SERVLET_RSP_PROP_NAME);
        final AuthorizationContext.RequestType requestType = (AuthorizationContext.RequestType) requestContext.getProperty(REQUEST_TYPE_PROP_NAME);
        final List<String> collectionNames = (List<String>) requestContext.getProperty(COLLECTION_LIST_PROP_NAME);
        final SolrParams solrParams = (SolrParams) requestContext.getProperty(SOLR_PARAMS_PROP_NAME);

        /*
         * HttpSolrCall has more involved logic to check whether a request requires authorization, but most of that
         * revolves around checking for (1) static paths (e.g. index.html) or (2) HttpSolrCall 'action's that don't need
         * authorization (e.g. request-forwarding)
         *
         * Since we don't invoke Jersey code in those particular cases we can ignore those checks here.
         */
        if (coreContainer.getAuthorizationPlugin() == null) {
            return;
        }
        log.info("JEGERLOW: Looks like we will have to attempt authz");
        final AuthorizationContext authzContext = getAuthzContext(servletRequest, requestType, collectionNames, solrParams, coreContainer);
        AuthorizationUtils.AuthorizationFailure authzFailure = AuthorizationUtils.authorize(servletRequest, servletResponse, coreContainer, authzContext);
        if (authzFailure != null) {
            final Response failureResponse = Response.status(authzFailure.getStatusCode())
                    .entity(authzFailure.getMessage())
                    .build();
            requestContext.abortWith(failureResponse);
        }
    }

    private AuthorizationContext getAuthzContext(HttpServletRequest servletRequest, AuthorizationContext.RequestType reqType,
                                                 List<String> collectionNames, SolrParams solrParams, CoreContainer cores) {
        return new HttpServletAuthorizationContext(servletRequest) {

            @Override
            public List<CollectionRequest> getCollectionRequests() {
                return AuthorizationUtils.getCollectionRequests(ServletUtils.getPathAfterContext(servletRequest),
                        collectionNames, solrParams);
            }

            @Override
            public Object getHandler() {
                return new PermissionNameProvider() {
                    @Override
                    public Name getPermissionName(AuthorizationContext request) {
                        return resourceInfo.getResourceMethod().getAnnotation(PermissionName.class).value();
                    }
                };
            }

            @Override
            public SolrParams getParams() {
                return solrParams;
            }

            @Override
            public RequestType getRequestType() {
                return reqType;
            }
        };
    }
}
