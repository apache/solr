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

import com.codahale.metrics.Timer;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.container.ContainerRequestContext;
import org.apache.solr.client.api.model.SolrJerseyResponse;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.PluginBag;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.security.AuthorizationContext;

/**
 * Keys used to store and retrieve values from the Jersey request context.
 *
 * <p>Properties are generally set in V2HttpCall's 'invokeJerseyRequest' and retrieved in individual
 * {@link javax.ws.rs.container.ContainerRequestFilter}s using {@link
 * ContainerRequestContext#getProperty(String)}
 */
public interface RequestContextKeys {
  String HTTP_SERVLET_REQ = HttpServletRequest.class.getName();
  String HTTP_SERVLET_RSP = HttpServletResponse.class.getName();
  String SOLR_QUERY_REQUEST = SolrQueryRequest.class.getName();
  String SOLR_QUERY_RESPONSE = SolrQueryResponse.class.getName();
  String CORE_CONTAINER = CoreContainer.class.getName();
  String RESOURCE_TO_RH_MAPPING = PluginBag.JaxrsResourceToHandlerMappings.class.getName();
  String SOLR_CORE = SolrCore.class.getName();
  String REQUEST_TYPE = AuthorizationContext.RequestType.class.getName();
  String SOLR_PARAMS = SolrParams.class.getName();
  String COLLECTION_LIST = "collection_name_list";
  String HANDLER_METRICS = RequestHandlerBase.HandlerMetrics.class.getName();
  String TIMER = Timer.Context.class.getName();
  String SOLR_JERSEY_RESPONSE = SolrJerseyResponse.class.getName();

  /**
   * A flag read by {@link NotFoundExceptionMapper} to suppress its normal error response
   *
   * <p>Used primarily to allow Solr to lookup certain APIs in multiple JAX-RS applications.
   *
   * @see NotFoundExceptionMapper
   */
  String SUPPRESS_ERROR_ON_NOT_FOUND_EXCEPTION = "ERROR_IF_RESOURCE_NOT_FOUND";

  /**
   * A flag set by {@link NotFoundExceptionMapper} indicating that a 404 error response was
   * suppressed.
   *
   * <p>Used primarily to allow Solr to lookup certian APIs in multiple JAX-RS applications.
   *
   * @see NotFoundExceptionMapper
   */
  String NOT_FOUND_FLAG = "RESOURCE_NOT_FOUND";
}
