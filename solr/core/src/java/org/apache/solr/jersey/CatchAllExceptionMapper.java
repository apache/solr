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

import static org.apache.solr.common.SolrException.ErrorCode.getErrorCode;
import static org.apache.solr.jersey.RequestContextKeys.HANDLER_METRICS;
import static org.apache.solr.jersey.RequestContextKeys.SOLR_JERSEY_RESPONSE;
import static org.apache.solr.jersey.RequestContextKeys.SOLR_QUERY_REQUEST;
import static org.apache.solr.jersey.RequestContextKeys.SOLR_QUERY_RESPONSE;

import java.lang.invoke.MethodHandles;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ResourceContext;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import org.apache.solr.client.api.model.SolrJerseyResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.handler.api.V2ApiUtils;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.servlet.ResponseUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO Create separate ExceptionMapper for WebApplicationException
/**
 * Flattens the exception and sets on a {@link SolrJerseyResponse}.
 *
 * <p>Format and behavior based on the exception handling in Solr's v1 requestHandler's. Also sets
 * metrics if present on the request context.
 */
public class CatchAllExceptionMapper implements ExceptionMapper<Exception> {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @Context public ResourceContext resourceContext;

  @Override
  public Response toResponse(Exception exception) {
    final ContainerRequestContext containerRequestContext =
        resourceContext.getResource(ContainerRequestContext.class);

    // Set the exception on the SolrQueryResponse.  Not to affect the actual response, but as
    // SolrDispatchFiler and HttpSolrCall use the presence of an exception as a marker of
    // success/failure for AuditLogging, and other logic.
    final SolrQueryResponse solrQueryResponse =
        (SolrQueryResponse) containerRequestContext.getProperty(SOLR_QUERY_RESPONSE);
    final SolrQueryRequest solrQueryRequest =
        (SolrQueryRequest) containerRequestContext.getProperty(SOLR_QUERY_REQUEST);
    if (exception instanceof WebApplicationException) {
      final WebApplicationException wae = (WebApplicationException) exception;
      final SolrException solrException =
          new SolrException(getErrorCode(wae.getResponse().getStatus()), wae.getMessage());
      solrQueryResponse.setException(solrException);
    } else {
      solrQueryResponse.setException(exception);
    }

    // Exceptions coming from the JAX-RS framework itself should be handled separately.
    if (exception instanceof WebApplicationException) {
      return processWebApplicationException((WebApplicationException) exception);
    }

    return processAndRespondToException(exception, solrQueryRequest, containerRequestContext);
  }

  public static Response processAndRespondToException(
      Exception exception,
      SolrQueryRequest solrQueryRequest,
      ContainerRequestContext containerRequestContext) {
    // First, handle any exception-related metrics
    final Exception normalizedException =
        RequestHandlerBase.normalizeReceivedException(solrQueryRequest, exception);
    final RequestHandlerBase.HandlerMetrics metrics =
        (RequestHandlerBase.HandlerMetrics) containerRequestContext.getProperty(HANDLER_METRICS);
    if (metrics != null) {
      RequestHandlerBase.processErrorMetricsOnException(normalizedException, metrics);
    }

    // Then, convert the exception into a SolrJerseyResponse (creating one as necessary
    // if response not found, etc.)
    return buildExceptionResponse(normalizedException, solrQueryRequest, containerRequestContext);
  }

  public static Response buildExceptionResponse(
      Exception normalizedException,
      SolrQueryRequest solrQueryRequest,
      ContainerRequestContext containerRequestContext) {
    final SolrJerseyResponse response =
        containerRequestContext.getProperty(SOLR_JERSEY_RESPONSE) == null
            ? new SolrJerseyResponse()
            : (SolrJerseyResponse) containerRequestContext.getProperty(SOLR_JERSEY_RESPONSE);
    response.error =
        ResponseUtils.getTypedErrorInfo(
            normalizedException,
            log,
            solrQueryRequest.getCore() != null
                && solrQueryRequest.getCore().getCoreContainer().hideStackTrace());
    response.responseHeader.status = response.error.code;
    final String mediaType =
        V2ApiUtils.getMediaTypeFromWtParam(solrQueryRequest, MediaType.APPLICATION_JSON);
    return Response.status(response.error.code).type(mediaType).entity(response).build();
  }

  private Response processWebApplicationException(WebApplicationException wae) {
    return wae.getResponse();
  }
}
