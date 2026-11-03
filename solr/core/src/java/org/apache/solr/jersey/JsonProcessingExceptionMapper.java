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

import static org.apache.solr.jersey.CatchAllExceptionMapper.processAndRespondToException;
import static org.apache.solr.jersey.RequestContextKeys.SOLR_QUERY_REQUEST;
import static org.apache.solr.jersey.RequestContextKeys.SOLR_QUERY_RESPONSE;

import com.fasterxml.jackson.core.JsonProcessingException;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ResourceContext;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.ExceptionMapper;
import org.apache.solr.common.SolrException;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

/**
 * An {@link ExceptionMapper} for the exceptions Jackson produces when a request body cannot be
 * parsed or its values cannot be converted to the types a JAX-RS resource expects.
 *
 * <p>These failures are caused by the request the client sent, so they are reported as a 400
 * carrying the parser's message, in the same error format as other V2 responses. Without this
 * mapper they fall through to {@link CatchAllExceptionMapper} and are reported as a 500.
 */
public class JsonProcessingExceptionMapper implements ExceptionMapper<JsonProcessingException> {

  @Context public ResourceContext resourceContext;

  @Override
  public Response toResponse(JsonProcessingException exception) {
    final ContainerRequestContext containerRequestContext =
        resourceContext.getResource(ContainerRequestContext.class);

    final SolrQueryResponse solrQueryResponse =
        (SolrQueryResponse) containerRequestContext.getProperty(SOLR_QUERY_RESPONSE);
    final SolrQueryRequest solrQueryRequest =
        (SolrQueryRequest) containerRequestContext.getProperty(SOLR_QUERY_REQUEST);

    final String detail =
        exception.getOriginalMessage() != null
            ? exception.getOriginalMessage()
            : exception.getMessage();
    final SolrException stashedException =
        new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Invalid request body: " + detail);
    solrQueryResponse.setException(stashedException);

    return processAndRespondToException(
        stashedException, solrQueryRequest, containerRequestContext);
  }
}
