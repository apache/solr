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

import javax.ws.rs.NotFoundException;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ResourceContext;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import org.apache.solr.api.V2HttpCall;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

/**
 * An {@link ExceptionMapper} for the exception produced by JAX-RS when no match could be found for
 * an incoming request.
 *
 * <p>If no special flags are set, this mapper returns an error response similar to that produced by
 * other V2 codepaths.
 *
 * <p>If the {@link RequestContextKeys#SUPPRESS_ERROR_ON_NOT_FOUND_EXCEPTION} flag is present, we
 * suppress the error response and merely set a flag indicating the lack of matching resource. This
 * is done in service of fielding requests whose resource might be registered in one of multiple
 * JAX-RS applications. See {@link V2HttpCall}'s "executeCoreRequest" method for more details.
 */
public class NotFoundExceptionMapper implements ExceptionMapper<NotFoundException> {

  @Context public ResourceContext resourceContext;

  @Override
  public Response toResponse(NotFoundException exception) {
    final ContainerRequestContext containerRequestContext =
        resourceContext.getResource(ContainerRequestContext.class);

    if (containerRequestContext
        .getPropertyNames()
        .contains(RequestContextKeys.SUPPRESS_ERROR_ON_NOT_FOUND_EXCEPTION)) {
      containerRequestContext.setProperty(RequestContextKeys.NOT_FOUND_FLAG, "NOT_FOUND_VALUE");
      return null;
    }

    final SolrQueryResponse solrQueryResponse =
        (SolrQueryResponse) containerRequestContext.getProperty(SOLR_QUERY_RESPONSE);
    final SolrQueryRequest solrQueryRequest =
        (SolrQueryRequest) containerRequestContext.getProperty(SOLR_QUERY_REQUEST);
    final SolrException stashedException =
        new SolrException(
            SolrException.ErrorCode.NOT_FOUND,
            "Cannot find API for the path: "
                + solrQueryRequest.getContext().get(CommonParams.PATH));
    solrQueryResponse.setException(stashedException);

    return processAndRespondToException(
        stashedException, solrQueryRequest, containerRequestContext);
  }
}
