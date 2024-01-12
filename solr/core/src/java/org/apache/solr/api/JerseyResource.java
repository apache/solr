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

package org.apache.solr.api;

import static org.apache.solr.jersey.RequestContextKeys.SOLR_JERSEY_RESPONSE;

import java.util.function.Supplier;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.Context;
import org.apache.solr.client.api.model.SolrJerseyResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.jersey.CatchAllExceptionMapper;
import org.apache.solr.servlet.HttpSolrCall;

/**
 * A marker parent type for all Jersey "resource" classes.
 *
 * <p>"Resources" in Jersey are classes that define one or more API endpoints. As such they're
 * analogous to the v1 {@link org.apache.solr.request.SolrRequestHandler} or the v2 {@link Api}.
 */
public class JerseyResource {

  @Context public ContainerRequestContext containerRequestContext;

  /**
   * Create an instance of the {@link SolrJerseyResponse} subclass; registering it with the Jersey
   * request-context upon creation.
   *
   * <p>This utility method primarily exists to allow Jersey resources to return error responses
   * that match those returned by Solr's v1 APIs.
   *
   * <p>When a severe-enough exception halts a v1 request, Solr generates a summary of the error and
   * attaches it to the {@link org.apache.solr.response.SolrQueryResponse} given to the request
   * handler. This SolrQueryResponse may already hold some portion of the normal "success" response
   * for that API.
   *
   * <p>The JAX-RS framework isn't well suited to mimicking responses of this sort, as the
   * "response" from a Jersey resource is its return value (instead of a mutable method parameter
   * that gets modified). This utility works around this limitation by attaching the eventual return
   * value of a JerseyResource to the context associated with the Jersey request, as soon as its
   * created. This allows partially-constructed responses to be accessed later in the case of an
   * exception.
   *
   * <p>In order to instantiate arbitrary SolrJerseyResponse subclasses, this utility uses
   * reflection to find and invoke the first (no-arg) constructor for the specified type.
   * SolrJerseyResponse subclasses without a no-arg constructor can be instantiated and registered
   * using {@link #instantiateJerseyResponse(Supplier)}
   *
   * @param clazz the SolrJerseyResponse class to instantiate and register
   * @see CatchAllExceptionMapper
   * @see HttpSolrCall#call()
   */
  @SuppressWarnings("unchecked")
  protected <T extends SolrJerseyResponse> T instantiateJerseyResponse(Class<T> clazz) {
    return instantiateJerseyResponse(
        () -> {
          try {
            return (T) clazz.getConstructors()[0].newInstance();
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        });
  }

  /**
   * Create an instance of the {@link SolrJerseyResponse} subclass; registering it with the Jersey
   * request-context upon creation.
   *
   * <p>This utility method primarily exists to allow Jersey resources to return responses,
   * especially error responses, that match some of the particulars of Solr's traditional/v1 APIs.
   * See the companion method {@link #instantiateJerseyResponse(Class)} for more details.
   *
   * @param instantiator a lambda to create the desired SolrJerseyResponse
   * @see CatchAllExceptionMapper
   * @see HttpSolrCall#call()
   */
  protected <T extends SolrJerseyResponse> T instantiateJerseyResponse(Supplier<T> instantiator) {
    final T instance = instantiator.get();
    if (containerRequestContext != null) {
      containerRequestContext.setProperty(SOLR_JERSEY_RESPONSE, instance);
    }
    return instance;
  }

  protected void ensureRequiredParameterProvided(String parameterName, Object parameterValue) {
    if (parameterValue == null) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST, "Missing required parameter: " + parameterName);
    }
  }

  protected void ensureRequiredRequestBodyProvided(Object requestBody) {
    if (requestBody == null) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST, "Required request-body is missing");
    }
  }
}
