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
import static org.apache.solr.jersey.container.ContainerRequestUtils.DEFAULT_SECURITY_CONTEXT;

import java.net.URI;
import javax.ws.rs.container.ContainerRequestContext;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.api.model.SchemaNameResponse;
import org.glassfish.jersey.internal.MapPropertiesDelegate;
import org.glassfish.jersey.server.ContainerRequest;
import org.junit.Test;

/** Unit tests for the {@link JerseyResource} parent class */
public class JerseyResourceTest extends SolrTestCaseJ4 {

  @Test
  public void testSetsResponseInContextUponCreation() {
    final ContainerRequestContext requestContext = createContext();
    final JerseyResource resource = new JerseyResource();
    resource.containerRequestContext = requestContext;
    assertTrue(requestContext.getPropertyNames().isEmpty());

    final SchemaNameResponse returned =
        resource.instantiateJerseyResponse(SchemaNameResponse.class);

    assertTrue(requestContext.getPropertyNames().contains(SOLR_JERSEY_RESPONSE));
    final SchemaNameResponse stashed =
        (SchemaNameResponse) requestContext.getProperty(SOLR_JERSEY_RESPONSE);
    assertEquals(stashed, returned);
  }

  private ContainerRequestContext createContext() {
    final URI baseUri = URI.create("http://localhost:8983/api/");
    return new ContainerRequest(
        baseUri, baseUri, "GET", DEFAULT_SECURITY_CONTEXT, new MapPropertiesDelegate());
  }
}
