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
import org.apache.http.HttpStatus;
import org.apache.solr.client.api.model.CreateCoreRequestBody;
import org.apache.solr.client.api.model.CreateCoreResponseBody;
import org.apache.solr.common.SolrException;
import org.apache.solr.jersey.JerseySolrTest;
import org.glassfish.jersey.server.ResourceConfig;
import org.junit.Test;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;

public class CreateCoreApiTest extends JerseySolrTest {
    @Override
    public Application configure(){
        ResourceConfig resourceConfig = (ResourceConfig) super.configure();
        resourceConfig.register(CreateCore.class);
        return resourceConfig;
    };
    @Test
    public void test_createCore_validResponse(){
        CreateCoreRequestBody createCoreRequestBody = new CreateCoreRequestBody();
        createCoreRequestBody.name = coreName;
        Response response = target("/cores")
                .request().post(Entity.entity(createCoreRequestBody, MediaType.APPLICATION_JSON_TYPE));
        var createCoreResponseBody = response.readEntity(CreateCoreResponseBody.class);
        assertEquals(HttpStatus.SC_OK, response.getStatus());
        assertEquals(createCoreRequestBody.name, createCoreResponseBody.core);
    }

    @Test
    public void testReportsError_createCore_missingRequiredParams(){
        CreateCoreRequestBody createCoreRequestBody = new CreateCoreRequestBody();
        Response response = target("/cores")
                .request().post(Entity.entity(createCoreRequestBody, MediaType.APPLICATION_JSON_TYPE));
        var createCoreResponseBody = response.readEntity(CreateCoreResponseBody.class);
        var errorMessage = response.readEntity(String.class);
        assertEquals(SolrException.ErrorCode.BAD_REQUEST, response.getStatus());
        assertTrue(errorMessage.contains("Missing Required Parameter"));
    }
    @Test
    public void testReportsError_createCore_NameAlreadyTaken(){
        when(coreContainer.create(coreName, any(), any(), any())).thenThrow(new SolrException(
                SolrException.ErrorCode.SERVER_ERROR, "Core with name '" + coreName + "' already exists."
        ));
        var createCoreRequestBody = new CreateCoreRequestBody();
        createCoreRequestBody.name = coreName;
        Response response = target("/cores")
                .request().post(Entity.entity(createCoreRequestBody, MediaType.APPLICATION_JSON_TYPE));
        var createCoreResponseBody = response.readEntity(CreateCoreResponseBody.class);
        var errorMessage = response.readEntity(String.class);
        assertEquals(SolrException.ErrorCode.SERVER_ERROR, response.getStatus());
        assertTrue(errorMessage.contains("already exists."));
    }

}
