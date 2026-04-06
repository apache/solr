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
package org.apache.solr.handler;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test that demonstrates NoOpRequestHandler can be used to disable implicit handlers like
 * SchemaHandler that are loaded via ImplicitPlugins.json.
 */
public class NoOpRequestHandlerTest extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-noop.xml", "schema.xml");
  }

  @Test
  public void testSchemaHandlerDisabled() {
    // Test that /schema endpoint is disabled and returns 403 FORBIDDEN
    SolrException exception =
        expectThrows(
            SolrException.class,
            () -> {
              try (SolrQueryRequest req = req("qt", "/schema")) {
                SolrQueryResponse rsp = new SolrQueryResponse();
                h.getCore().execute(h.getCore().getRequestHandler("/schema"), req, rsp);
                if (rsp.getException() != null) {
                  throw rsp.getException();
                }
              }
            });

    assertEquals(
        "Should return FORBIDDEN status code",
        SolrException.ErrorCode.FORBIDDEN.code,
        exception.code());
    assertTrue(
        "Error message should indicate endpoint has been disabled",
        exception.getMessage().contains("has been disabled"));
  }

  @Test
  public void testSchemaHandlerSubPathDisabled() {
    // Test that /schema/fields endpoint is also disabled
    SolrException exception =
        expectThrows(
            SolrException.class,
            () -> {
              try (SolrQueryRequest req = req("qt", "/schema/fields")) {
                SolrQueryResponse rsp = new SolrQueryResponse();
                h.getCore().execute(h.getCore().getRequestHandler("/schema"), req, rsp);
                if (rsp.getException() != null) {
                  throw rsp.getException();
                }
              }
            });

    assertEquals(
        "Should return FORBIDDEN status code",
        SolrException.ErrorCode.FORBIDDEN.code,
        exception.code());
  }

  @Test
  public void testNoOpHandlerRegistered() {
    // Verify that the NoOpRequestHandler is actually registered at /schema
    assertNotNull("Schema handler should be registered", h.getCore().getRequestHandler("/schema"));
    assertTrue(
        "Handler at /schema should be NoOpRequestHandler",
        h.getCore().getRequestHandler("/schema") instanceof NoOpRequestHandler);
  }

  @Test
  public void testOtherHandlersStillWork() {
    assertQ("Standard query handler should still work", req("q", "*:*"), "//result[@numFound='0']");
  }
}
