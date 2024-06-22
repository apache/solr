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
package org.apache.solr.client.solrj.util;

import org.apache.solr.SolrTestCase;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.HealthCheckRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.RequestWriter;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.request.V2Request;
import org.junit.Test;

/**
 * @since solr 1.3
 */
public class ClientUtilsTest extends SolrTestCase {

  public void testEscapeQuery() {
    assertEquals("nochange", ClientUtils.escapeQueryChars("nochange"));
    assertEquals("12345", ClientUtils.escapeQueryChars("12345"));
    assertEquals("with\\ space", ClientUtils.escapeQueryChars("with space"));
    assertEquals("h\\:ello\\!", ClientUtils.escapeQueryChars("h:ello!"));
    assertEquals("h\\~\\!", ClientUtils.escapeQueryChars("h~!"));
  }

  @Test
  public void testDeterminesWhenToUseDefaultCollection() {
    final var noDefaultNeededRequest = new CollectionAdminRequest.List();
    final var defaultNeededRequest = new UpdateRequest();

    assertFalse(
        "Expected default-coll to be skipped for collection-agnostic request",
        ClientUtils.shouldApplyDefaultCollection(null, noDefaultNeededRequest));
    assertTrue(
        "Expected default-coll to be used for collection-based request",
        ClientUtils.shouldApplyDefaultCollection(null, defaultNeededRequest));
    assertFalse(
        "Expected default-coll to be skipped when a collection is explicitly provided",
        ClientUtils.shouldApplyDefaultCollection("someCollection", defaultNeededRequest));
  }

  @Test
  public void testUrlBuilding() throws Exception {
    final var rw = new RequestWriter();
    // Simple case, non-collection request
    {
      final var request = new HealthCheckRequest();
      final var url = ClientUtils.buildRequestUrl(request, rw, "http://localhost:8983/solr", null);
      assertEquals("http://localhost:8983/solr/admin/info/health", url);
    }

    // Simple case, collection request
    {
      final var request = new QueryRequest();
      final var url =
          ClientUtils.buildRequestUrl(request, rw, "http://localhost:8983/solr", "someColl");
      assertEquals("http://localhost:8983/solr/someColl/select", url);
    }

    // Uses SolrRequest.getBasePath() to override baseUrl
    {
      final var request = new HealthCheckRequest();
      request.setBasePath("http://alternate-url:7574/solr");
      final var url = ClientUtils.buildRequestUrl(request, rw, "http://localhost:8983/solr", null);
      assertEquals("http://alternate-url:7574/solr/admin/info/health", url);
    }

    // V2 path is correct when solr.v2RealPath sysprop set
    {
      System.setProperty("solr.v2RealPath", "true");
      final var request = new V2Request.Builder("/collections").build();
      final var url = ClientUtils.buildRequestUrl(request, rw, "http://localhost:8983/solr", null);
      assertEquals("http://localhost:8983/solr/____v2/collections", url);
    }

    // V2 path is correct when solr.v2RealPath sysprop NOT set
    {
      System.clearProperty("solr.v2RealPath");
      final var request = new V2Request.Builder("/collections").build();
      final var url = ClientUtils.buildRequestUrl(request, rw, "http://localhost:8983/solr", null);
      assertEquals("http://localhost:8983/api/collections", url);
    }

    // Ignores collection when not needed (i.e. obeys SolrRequest.requiresCollection)
    {
      final var request = new HealthCheckRequest();
      final var url =
          ClientUtils.buildRequestUrl(
              request, rw, "http://localhost:8983/solr", "unneededCollection");
      assertEquals("http://localhost:8983/solr/admin/info/health", url);
    }
  }
}
