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

import org.apache.lucene.tests.util.TestUtil;
import org.apache.solr.SolrTestCase;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.HealthCheckRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.search.QueryParsing;
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

  // FYI also tested via org.apache.solr.common.params.SolrParamTest.testLocalParamRoundTripParsing
  public void testEncodeLocalParamValRoundTrip() throws Exception {
    // Values that require quoting (whitespace, '}', or a leading '$') must round-trip through
    // Solr's own local-params reader, in particular values containing a literal backslash or
    // single quote.
    assertRoundTrips("");
    assertRoundTrips("'leadingQuote");
    assertRoundTrips("\"leadingDoubleQuote");
    assertRoundTrips("plain");
    assertRoundTrips("has space");
    assertRoundTrips("trailing}brace");
    assertRoundTrips("has'quote and space");
    assertRoundTrips("has\\backslash and space");
    assertRoundTrips("both\\'kinds together");
    assertRoundTrips("$dollarPrefixed");
    assertRoundTrips("$dollarPrefixed with space");
    assertRoundTrips("$\\'mix of everything");

    for (int i = 0; i < 100; i++) {
      assertRoundTrips(TestUtil.randomUnicodeString(random()));
    }
  }

  private void assertRoundTrips(String original) throws Exception {
    String encoded = ClientUtils.encodeLocalParamVal(original);
    String txt = "{!key=" + encoded + "}";
    ModifiableSolrParams target = new ModifiableSolrParams();
    QueryParsing.parseLocalParams(txt, 0, target, null);
    assertEquals(
        "encodeLocalParamVal(" + original + ") -> " + encoded + " did not round-trip",
        original,
        target.get("key"));

    // Also confirm the encoded value doesn't swallow whatever follows it.
    String txtFollowedByAnother = "{!key=" + encoded + " next=followed}";
    ModifiableSolrParams targetFollowedByAnother = new ModifiableSolrParams();
    QueryParsing.parseLocalParams(txtFollowedByAnother, 0, targetFollowedByAnother, null);
    assertEquals(
        "encodeLocalParamVal(" + original + ") -> " + encoded + " swallowed the next local param",
        "followed",
        targetFollowedByAnother.get("next"));
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
    // Simple case, non-collection request
    {
      final var request = new HealthCheckRequest();
      final var url = ClientUtils.buildRequestUrl(request, "http://localhost:8983/solr", null);
      assertEquals("http://localhost:8983/solr/admin/info/health", url);
    }

    // Simple case, collection request
    {
      final var request = new QueryRequest();
      final var url =
          ClientUtils.buildRequestUrl(request, "http://localhost:8983/solr", "someColl");
      assertEquals("http://localhost:8983/solr/someColl/select", url);
    }

    // Ignores collection when not needed (i.e. obeys SolrRequest.requiresCollection)
    {
      final var request = new HealthCheckRequest();
      final var url =
          ClientUtils.buildRequestUrl(request, "http://localhost:8983/solr", "unneededCollection");
      assertEquals("http://localhost:8983/solr/admin/info/health", url);
    }
  }
}
