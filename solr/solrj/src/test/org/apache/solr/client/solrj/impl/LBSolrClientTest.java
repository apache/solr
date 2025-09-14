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

package org.apache.solr.client.solrj.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.solr.SolrTestCase;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.junit.Test;

public class LBSolrClientTest extends SolrTestCase {

  private static final List<LBSolrClient.Endpoint> SOLR_ENDPOINTS =
      List.of("1", "2", "3", "4").stream()
          .map(url -> new LBSolrClient.Endpoint(url))
          .collect(Collectors.toList());

  @Test
  public void testEndpointCorrectlyBuildsFullUrl() {
    final var baseUrlEndpoint = new LBSolrClient.Endpoint("http://localhost:8983/solr");
    assertEquals("http://localhost:8983/solr", baseUrlEndpoint.getUrl());
    assertEquals("http://localhost:8983/solr", baseUrlEndpoint.getBaseUrl());
    assertNull(
        "Expected core to be null, but was: " + baseUrlEndpoint.getCore(),
        baseUrlEndpoint.getCore());

    final var coreEndpoint = new LBSolrClient.Endpoint("http://localhost:8983/solr", "collection1");
    assertEquals("http://localhost:8983/solr/collection1", coreEndpoint.getUrl());
    assertEquals("http://localhost:8983/solr", coreEndpoint.getBaseUrl());
    assertEquals("collection1", coreEndpoint.getCore());
  }

  @Test
  public void testEndpointNormalizesProvidedBaseUrl() {
    final var normalizedBaseUrl = "http://localhost:8983/solr";
    final var noTrailingSlash = new LBSolrClient.Endpoint(normalizedBaseUrl);
    final var trailingSlash = new LBSolrClient.Endpoint(normalizedBaseUrl + "/");

    assertEquals(normalizedBaseUrl, noTrailingSlash.getBaseUrl());
    assertEquals(normalizedBaseUrl, noTrailingSlash.getUrl());
    assertEquals(normalizedBaseUrl, trailingSlash.getBaseUrl());
    assertEquals(normalizedBaseUrl, trailingSlash.getUrl());
  }

  @Test
  public void testEndpointFactoryParsesUrlsCorrectly() {
    final var parsedFromBaseUrl =
        LBSolrClient.Endpoint.from("http://localhost:8983/solr" + rareTrailingSlash());
    assertEquals("http://localhost:8983/solr", parsedFromBaseUrl.getBaseUrl());
    assertNull(
        "Expected core to be null, but was: " + parsedFromBaseUrl.getCore(),
        parsedFromBaseUrl.getCore());

    final var parsedFromCoreUrl =
        LBSolrClient.Endpoint.from("http://localhost:8983/solr/collection1" + rareTrailingSlash());
    assertEquals("http://localhost:8983/solr", parsedFromCoreUrl.getBaseUrl());
    assertEquals("collection1", parsedFromCoreUrl.getCore());
  }

  @Test
  public void testServerIterator() throws SolrServerException {
    LBSolrClient.Req req = new LBSolrClient.Req(new QueryRequest(), SOLR_ENDPOINTS);
    LBSolrClient.EndpointIterator endpointIterator =
        new LBSolrClient.EndpointIterator(req, new HashMap<>());
    List<LBSolrClient.Endpoint> actualServers = new ArrayList<>();
    while (endpointIterator.hasNext()) {
      actualServers.add(endpointIterator.nextOrError());
    }
    assertEquals(SOLR_ENDPOINTS, actualServers);
    assertFalse(endpointIterator.hasNext());
    LuceneTestCase.expectThrows(SolrServerException.class, endpointIterator::nextOrError);
  }

  @Test
  public void testServerIteratorWithZombieServers() throws SolrServerException {
    HashMap<String, LBSolrClient.EndpointWrapper> zombieServers = new HashMap<>();
    LBSolrClient.Req req = new LBSolrClient.Req(new QueryRequest(), SOLR_ENDPOINTS);
    LBSolrClient.EndpointIterator endpointIterator =
        new LBSolrClient.EndpointIterator(req, zombieServers);
    zombieServers.put("2", new LBSolrClient.EndpointWrapper(new LBSolrClient.Endpoint("2")));

    assertTrue(endpointIterator.hasNext());
    assertEquals(new LBSolrClient.Endpoint("1"), endpointIterator.nextOrError());
    assertTrue(endpointIterator.hasNext());
    assertEquals(new LBSolrClient.Endpoint("3"), endpointIterator.nextOrError());
    assertTrue(endpointIterator.hasNext());
    assertEquals(new LBSolrClient.Endpoint("4"), endpointIterator.nextOrError());

    // Try those on the Zombie list after all other possibilities are exhausted.
    assertTrue(endpointIterator.hasNext());
    assertEquals(new LBSolrClient.Endpoint("2"), endpointIterator.nextOrError());
  }

  @Test
  public void testServerIteratorTimeAllowed() throws SolrServerException, InterruptedException {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set(CommonParams.TIME_ALLOWED, 300);
    LBSolrClient.Req req = new LBSolrClient.Req(new QueryRequest(params), SOLR_ENDPOINTS, 2);
    LBSolrClient.EndpointIterator endpointIterator =
        new LBSolrClient.EndpointIterator(req, new HashMap<>());
    assertTrue(endpointIterator.hasNext());
    endpointIterator.nextOrError();
    Thread.sleep(300);
    LuceneTestCase.expectThrows(SolrServerException.class, endpointIterator::nextOrError);
  }

  @Test
  public void testServerIteratorMaxRetry() throws SolrServerException {
    LBSolrClient.Req req = new LBSolrClient.Req(new QueryRequest(), SOLR_ENDPOINTS, 2);
    LBSolrClient.EndpointIterator endpointIterator =
        new LBSolrClient.EndpointIterator(req, new HashMap<>());
    assertTrue(endpointIterator.hasNext());
    endpointIterator.nextOrError();
    assertTrue(endpointIterator.hasNext());
    endpointIterator.nextOrError();
    LuceneTestCase.expectThrows(SolrServerException.class, endpointIterator::nextOrError);
  }

  private String rareTrailingSlash() {
    if (rarely()) {
      return "/";
    }
    return "";
  }
}
