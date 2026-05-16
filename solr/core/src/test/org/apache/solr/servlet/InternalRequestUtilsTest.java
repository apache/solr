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

package org.apache.solr.servlet;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import jakarta.servlet.http.HttpServletRequest;
import org.apache.solr.SolrTestCase;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.core.RateLimiterConfig;
import org.junit.BeforeClass;
import org.junit.Test;

public class InternalRequestUtilsTest extends SolrTestCase {

  @BeforeClass
  public static void beforeClass() {
    SolrTestCaseJ4.assumeWorkingMockito();
  }

  @Test
  public void detectedViaHeader() {
    HttpServletRequest req = mock(HttpServletRequest.class);
    when(req.getHeader(CommonParams.SOLR_REQUEST_CONTEXT_PARAM))
        .thenReturn(SolrRequest.SolrClientContext.SERVER.toString());
    assertTrue(InternalRequestUtils.isInternalServerRequest(req));
  }

  @Test
  public void detectedViaIsShardParam() {
    HttpServletRequest req = mock(HttpServletRequest.class);
    when(req.getQueryString()).thenReturn("q=*&isShard=true&wt=javabin");
    assertTrue(InternalRequestUtils.isInternalServerRequest(req));
  }

  @Test
  public void detectedViaDistribFromParam() {
    HttpServletRequest req = mock(HttpServletRequest.class);
    when(req.getQueryString()).thenReturn("distrib.from=node1&commit=true");
    assertTrue(InternalRequestUtils.isInternalServerRequest(req));
  }

  @Test
  public void falseForNormalRequest() {
    HttpServletRequest req = mock(HttpServletRequest.class);
    when(req.getQueryString()).thenReturn("q=test&wt=json");
    assertFalse(InternalRequestUtils.isInternalServerRequest(req));
  }

  // Regression: RateLimitManager must consult InternalRequestUtils so that internal traffic
  // identified only by the isShard / distrib.from query params (and lacking the
  // Solr-Request-Context header) is still admitted unconditionally. Previously the manager
  // only checked the header, so a sub-shard request without it could be rate-limited and
  // cause distributed deadlock.
  @Test
  public void rateLimitManager_bypassesInternalShardRequestWithoutHeader() throws Exception {
    RateLimitManager manager = new RateLimitManager();
    manager.registerRequestRateLimiter(
        new AlwaysRejectRateLimiter(), SolrRequest.SolrRequestType.QUERY);

    HttpServletRequest req = mock(HttpServletRequest.class);
    when(req.getHeader(CommonParams.SOLR_REQUEST_TYPE_PARAM))
        .thenReturn(SolrRequest.SolrRequestType.QUERY.toString());
    when(req.getQueryString()).thenReturn("q=*&isShard=true");

    RequestRateLimiter.SlotReservation result = manager.handleRequest(req);
    assertNotNull(
        "Internal sub-shard request must bypass rate limiting even when the limiter would reject",
        result);
    result.close();
  }

  @Test
  public void rateLimitManager_stillRejectsNonInternalQuery() throws Exception {
    RateLimitManager manager = new RateLimitManager();
    manager.registerRequestRateLimiter(
        new AlwaysRejectRateLimiter(), SolrRequest.SolrRequestType.QUERY);

    HttpServletRequest req = mock(HttpServletRequest.class);
    when(req.getHeader(CommonParams.SOLR_REQUEST_TYPE_PARAM))
        .thenReturn(SolrRequest.SolrRequestType.QUERY.toString());
    when(req.getQueryString()).thenReturn("q=test&wt=json");

    RequestRateLimiter.SlotReservation result = manager.handleRequest(req);
    assertNull("Non-internal request must be subject to the registered limiter", result);
  }

  private static final class AlwaysRejectRateLimiter extends RequestRateLimiter {
    AlwaysRejectRateLimiter() {
      super(new RateLimiterConfig(SolrRequest.SolrRequestType.QUERY));
    }

    @Override
    public SlotReservation handleRequest() {
      return null;
    }
  }
}
