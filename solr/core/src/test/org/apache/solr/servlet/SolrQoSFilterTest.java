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

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import jakarta.servlet.AsyncContext;
import jakarta.servlet.AsyncListener;
import jakarta.servlet.FilterChain;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.solr.SolrTestCase;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrRequest.SolrRequestType;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.util.circuitbreaker.CircuitBreaker;
import org.apache.solr.util.circuitbreaker.CircuitBreakerRegistry;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

public class SolrQoSFilterTest extends SolrTestCase {

  @BeforeClass
  public static void beforeClass() {
    SolrTestCaseJ4.assumeWorkingMockito();
  }

  @After
  public void cleanup() {
    System.clearProperty(SolrQoSFilter.SYSPROP_QOS_ENABLED);
    System.clearProperty(SolrQoSFilter.SYSPROP_MAX_SUSPENDED);
    System.clearProperty(SolrQoSFilter.SYSPROP_SUSPEND_TIMEOUT_MS);
    System.clearProperty(SolrQoSFilter.SYSPROP_CHECK_INTERVAL_MS);
    System.clearProperty(SolrQoSFilter.SYSPROP_EVAL_INTERVAL_MS);
    System.clearProperty(SolrQoSFilter.SYSPROP_DRAIN_BUDGET);
    System.clearProperty(SolrQoSFilter.SYSPROP_HIGH_SHARE);
    System.clearProperty(SolrQoSFilter.SYSPROP_MEDIUM_SHARE);
    System.clearProperty(SolrQoSFilter.SYSPROP_HIGH_DRAIN_MULTIPLIER);
    System.clearProperty(SolrQoSFilter.SYSPROP_LOW_DRAIN_MULTIPLIER);
    CircuitBreakerRegistry.deregisterGlobal();
  }

  // -------- inferRequestType --------

  @Test
  public void inferRequestType_usesHeaderWhenPresent() {
    HttpServletRequest req = mock(HttpServletRequest.class);
    when(req.getHeader(CommonParams.SOLR_REQUEST_TYPE_PARAM)).thenReturn("UPDATE");
    assertEquals(SolrRequestType.UPDATE, SolrQoSFilter.inferRequestType(req));
  }

  @Test
  public void inferRequestType_unknownHeaderFallsBackToPath() {
    HttpServletRequest req = mock(HttpServletRequest.class);
    when(req.getHeader(CommonParams.SOLR_REQUEST_TYPE_PARAM)).thenReturn("BOGUS");
    when(req.getRequestURI()).thenReturn("/solr/c1/update");
    assertEquals(SolrRequestType.UPDATE, SolrQoSFilter.inferRequestType(req));
  }

  @Test
  public void inferRequestType_defaultsToQuery() {
    HttpServletRequest req = mock(HttpServletRequest.class);
    when(req.getRequestURI()).thenReturn("/solr/c1/select");
    assertEquals(SolrRequestType.QUERY, SolrQoSFilter.inferRequestType(req));
  }

  @Test
  public void inferRequestType_securityOrAdminTreatedAsQuery() {
    HttpServletRequest req = mock(HttpServletRequest.class);
    when(req.getHeader(CommonParams.SOLR_REQUEST_TYPE_PARAM)).thenReturn("ADMIN");
    assertEquals(SolrRequestType.QUERY, SolrQoSFilter.inferRequestType(req));
  }

  // -------- inferPriority --------

  @Test
  public void inferPriority_pingIsHigh() {
    HttpServletRequest req = mock(HttpServletRequest.class);
    when(req.getRequestURI()).thenReturn("/solr/c1/admin/ping");
    assertEquals(
        SolrQoSFilter.PRIORITY_HIGH, SolrQoSFilter.inferPriority(req, SolrRequestType.QUERY));
  }

  @Test
  public void inferPriority_userQueryIsMedium() {
    HttpServletRequest req = mock(HttpServletRequest.class);
    when(req.getRequestURI()).thenReturn("/solr/c1/select");
    assertEquals(
        SolrQoSFilter.PRIORITY_MEDIUM, SolrQoSFilter.inferPriority(req, SolrRequestType.QUERY));
  }

  @Test
  public void inferPriority_updateIsLow() {
    HttpServletRequest req = mock(HttpServletRequest.class);
    when(req.getRequestURI()).thenReturn("/solr/c1/update");
    assertEquals(
        SolrQoSFilter.PRIORITY_LOW, SolrQoSFilter.inferPriority(req, SolrRequestType.UPDATE));
  }

  // -------- end-to-end suspend/drain --------

  @Test
  public void disabled_isPassThrough() throws Exception {
    SolrQoSFilter filter = newFilter(false);
    try {
      HttpServletRequest req = mock(HttpServletRequest.class);
      HttpServletResponse res = mock(HttpServletResponse.class);
      FilterChain chain = mock(FilterChain.class);
      filter.doFilter(req, res, chain);
      verify(chain, times(1)).doFilter(req, res);
      verify(req, never()).startAsync();
      assertEquals(0, filter.getSuspendedCount());
    } finally {
      filter.destroy();
    }
  }

  @Test
  public void disabled_failsFastWhenBreakerTripped() throws Exception {
    // Regression: when QoS queueing is disabled (the default), a tripped breaker must still
    // reject the request synchronously — preserving the pre-filter SearchHandler /
    // ContentStreamHandlerBase behavior. Without this, upgrading users would silently lose
    // circuit-breaker enforcement.
    ToggleCircuitBreaker breaker = new ToggleCircuitBreaker();
    breaker.setRequestTypes(java.util.List.of("QUERY"));
    CircuitBreakerRegistry.registerGlobal(breaker);
    breaker.tripped = true;

    SolrQoSFilter filter = newFilter(false);
    try {
      HttpServletRequest req = mock(HttpServletRequest.class);
      when(req.getRequestURI()).thenReturn("/solr/c1/select");
      HttpServletResponse res = mock(HttpServletResponse.class);
      FilterChain chain = mock(FilterChain.class);

      filter.doFilter(req, res, chain);

      verify(chain, never()).doFilter(req, res);
      verify(req, never()).startAsync();
      verify(res).sendError(eq(CircuitBreaker.getExceptionErrorCode().code), anyString());
      assertEquals(0, filter.getSuspendedCount());
    } finally {
      filter.destroy();
    }
  }

  @Test
  public void disabled_internalShardRequestBypassesFailFast() throws Exception {
    // Internal sub-shard requests must always pass through, even when a breaker is tripped and
    // QoS is disabled — refusing them would risk distributed deadlock.
    ToggleCircuitBreaker breaker = new ToggleCircuitBreaker();
    breaker.setRequestTypes(java.util.List.of("QUERY"));
    CircuitBreakerRegistry.registerGlobal(breaker);
    breaker.tripped = true;

    SolrQoSFilter filter = newFilter(false);
    try {
      HttpServletRequest req = mock(HttpServletRequest.class);
      when(req.getRequestURI()).thenReturn("/solr/c1/select");
      when(req.getQueryString()).thenReturn("q=*&isShard=true");
      HttpServletResponse res = mock(HttpServletResponse.class);
      FilterChain chain = mock(FilterChain.class);

      filter.doFilter(req, res, chain);

      verify(chain, times(1)).doFilter(req, res);
      verify(res, never()).sendError(anyInt(), anyString());
    } finally {
      filter.destroy();
    }
  }

  @Test
  public void noBreakerTripped_passesThrough() throws Exception {
    SolrQoSFilter filter = newFilter(true);
    try {
      HttpServletRequest req = mock(HttpServletRequest.class);
      when(req.getRequestURI()).thenReturn("/solr/c1/select");
      HttpServletResponse res = mock(HttpServletResponse.class);
      FilterChain chain = mock(FilterChain.class);

      filter.doFilter(req, res, chain);

      verify(chain, times(1)).doFilter(req, res);
      verify(req, never()).startAsync();
      assertEquals(0, filter.getSuspendedCount());
      assertEquals(0L, filter.getTotalSuspendedCount());
    } finally {
      filter.destroy();
    }
  }

  @Test
  public void breakerTripped_suspendsRequest() throws Exception {
    ToggleCircuitBreaker breaker = new ToggleCircuitBreaker();
    breaker.setRequestTypes(java.util.List.of("QUERY"));
    CircuitBreakerRegistry.registerGlobal(breaker);
    breaker.tripped = true;

    SolrQoSFilter filter = newFilter(true);
    try {
      HttpServletRequest req = mock(HttpServletRequest.class);
      when(req.getRequestURI()).thenReturn("/solr/c1/select");
      HttpServletResponse res = mock(HttpServletResponse.class);
      FilterChain chain = mock(FilterChain.class);

      AsyncContext ac = mock(AsyncContext.class);
      when(req.startAsync()).thenReturn(ac);
      when(ac.getRequest()).thenReturn(req);

      filter.doFilter(req, res, chain);

      // Request was suspended, NOT dispatched downstream.
      verify(chain, never()).doFilter(req, res);
      verify(req, times(1)).startAsync();
      verify(ac, times(1)).setTimeout(anyLong());
      verify(ac, atLeastOnce()).addListener(org.mockito.ArgumentMatchers.<AsyncListener>any());
      assertEquals(1, filter.getSuspendedCount());
      assertEquals(1L, filter.getTotalSuspendedCount());
    } finally {
      filter.destroy();
    }
  }

  @Test
  public void drainDispatchesWhenBreakerClears() throws Exception {
    ToggleCircuitBreaker breaker = new ToggleCircuitBreaker();
    breaker.setRequestTypes(java.util.List.of("QUERY"));
    CircuitBreakerRegistry.registerGlobal(breaker);
    breaker.tripped = true;

    SolrQoSFilter filter = newFilter(true);
    try {
      HttpServletRequest req = mock(HttpServletRequest.class);
      when(req.getRequestURI()).thenReturn("/solr/c1/select");
      HttpServletResponse res = mock(HttpServletResponse.class);
      FilterChain chain = mock(FilterChain.class);

      AtomicReference<Object> resumedAttr = new AtomicReference<>();
      Map<String, Object> attrs = new HashMap<>();
      org.mockito.Mockito.doAnswer(
              inv -> {
                attrs.put(inv.getArgument(0), inv.getArgument(1));
                return null;
              })
          .when(req)
          .setAttribute(anyString(), org.mockito.ArgumentMatchers.any());
      when(req.getAttribute(anyString())).thenAnswer(inv -> attrs.get(inv.getArgument(0)));

      AsyncContext ac = mock(AsyncContext.class);
      when(req.startAsync()).thenReturn(ac);
      when(ac.getRequest()).thenReturn(req);
      org.mockito.Mockito.doAnswer(
              inv -> {
                resumedAttr.set(attrs.get(SolrQoSFilter.class.getName() + ".resumed"));
                return null;
              })
          .when(ac)
          .dispatch();

      filter.doFilter(req, res, chain);
      assertEquals(1, filter.getSuspendedCount());

      // Clear the breaker and drain.
      breaker.tripped = false;
      filter.drainNow();

      verify(ac, times(1)).dispatch();
      assertEquals(0, filter.getSuspendedCount());
      assertEquals(1L, filter.getTotalResumedCount());
      assertEquals(Boolean.TRUE, resumedAttr.get());
    } finally {
      filter.destroy();
    }
  }

  @Test
  public void evaluationCacheAmortizesBreakerChecks() throws Exception {
    CountingTrippedBreaker breaker = new CountingTrippedBreaker();
    breaker.setRequestTypes(java.util.List.of("QUERY"));
    CircuitBreakerRegistry.registerGlobal(breaker);

    // Long eval interval — many successive calls should share one underlying isTripped() call.
    System.setProperty(SolrQoSFilter.SYSPROP_QOS_ENABLED, "true");
    System.setProperty(SolrQoSFilter.SYSPROP_CHECK_INTERVAL_MS, "60000");
    System.setProperty(SolrQoSFilter.SYSPROP_EVAL_INTERVAL_MS, "60000");
    SolrQoSFilter filter = new SolrQoSFilter();
    filter.initForTest(null);
    try {
      HttpServletResponse res = mock(HttpServletResponse.class);
      FilterChain chain = mock(FilterChain.class);
      for (int i = 0; i < 50; i++) {
        HttpServletRequest req = mock(HttpServletRequest.class);
        when(req.getRequestURI()).thenReturn("/solr/c1/select");
        filter.doFilter(req, res, chain);
      }
      assertEquals(
          "isTripped() should be invoked once per evaluation interval, not per request",
          1,
          breaker.calls.get());
      // All 50 requests passed through (breaker reports not-tripped).
      verify(chain, times(50)).doFilter(org.mockito.ArgumentMatchers.any(), eq(res));
    } finally {
      filter.destroy();
    }
  }

  @Test
  public void rejectsWhenSuspendedQueueFull() throws Exception {
    ToggleCircuitBreaker breaker = new ToggleCircuitBreaker();
    breaker.setRequestTypes(java.util.List.of("QUERY"));
    CircuitBreakerRegistry.registerGlobal(breaker);
    breaker.tripped = true;

    // maxSuspended=2 partitions to HIGH=1, MEDIUM=1, LOW=0 under default shares — enough for one
    // MEDIUM-lane request before the next is rejected by the per-priority cap.
    System.setProperty(SolrQoSFilter.SYSPROP_MAX_SUSPENDED, "2");
    SolrQoSFilter filter = newFilter(true);
    try {
      // Fill the single MEDIUM slot.
      HttpServletRequest req1 = mock(HttpServletRequest.class);
      when(req1.getRequestURI()).thenReturn("/solr/c1/select");
      AsyncContext ac1 = mock(AsyncContext.class);
      when(req1.startAsync()).thenReturn(ac1);
      when(ac1.getRequest()).thenReturn(req1);
      filter.doFilter(req1, mock(HttpServletResponse.class), mock(FilterChain.class));
      assertEquals(1, filter.getSuspendedCount());

      // Second MEDIUM request should be rejected because the MEDIUM lane cap is hit.
      HttpServletRequest req2 = mock(HttpServletRequest.class);
      when(req2.getRequestURI()).thenReturn("/solr/c1/select");
      HttpServletResponse res2 = mock(HttpServletResponse.class);
      FilterChain chain2 = mock(FilterChain.class);
      filter.doFilter(req2, res2, chain2);

      verify(req2, never()).startAsync();
      verify(chain2, never()).doFilter(req2, res2);
      verify(res2).sendError(anyInt(), anyString());
      assertEquals(1L, filter.getTotalRejectedCount());
    } finally {
      filter.destroy();
    }
  }

  // -------- priority header / per-priority caps / drain budget --------

  @Test
  public void inferPriority_headerOverridesPathHeuristic() {
    HttpServletRequest req = mock(HttpServletRequest.class);
    when(req.getHeader(SolrQoSFilter.HEADER_REQUEST_PRIORITY)).thenReturn("HIGH");
    when(req.getRequestURI()).thenReturn("/solr/c1/update");
    // Without the header, /update + UPDATE type would be PRIORITY_LOW. The header wins.
    assertEquals(
        SolrQoSFilter.PRIORITY_HIGH, SolrQoSFilter.inferPriority(req, SolrRequestType.UPDATE));
  }

  @Test
  public void inferPriority_headerCaseInsensitive() {
    HttpServletRequest req = mock(HttpServletRequest.class);
    when(req.getHeader(SolrQoSFilter.HEADER_REQUEST_PRIORITY)).thenReturn("low");
    when(req.getRequestURI()).thenReturn("/solr/c1/select");
    assertEquals(
        SolrQoSFilter.PRIORITY_LOW, SolrQoSFilter.inferPriority(req, SolrRequestType.QUERY));
  }

  @Test
  public void inferPriority_unknownHeaderFallsBackToHeuristic() {
    HttpServletRequest req = mock(HttpServletRequest.class);
    when(req.getHeader(SolrQoSFilter.HEADER_REQUEST_PRIORITY)).thenReturn("BOGUS");
    when(req.getRequestURI()).thenReturn("/solr/c1/admin/ping");
    // Unknown value — fall through to the path heuristic, which classifies admin/ping as HIGH.
    assertEquals(
        SolrQoSFilter.PRIORITY_HIGH, SolrQoSFilter.inferPriority(req, SolrRequestType.QUERY));
  }

  @Test
  public void priorityCaps_partitionMaxSuspendedAcrossLanes() {
    System.setProperty(SolrQoSFilter.SYSPROP_MAX_SUSPENDED, "1000");
    SolrQoSFilter filter = newFilter(true);
    try {
      int high = filter.getPriorityCap(SolrQoSFilter.PRIORITY_HIGH);
      int medium = filter.getPriorityCap(SolrQoSFilter.PRIORITY_MEDIUM);
      int low = filter.getPriorityCap(SolrQoSFilter.PRIORITY_LOW);
      assertEquals(
          "lanes must sum to maxSuspendedRequests so the global cap is implicit",
          1000,
          high + medium + low);
      assertTrue("HIGH lane must reserve some headroom for probes", high >= 1);
      assertTrue("MEDIUM lane should be the largest under default shares", medium > high);
      assertTrue("MEDIUM lane should be the largest under default shares", medium > low);
    } finally {
      filter.destroy();
    }
  }

  @Test
  public void lowFloodDoesNotRejectHigh() throws Exception {
    // Regression: under the old single-counter cap, a flood of LOW-priority updates could fill
    // all 1024 slots and reject incoming admin/ping probes. Per-priority caps must prevent that.
    ToggleCircuitBreaker queryBreaker = new ToggleCircuitBreaker();
    queryBreaker.setRequestTypes(java.util.List.of("QUERY"));
    CircuitBreakerRegistry.registerGlobal(queryBreaker);
    ToggleCircuitBreaker updateBreaker = new ToggleCircuitBreaker();
    updateBreaker.setRequestTypes(java.util.List.of("UPDATE"));
    CircuitBreakerRegistry.registerGlobal(updateBreaker);
    queryBreaker.tripped = true;
    updateBreaker.tripped = true;

    // Small total so we can saturate the LOW lane without spinning thousands of mocks.
    System.setProperty(SolrQoSFilter.SYSPROP_MAX_SUSPENDED, "20");
    // Force a deterministic split: HIGH=2, MEDIUM=10, LOW=8 (rounded from share math).
    System.setProperty(SolrQoSFilter.SYSPROP_HIGH_SHARE, "0.10");
    System.setProperty(SolrQoSFilter.SYSPROP_MEDIUM_SHARE, "0.50");
    SolrQoSFilter filter = newFilter(true);
    try {
      int lowCap = filter.getPriorityCap(SolrQoSFilter.PRIORITY_LOW);
      int highCap = filter.getPriorityCap(SolrQoSFilter.PRIORITY_HIGH);
      assertTrue("test setup must yield non-zero LOW cap", lowCap > 0);
      assertTrue("test setup must yield non-zero HIGH cap", highCap > 0);

      // Saturate LOW with updates.
      for (int i = 0; i < lowCap; i++) {
        HttpServletRequest req = mock(HttpServletRequest.class);
        when(req.getRequestURI()).thenReturn("/solr/c1/update");
        AsyncContext ac = mock(AsyncContext.class);
        when(req.startAsync()).thenReturn(ac);
        when(ac.getRequest()).thenReturn(req);
        filter.doFilter(req, mock(HttpServletResponse.class), mock(FilterChain.class));
      }
      assertEquals(lowCap, filter.getSuspendedCountForPriority(SolrQoSFilter.PRIORITY_LOW));
      assertEquals(0L, filter.getTotalRejectedCount());

      // Now an admin/ping request (HIGH) must still be admitted, not rejected — even though LOW
      // is at its cap.
      HttpServletRequest highReq = mock(HttpServletRequest.class);
      when(highReq.getRequestURI()).thenReturn("/solr/c1/admin/ping");
      AsyncContext highAc = mock(AsyncContext.class);
      when(highReq.startAsync()).thenReturn(highAc);
      when(highAc.getRequest()).thenReturn(highReq);
      filter.doFilter(highReq, mock(HttpServletResponse.class), mock(FilterChain.class));

      assertEquals(
          "HIGH lane must accept the probe even when LOW is saturated",
          1,
          filter.getSuspendedCountForPriority(SolrQoSFilter.PRIORITY_HIGH));
      assertEquals("HIGH probe must not be rejected", 0L, filter.getTotalRejectedCount());
    } finally {
      filter.destroy();
    }
  }

  @Test
  public void priorityCapEnforcedPerLane() throws Exception {
    ToggleCircuitBreaker breaker = new ToggleCircuitBreaker();
    breaker.setRequestTypes(java.util.List.of("UPDATE"));
    CircuitBreakerRegistry.registerGlobal(breaker);
    breaker.tripped = true;

    System.setProperty(SolrQoSFilter.SYSPROP_MAX_SUSPENDED, "20");
    System.setProperty(SolrQoSFilter.SYSPROP_HIGH_SHARE, "0.10");
    System.setProperty(SolrQoSFilter.SYSPROP_MEDIUM_SHARE, "0.50");
    SolrQoSFilter filter = newFilter(true);
    try {
      int lowCap = filter.getPriorityCap(SolrQoSFilter.PRIORITY_LOW);
      assertTrue(lowCap > 0);
      // Saturate LOW.
      for (int i = 0; i < lowCap; i++) {
        HttpServletRequest req = mock(HttpServletRequest.class);
        when(req.getRequestURI()).thenReturn("/solr/c1/update");
        AsyncContext ac = mock(AsyncContext.class);
        when(req.startAsync()).thenReturn(ac);
        when(ac.getRequest()).thenReturn(req);
        filter.doFilter(req, mock(HttpServletResponse.class), mock(FilterChain.class));
      }
      // Next LOW request must be rejected — its lane is full.
      HttpServletRequest extra = mock(HttpServletRequest.class);
      when(extra.getRequestURI()).thenReturn("/solr/c1/update");
      HttpServletResponse extraRes = mock(HttpServletResponse.class);
      FilterChain extraChain = mock(FilterChain.class);
      filter.doFilter(extra, extraRes, extraChain);

      verify(extra, never()).startAsync();
      verify(extraChain, never()).doFilter(extra, extraRes);
      verify(extraRes).sendError(anyInt(), anyString());
      assertEquals(1L, filter.getTotalRejectedCount());
    } finally {
      filter.destroy();
    }
  }

  @Test
  public void drainBudgetScalesByPriority() {
    System.setProperty(SolrQoSFilter.SYSPROP_DRAIN_BUDGET, "100");
    System.setProperty(SolrQoSFilter.SYSPROP_HIGH_DRAIN_MULTIPLIER, "5.0");
    System.setProperty(SolrQoSFilter.SYSPROP_LOW_DRAIN_MULTIPLIER, "0.25");
    SolrQoSFilter filter = newFilter(true);
    try {
      assertEquals(500, filter.getPriorityDrainBudget(SolrQoSFilter.PRIORITY_HIGH));
      assertEquals(100, filter.getPriorityDrainBudget(SolrQoSFilter.PRIORITY_MEDIUM));
      assertEquals(25, filter.getPriorityDrainBudget(SolrQoSFilter.PRIORITY_LOW));
    } finally {
      filter.destroy();
    }
  }

  @Test
  public void drainBudget_autoScalesForLargeMaxSuspended() {
    // Without an explicit drainBudget override, very large maxSuspendedRequests must auto-scale
    // the budget so a saturated queue can drain before requests time out.
    System.setProperty(SolrQoSFilter.SYSPROP_QOS_ENABLED, "true");
    System.setProperty(SolrQoSFilter.SYSPROP_MAX_SUSPENDED, "100000");
    System.setProperty(SolrQoSFilter.SYSPROP_SUSPEND_TIMEOUT_MS, "5000");
    System.setProperty(SolrQoSFilter.SYSPROP_CHECK_INTERVAL_MS, "100");
    System.setProperty(SolrQoSFilter.SYSPROP_EVAL_INTERVAL_MS, "0");
    SolrQoSFilter filter = new SolrQoSFilter();
    filter.initForTest(null);
    try {
      // Formula: 2 × maxSuspended / (timeout / interval) = 2 × 100000 / 50 = 4000.
      assertEquals(4000, filter.getPriorityDrainBudget(SolrQoSFilter.PRIORITY_MEDIUM));
    } finally {
      filter.destroy();
    }
  }

  @Test
  public void drainBudget_floorsAtDefaultForSmallConfigs() {
    // For default-sized deployments the auto formula yields a small number; floor at the static
    // default so behavior is unchanged for users who didn't tune.
    System.setProperty(SolrQoSFilter.SYSPROP_QOS_ENABLED, "true");
    System.setProperty(SolrQoSFilter.SYSPROP_MAX_SUSPENDED, "1024");
    System.setProperty(SolrQoSFilter.SYSPROP_SUSPEND_TIMEOUT_MS, "5000");
    System.setProperty(SolrQoSFilter.SYSPROP_CHECK_INTERVAL_MS, "100");
    System.setProperty(SolrQoSFilter.SYSPROP_EVAL_INTERVAL_MS, "0");
    SolrQoSFilter filter = new SolrQoSFilter();
    filter.initForTest(null);
    try {
      assertEquals(
          SolrQoSFilter.DEFAULT_DRAIN_BUDGET,
          filter.getPriorityDrainBudget(SolrQoSFilter.PRIORITY_MEDIUM));
    } finally {
      filter.destroy();
    }
  }

  @Test
  public void drainBudget_explicitOverrideWinsOverAutoScale() {
    // Even with maxSuspended that would auto-scale to a higher number, an explicit user value
    // must be honored exactly.
    System.setProperty(SolrQoSFilter.SYSPROP_MAX_SUSPENDED, "100000");
    System.setProperty(SolrQoSFilter.SYSPROP_DRAIN_BUDGET, "50");
    SolrQoSFilter filter = newFilter(true);
    try {
      assertEquals(50, filter.getPriorityDrainBudget(SolrQoSFilter.PRIORITY_MEDIUM));
    } finally {
      filter.destroy();
    }
  }

  @Test
  public void qosEnabledByDefault() {
    // The default flipped to enabled=true once per-priority caps closed the priority-inversion
    // bug. New deployments get async suspend/queueing without having to opt in.
    System.clearProperty(SolrQoSFilter.SYSPROP_QOS_ENABLED);
    SolrQoSFilter filter = new SolrQoSFilter();
    try {
      filter.initForTest(null);
      // With QoS enabled, a tripped breaker on a normal /select request should result in async
      // suspension (startAsync called), not a synchronous fail-fast.
      ToggleCircuitBreaker breaker = new ToggleCircuitBreaker();
      breaker.setRequestTypes(java.util.List.of("QUERY"));
      CircuitBreakerRegistry.registerGlobal(breaker);
      breaker.tripped = true;

      HttpServletRequest req = mock(HttpServletRequest.class);
      when(req.getRequestURI()).thenReturn("/solr/c1/select");
      AsyncContext ac = mock(AsyncContext.class);
      when(req.startAsync()).thenReturn(ac);
      when(ac.getRequest()).thenReturn(req);
      filter.doFilter(req, mock(HttpServletResponse.class), mock(FilterChain.class));

      verify(req, times(1)).startAsync();
      assertEquals(1, filter.getSuspendedCount());
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      filter.destroy();
    }
  }

  // -------- helpers --------

  private static SolrQoSFilter newFilter(boolean enabled) {
    if (enabled) {
      System.setProperty(SolrQoSFilter.SYSPROP_QOS_ENABLED, "true");
    } else {
      System.setProperty(SolrQoSFilter.SYSPROP_QOS_ENABLED, "false");
    }
    // Long check interval so the background scheduler doesn't fire during tests
    // that drive drain manually via drainNow().
    System.setProperty(SolrQoSFilter.SYSPROP_CHECK_INTERVAL_MS, "60000");
    // Disable evaluation cache so suspend/drain transitions in tests aren't masked by a
    // stale cached scan.
    System.setProperty(SolrQoSFilter.SYSPROP_EVAL_INTERVAL_MS, "0");
    SolrQoSFilter f = new SolrQoSFilter();
    f.initForTest(null);
    return f;
  }

  private static final class ToggleCircuitBreaker extends CircuitBreaker {
    volatile boolean tripped = false;

    @Override
    public boolean isTripped() {
      return tripped;
    }

    @Override
    public String getErrorMessage() {
      return "test breaker tripped";
    }
  }

  private static final class CountingTrippedBreaker extends CircuitBreaker {
    final java.util.concurrent.atomic.AtomicInteger calls =
        new java.util.concurrent.atomic.AtomicInteger();

    @Override
    public boolean isTripped() {
      calls.incrementAndGet();
      return false;
    }

    @Override
    public String getErrorMessage() {
      return "counting breaker";
    }
  }
}
