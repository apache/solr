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

import java.util.HashMap;
import java.util.Map;

import javax.servlet.AsyncContext;
import javax.servlet.AsyncEvent;
import javax.servlet.AsyncListener;
import javax.servlet.ServletRequest;

import org.apache.solr.SolrTestCase;
import org.apache.solr.common.util.BufferMetrics;
import org.apache.solr.common.util.ExpandableBuffers;
import org.apache.solr.common.util.PooledBufferHandle;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for the {@link PooledBufferHandle}-based {@code "responseBuffer"} lifecycle in
 * {@link SolrDispatchFilter}.
 *
 * <p>Tests cover every terminal path identified in the agrona-buffer-hardening plan (Worker D /
 * LANE-QRESP) and assert:
 * <ul>
 *   <li>Exactly-one release per request (invariant #1).</li>
 *   <li>{@link BufferMetrics#getDoubleReleaseDetected()} == 0 on the double-fire path.</li>
 *   <li>Pool active-handle count returns to baseline after each path.</li>
 * </ul>
 *
 * <p>These tests exercise only the dispatch-filter's buffer lifecycle logic via the static
 * {@code releaseResponseBufferHandle} helper and the {@link SolrDispatchFilter.SolrAsyncListener}
 * class — no full Solr container is required.
 */
public class ResponseBufferLifecycleTest extends SolrTestCase {

  private BufferMetrics metrics;
  private long doubleReleaseBefore;
  private long activeHandleCountBefore;

  @Before
  public void setUp() throws Exception {
    super.setUp();
    // Disable poison so double-close does NOT throw — the test asserts the counter only.
    // Individual tests that want the throw behaviour enable poison themselves.
    PooledBufferHandle.setPoisonForTest(false);
    metrics = BufferMetrics.getInstance();
    doubleReleaseBefore = metrics.getDoubleReleaseDetected();
    activeHandleCountBefore = metrics.getActiveHandleCount();
  }

  @After
  public void tearDown() throws Exception {
    PooledBufferHandle.setPoisonForTest(false);
    super.tearDown();
  }

  // -----------------------------------------------------------------------
  // Helpers
  // -----------------------------------------------------------------------

  /** Create a mock ServletRequest that carries a live PooledBufferHandle attribute. */
  private MockServletRequest requestWithHandle() {
    PooledBufferHandle handle = PooledBufferHandle.acquire(
        ExpandableBuffers.getInstance(), -1, true, "test-responseBuffer");
    MockServletRequest req = new MockServletRequest();
    req.setAttribute("responseBuffer", handle);
    return req;
  }

  /** Retrieve the handle stored in a mock request (may be null after release). */
  private static PooledBufferHandle handleFrom(MockServletRequest req) {
    return (PooledBufferHandle) req.getAttribute("responseBuffer");
  }

  /** Build a mock AsyncEvent whose getSuppliedRequest() returns the given request. */
  private static AsyncEvent asyncEvent(MockServletRequest request) {
    AsyncContext asyncContext = mock(AsyncContext.class);
    AsyncEvent event = mock(AsyncEvent.class);
    when(event.getSuppliedRequest()).thenReturn(request);
    when(event.getAsyncContext()).thenReturn(asyncContext);
    return event;
  }

  /** Assert invariant: active handle count returned to the pre-test baseline. */
  private void assertPoolBaseline() {
    assertEquals("active-handle count must return to pre-test baseline",
        activeHandleCountBefore, metrics.getActiveHandleCount());
  }

  /** Assert no new double-release events since the test started. */
  private void assertNoNewDoubleRelease() {
    assertEquals("doubleReleaseDetected must not increase",
        doubleReleaseBefore, metrics.getDoubleReleaseDetected());
  }

  // -----------------------------------------------------------------------
  // 1. releaseResponseBufferHandle — no attribute (null-safe)
  // -----------------------------------------------------------------------

  @Test
  public void testReleaseWithNoAttribute_isNoOp() {
    // Request that never called writeQueryResponse — no "responseBuffer" attribute.
    MockServletRequest req = new MockServletRequest();
    SolrDispatchFilter.releaseResponseBufferHandle(req);
    assertNoNewDoubleRelease();
    assertPoolBaseline();
  }

  @Test
  public void testReleaseWithNullRequest_isNoOp() {
    SolrDispatchFilter.releaseResponseBufferHandle(null);
    assertNoNewDoubleRelease();
    assertPoolBaseline();
  }

  // -----------------------------------------------------------------------
  // 2. Synchronous / non-async dispatch path
  // -----------------------------------------------------------------------

  @Test
  public void testSynchronousPath_releasesHandleExactlyOnce() {
    MockServletRequest req = requestWithHandle();
    PooledBufferHandle handle = handleFrom(req);
    assertFalse("handle must not be released yet", handle.isReleased());

    // Simulate what doFilter's sync-path finally block does.
    SolrDispatchFilter.releaseResponseBufferHandle(req);

    assertTrue("handle must be released after sync-path cleanup", handle.isReleased());
    assertNull("attribute must be cleared after release", req.getAttribute("responseBuffer"));
    assertNoNewDoubleRelease();
    assertPoolBaseline();
  }

  @Test
  public void testSynchronousPath_secondCallIsNoOp() {
    MockServletRequest req = requestWithHandle();
    PooledBufferHandle handle = handleFrom(req);

    SolrDispatchFilter.releaseResponseBufferHandle(req); // first — real release
    SolrDispatchFilter.releaseResponseBufferHandle(req); // second — attribute already null, no-op

    assertTrue(handle.isReleased());
    // Attribute was cleared on the first call; second call finds null → no new double-release.
    assertNoNewDoubleRelease();
    assertPoolBaseline();
  }

  // -----------------------------------------------------------------------
  // 3. Async onComplete path
  // -----------------------------------------------------------------------

  @Test
  public void testAsyncOnComplete_releasesHandleExactlyOnce() throws Exception {
    MockServletRequest req = requestWithHandle();
    PooledBufferHandle handle = handleFrom(req);
    AsyncEvent event = asyncEvent(req);

    SolrDispatchFilter.SolrAsyncListener listener = new SolrDispatchFilter.SolrAsyncListener();
    listener.onComplete(event);

    assertTrue(handle.isReleased());
    assertNull(req.getAttribute("responseBuffer"));
    assertNoNewDoubleRelease();
    assertPoolBaseline();
  }

  // -----------------------------------------------------------------------
  // 4. Async onError path
  // -----------------------------------------------------------------------

  @Test
  public void testAsyncOnError_releasesHandleExactlyOnce() throws Exception {
    MockServletRequest req = requestWithHandle();
    PooledBufferHandle handle = handleFrom(req);
    AsyncEvent event = asyncEvent(req);

    SolrDispatchFilter.SolrAsyncListener listener = new SolrDispatchFilter.SolrAsyncListener();
    listener.onError(event);

    assertTrue(handle.isReleased());
    assertNull(req.getAttribute("responseBuffer"));
    assertNoNewDoubleRelease();
    assertPoolBaseline();
  }

  // -----------------------------------------------------------------------
  // 5. Double-fire path: onError then onComplete (the core double-release hazard)
  //    This tests the exact sequence described in the agrona-buffer-hardening plan and the
  //    critic review: Jetty calls onError (application does NOT call complete()), then the
  //    container fires onComplete.  Both callbacks reach releaseResponseBufferHandle.
  //    The PooledBufferHandle CAS must guarantee exactly-one release (zero new
  //    doubleReleaseDetected) because the attribute is cleared on the first call.
  // -----------------------------------------------------------------------

  @Test
  public void testOnErrorThenOnComplete_doubleFireDoesNotDoubleRelease() throws Exception {
    // Poison OFF so the second close() doesn't throw — we assert the counter stays at zero.
    PooledBufferHandle.setPoisonForTest(false);

    MockServletRequest req = requestWithHandle();
    PooledBufferHandle handle = handleFrom(req);
    AsyncEvent event = asyncEvent(req);

    SolrDispatchFilter.SolrAsyncListener listener = new SolrDispatchFilter.SolrAsyncListener();
    listener.onError(event);    // first terminal event: releases the handle, clears attribute
    listener.onComplete(event); // container fires onComplete after onError: attribute is null → no-op

    assertTrue("handle must be released after error", handle.isReleased());
    assertNull("attribute must remain null after double-fire", req.getAttribute("responseBuffer"));
    // The attribute was cleared by onError, so onComplete sees null and never calls handle.close().
    // Therefore doubleReleaseDetected must NOT increase.
    assertEquals("double-fire must NOT increment doubleReleaseDetected",
        doubleReleaseBefore, metrics.getDoubleReleaseDetected());
    assertPoolBaseline();
  }

  @Test
  public void testOnCompleteThenOnError_doubleFireDoesNotDoubleRelease() throws Exception {
    // Reverse order: onComplete fires first, then onError.
    PooledBufferHandle.setPoisonForTest(false);

    MockServletRequest req = requestWithHandle();
    PooledBufferHandle handle = handleFrom(req);
    AsyncEvent event = asyncEvent(req);

    SolrDispatchFilter.SolrAsyncListener listener = new SolrDispatchFilter.SolrAsyncListener();
    listener.onComplete(event); // releases
    listener.onError(event);    // attribute null → no-op

    assertTrue(handle.isReleased());
    assertNull(req.getAttribute("responseBuffer"));
    assertEquals(doubleReleaseBefore, metrics.getDoubleReleaseDetected());
    assertPoolBaseline();
  }

  // -----------------------------------------------------------------------
  // 6. Async onTimeout path
  //    onTimeout calls asyncContext.complete() which transitively fires onComplete.
  //    We simulate this by calling both callbacks and asserting exactly-once release.
  // -----------------------------------------------------------------------

  @Test
  public void testOnTimeoutThenOnComplete_exactlyOneRelease() throws Exception {
    MockServletRequest req = requestWithHandle();
    PooledBufferHandle handle = handleFrom(req);

    // onTimeout fires onComplete via asyncContext.complete().
    // We simulate the full sequence: onTimeout (which calls complete() on the mock — a no-op
    // on the mock itself) followed by onComplete being invoked by the container.
    AsyncEvent event = asyncEvent(req);

    // Mock the response for onTimeout's setStatus call.
    javax.servlet.http.HttpServletResponse mockResp = mock(javax.servlet.http.HttpServletResponse.class);
    when(event.getSuppliedResponse()).thenReturn(mockResp);

    SolrDispatchFilter.SolrAsyncListener listener = new SolrDispatchFilter.SolrAsyncListener();
    // onTimeout sets the status and calls asyncContext.complete() (which on our mock is a no-op).
    listener.onTimeout(event);
    // The container then fires onComplete → releaseResponseBufferHandle.
    listener.onComplete(event);

    assertTrue(handle.isReleased());
    assertNull(req.getAttribute("responseBuffer"));
    assertNoNewDoubleRelease();
    assertPoolBaseline();
  }

  // -----------------------------------------------------------------------
  // 7. Client-abort path: simulate abrupt abandon (no buffer acquired)
  // -----------------------------------------------------------------------

  @Test
  public void testClientAbort_noBufferAcquired_isNoOp() throws Exception {
    // When the request is aborted before writeQueryResponse acquires a buffer,
    // the "responseBuffer" attribute is absent.  All terminal callbacks must be safe.
    MockServletRequest req = new MockServletRequest(); // no handle set
    AsyncEvent event = asyncEvent(req);
    javax.servlet.http.HttpServletResponse mockResp = mock(javax.servlet.http.HttpServletResponse.class);
    when(event.getSuppliedResponse()).thenReturn(mockResp);

    SolrDispatchFilter.SolrAsyncListener listener = new SolrDispatchFilter.SolrAsyncListener();
    listener.onError(event);
    listener.onComplete(event);

    assertNoNewDoubleRelease();
    assertPoolBaseline();
  }

  // -----------------------------------------------------------------------
  // 8. Error response path (small heap response — no pooled buffer acquired)
  //    When sendException takes the early-exit path (no solrRequest → plain byte[] body),
  //    writeQueryResponse is never called, so no handle is in the attribute.
  //    The async listener must handle this gracefully.
  // -----------------------------------------------------------------------

  @Test
  public void testErrorResponseWithoutPooledBuffer_listenerIsNoOp() throws Exception {
    MockServletRequest req = new MockServletRequest(); // no "responseBuffer" attribute
    AsyncEvent event = asyncEvent(req);

    SolrDispatchFilter.SolrAsyncListener listener = new SolrDispatchFilter.SolrAsyncListener();
    listener.onComplete(event);

    assertNoNewDoubleRelease();
    assertPoolBaseline();
  }

  // -----------------------------------------------------------------------
  // 9. Pool retained-bytes baseline — acquire and full-release returns to baseline
  // -----------------------------------------------------------------------

  @Test
  public void testPoolBaselineReturnedAfterRelease() {
    long beforeCount = metrics.getActiveHandleCount();
    long beforeBytes = metrics.getActiveHandleBytes();

    MockServletRequest req = requestWithHandle();
    PooledBufferHandle handle = handleFrom(req);

    assertEquals(beforeCount + 1, metrics.getActiveHandleCount());
    assertTrue(metrics.getActiveHandleBytes() >= beforeBytes + handle.capacity());

    SolrDispatchFilter.releaseResponseBufferHandle(req);

    assertEquals("active-handle count must return to baseline", beforeCount, metrics.getActiveHandleCount());
    assertEquals("active-handle bytes must return to baseline", beforeBytes, metrics.getActiveHandleBytes());
  }

  // -----------------------------------------------------------------------
  // Minimal ServletRequest stub (map-backed attributes, no Jetty dependencies)
  // -----------------------------------------------------------------------

  /**
   * Minimal {@link ServletRequest} stub that stores attributes in a {@link HashMap}.
   * Only {@code getAttribute} / {@code setAttribute} are implemented; all other methods
   * throw {@link UnsupportedOperationException}.  Mockito cannot stub final methods on
   * Jetty's concrete request classes, so we use a hand-rolled stub here.
   */
  private static class MockServletRequest implements ServletRequest {
    private final Map<String, Object> attrs = new HashMap<>();

    @Override public Object getAttribute(String name) { return attrs.get(name); }
    @Override public void setAttribute(String name, Object o) { attrs.put(name, o); }
    @Override public void removeAttribute(String name) { attrs.remove(name); }

    @Override public java.util.Enumeration<String> getAttributeNames() { throw new UnsupportedOperationException(); }
    @Override public String getCharacterEncoding() { throw new UnsupportedOperationException(); }
    @Override public void setCharacterEncoding(String env) { throw new UnsupportedOperationException(); }
    @Override public int getContentLength() { throw new UnsupportedOperationException(); }
    @Override public String getContentType() { throw new UnsupportedOperationException(); }
    @Override public javax.servlet.ServletInputStream getInputStream() { throw new UnsupportedOperationException(); }
    @Override public String getParameter(String name) { throw new UnsupportedOperationException(); }
    @Override public java.util.Enumeration<String> getParameterNames() { throw new UnsupportedOperationException(); }
    @Override public String[] getParameterValues(String name) { throw new UnsupportedOperationException(); }
    @Override public java.util.Map<String, String[]> getParameterMap() { throw new UnsupportedOperationException(); }
    @Override public String getProtocol() { throw new UnsupportedOperationException(); }
    @Override public String getScheme() { throw new UnsupportedOperationException(); }
    @Override public String getServerName() { throw new UnsupportedOperationException(); }
    @Override public int getServerPort() { throw new UnsupportedOperationException(); }
    @Override public java.io.BufferedReader getReader() { throw new UnsupportedOperationException(); }
    @Override public String getRemoteAddr() { throw new UnsupportedOperationException(); }
    @Override public String getRemoteHost() { throw new UnsupportedOperationException(); }
    @Override public java.util.Locale getLocale() { throw new UnsupportedOperationException(); }
    @Override public java.util.Enumeration<java.util.Locale> getLocales() { throw new UnsupportedOperationException(); }
    @Override public boolean isSecure() { throw new UnsupportedOperationException(); }
    @Override public javax.servlet.RequestDispatcher getRequestDispatcher(String path) { throw new UnsupportedOperationException(); }
    @SuppressWarnings("deprecation")
    @Override public String getRealPath(String path) { throw new UnsupportedOperationException(); }
    @Override public int getRemotePort() { throw new UnsupportedOperationException(); }
    @Override public String getLocalName() { throw new UnsupportedOperationException(); }
    @Override public String getLocalAddr() { throw new UnsupportedOperationException(); }
    @Override public int getLocalPort() { throw new UnsupportedOperationException(); }
    @Override public javax.servlet.ServletContext getServletContext() { throw new UnsupportedOperationException(); }
    @Override public javax.servlet.AsyncContext startAsync() { throw new UnsupportedOperationException(); }
    @Override public javax.servlet.AsyncContext startAsync(ServletRequest req, javax.servlet.ServletResponse resp) { throw new UnsupportedOperationException(); }
    @Override public boolean isAsyncStarted() { return false; }
    @Override public boolean isAsyncSupported() { return false; }
    @Override public javax.servlet.AsyncContext getAsyncContext() { throw new UnsupportedOperationException(); }
    @Override public javax.servlet.DispatcherType getDispatcherType() { throw new UnsupportedOperationException(); }
    @Override public long getContentLengthLong() { throw new UnsupportedOperationException(); }
  }
}
