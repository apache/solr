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

package org.apache.solr.util;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.util.circuitbreaker.CPUCircuitBreaker;
import org.apache.solr.util.circuitbreaker.CircuitBreaker;
import org.apache.solr.util.circuitbreaker.CircuitBreakerRegistry;
import org.apache.solr.util.circuitbreaker.LoadAverageCircuitBreaker;
import org.apache.solr.util.circuitbreaker.MemoryCircuitBreaker;
import org.junit.After;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestCircuitBreakers extends SolrTestCaseJ4 {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final CircuitBreaker dummyMemBreaker = new MemoryCircuitBreaker();

  @BeforeClass
  public static void setUpClass() throws Exception {
    System.setProperty("filterCache.enabled", "false");
    System.setProperty("queryResultCache.enabled", "false");
    System.setProperty("documentCache.enabled", "true");
    System.setProperty("solr.metrics.jvm.enabled", "true");
    System.clearProperty(CircuitBreaker.SYSPROP_SOLR_CIRCUITBREAKER_ERRORCODE);

    initCore("solrconfig-pluggable-circuitbreaker.xml", "schema.xml");
    indexDocs();
  }

  protected static void indexDocs() {
    removeAllExistingCircuitBreakers();
    for (int i = 0; i < 20; i++) {
      assertU(adoc("name", "john smith", "id", "1"));
      assertU(adoc("name", "johathon smith", "id", "2"));
      assertU(adoc("name", "john percival smith", "id", "3"));
      assertU(adoc("id", "1", "title", "this is a title.", "inStock_b1", "true"));
      assertU(adoc("id", "2", "title", "this is another title.", "inStock_b1", "true"));
      assertU(adoc("id", "3", "title", "Mary had a little lamb.", "inStock_b1", "false"));

      // commit inside the loop to get multiple segments to make search as realistic as possible
      assertU(commit());
    }
  }

  @Override
  public void tearDown() throws Exception {
    super.tearDown();
    dummyMemBreaker.close();
    removeAllExistingCircuitBreakers();
  }

  @After
  public void after() {
    removeAllExistingCircuitBreakers();
  }

  public void testCBAlwaysTripsWithCorrectCode() {
    synchronized (this) {
      List.of(
              -1,
              SolrException.ErrorCode.TOO_MANY_REQUESTS.code,
              SolrException.ErrorCode.SERVICE_UNAVAILABLE.code,
              SolrException.ErrorCode.BAD_REQUEST.code)
          .forEach(
              code -> {
                removeAllExistingCircuitBreakers();
                if (code > 0) {
                  System.setProperty(
                      CircuitBreaker.SYSPROP_SOLR_CIRCUITBREAKER_ERRORCODE, String.valueOf(code));
                }
                CircuitBreaker circuitBreaker = new MockCircuitBreaker(true);
                h.getCore().getCircuitBreakerRegistry().register(circuitBreaker);
                int expected = (code == -1) ? SolrException.ErrorCode.TOO_MANY_REQUESTS.code : code;
                assertEquals(
                    "Configured circuit-breaker error code",
                    expected,
                    CircuitBreaker.getExceptionErrorCode().code);
                List<CircuitBreaker> tripped =
                    h.getCore()
                        .getCircuitBreakerRegistry()
                        .checkTripped(SolrRequest.SolrRequestType.QUERY);
                assertNotNull("Tripped breaker should be reported by registry", tripped);
                assertEquals(1, tripped.size());
                System.clearProperty(CircuitBreaker.SYSPROP_SOLR_CIRCUITBREAKER_ERRORCODE);
              });
    }
  }

  public void testGlobalCBAlwaysTrips() {
    removeAllExistingCircuitBreakers();

    CircuitBreaker circuitBreaker = new MockCircuitBreaker(true);
    CircuitBreakerRegistry.registerGlobal(circuitBreaker);

    List<CircuitBreaker> tripped =
        CircuitBreakerRegistry.checkTrippedGlobal(SolrRequest.SolrRequestType.QUERY);
    assertNotNull("Global breaker should be reported as tripped", tripped);
    assertEquals(1, tripped.size());
    assertSame(circuitBreaker, tripped.get(0));
  }

  public void testCBsCanBeMarkedAsWarnOnly() throws Exception {
    removeAllExistingCircuitBreakers();

    final var warnCircuitBreaker = new MockCircuitBreaker(true);
    CircuitBreakerRegistry.registerGlobal(warnCircuitBreaker);

    // CB is warnOnly=false by default — registry reports it as tripped.
    assertNotNull(
        "Hard-tripped breaker should be returned by checkTrippedGlobal",
        CircuitBreakerRegistry.checkTrippedGlobal(SolrRequest.SolrRequestType.QUERY));

    // When warnOnly=true is set, checkTripped (per-core) still includes it (legacy behavior),
    // but checkTrippedGlobal — the admission-control variant — filters it out.
    warnCircuitBreaker.setWarnOnly(true);
    final var tripList =
        h.getCore().getCircuitBreakerRegistry().checkTripped(SolrRequest.SolrRequestType.QUERY);
    assertEquals(1, tripList.size());
    assertEquals(warnCircuitBreaker, tripList.get(0));
    assertNull(
        "Warn-only breakers must not be admission-control-tripped",
        CircuitBreakerRegistry.checkTrippedGlobal(SolrRequest.SolrRequestType.QUERY));
    h.query(req("name:\"john smith\""));
  }

  public void testGlobalCBsCanBeParsedFromSystemProperties() {
    final var props = new Properties();
    props.setProperty("solr.circuitbreaker.query.mem", "12");
    props.setProperty("solr.circuitbreaker.update.loadavg", "3.4");
    props.setProperty("solr.circuitbreaker.update.cpu", "56");
    // Ensure 'warnOnly' property is picked up.
    props.setProperty("solr.circuitbreaker.update.cpu.warnonly", "true");
    System.setProperties(props);

    final var parsedBreakers =
        CircuitBreakerRegistry.parseCircuitBreakersFromProperties(h.getCoreContainer()).stream()
            .sorted(Comparator.comparing(breaker -> breaker.toString()))
            .collect(Collectors.toList());

    assertEquals(3, parsedBreakers.size());

    assertTrue(
        "Expected CPUCircuitBreaker, but got " + parsedBreakers.get(0).getClass().getName(),
        parsedBreakers.get(0) instanceof CPUCircuitBreaker);
    final var cpuBreaker = (CPUCircuitBreaker) parsedBreakers.get(0);
    assertEquals(56.0, cpuBreaker.getCpuUsageThreshold(), 0.1);
    assertEquals(true, cpuBreaker.isWarnOnly());
    assertEquals(Set.of(SolrRequest.SolrRequestType.UPDATE), cpuBreaker.getRequestTypes());

    assertTrue(
        "Expected LoadAverageCircuitBreaker, but got " + parsedBreakers.get(1).getClass().getName(),
        parsedBreakers.get(1) instanceof LoadAverageCircuitBreaker);
    final var loadAvgBreaker = (LoadAverageCircuitBreaker) parsedBreakers.get(1);
    assertEquals(3.4, loadAvgBreaker.getLoadAverageThreshold(), 0.1);
    assertEquals(false, loadAvgBreaker.isWarnOnly());
    assertEquals(Set.of(SolrRequest.SolrRequestType.UPDATE), loadAvgBreaker.getRequestTypes());

    assertTrue(
        "Expected MemoryCircuitBreaker, but got " + parsedBreakers.get(2).getClass().getName(),
        parsedBreakers.get(2) instanceof MemoryCircuitBreaker);
    final var memBreaker = (MemoryCircuitBreaker) parsedBreakers.get(2);
    assertEquals(false, memBreaker.isWarnOnly());
    assertEquals(Set.of(SolrRequest.SolrRequestType.QUERY), memBreaker.getRequestTypes());
  }

  @SuppressWarnings("resource")
  public void testCBAlwaysTripsInvalidErrorCodeSysProp() {
    synchronized (this) {
      List.of("foo", 123, 999, 888)
          .forEach(
              code -> {
                System.setProperty(
                    CircuitBreaker.SYSPROP_SOLR_CIRCUITBREAKER_ERRORCODE, String.valueOf(code));
                SolrException ex =
                    expectThrows(SolrException.class, () -> new MockCircuitBreaker(true));
                assertTrue(ex.getMessage().contains("Invalid error code"));
              });
    }
  }

  public void testCBFakeMemoryPressure() throws Exception {
    removeAllExistingCircuitBreakers();
    CircuitBreakerRegistry registry = h.getCore().getCircuitBreakerRegistry();

    // No breakers registered: registry reports nothing tripped.
    assertNull(registry.checkTrippedLocal(SolrRequest.SolrRequestType.QUERY));
    assertNull(registry.checkTrippedLocal(SolrRequest.SolrRequestType.UPDATE));

    MemoryCircuitBreaker searchBreaker = new FakeMemoryPressureCircuitBreaker();
    searchBreaker.setThreshold(80);
    // Default request type is "query"
    registry.register(searchBreaker);

    // Query type now reports tripped, update does not (different default request type set).
    List<CircuitBreaker> queryTripped =
        registry.checkTrippedLocal(SolrRequest.SolrRequestType.QUERY);
    assertNotNull(queryTripped);
    assertEquals(1, queryTripped.size());
    List<CircuitBreaker> updateTripped =
        registry.checkTrippedLocal(SolrRequest.SolrRequestType.UPDATE);
    assertTrue("Update breakers should be empty", updateTripped == null || updateTripped.isEmpty());

    MemoryCircuitBreaker updateBreaker = new FakeMemoryPressureCircuitBreaker();
    updateBreaker.setThreshold(75);
    updateBreaker.setRequestTypes(List.of("update"));
    registry.register(updateBreaker);

    updateTripped = registry.checkTrippedLocal(SolrRequest.SolrRequestType.UPDATE);
    assertNotNull(updateTripped);
    assertEquals(1, updateTripped.size());
  }

  public void testBadRequestType() {

    expectThrows(
        IllegalArgumentException.class,
        () -> dummyMemBreaker.setRequestTypes(List.of("badRequestType")));
  }

  public void testBuildingMemoryPressure() {
    MemoryCircuitBreaker circuitBreaker = new BuildingUpMemoryPressureCircuitBreaker();
    circuitBreaker.setThreshold(75);

    assertThatHighQueryLoadTrips(circuitBreaker, 1);
  }

  public void testFakeCPUCircuitBreaker() {
    CPUCircuitBreaker circuitBreaker = new FakeCPUCircuitBreaker(h.getCore().getCoreContainer());
    circuitBreaker.setThreshold(75);

    assertThatHighQueryLoadTrips(circuitBreaker, 5);
  }

  public void testFakeLoadAverageCircuitBreaker() {
    LoadAverageCircuitBreaker circuitBreaker = new FakeLoadAverageCircuitBreaker();
    circuitBreaker.setThreshold(75);

    assertThatHighQueryLoadTrips(circuitBreaker, 5);
  }

  /**
   * Common assert method to be reused in tests. Verifies that the breaker's {@code isTripped()}
   * method returns true the expected number of times across {@code numIterations} successive
   * invocations.
   *
   * @param circuitBreaker the breaker to test
   * @param numShouldTrip the number of {@code isTripped()} calls (out of 5) that should report the
   *     breaker as tripped
   */
  private void assertThatHighQueryLoadTrips(CircuitBreaker circuitBreaker, int numShouldTrip) {
    removeAllExistingCircuitBreakers();

    h.getCore().getCircuitBreakerRegistry().register(circuitBreaker);

    int trippedCount = 0;
    for (int i = 0; i < 5; i++) {
      if (circuitBreaker.isTripped()) {
        trippedCount++;
      }
    }
    assertEquals(
        "Number of tripped reports does not match expectation", numShouldTrip, trippedCount);
  }

  public void testResponseWithCBTiming() {
    removeAllExistingCircuitBreakers();

    assertQ(
        req("q", "*:*", CommonParams.DEBUG_QUERY, "true"),
        "//str[@name='rawquerystring']='*:*'",
        "//str[@name='querystring']='*:*'",
        "//str[@name='parsedquery']='MatchAllDocsQuery(*:*)'",
        "//str[@name='parsedquery_toString']='*:*'",
        "count(//lst[@name='explain']/*)=3",
        "//lst[@name='explain']/str[@name='1']",
        "//lst[@name='explain']/str[@name='2']",
        "//lst[@name='explain']/str[@name='3']",
        "//str[@name='QParser']",
        "count(//lst[@name='timing']/*)=3",
        "//lst[@name='timing']/double[@name='time']",
        "count(//lst[@name='prepare']/*)>0",
        "//lst[@name='prepare']/double[@name='time']",
        "count(//lst[@name='process']/*)>0",
        "//lst[@name='process']/double[@name='time']");

    // Registering a (non-tripped) circuit breaker no longer adds an entry to the handler-level
    // timing tree — admission control has been moved to SolrQoSFilter and no longer wraps
    // SearchHandler.processComponents in an RTimerTree subtree.
    CircuitBreaker circuitBreaker = new MockCircuitBreaker(false);
    h.getCore().getCircuitBreakerRegistry().register(circuitBreaker);

    assertQ(
        req("q", "*:*", CommonParams.DEBUG_QUERY, "true"),
        "//str[@name='rawquerystring']='*:*'",
        "//str[@name='querystring']='*:*'",
        "//str[@name='parsedquery']='MatchAllDocsQuery(*:*)'",
        "//str[@name='parsedquery_toString']='*:*'",
        "count(//lst[@name='explain']/*)=3",
        "//lst[@name='explain']/str[@name='1']",
        "//lst[@name='explain']/str[@name='2']",
        "//lst[@name='explain']/str[@name='3']",
        "//str[@name='QParser']",
        "count(//lst[@name='timing']/*)=3",
        "//lst[@name='timing']/double[@name='time']",
        "count(//lst[@name='prepare']/*)>0",
        "//lst[@name='prepare']/double[@name='time']",
        "count(//lst[@name='process']/*)>0",
        "//lst[@name='process']/double[@name='time']");
  }

  private static void removeAllExistingCircuitBreakers() {
    try {
      h.getCore().getCircuitBreakerRegistry().deregisterAll();
    } catch (IOException e) {
      fail("Failed to unload circuit breakers");
    }
  }

  private static class MockCircuitBreaker extends MemoryCircuitBreaker {

    private final boolean tripped;

    public MockCircuitBreaker(boolean tripped) {
      this.tripped = tripped;
    }

    @Override
    public boolean isTripped() {
      return this.tripped;
    }
  }

  private static class FakeMemoryPressureCircuitBreaker extends MemoryCircuitBreaker {
    public FakeMemoryPressureCircuitBreaker() {
      super();
    }

    @Override
    protected long getAvgMemoryUsage() {
      return Long.MAX_VALUE;
    }
  }

  private static class BuildingUpMemoryPressureCircuitBreaker extends MemoryCircuitBreaker {
    private AtomicInteger count;

    public BuildingUpMemoryPressureCircuitBreaker() {
      super();
      this.count = new AtomicInteger(0);
    }

    @Override
    protected long getAvgMemoryUsage() {
      int localCount = count.getAndIncrement();

      if (localCount >= 4) {
        // TODO: To be removed
        if (log.isInfoEnabled()) {
          String logMessage =
              "Blocking query from BuildingUpMemoryPressureCircuitBreaker for count " + localCount;
          log.info(logMessage);
        }
        return Long.MAX_VALUE;
      }

      // TODO: To be removed
      if (log.isInfoEnabled()) {
        String logMessage =
            "BuildingUpMemoryPressureCircuitBreaker: Returning unblocking value for count "
                + localCount;
        log.info(logMessage);
      }
      return Long.MIN_VALUE; // Random number guaranteed to not trip the circuit breaker
    }
  }

  private static class FakeCPUCircuitBreaker extends CPUCircuitBreaker {
    public FakeCPUCircuitBreaker(CoreContainer coreContainer) {
      super(coreContainer);
    }

    @Override
    protected double calculateLiveCPUUsage() {
      return Double.MAX_VALUE;
    }
  }

  private static class FakeLoadAverageCircuitBreaker extends LoadAverageCircuitBreaker {
    @Override
    protected double calculateLiveLoadAverage() {
      return Double.MAX_VALUE;
    }
  }
}
