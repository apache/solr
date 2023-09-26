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

import static org.hamcrest.CoreMatchers.containsString;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.apache.solr.core.SolrCore;
import org.apache.solr.util.circuitbreaker.CPUCircuitBreaker;
import org.apache.solr.util.circuitbreaker.CircuitBreaker;
import org.apache.solr.util.circuitbreaker.CircuitBreakerManager;
import org.apache.solr.util.circuitbreaker.LoadAverageCircuitBreaker;
import org.apache.solr.util.circuitbreaker.MemoryCircuitBreaker;
import org.hamcrest.MatcherAssert;
import org.junit.After;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BaseTestCircuitBreaker extends SolrTestCaseJ4 {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final CircuitBreaker dummyMemBreaker = new MemoryCircuitBreaker();
  private static final CircuitBreaker dummyCBManager = new CircuitBreakerManager();

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
    dummyCBManager.close();
  }

  @After
  public void after() {
    removeAllExistingCircuitBreakers();
  }

  public void testCBAlwaysTrips() {
    removeAllExistingCircuitBreakers();

    CircuitBreaker circuitBreaker = new MockCircuitBreaker(true);

    h.getCore().getCircuitBreakerRegistry().register(circuitBreaker);

    expectThrows(
        SolrException.class,
        () -> {
          h.query(req("name:\"john smith\""));
        });
  }

  public void testCBFakeMemoryPressure() throws Exception {
    removeAllExistingCircuitBreakers();

    // Update and query will not trip
    h.update(
        "<add><doc><field name=\"id\">1</field><field name=\"name\">john smith</field></doc></add>");
    h.query(req("name:\"john smith\""));

    MemoryCircuitBreaker searchBreaker = new FakeMemoryPressureCircuitBreaker();
    searchBreaker.setThreshold(80);
    // Default request type is "query"
    // searchBreaker.setRequestTypes(List.of("query"));
    h.getCore().getCircuitBreakerRegistry().register(searchBreaker);

    // Query will trip, but not update due to defaults
    expectThrows(SolrException.class, () -> h.query(req("name:\"john smith\"")));
    h.update(
        "<add><doc><field name=\"id\">2</field><field name=\"name\">john smith</field></doc></add>");

    MemoryCircuitBreaker updateBreaker = new FakeMemoryPressureCircuitBreaker();
    updateBreaker.setThreshold(75);
    updateBreaker.setRequestTypes(List.of("update"));
    h.getCore().getCircuitBreakerRegistry().register(updateBreaker);

    // Now also update will trip
    expectThrows(
        SolrException.class,
        () ->
            h.update(
                "<add><doc><field name=\"id\">1</field><field name=\"name\">john smith</field></doc></add>"));
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
    CPUCircuitBreaker circuitBreaker = new FakeCPUCircuitBreaker(h.getCore());
    circuitBreaker.setThreshold(75);

    assertThatHighQueryLoadTrips(circuitBreaker, 5);
  }

  public void testFakeLoadAverageCircuitBreaker() {
    LoadAverageCircuitBreaker circuitBreaker = new FakeLoadAverageCircuitBreaker();
    circuitBreaker.setThreshold(75);

    assertThatHighQueryLoadTrips(circuitBreaker, 5);
  }

  /**
   * Common assert method to be reused in tests
   *
   * @param circuitBreaker the breaker to test
   * @param numShouldTrip the number of queries that should trip the breaker
   */
  private void assertThatHighQueryLoadTrips(CircuitBreaker circuitBreaker, int numShouldTrip) {
    removeAllExistingCircuitBreakers();

    h.getCore().getCircuitBreakerRegistry().register(circuitBreaker);

    AtomicInteger failureCount = new AtomicInteger();

    ExecutorService executor =
        ExecutorUtil.newMDCAwareCachedThreadPool(new SolrNamedThreadFactory("TestCircuitBreaker"));
    try {
      List<Future<?>> futures = new ArrayList<>();

      for (int i = 0; i < 5; i++) {
        Future<?> future =
            executor.submit(
                () -> {
                  try {
                    h.query(req("name:\"john smith\""));
                  } catch (SolrException e) {
                    MatcherAssert.assertThat(
                        e.getMessage(), containsString("Circuit Breakers tripped"));
                    failureCount.incrementAndGet();
                  } catch (Exception e) {
                    throw new RuntimeException(e.getMessage());
                  }
                });

        futures.add(future);
      }

      for (Future<?> future : futures) {
        try {
          future.get();
        } catch (Exception e) {
          throw new RuntimeException(e.getMessage());
        }
      }
    } finally {
      ExecutorUtil.shutdownAndAwaitTermination(executor);
      assertEquals("Number of failed queries is not correct", numShouldTrip, failureCount.get());
    }
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
        "count(//lst[@name='timing']/*)=4",
        "//lst[@name='timing']/double[@name='time']",
        "count(//lst[@name='circuitbreaker']/*)>0",
        "//lst[@name='circuitbreaker']/double[@name='time']",
        "count(//lst[@name='prepare']/*)>0",
        "//lst[@name='prepare']/double[@name='time']",
        "count(//lst[@name='process']/*)>0",
        "//lst[@name='process']/double[@name='time']");
  }

  public void testErrorCode() throws Exception {
    assertEquals(
        SolrException.ErrorCode.SERVICE_UNAVAILABLE,
        CircuitBreaker.getErrorCode(List.of(dummyCBManager)));
    assertEquals(
        SolrException.ErrorCode.TOO_MANY_REQUESTS,
        CircuitBreaker.getErrorCode(List.of(dummyMemBreaker)));
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
      super(1, 1);
    }

    @Override
    protected long getAvgMemoryUsage() {
      return Long.MAX_VALUE;
    }
  }

  private static class BuildingUpMemoryPressureCircuitBreaker extends MemoryCircuitBreaker {
    private AtomicInteger count;

    public BuildingUpMemoryPressureCircuitBreaker() {
      super(1, 1);
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
    public FakeCPUCircuitBreaker(SolrCore core) {
      super(core);
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
