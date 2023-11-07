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

package org.apache.solr.util.circuitbreaker;

import com.google.common.annotations.VisibleForTesting;
import java.io.Closeable;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.solr.client.solrj.SolrRequest.SolrRequestType;
import org.apache.solr.common.SolrException;
import org.apache.solr.core.CoreContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Keeps track of all registered circuit breaker instances for various request types. Responsible
 * for a holistic view of whether a circuit breaker has tripped or not. Circuit breakers may be
 * registered globally and/or per-core. This registry has one instance per core, but keeps a static
 * map of globally registered Circuit Breakers that are always checked.
 *
 * @lucene.experimental
 * @since 9.4
 */
public class CircuitBreakerRegistry implements Closeable {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private final Map<SolrRequestType, List<CircuitBreaker>> circuitBreakerMap = new HashMap<>();
  private static final Map<SolrRequestType, List<CircuitBreaker>> globalCircuitBreakerMap =
      new HashMap<>();
  private static final Pattern SYSPROP_REGEX =
      Pattern.compile("solr.circuitbreaker\\.(update|query)\\.(cpu|mem|loadavg)");
  public static final String SYSPROP_PREFIX = "solr.circuitbreaker.";
  public static final String SYSPROP_UPDATE_CPU = SYSPROP_PREFIX + "update.cpu";
  public static final String SYSPROP_UPDATE_MEM = SYSPROP_PREFIX + "update.mem";
  public static final String SYSPROP_UPDATE_LOADAVG = SYSPROP_PREFIX + "update.loadavg";
  public static final String SYSPROP_QUERY_CPU = SYSPROP_PREFIX + "query.cpu";
  public static final String SYSPROP_QUERY_MEM = SYSPROP_PREFIX + "query.mem";
  public static final String SYSPROP_QUERY_LOADAVG = SYSPROP_PREFIX + "query.loadavg";

  public CircuitBreakerRegistry(CoreContainer coreContainer) {
    initGlobal(coreContainer);
  }

  private static void initGlobal(CoreContainer coreContainer) {
    // Read system properties to register global circuit breakers for update and query:
    // Example: solr.circuitbreaker.update.cpu = 50
    System.getProperties().keySet().stream()
        .map(k -> SYSPROP_REGEX.matcher(k.toString()))
        .filter(Matcher::matches)
        .collect(Collectors.groupingBy(m -> m.group(2) + ":" + System.getProperty(m.group(0))))
        .forEach(
            (breakerAndValue, breakers) -> {
              CircuitBreaker breaker;
              String[] breakerAndValueArr = breakerAndValue.split(":");
              switch (breakerAndValueArr[0]) {
                case "cpu":
                  breaker =
                      new CPUCircuitBreaker(coreContainer)
                          .setThreshold(Double.parseDouble(breakerAndValueArr[1]));
                  break;
                case "mem":
                  breaker =
                      new MemoryCircuitBreaker()
                          .setThreshold(Double.parseDouble(breakerAndValueArr[1]));
                  break;
                case "loadavg":
                  breaker =
                      new LoadAverageCircuitBreaker()
                          .setThreshold(Double.parseDouble(breakerAndValueArr[1]));
                  break;
                default:
                  throw new IllegalArgumentException(
                      "Unknown circuit breaker type: " + breakerAndValueArr[0]);
              }
              breaker.setRequestTypes(
                  breakers.stream().map(m -> m.group(1)).collect(Collectors.toList()));
              registerGlobal(breaker);
              if (log.isInfoEnabled()) {
                log.info(
                    "Registered global circuit breaker {} for request type(s) {}",
                    breakerAndValue,
                    breaker.getRequestTypes());
              }
            });
  }

  /** List all registered circuit breakers for global context */
  public static Set<CircuitBreaker> listGlobal() {
    return globalCircuitBreakerMap.values().stream()
        .flatMap(List::stream)
        .collect(Collectors.toSet());
  }

  /** Register a circuit breaker for a core */
  public void register(CircuitBreaker circuitBreaker) {
    synchronized (circuitBreakerMap) {
      circuitBreaker
          .getRequestTypes()
          .forEach(
              r -> {
                List<CircuitBreaker> list =
                    circuitBreakerMap.computeIfAbsent(r, k -> new ArrayList<>());
                list.add(circuitBreaker);
                if (log.isInfoEnabled()) {
                  log.info(
                      "Registered circuit breaker {} for request type(s) {}",
                      circuitBreaker.getClass().getSimpleName(),
                      r);
                }
              });
    }
  }

  /** Register a global circuit breaker */
  public static void registerGlobal(CircuitBreaker circuitBreaker) {
    circuitBreaker
        .getRequestTypes()
        .forEach(
            r -> {
              List<CircuitBreaker> list =
                  globalCircuitBreakerMap.computeIfAbsent(r, k -> new ArrayList<>());
              list.add(circuitBreaker);
            });
  }

  @VisibleForTesting
  public void deregisterAll() throws IOException {
    this.close();
    deregisterGlobal();
  }

  @VisibleForTesting
  public static void deregisterGlobal() {
    closeGlobal();
  }

  /**
   * Check and return circuit breakers that have triggered
   *
   * @param requestType {@link SolrRequestType} to check for.
   * @return CircuitBreakers which have triggered, null otherwise.
   */
  public List<CircuitBreaker> checkTripped(SolrRequestType requestType) {
    Map<SolrRequestType, List<CircuitBreaker>> combinedMap = getCombinedMap();
    final List<CircuitBreaker> breakersOfType = combinedMap.get(requestType);
    List<CircuitBreaker> triggeredCircuitBreakers = null;
    for (CircuitBreaker circuitBreaker : breakersOfType) {
      if (circuitBreaker.isTripped()) {
        if (triggeredCircuitBreakers == null) {
          triggeredCircuitBreakers = new ArrayList<>();
        }

        triggeredCircuitBreakers.add(circuitBreaker);
      }
    }

    return triggeredCircuitBreakers;
  }

  /**
   * Construct the final error message to be printed when circuit breakers trip.
   *
   * @param circuitBreakerList Input list for circuit breakers.
   * @return Constructed error message.
   */
  public static String toErrorMessage(List<CircuitBreaker> circuitBreakerList) {
    StringBuilder sb = new StringBuilder();

    for (CircuitBreaker circuitBreaker : circuitBreakerList) {
      sb.append(circuitBreaker.getErrorMessage());
      sb.append("\n");
    }

    return sb.toString();
  }

  public boolean isEnabled(SolrRequestType requestType) {
    return circuitBreakerMap.containsKey(requestType)
        || globalCircuitBreakerMap.containsKey(requestType);
  }

  @Override
  public void close() throws IOException {
    synchronized (circuitBreakerMap) {
      closeCircuitBreakers(
          circuitBreakerMap.values().stream().flatMap(List::stream).collect(Collectors.toList()));
      circuitBreakerMap.clear();
    }
  }

  private static void closeGlobal() {
    synchronized (globalCircuitBreakerMap) {
      closeCircuitBreakers(
          globalCircuitBreakerMap.values().stream()
              .flatMap(List::stream)
              .collect(Collectors.toList()));
      globalCircuitBreakerMap.clear();
    }
  }

  /**
   * Close a list of circuit breakers, tracing any failures.
   *
   * @throws SolrException if any CB fails to close
   */
  private static void closeCircuitBreakers(List<CircuitBreaker> breakers) {
    final AtomicInteger closeFailedCounter = new AtomicInteger(0);
    breakers.forEach(
        it -> {
          try {
            if (log.isDebugEnabled()) {
              log.debug(
                  "Closing circuit breaker {} for request type(s) {}",
                  it.getClass().getSimpleName(),
                  it.getRequestTypes());
            }
            it.close();
          } catch (IOException e) {
            if (log.isErrorEnabled()) {
              log.error(
                  String.format(
                      Locale.ROOT,
                      "Failed to close circuit breaker %s for request type(s) %s",
                      it.getClass().getSimpleName(),
                      it.getRequestTypes()),
                  e);
            }
            closeFailedCounter.incrementAndGet();
          }
        });
    if (closeFailedCounter.get() > 0) {
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR,
          "Failed to close " + closeFailedCounter.get() + " circuit breakers");
    }
  }

  /**
   * Return a combined map of local and global circuit breaker maps, joining the two maps in a
   * streaming fashion
   */
  private Map<SolrRequestType, List<CircuitBreaker>> getCombinedMap() {
    Map<SolrRequestType, List<CircuitBreaker>> combinedMap = new HashMap<>(circuitBreakerMap);
    globalCircuitBreakerMap.forEach(
        (k, v) ->
            combinedMap.merge(
                k,
                v,
                (v1, v2) -> {
                  List<CircuitBreaker> newList = new ArrayList<>();
                  newList.addAll(v1);
                  newList.addAll(v2);
                  return newList;
                }));
    return combinedMap;
  }
}
