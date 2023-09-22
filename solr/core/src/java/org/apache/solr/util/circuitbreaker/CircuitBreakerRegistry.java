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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.solr.client.solrj.SolrRequest.SolrRequestType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Keeps track of all registered circuit breaker instances for various request types. Responsible
 * for a holistic view of whether a circuit breaker has tripped or not.
 *
 * @lucene.experimental
 * @since 9.4
 */
public class CircuitBreakerRegistry implements Closeable {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final Map<SolrRequestType, List<CircuitBreaker>> circuitBreakerMap = new HashMap<>();

  public CircuitBreakerRegistry() {}

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

  @VisibleForTesting
  public void deregisterAll() throws IOException {
    this.close();
  }

  /**
   * Check and return circuit breakers that have triggered
   *
   * @param requestType {@link SolrRequestType} to check for.
   * @return CircuitBreakers which have triggered, null otherwise.
   */
  public List<CircuitBreaker> checkTripped(SolrRequestType requestType) {
    List<CircuitBreaker> triggeredCircuitBreakers = null;

    for (CircuitBreaker circuitBreaker :
        circuitBreakerMap.getOrDefault(requestType, Collections.emptyList())) {
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
    return circuitBreakerMap.containsKey(requestType);
  }

  @Override
  public void close() throws IOException {
    synchronized (circuitBreakerMap) {
      final AtomicInteger closeFailedCounter = new AtomicInteger(0);
      circuitBreakerMap
          .values()
          .forEach(
              list ->
                  list.forEach(
                      it -> {
                        try {
                          if (log.isDebugEnabled()) {
                            log.debug(
                                "Closed circuit breaker {} for request type(s) {}",
                                it.getClass().getSimpleName(),
                                it.getRequestTypes());
                          }
                          it.close();
                        } catch (IOException e) {
                          if (log.isErrorEnabled()) {
                            log.error(
                                String.format(
                                    Locale.ROOT,
                                    "Failed to close circuit breaker %s",
                                    it.getClass().getSimpleName()),
                                e);
                          }
                          closeFailedCounter.incrementAndGet();
                        }
                      }));
      circuitBreakerMap.clear();
      if (closeFailedCounter.get() > 0) {
        throw new IOException("Failed to close " + closeFailedCounter.get() + " circuit breakers");
      }
    }
  }
}
