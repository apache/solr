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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.solr.client.solrj.SolrRequest.SolrRequestType;

/**
 * Keeps track of all registered circuit breaker instances for various request types. Responsible
 * for a holistic view of whether a circuit breaker has tripped or not.
 *
 * @lucene.experimental
 * @since 9.4
 */
public class CircuitBreakerRegistry {

  private final Map<SolrRequestType, List<CircuitBreaker>> circuitBreakerMap = new HashMap<>();

  public CircuitBreakerRegistry() {}

  public void register(CircuitBreaker circuitBreaker) {
    circuitBreaker
        .getRequestTypes()
        .forEach(
            r -> {
              List<CircuitBreaker> list =
                  circuitBreakerMap.computeIfAbsent(r, k -> new ArrayList<>());
              list.add(circuitBreaker);
            });
  }

  @VisibleForTesting
  public void deregisterAll() {
    circuitBreakerMap.clear();
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
}
