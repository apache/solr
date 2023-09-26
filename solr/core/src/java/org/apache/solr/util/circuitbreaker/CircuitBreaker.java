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

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.solr.client.solrj.SolrRequest.SolrRequestType;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.util.SolrPluginUtils;
import org.apache.solr.util.plugin.NamedListInitializedPlugin;

/**
 * Default base class to define circuit breaker plugins for Solr. <b>Still experimental, may
 * change</b>
 *
 * <p>There are two (typical) ways to use circuit breakers:
 *
 * <ol>
 *   <li>Have them checked at admission control by default (use CircuitBreakerRegistry for the
 *       same).
 *   <li>Use the circuit breaker in a specific code path(s).
 * </ol>
 *
 * @lucene.experimental
 */
public abstract class CircuitBreaker implements NamedListInitializedPlugin, Closeable {
  // Only query requests are checked by default
  private Set<SolrRequestType> requestTypes = Set.of(SolrRequestType.QUERY);
  private final List<SolrRequestType> SUPPORTED_TYPES =
      List.of(SolrRequestType.QUERY, SolrRequestType.UPDATE);

  @Override
  public void init(NamedList<?> args) {
    SolrPluginUtils.invokeSetters(this, args);
  }

  public CircuitBreaker() {}

  /** Check if circuit breaker is tripped. */
  public abstract boolean isTripped();

  /** Get error message when the circuit breaker triggers */
  public abstract String getErrorMessage();

  @Override
  public void close() throws IOException {
    // Nothing to do by default
  }

  /**
   * Set the request types for which this circuit breaker should be checked. If not called, the
   * circuit breaker will be checked for the {@link SolrRequestType#QUERY} request type only.
   *
   * @param requestTypes list of strings representing request types
   * @throws IllegalArgumentException if the request type is not valid
   */
  public void setRequestTypes(List<String> requestTypes) {
    this.requestTypes =
        requestTypes.stream()
            .map(t -> SolrRequestType.valueOf(t.toUpperCase(Locale.ROOT)))
            .peek(
                t -> {
                  if (!SUPPORTED_TYPES.contains(t)) {
                    throw new IllegalArgumentException(
                        String.format(
                            Locale.ROOT,
                            "Request type %s is not supported for circuit breakers",
                            t.name()));
                  }
                })
            .collect(Collectors.toSet());
  }

  public Set<SolrRequestType> getRequestTypes() {
    return requestTypes;
  }

  /**
   * Return the proper error code to use in exception. For legacy use of {@link CircuitBreaker} we
   * return 503 for backward compatibility, else return 429.
   *
   * @deprecated Remove in 10.0
   */
  @Deprecated(since = "9.4")
  public static SolrException.ErrorCode getErrorCode(List<CircuitBreaker> trippedCircuitBreakers) {
    if (trippedCircuitBreakers != null
        && trippedCircuitBreakers.stream().anyMatch(cb -> cb instanceof CircuitBreakerManager)) {
      return SolrException.ErrorCode.SERVICE_UNAVAILABLE;
    } else {
      return SolrException.ErrorCode.TOO_MANY_REQUESTS;
    }
  }
}
