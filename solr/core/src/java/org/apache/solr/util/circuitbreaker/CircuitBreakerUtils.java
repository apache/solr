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

import static org.apache.solr.common.params.CommonParams.FAILURE;
import static org.apache.solr.common.params.CommonParams.STATUS;

import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.CollectionUtil;
import org.apache.solr.response.SolrQueryResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CircuitBreakerUtils {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /**
   * @param trippedBreakers a (potentially null or empty) list of tripped breakers that may
   *     short-circuit this request
   * @return true if any enabled "hard" (i.e. non-warnOnly) breakers were tripped; false otherwise
   */
  public static boolean reportErrorIfBreakersTripped(
      SolrQueryResponse rsp, List<CircuitBreaker> trippedBreakers) {
    if (CollectionUtil.isEmpty(trippedBreakers)) {
      return false;
    }

    final var warnOnlyBreakers =
        trippedBreakers.stream().filter(CircuitBreaker::isWarnOnly).collect(Collectors.toList());
    if (CollectionUtil.isNotEmpty(warnOnlyBreakers)) {
      if (log.isWarnEnabled()) {
        log.warn(
            "'warnOnly' circuit-breakers tripped for request: {}",
            CircuitBreakerRegistry.toErrorMessage(warnOnlyBreakers));
      }
    }

    final var hardBreakers =
        trippedBreakers.stream()
            .filter(Predicate.not(CircuitBreaker::isWarnOnly))
            .collect(Collectors.toList());
    if (CollectionUtil.isEmpty(hardBreakers)) {
      return false;
    }

    // Build the error message and add it to the response.
    String errorMessage = CircuitBreakerRegistry.toErrorMessage(hardBreakers);
    rsp.add(STATUS, FAILURE);
    rsp.setException(
        new SolrException(
            CircuitBreaker.getErrorCode(hardBreakers), "Circuit Breakers tripped " + errorMessage));
    return true;
  }
}
