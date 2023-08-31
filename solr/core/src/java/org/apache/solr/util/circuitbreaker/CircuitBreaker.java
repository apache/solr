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
public abstract class CircuitBreaker implements NamedListInitializedPlugin {

  @Override
  public void init(NamedList<?> args) {
    SolrPluginUtils.invokeSetters(this, args);
  }

  public CircuitBreaker() {}

  /** Check if circuit breaker is tripped. */
  public abstract boolean isTripped();

  /** Get debug useful info. */
  public abstract String getDebugInfo();

  /** Get error message when the circuit breaker triggers */
  public abstract String getErrorMessage();
}
