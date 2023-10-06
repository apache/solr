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

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import org.apache.solr.common.util.NamedList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Single CircuitBreaker that registers both a Memory and a LoadAverage CircuitBreaker. This is only
 * for backward compatibility with the 9.x versions prior to 9.4.
 *
 * @deprecated Use individual Circuit Breakers instead
 */
@Deprecated(since = "9.4")
public class CircuitBreakerManager extends CircuitBreaker {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private boolean cpuEnabled;
  private boolean memEnabled;
  private int memThreshold = 100;
  private int cpuThreshold = 100;
  private MemoryCircuitBreaker memCB;
  private LoadAverageCircuitBreaker cpuCB;

  public CircuitBreakerManager() {
    super();
  }

  @Override
  public boolean isTripped() {
    return (memEnabled && memCB.isTripped()) || (cpuEnabled && cpuCB.isTripped());
  }

  @Override
  public String getErrorMessage() {
    StringBuilder sb = new StringBuilder();
    if (memEnabled) {
      sb.append(memCB.getErrorMessage());
    }
    if (memEnabled && cpuEnabled) {
      sb.append("\n");
    }
    if (cpuEnabled) {
      sb.append(cpuCB.getErrorMessage());
    }
    return sb.toString();
  }

  @Override
  public void init(NamedList<?> args) {
    super.init(args);
    log.warn("CircuitBreakerManager is deprecated. Use individual Circuit Breakers instead");
    if (memEnabled) {
      memCB = new MemoryCircuitBreaker();
      memCB.setThreshold(memThreshold);
    }
    if (cpuEnabled) {
      // In SOLR-15056 CPUCircuitBreaker was renamed to LoadAverageCircuitBreaker, need back-compat
      cpuCB = new LoadAverageCircuitBreaker();
      cpuCB.setThreshold(cpuThreshold);
    }
  }

  @Override
  public void close() throws IOException {
    try {
      if (memEnabled) {
        memCB.close();
      }
    } finally {
      if (cpuEnabled) {
        cpuCB.close();
      }
    }
  }

  // The methods below will be called by super class during init
  public void setMemEnabled(String enabled) {
    this.memEnabled = Boolean.getBoolean(enabled);
  }

  public void setMemThreshold(int threshold) {
    this.memThreshold = threshold;
  }

  public void setMemThreshold(String threshold) {
    this.memThreshold = Integer.parseInt(threshold);
  }

  public void setCpuEnabled(String enabled) {
    this.cpuEnabled = Boolean.getBoolean(enabled);
  }

  public void setCpuThreshold(int threshold) {
    this.cpuThreshold = threshold;
  }

  public void setCpuThreshold(String threshold) {
    this.cpuThreshold = Integer.parseInt(threshold);
  }
}
