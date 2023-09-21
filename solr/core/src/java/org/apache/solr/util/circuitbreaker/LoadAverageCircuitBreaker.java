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

import java.lang.invoke.MethodHandles;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tracks current system load average and triggers if the specified threshold is breached.
 *
 * <p>This circuit breaker gets the load average (length of the run queue) over the last minute and
 * uses that data to take a decision. We depend on OperatingSystemMXBean which does not allow a
 * configurable interval of collection of data.
 */
public class LoadAverageCircuitBreaker extends CircuitBreaker {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final OperatingSystemMXBean operatingSystemMXBean =
      ManagementFactory.getOperatingSystemMXBean();

  private double loadAverageThreshold;

  // Assumption -- the value of these parameters will be set correctly before invoking
  // getDebugInfo()
  private static final ThreadLocal<Double> seenLoadAverage = ThreadLocal.withInitial(() -> 0.0);

  private static final ThreadLocal<Double> allowedLoadAverage = ThreadLocal.withInitial(() -> 0.0);

  public LoadAverageCircuitBreaker() {
    super();
  }

  @Override
  public boolean isTripped() {
    double localAllowedLoadAverage = getLoadAverageThreshold();
    double localSeenLoadAverage = calculateLiveLoadAverage();

    if (localSeenLoadAverage < 0) {
      if (log.isWarnEnabled()) {
        String msg = "Unable to get load average";

        log.warn(msg);
      }

      return false;
    }

    allowedLoadAverage.set(localAllowedLoadAverage);

    seenLoadAverage.set(localSeenLoadAverage);

    return (localSeenLoadAverage >= localAllowedLoadAverage);
  }

  @Override
  public String getErrorMessage() {
    return "Load Average Circuit Breaker triggered as seen load average is above allowed threshold."
        + "Seen load average "
        + seenLoadAverage.get()
        + " and allocated threshold "
        + allowedLoadAverage.get();
  }

  public void setThreshold(double thresholdValueUnbounded) {
    if (thresholdValueUnbounded <= 0) {
      throw new IllegalStateException("Threshold cannot be less than or equal to zero");
    }
    loadAverageThreshold = thresholdValueUnbounded;
  }

  public double getLoadAverageThreshold() {
    return loadAverageThreshold;
  }

  protected double calculateLiveLoadAverage() {
    return operatingSystemMXBean.getSystemLoadAverage();
  }
}
