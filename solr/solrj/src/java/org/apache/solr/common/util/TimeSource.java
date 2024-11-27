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
package org.apache.solr.common.util;

import java.util.concurrent.TimeUnit;

/**
 * Source of time.
 *
 * <p>NOTE: depending on implementation returned values may not be related in any way to the current
 * Epoch or calendar time, and they may even be negative - but the API guarantees that they are
 * always monotonically increasing.
 */
public abstract class TimeSource {

  /**
   * Return a time value, in nanosecond units. Depending on implementation this value may or may not
   * be related to Epoch time.
   */
  public abstract long getTimeNs();

  /**
   * Return Epoch time. Implementations that are not natively based on epoch time may return values
   * that are consistently off by a (small) fixed number of milliseconds from the actual epoch time.
   */
  public abstract long getEpochTimeNs();

  /**
   * Return both the source's time value and the corresponding epoch time value. This method ensures
   * that epoch time calculations use the same internal value of time as that reported by {@link
   * #getTimeNs()}.
   *
   * @return an array where the first element is {@link #getTimeNs()} and the second element is
   *     {@link #getEpochTimeNs()}.
   */
  public abstract long[] getTimeAndEpochNs();

  /**
   * Sleep according to this source's notion of time. E.g. accelerated time source such as {@link
   * TimeSources.SimTimeSource} will sleep proportionally shorter, according to its multiplier.
   *
   * @param ms number of milliseconds to sleep
   * @throws InterruptedException when the current thread is interrupted
   */
  public abstract void sleep(long ms) throws InterruptedException;

  /**
   * This method allows using TimeSource with APIs that require providing just plain time intervals,
   * e.g. {@link Object#wait(long)}. Values returned by this method are adjusted according to the
   * time source's notion of time - e.g. accelerated time source provided by {@link
   * TimeSources.SimTimeSource} will return intervals that are proportionally shortened by the
   * multiplier.
   *
   * <p>NOTE: converting small values may significantly affect precision of the returned values due
   * to rounding, especially for accelerated time source, so care should be taken to use time units
   * that result in relatively large values. For example, converting a value of 1 expressed in
   * seconds would result in less precision than converting a value of 1000 expressed in
   * milliseconds.
   *
   * @param fromUnit source unit
   * @param value original value
   * @param toUnit target unit
   * @return converted value, possibly scaled by the source's notion of accelerated time (see {@link
   *     TimeSources.SimTimeSource})
   */
  public abstract long convertDelay(TimeUnit fromUnit, long value, TimeUnit toUnit);

  @Override
  public String toString() {
    return getClass().getSimpleName();
  }
}
