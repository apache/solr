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
   * Implementation that uses {@link System#currentTimeMillis()}. This implementation's {@link
   * #getTimeNs()} returns the same values as {@link #getEpochTimeNs()}.
   *
   * @deprecated Use {@link TimeSources.CurrentTimeSource} instead.
   */
  @Deprecated(since = "9.8", forRemoval = true)
  public static final class CurrentTimeSource extends TimeSource {

    @Override
    @SuppressForbidden(reason = "Needed to provide timestamps based on currentTimeMillis.")
    public long getTimeNs() {
      return TimeUnit.NANOSECONDS.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS);
    }

    @Override
    public long getEpochTimeNs() {
      return getTimeNs();
    }

    @Override
    public long[] getTimeAndEpochNs() {
      long time = getTimeNs();
      return new long[] {time, time};
    }

    @Override
    public void sleep(long ms) throws InterruptedException {
      Thread.sleep(ms);
    }

    @Override
    public long convertDelay(TimeUnit fromUnit, long value, TimeUnit toUnit) {
      return toUnit.convert(value, fromUnit);
    }
  }

  /**
   * Implementation that uses {@link System#nanoTime()}. Epoch time is initialized using {@link
   * CurrentTimeSource}, and then calculated as the elapsed number of nanoseconds as measured by
   * this implementation.
   *
   * @deprecated Use {@link TimeSources.NanoTimeSource} instead.
   */
  @Deprecated(since = "9.8", forRemoval = true)
  public static final class NanoTimeSource extends TimeSource {
    private final long epochStart;
    private final long nanoStart;

    public NanoTimeSource() {
      epochStart = TimeSources.CURRENT_TIME.getTimeNs();
      nanoStart = System.nanoTime();
    }

    @Override
    public long getTimeNs() {
      return System.nanoTime();
    }

    @Override
    public long getEpochTimeNs() {
      return epochStart + getTimeNs() - nanoStart;
    }

    @Override
    public long[] getTimeAndEpochNs() {
      long time = getTimeNs();
      return new long[] {time, epochStart + time - nanoStart};
    }

    @Override
    public void sleep(long ms) throws InterruptedException {
      Thread.sleep(ms);
    }

    @Override
    public long convertDelay(TimeUnit fromUnit, long value, TimeUnit toUnit) {
      return toUnit.convert(value, fromUnit);
    }
  }

  /**
   * Implementation that uses {@link TimeSources#NANO_TIME} accelerated by a double multiplier.
   *
   * @deprecated Use {@link TimeSources.SimTimeSource} instead.
   */
  @Deprecated(since = "9.8", forRemoval = true)
  public static final class SimTimeSource extends TimeSource {

    final double multiplier;
    final long nanoStart;
    final long epochStart;

    /**
     * Create a simulated time source that runs faster than real time by a multiplier.
     *
     * @param multiplier must be greater than 0.0
     */
    public SimTimeSource(double multiplier) {
      this.multiplier = multiplier;
      epochStart = TimeSources.CURRENT_TIME.getTimeNs();
      nanoStart = TimeSources.NANO_TIME.getTimeNs();
    }

    @Override
    public long getTimeNs() {
      return nanoStart
          + Math.round((double) (TimeSources.NANO_TIME.getTimeNs() - nanoStart) * multiplier);
    }

    @Override
    public long getEpochTimeNs() {
      return epochStart + getTimeNs() - nanoStart;
    }

    @Override
    public long[] getTimeAndEpochNs() {
      long time = getTimeNs();
      return new long[] {time, epochStart + time - nanoStart};
    }

    @Override
    public void sleep(long ms) throws InterruptedException {
      ms = Math.round((double) ms / multiplier);
      Thread.sleep(ms);
    }

    @Override
    public long convertDelay(TimeUnit fromUnit, long value, TimeUnit toUnit) {
      long nano = Math.round((double) TimeUnit.NANOSECONDS.convert(value, fromUnit) / multiplier);
      return toUnit.convert(nano, TimeUnit.NANOSECONDS);
    }

    @Override
    public String toString() {
      return super.toString() + ":" + multiplier;
    }
  }

  /**
   * This instance uses {@link CurrentTimeSource} for generating timestamps.
   *
   * @deprecated Use {@link TimeSources#CURRENT_TIME} instead.
   */
  @Deprecated(since = "9.8", forRemoval = true)
  public static final TimeSource CURRENT_TIME = TimeSources.CURRENT_TIME;

  /**
   * This instance uses {@link NanoTimeSource} for generating timestamps.
   *
   * @deprecated Use {@link TimeSources#CURRENT_TIME} instead.
   */
  @Deprecated(since = "9.8", forRemoval = true)
  public static final TimeSource NANO_TIME = TimeSources.NANO_TIME;

  /**
   * Obtain an instance of time source.
   *
   * @param type supported types: <code>currentTime</code>, <code>nanoTime</code> and accelerated
   *     time with a double factor in the form of <code>simTime:FACTOR</code>, e.g. <code>
   *     simTime:2.5
   *     </code>
   * @return one of the supported types
   * @deprecated Use {@link TimeSources#get(String)} instead.
   */
  @Deprecated(since = "9.8", forRemoval = true)
  public static TimeSource get(String type) {
    return TimeSources.get(type);
  }

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
   * SimTimeSource} will sleep proportionally shorter, according to its multiplier.
   *
   * @param ms number of milliseconds to sleep
   * @throws InterruptedException when the current thread is interrupted
   */
  public abstract void sleep(long ms) throws InterruptedException;

  /**
   * This method allows using TimeSource with APIs that require providing just plain time intervals,
   * e.g. {@link Object#wait(long)}. Values returned by this method are adjusted according to the
   * time source's notion of time - e.g. accelerated time source provided by {@link SimTimeSource}
   * will return intervals that are proportionally shortened by the multiplier.
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
   *     SimTimeSource})
   */
  public abstract long convertDelay(TimeUnit fromUnit, long value, TimeUnit toUnit);

  @Override
  public String toString() {
    return getClass().getSimpleName();
  }
}
