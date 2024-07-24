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
package org.apache.solr.metrics;

import com.codahale.metrics.Clock;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Reservoir;
import com.codahale.metrics.Snapshot;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/**
 * A special Histogram that conceptually updates at regular intervals, according to the max value of
 * a counter over the preceding interval. This is appropriate for lazily tracking ambient values
 * (like "number of outstanding active requests"), where we are interested in:
 *
 * <ul>
 *   <li><i>max</i> values over a unit-granularity time-range (as opposed to arbitrary values that
 *       could be retrieved by sampling)
 *   <li>aggregate values not skewed by activity (i.e., the number of events triggering value
 *       recording should not matter -- thus we want to record values at a constant rate)
 *   <li>not dedicating a thread to actually recording values at regular wall-clock intervals
 * </ul>
 *
 * <p>The {@link #update(long)} methods should be invoked with positive and negative args, analogous
 * to {@link com.codahale.metrics.Counter#inc(long)} and {@link
 * com.codahale.metrics.Counter#dec(long)}, respectively. The effect is the same as a histogram
 * regularly sampling a counter's max value over a given interval; but in implementation, samples
 * are lazily backfilled upon write (value change {@link #update(long)}) or read ({@link
 * #getSnapshot()}) operations.
 */
public class MaxHistogram extends Histogram {

  private final long intervalNanos;

  private final AtomicLong val = new AtomicLong();

  private final AtomicLong lastUpdate;

  private volatile long lastValue = 0;

  private final AtomicLong maxSinceLastUpdate = new AtomicLong(0);

  private final long maxBackdateNanos;

  private final Clock clock;

  private final RelayClock relayClock;

  private static class RelayClock extends Clock {

    private long nextTick;

    private RelayClock(long initialTick) {
      nextTick = initialTick;
    }

    @Override
    public long getTick() {
      return nextTick;
    }

    public void setTick(long value) {
      nextTick = value;
    }
  }

  /**
   * Constructs a new {@link MaxHistogram} instance.
   *
   * @param intervalMillis this is the unit interval at which values will be recorded into the
   *     underlying histogram (the max value for the immediately preceding interval). This
   *     granularity is somewhat arbitrary, and the default of 1000ms (1s) should be sufficient for
   *     most applications.
   * @param maxBackdateSeconds limits how far back value updates will be lazily backfilled upon
   *     value change or read access. Without this, the first value change or read access after a
   *     long period of inactivity could result in backfilling an arbitrarily large number of
   *     intervals. This should ordinarily be set to align with the time window of the reservoir (or
   *     300s/5m for the default configuration of {@link
   *     com.codahale.metrics.ExponentiallyDecayingReservoir}).
   * @param clock the backing clock to use for the reservoir
   * @param reservoirFunction because {@link MaxHistogram} internally shims the clock actually used
   *     by the underlying reservoir, the reservoir cannot be constructed directly. This function
   *     supports constructing a reservoir based on the shimmed clock
   * @return a new {@link MaxHistogram} instance
   */
  public static MaxHistogram newInstance(
      int intervalMillis,
      int maxBackdateSeconds,
      Clock clock,
      Function<Clock, Reservoir> reservoirFunction) {
    RelayClock relayClock = new RelayClock(clock.getTick());
    return new MaxHistogram(
        intervalMillis, maxBackdateSeconds, reservoirFunction.apply(relayClock), relayClock, clock);
  }

  private MaxHistogram(
      int intervalMillis,
      int maxBackdateSeconds,
      Reservoir reservoir,
      Clock relayClock,
      Clock clock) {
    super(reservoir);
    this.intervalNanos = TimeUnit.MILLISECONDS.toNanos(intervalMillis);
    if (maxBackdateSeconds == -1) {
      this.maxBackdateNanos = Long.MAX_VALUE; // effectively no limit
    } else if (TimeUnit.SECONDS.toMillis(maxBackdateSeconds) >= intervalMillis) {
      // align `maxBackdateNanos` to a multiple of `intervalNanos`
      long requestedMaxBackdate = TimeUnit.SECONDS.toNanos(maxBackdateSeconds);
      this.maxBackdateNanos = requestedMaxBackdate - (requestedMaxBackdate % intervalNanos);
    } else {
      throw new IllegalArgumentException(
          "maxBackdateSeconds must be -1 (unlimited) or >= "
              + TimeUnit.MILLISECONDS.toSeconds(intervalMillis)
              + "; found "
              + maxBackdateSeconds);
    }
    this.relayClock = (RelayClock) relayClock;
    this.clock = clock;
    this.lastUpdate = new AtomicLong(clock.getTick());
  }

  @Override
  public Snapshot getSnapshot() {
    maybeFlush(lastValue);
    return super.getSnapshot();
  }

  private boolean maybeFlush(long value) {
    long lastUpdate = this.lastUpdate.get();
    long now = clock.getTick();
    long diff = now - lastUpdate;
    if (diff >= intervalNanos) {
      if (diff > maxBackdateNanos) {
        // only update values as far back as `maxBackdateNanos`.
        // This prevents a potential update storm after the value
        // has not been updated in some time.
        diff = maxBackdateNanos;
        lastUpdate = now - maxBackdateNanos;
      }
      int intervalCount = Math.toIntExact(diff / intervalNanos);
      long backdate = lastUpdate + (intervalNanos * intervalCount);
      if (this.lastUpdate.compareAndSet(lastUpdate, backdate)) {
        long maxExtant = maxSinceLastUpdate.getAndSet(value);
        long tick = lastUpdate;
        synchronized (relayClock) {
          for (int i = intervalCount; i > 0; i--) {
            relayClock.setTick(tick += intervalNanos);
            super.update(maxExtant);
          }
          relayClock.setTick(now);
        }
        return true;
      }
    }
    return false;
  }

  @Override
  public void update(final long inc) {
    final long value = val.addAndGet(inc);
    lastValue = value;
    if (maybeFlush(value)) {
      return;
    }
    // either we're not past the interval, or someone else is responsible for
    // calling `super.update()`
    long maxExtant = maxSinceLastUpdate.get();
    long witness = maxExtant;
    while (value > maxExtant
        && (maxExtant = maxSinceLastUpdate.compareAndExchange(witness, value)) != witness) {
      // keep trying
      witness = maxExtant;
    }
  }
}
