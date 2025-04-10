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
package org.apache.solr.client.solrj.impl;

import java.util.concurrent.TimeUnit;

/**
 * A fake time source for testing that allows precise control over the passage of time. Instead of
 * relying on real system time and Thread.sleep(), tests can use this to advance time manually and
 * test time-dependent logic deterministically.
 */
public class FakeTimeSource implements TimeSource {

  private long nanoTime = 0;

  @Override
  public long nanoTime() {
    return nanoTime;
  }

  /**
   * Advances the fake time by the specified number of nanoseconds.
   *
   * @param nanos The number of nanoseconds to advance the clock
   */
  public void advanceNanos(long nanos) {
    if (nanos < 0) {
      throw new IllegalArgumentException("Cannot advance time backwards");
    }
    nanoTime += nanos;
  }

  /**
   * Advances the fake time by the specified time amount.
   *
   * @param time The amount of time to advance
   * @param unit The time unit
   */
  public void advance(long time, TimeUnit unit) {
    advanceNanos(unit.toNanos(time));
  }

  /**
   * Advances the fake time by the specified number of milliseconds.
   *
   * @param millis The number of milliseconds to advance the clock
   */
  public void advanceMillis(long millis) {
    advance(millis, TimeUnit.MILLISECONDS);
  }
}
