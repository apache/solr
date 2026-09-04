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
package org.apache.solr.util;

import java.util.concurrent.TimeUnit;
import org.apache.solr.SolrTestCase;
import org.apache.solr.common.util.TimeSource;

/** Tests {@link TimeOut}. */
public class TimeOutTest extends SolrTestCase {

  public void testConstructorOverflowProtection() {
    TimeOut timeOut =
        new TimeOut(Long.MAX_VALUE, TimeUnit.MILLISECONDS, new MockTimeSource(0L, 0L));
    assertFalse(timeOut.hasTimedOut());
    assertTrue(
        timeOut.timeLeft(TimeUnit.MILLISECONDS) >= TimeUnit.NANOSECONDS.toMillis(Long.MAX_VALUE));
    assertEquals(0L, timeOut.timeElapsed(TimeUnit.MILLISECONDS));

    timeOut =
        new TimeOut(Long.MAX_VALUE, TimeUnit.MILLISECONDS, new MockTimeSource(Long.MIN_VALUE, 0L));
    assertFalse(timeOut.hasTimedOut());
    assertTrue(
        timeOut.timeLeft(TimeUnit.MILLISECONDS) >= TimeUnit.NANOSECONDS.toMillis(Long.MAX_VALUE));
    assertEquals(0L, timeOut.timeElapsed(TimeUnit.MILLISECONDS));
  }

  private static class MockTimeSource extends TimeSource {

    private final long timeNs;
    private final long epochTimeNs;

    MockTimeSource(long timeNs, long epochTimeNs) {
      this.timeNs = timeNs;
      this.epochTimeNs = epochTimeNs;
    }

    @Override
    public long getTimeNs() {
      return timeNs;
    }

    @Override
    public long getEpochTimeNs() {
      return epochTimeNs;
    }

    @Override
    public long[] getTimeAndEpochNs() {
      return new long[] {timeNs, epochTimeNs};
    }

    @Override
    public void sleep(long ms) {
      throw new UnsupportedOperationException();
    }

    @Override
    public long convertDelay(TimeUnit fromUnit, long value, TimeUnit toUnit) {
      return fromUnit.convert(value, toUnit);
    }
  }
}
