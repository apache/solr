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
