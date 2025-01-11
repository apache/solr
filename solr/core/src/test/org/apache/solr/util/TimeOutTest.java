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

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.solr.SolrTestCase;
import org.apache.solr.common.util.TimeSource;

public class TimeOutTest extends SolrTestCase {
  public void testHasTimedOut() {
    TimeSource mockTimeSource = mock(TimeSource.class);
    when(mockTimeSource.getTimeNs()).thenReturn(Long.valueOf(10)).thenReturn(Long.valueOf(50));

    TimeOut timeOut = new TimeOut(20, NANOSECONDS, mockTimeSource);
    assertTrue(timeOut.hasTimedOut());
  }

  public void testHasNotTimedOut() {
    TimeSource mockTimeSource = mock(TimeSource.class);
    when(mockTimeSource.getTimeNs()).thenReturn(Long.valueOf(10)).thenReturn(Long.valueOf(11));

    TimeOut timeOut = new TimeOut(20, NANOSECONDS, mockTimeSource);
    assertFalse(timeOut.hasTimedOut());
  }

  public void testTimeLeft() {
    TimeSource mockTimeSource = mock(TimeSource.class);
    when(mockTimeSource.getTimeNs()).thenReturn(Long.valueOf(10)).thenReturn(Long.valueOf(15));

    TimeOut timeOut = new TimeOut(90, NANOSECONDS, mockTimeSource);
    assertEquals(timeOut.timeLeft(NANOSECONDS), 85);
  }

  public void testTimeElapsed() {
    TimeSource mockTimeSource = mock(TimeSource.class);
    when(mockTimeSource.getTimeNs()).thenReturn(Long.valueOf(10)).thenReturn(Long.valueOf(25));

    TimeOut timeOut = new TimeOut(70, NANOSECONDS, mockTimeSource);
    assertEquals(timeOut.timeElapsed(NANOSECONDS), 15);
  }
}
