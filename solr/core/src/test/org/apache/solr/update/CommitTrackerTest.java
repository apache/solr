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
package org.apache.solr.update;

import java.lang.invoke.MethodHandles;
import java.util.Random;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.util.LogLevel;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@LogLevel("org.apache.solr.update=INFO")
public class CommitTrackerTest extends SolrTestCaseJ4 {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @Test
  public void testAlignCommitTime() {
    long commitMaxTime = 60000;

    long time1 = CommitTracker.alignCommitMaxTime("A", commitMaxTime, 0);
    assertTrue(time1 >= commitMaxTime / 2 && time1 < commitMaxTime * 3 / 2);
    long time2 = CommitTracker.alignCommitMaxTime("A", commitMaxTime, 1000);
    assertTrue(time2 >= commitMaxTime / 2 && time2 < commitMaxTime * 3 / 2);
    // jitter should be same for same collection now 1sec has past hence adjusted time should be
    // 1sec less (wait for 1 less sec when in the same commit frame)
    // This COULD fail if the jitter is > 59 sec, but we know that's not the case for "A"
    assertEquals(time1 - 1000, time2);

    long time3 = CommitTracker.alignCommitMaxTime("B", commitMaxTime, 0);
    assertTrue(time3 >= commitMaxTime / 2 && time3 < commitMaxTime * 3 / 2);
    assertNotEquals(time1, time3); // different collection different jitter

    for (int i = 0; i < 1000; i++) {
      String coll = generateRandomString();
      long waitTime = CommitTracker.alignCommitMaxTime(coll, commitMaxTime, 0);
      assertTrue(waitTime >= commitMaxTime / 2 && waitTime < commitMaxTime * 3 / 2);
      long nextWaitTime =
          CommitTracker.alignCommitMaxTime(
              coll,
              commitMaxTime,
              waitTime); // right at the commit point, it should ask to wait for full 60sec again
      assertEquals(commitMaxTime, nextWaitTime);
    }
  }

  private static String generateRandomString() {
    int length = 10;
    Random random = random();
    StringBuilder sb = new StringBuilder(length);
    for (int i = 0; i < length; i++) {
      char randomChar = (char) ('a' + random.nextInt(26));
      sb.append(randomChar);
    }

    return sb.toString();
  }
}
