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
package org.apache.solr.search;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.solr.SolrTestCaseJ4;
import org.junit.Test;

public class CallerSpecificQueryLimitTest extends SolrTestCaseJ4 {

  private static class LimitedWorker {
    private final CallerSpecificQueryLimit limit;

    static class NestedLimitedWorker {
      private final CallerSpecificQueryLimit limit;

      NestedLimitedWorker(CallerSpecificQueryLimit limit) {
        this.limit = limit;
      }

      public void doWork() {
        limit.shouldExit();
      }
    }

    private NestedLimitedWorker nestedLimitedWorker;

    LimitedWorker(CallerSpecificQueryLimit limit) {
      this.limit = limit;
      nestedLimitedWorker = new NestedLimitedWorker(limit);
    }

    public void doWork() {
      nestedLimitedWorker.doWork();
      limit.shouldExit();
    }
  }

  private static class LimitedWorker2 {
    private final CallerSpecificQueryLimit limit;

    LimitedWorker2(CallerSpecificQueryLimit limit) {
      this.limit = limit;
    }

    public void doWork() {
      limit.shouldExit();
    }
  }

  private static final boolean[] values = {true, false};

  @Test
  public void testLimits() {
    for (boolean withNested : values) {
      for (boolean withMethod : values) {
        for (boolean withCount : values) {
          for (boolean shouldTrip : values) {
            doTestWithLimit(withNested, withMethod, withCount, shouldTrip);
          }
        }
      }
    }
  }

  private void doTestWithLimit(
      boolean withNested, boolean withMethod, boolean withCount, boolean shouldTrip) {
    List<String> nonMatchingCallerExprs = new ArrayList<>();
    List<String> matchingCallCounts = new ArrayList<>();
    String matchingClassName =
        withNested
            ? LimitedWorker.NestedLimitedWorker.class.getSimpleName()
            : LimitedWorker.class.getSimpleName();
    String callerExpr = matchingClassName;
    if (withMethod) {
      callerExpr += ".doWork";
    }
    int count = 1;
    if (withCount) {
      count = random().nextInt(9) + 1;
      if (shouldTrip) {
        callerExpr += ":" + count;
      } else {
        callerExpr += ":-" + count;
      }
    } else if (!shouldTrip) {
      callerExpr += ":-1"; // no limit, should not trip
    }
    nonMatchingCallerExprs.add(LimitedWorker2.class.getSimpleName());
    nonMatchingCallerExprs.add(LimitedWorker2.class.getSimpleName() + ".doWork");
    if (withNested) {
      nonMatchingCallerExprs.add(LimitedWorker.class.getSimpleName());
    } else {
      nonMatchingCallerExprs.add(LimitedWorker.NestedLimitedWorker.class.getSimpleName());
    }
    matchingCallCounts.add(callerExpr);
    if (!withMethod) {
      matchingCallCounts.add(matchingClassName + ".doWork");
    }

    CallerSpecificQueryLimit limit = new CallerSpecificQueryLimit(callerExpr);
    LimitedWorker limitedWorker = new LimitedWorker(limit);
    LimitedWorker2 limitedWorker2 = new LimitedWorker2(limit);
    for (int i = 0; i < count * 2; i++) {
      limitedWorker2.doWork();
      limitedWorker.doWork();
    }
    Set<String> trippedBy = limit.getTrippedBy();
    if (shouldTrip) {
      assertFalse("Limit should have been tripped, callerExpr: " + callerExpr, trippedBy.isEmpty());
      for (String nonMatchingCallerExpr : nonMatchingCallerExprs) {
        assertFalse(
            "Limit should not have been tripped by "
                + nonMatchingCallerExpr
                + " but was: "
                + trippedBy,
            trippedBy.contains(nonMatchingCallerExpr));
      }
    } else {
      assertTrue(
          "Limit should not have been tripped, callerExpr: "
              + callerExpr
              + ", trippedBy: "
              + trippedBy,
          trippedBy.isEmpty());
    }
    Map<String, Integer> callCounts = limit.getCallCounts();
    for (String matchingCallCount : matchingCallCounts) {
      assertTrue(
          "Call count for " + matchingCallCount + " should be > 0, callCounts: " + callCounts,
          callCounts.getOrDefault(matchingCallCount, 0) > 0);
    }
  }
}
