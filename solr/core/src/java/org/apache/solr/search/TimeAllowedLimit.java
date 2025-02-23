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

import static java.lang.System.nanoTime;

import java.util.concurrent.TimeUnit;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.request.SolrQueryRequest;

/**
 * Enforces a wall clock based timeout on a given SolrQueryRequest. This class holds the logic for
 * the {@code timeAllowed} query parameter. Note that timeAllowed will be ignored for
 * <strong><em>local</em></strong> processing of sub-queries in cases where the parent query already
 * has {@code timeAllowed} set. Essentially only one timeAllowed can be specified for any thread
 * executing a query. This is to ensure that subqueries don't escape from the intended limit
 */
public class TimeAllowedLimit implements QueryLimit {

  private final long timeoutAt;
  private final long timingSince;

  /**
   * Create an object to represent a time limit for the current request.
   *
   * @param req A solr request that has a value for {@code timeAllowed}
   * @throws IllegalArgumentException if the request does not contain timeAllowed parameter. This
   *     should be validated with {@link #hasTimeLimit(SolrQueryRequest)} prior to constructing this
   *     object
   */
  public TimeAllowedLimit(SolrQueryRequest req) {
    // reduce by time already spent
    long reqTimeAllowed = req.getParams().getLong(CommonParams.TIME_ALLOWED, -1L);

    if (reqTimeAllowed == -1L) {
      throw new IllegalArgumentException(
          "Check for limit with hasTimeLimit(req) before creating a TimeAllowedLimit");
    }
    long timeAlreadySpent = (long) req.getRequestTimer().getTime();
    long now = nanoTime();
    long timeAllowed = reqTimeAllowed - timeAlreadySpent;
    long nanosAllowed = TimeUnit.NANOSECONDS.convert(timeAllowed, TimeUnit.MILLISECONDS);
    timeoutAt = now + nanosAllowed;
    timingSince = now - timeAlreadySpent;
  }

  /** Return true if the current request has a parameter with a valid value of the limit. */
  static boolean hasTimeLimit(SolrQueryRequest req) {
    return req.getParams().getLong(CommonParams.TIME_ALLOWED, -1L) >= 0L;
  }

  /** Return true if a max limit value is set and the current usage has exceeded the limit. */
  @Override
  public boolean shouldExit() {
    return timeoutAt - nanoTime() < 0L;
  }

  @Override
  public Object currentValue() {
    return nanoTime() - timingSince;
  }
}
