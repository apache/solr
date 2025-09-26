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

import java.lang.invoke.MethodHandles;
import java.util.concurrent.TimeUnit;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.handler.component.ShardRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Enforces a wall clock based timeout on a given SolrQueryRequest. This class holds the logic for
 * the {@code timeAllowed} query parameter. Note that timeAllowed will be ignored for
 * <strong><em>local</em></strong> processing of sub-queries in cases where the parent query already
 * has {@code timeAllowed} set. Essentially only one timeAllowed can be specified for any thread
 * executing a query. This is to ensure that subqueries don't escape from the intended limit.
 * <p>Distributed requests will approximately track the original starting point of the parent request.
 * Shard requests may be skipped if the limit would run out shortly after they are sent - this in-flight
 * allowance is determined by {@link #INFLIGHT_PARAM} in milliseconds, with the default value of {@link #DEFAULT_INFLIGHT_MS}.</p>
 */
public class TimeAllowedLimit implements QueryLimit {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final String USED_PARAM = "_taUsed";
  public static final String INFLIGHT_PARAM = "timeAllowed.inflight";

  /** Arbitrary small amount of time to account for network flight time in ms */
  public static final long DEFAULT_INFLIGHT_MS = 2L;

  private final long reqTimeAllowedMs;
  private final long reqInflightMs;
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
    // original timeAllowed in milliseconds
    reqTimeAllowedMs = req.getParams().getLong(CommonParams.TIME_ALLOWED, -1L);

    if (reqTimeAllowedMs == -1L) {
      throw new IllegalArgumentException(
          "Check for limit with hasTimeLimit(req) before creating a TimeAllowedLimit");
    }
    // time already spent locally before this limit was initialized, in milliseconds
    long timeAlreadySpentMs = (long) req.getRequestTimer().getTime();
    long parentUsedMs = req.getParams().getLong(USED_PARAM, -1L);
    reqInflightMs = req.getParams().getLong(INFLIGHT_PARAM, DEFAULT_INFLIGHT_MS);
    if (parentUsedMs != -1L) {
      // this is a sub-request of a request that already had timeAllowed set.
      // We have to deduct the time already used by the parent request.
      log.debug("parentUsedMs: {}", parentUsedMs);
      timeAlreadySpentMs += parentUsedMs;
    }
    long nowNs = nanoTime();
    long remainingTimeAllowedMs = reqTimeAllowedMs - timeAlreadySpentMs;
    log.debug("remainingTimeAllowedMs: {}", remainingTimeAllowedMs);
    long remainingTimeAllowedNs = TimeUnit.MILLISECONDS.toNanos(remainingTimeAllowedMs);
    timeoutAt = nowNs + remainingTimeAllowedNs;
    timingSince = nowNs - TimeUnit.MILLISECONDS.toNanos(timeAlreadySpentMs);
  }

  @Override
  public boolean adjustShardRequestLimit(ShardRequest sreq, String shard, ModifiableSolrParams params) {
    long usedTimeAllowedMs = TimeUnit.NANOSECONDS.toMillis(nanoTime() - timingSince);
    // increase by the expected in-flight time
    usedTimeAllowedMs += reqInflightMs;
    boolean result = false;
    if (usedTimeAllowedMs >= reqTimeAllowedMs) {
      // there's no point in sending this request to the shard because the time will run out
      // before it's processed at the target
      result = true;
    }
    params.set(USED_PARAM, Long.toString(usedTimeAllowedMs));
    log.debug("adjustShardRequestLimit: used {} ms (skip? {})", usedTimeAllowedMs, result);
    return result;
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

  /** Return elapsed time in nanoseconds since this limit was started. */
  @Override
  public Object currentValue() {
    return nanoTime() - timingSince;
  }
}
