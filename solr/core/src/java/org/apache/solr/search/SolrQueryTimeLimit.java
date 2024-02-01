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
import org.apache.lucene.index.QueryTimeout;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.request.SolrQueryRequest;

public class SolrQueryTimeLimit implements QueryTimeout {

  private final long timeoutAt;

  /**
   * Create an object to represent a time limit for the current request.
   *
   * @param req A sollr request that has a value for {@code timeAllowed}
   * @throws IllegalArgumentException if the request does not contain timeAllowed parameter. This
   *     should be validated with {@link #hasTimeLimit(SolrQueryRequest)} prior to constructing this
   *     object
   */
  public SolrQueryTimeLimit(SolrQueryRequest req) {
    // reduce by time already spent
    long reqTimeAllowed = req.getParams().getLong(CommonParams.TIME_ALLOWED, -1L);

    if (reqTimeAllowed == -1L) {
      throw new IllegalArgumentException(
          "Check for limit with hasTimeLimit(req) before creating a SolrQuerTimeLimit");
    }
    long timeAllowed = reqTimeAllowed - (long) req.getRequestTimer().getTime();
    long nanosAllowed = TimeUnit.NANOSECONDS.convert(timeAllowed, TimeUnit.MILLISECONDS);
    timeoutAt = nanoTime() + nanosAllowed;
  }

  static boolean hasTimeLimit(SolrQueryRequest req) {
    return req.getParams().getLong(CommonParams.TIME_ALLOWED, -1L) >= 0L;
  }

  @Override
  public boolean shouldExit() {
    return timeoutAt - nanoTime() < 0L;
  }
}
