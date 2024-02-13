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

import static org.apache.solr.search.CpuQueryTimeLimit.hasCpuLimit;
import static org.apache.solr.search.SolrQueryTimeLimit.hasTimeLimit;

import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.index.QueryTimeout;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.util.ThreadCpuTime;

/**
 * Represents the limitations on the query. These limits might be wall clock time, cpu time, memory,
 * or other resource limits. Exceeding any specified limit will cause {@link #shouldExit()} to
 * return true the next time it is checked (it may be checked in either Lucene code or Solr code)
 */
public class QueryLimits implements QueryTimeout {
  private final List<QueryTimeout> limits =
      new ArrayList<>(3); // timeAllowed, cpu, and memory anticipated

  public static QueryLimits NONE = new QueryLimits();

  // initialize here for consistency between logging and monitoring
  private ThreadCpuTime threadCpuTime = new ThreadCpuTime();

  private QueryLimits() {}

  /**
   * Implementors of a Query Limit should add an if block here to activate it, and typically this if
   * statement will hinge on hasXXXLimit() static method attached to the implementation class.
   *
   * @param req the current SolrQueryRequest.
   */
  public QueryLimits(SolrQueryRequest req) {
    if (hasTimeLimit(req)) {
      limits.add(new SolrQueryTimeLimit(req));
    }
    if (hasCpuLimit(req)) {
      limits.add(new CpuQueryTimeLimit(req, threadCpuTime));
    }
  }

  /**
   * Return the instance used for reporting and monitoring total CPU time spent on processing this
   * request and its child requests.
   */
  public ThreadCpuTime getThreadCpuTime() {
    return threadCpuTime;
  }

  @Override
  public boolean shouldExit() {
    for (QueryTimeout limit : limits) {
      if (limit.shouldExit()) {
        return true;
      }
    }
    return false;
  }

  /**
   * Method to diagnose limit exceeded. Note that while this should always list the exceeded limit,
   * it may also nominate additional limits that have been exceeded since the actual check that
   * cause the failure. This gap is intentional to avoid overly complicated (and possibly expensive)
   * tracking code that would have to run within the shouldExit method. This method should only be
   * used to report a failure since it incurs the cost of rechecking every configured limit and does
   * not short circuit.
   *
   * @return A string describing the state pass/fail state of each limit specified for this request.
   */
  public String limitStatusMessage() {
    StringBuilder sb = new StringBuilder();
    boolean first = true;
    for (QueryTimeout limit : limits) {
      if (first) {
        first = false;
        sb.append("Query limits:");
      }
      sb.append("[");
      sb.append(limit.getClass().getSimpleName());
      sb.append(":");
      sb.append(limit.shouldExit() ? "LIMIT EXCEEDED" : "within limit");
      sb.append("]");
    }
    if (sb.length() == 0) {
      return "This request is unlimited.";
    } else {
      return sb.toString();
    }
  }

  public boolean isTimeoutEnabled() {
    return !limits.isEmpty();
  }
}
