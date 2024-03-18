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

import static org.apache.solr.search.CpuAllowedLimit.hasCpuLimit;
import static org.apache.solr.search.TimeAllowedLimit.hasTimeLimit;

import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.index.QueryTimeout;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.util.TestInjection;

/**
 * Represents the limitations on the query. These limits might be wall clock time, cpu time, memory,
 * or other resource limits. Exceeding any specified limit will cause {@link #shouldExit()} to
 * return true the next time it is checked (it may be checked in either Lucene code or Solr code)
 */
public class QueryLimits implements QueryTimeout {
  public static final String UNLIMITED = "This request is unlimited.";
  private final List<QueryTimeout> limits =
      new ArrayList<>(3); // timeAllowed, cpu, and memory anticipated

  public static QueryLimits NONE = new QueryLimits();

  private final SolrQueryResponse rsp;
  private final boolean allowPartialResults;

  // short-circuit the checks if any limit has been tripped
  private volatile boolean limitsTripped = false;

  private QueryLimits() {
    this(null, null);
  }

  /**
   * Implementors of a Query Limit should add an if block here to activate it, and typically this if
   * statement will hinge on hasXXXLimit() static method attached to the implementation class.
   *
   * @param req the current SolrQueryRequest.
   * @param rsp the current SolrQueryResponse.
   */
  public QueryLimits(SolrQueryRequest req, SolrQueryResponse rsp) {
    this.rsp = rsp;
    this.allowPartialResults =
        req != null ? req.getParams().getBool(CommonParams.PARTIAL_RESULTS, true) : true;
    if (req != null) {
      if (hasTimeLimit(req)) {
        limits.add(new TimeAllowedLimit(req));
      }
      if (hasCpuLimit(req)) {
        limits.add(new CpuAllowedLimit(req));
      }
    }
    // for testing
    if (TestInjection.queryTimeout != null) {
      limits.add(TestInjection.queryTimeout);
    }
  }

  @Override
  public boolean shouldExit() {
    if (limitsTripped) {
      return true;
    }
    for (QueryTimeout limit : limits) {
      if (limit.shouldExit()) {
        limitsTripped = true;
        break;
      }
    }
    return limitsTripped;
  }

  /**
   * Format an exception message with optional label and details from {@link #limitStatusMessage()}.
   */
  public String formatExceptionMessage(String label) {
    return "Limits exceeded!"
        + (label != null ? " (" + label + ")" : "")
        + ": "
        + limitStatusMessage();
  }

  /**
   * If limit is reached then depending on the request param {@link CommonParams#PARTIAL_RESULTS}
   * either mark it as partial result in the response and signal the caller to return, or throw an
   * exception.
   *
   * @param label optional label to indicate the caller.
   * @return true if the caller should stop processing and return partial results, false otherwise.
   * @throws QueryLimitsExceededException if {@link CommonParams#PARTIAL_RESULTS} request parameter
   *     is false and limits have been reached.
   */
  public boolean maybeExitWithPartialResults(String label) throws QueryLimitsExceededException {
    if (isLimitsEnabled() && shouldExit()) {
      if (allowPartialResults) {
        if (rsp != null) {
          rsp.setPartialResults();
          rsp.addPartialResponseDetail(formatExceptionMessage(label));
        }
        return true;
      } else {
        throw new QueryLimitsExceededException(formatExceptionMessage(label));
      }
    } else {
      return false;
    }
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
    if (limits.isEmpty()) {
      return UNLIMITED;
    }
    StringBuilder sb = new StringBuilder("Query limits: ");
    for (QueryTimeout limit : limits) {
      sb.append("[");
      sb.append(limit.getClass().getSimpleName());
      sb.append(":");
      sb.append(limit.shouldExit() ? "LIMIT EXCEEDED" : "within limit");
      sb.append("]");
    }
    return sb.toString();
  }

  /** Return true if there are any limits enabled for the current request. */
  public boolean isLimitsEnabled() {
    return !limits.isEmpty();
  }

  /**
   * Helper method to retrieve the current QueryLimits from {@link SolrRequestInfo#getRequestInfo()}
   * if it exists, otherwise it returns {@link #NONE}.
   */
  public static QueryLimits getCurrentLimits() {
    final SolrRequestInfo info = SolrRequestInfo.getRequestInfo();
    return info != null ? info.getLimits() : NONE;
  }
}
