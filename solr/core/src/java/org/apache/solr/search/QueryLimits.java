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
import org.apache.lucene.index.QueryTimeout;
import org.apache.solr.request.SolrQueryRequest;

/**
 * Represents the limitations on the query. These limits might be wall clock time, cpu time memory,
 * or other resource limits. Exceeding any specified limit will cause {@link #shouldExit()} to
 * return true the next time it is checked (it may be checked in either Lucene code or Solr code)
 */
public class QueryLimits implements QueryTimeout {
  private final List<QueryTimeout> limits = new ArrayList<>();

  public QueryLimits(SolrQueryRequest req) {
    if (SolrQueryTimeLimit.hasTimeLimit(req)) {
      limits.add(new SolrQueryTimeLimit(req));
    }
  }

  @Override
  public boolean shouldExit() {
    return limits.stream().anyMatch(QueryTimeout::shouldExit);
  }

  public boolean isTimeoutEnabled() {
    return !limits.isEmpty();
  }
}
