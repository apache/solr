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

package org.apache.solr.servlet;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.request.beans.RateLimiterPayload;
import org.apache.solr.core.RateLimiterConfig;
import org.apache.solr.util.SolrJacksonAnnotationInspector;

/**
 * Implementation of RequestRateLimiter specific to query request types. Most of the actual work is
 * delegated to the parent class but specific configurations and parsing are handled by this class.
 */
public class QueryRateLimiter extends RequestRateLimiter {
  private static final ObjectMapper mapper = SolrJacksonAnnotationInspector.createObjectMapper();

  public QueryRateLimiter(RateLimiterConfig config) {
    super(config);
  }

  public static RateLimiterConfig processConfigChange(
      RateLimiterConfig rateLimiterConfig, RateLimiterPayload rateLimiterMeta) throws IOException {

    // default rate limiter
    SolrRequest.SolrRequestType requestType = SolrRequest.SolrRequestType.QUERY;
    if (rateLimiterMeta.priorityBasedEnabled) {
      requestType = SolrRequest.SolrRequestType.PRIORITY_BASED;
    }

    if (rateLimiterConfig == null || rateLimiterConfig.shouldUpdate(rateLimiterMeta)) {
      // no prior config, or config has changed; return the new config
      return new RateLimiterConfig(requestType, rateLimiterMeta);
    } else {
      return null;
    }
  }
}
