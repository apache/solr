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

import static org.apache.solr.core.RateLimiterConfig.RL_CONFIG_KEY;

import java.io.IOException;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.solr.client.solrj.request.beans.RateLimiterPayload;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.RateLimiterConfig;

/**
 * Implementation of RequestRateLimiter specific to query request types. Most of the actual work is
 * delegated to the parent class but specific configurations and parsing are handled by this class.
 */
public class QueryRateLimiter extends RequestRateLimiter {

  public QueryRateLimiter(Supplier<SolrZkClient.NodeData> solrZkClient) {
    super(RateLimitManager.constructQueryRateLimiterConfig(solrZkClient));
  }

  public QueryRateLimiter(RateLimiterConfig cfg) {
    super(cfg);
  }

  public void processConfigChange(Map<String, Object> properties) throws IOException {
    RateLimiterConfig rateLimiterConfig = getRateLimiterConfig();
    byte[] configInput = Utils.toJSON(properties.get(RL_CONFIG_KEY));

    if (configInput == null || configInput.length == 0) {
      return;
    }

    RateLimiterPayload rateLimiterMeta =
        RateLimitManager.mapper.readValue(configInput, RateLimiterPayload.class);

    RateLimitManager.constructQueryRateLimiterConfigInternal(rateLimiterMeta, rateLimiterConfig);
  }
}
