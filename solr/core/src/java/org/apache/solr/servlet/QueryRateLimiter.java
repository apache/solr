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

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.Map;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.request.beans.RateLimiterPayload;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.RateLimiterConfig;
import org.apache.solr.util.SolrJacksonAnnotationInspector;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

/**
 * Implementation of RequestRateLimiter specific to query request types. Most of the actual work is
 * delegated to the parent class but specific configurations and parsing are handled by this class.
 */
public class QueryRateLimiter extends RequestRateLimiter {
  private static final ObjectMapper mapper = SolrJacksonAnnotationInspector.createObjectMapper();

  public QueryRateLimiter(SolrZkClient solrZkClient) {
    super(constructQueryRateLimiterConfig(solrZkClient));
  }

  public QueryRateLimiter(RateLimiterConfig config) {
    super(config);
  }

  public static RateLimiterConfig processConfigChange(
      SolrRequest.SolrRequestType requestType,
      RateLimiterConfig rateLimiterConfig,
      Map<String, Object> properties)
      throws IOException {
    byte[] configInput = Utils.toJSON(properties.get(RL_CONFIG_KEY));

    RateLimiterPayload rateLimiterMeta;
    if (configInput == null || configInput.length == 0) {
      rateLimiterMeta = null;
    } else {
      rateLimiterMeta = mapper.readValue(configInput, RateLimiterPayload.class);
    }

    if (rateLimiterConfig == null || rateLimiterConfig.shouldUpdate(rateLimiterMeta)) {
      // no prior config, or config has changed; return the new config
      return new RateLimiterConfig(requestType, rateLimiterMeta);
    } else {
      return null;
    }
  }

  // To be used in initialization
  @SuppressWarnings({"unchecked"})
  private static RateLimiterConfig constructQueryRateLimiterConfig(SolrZkClient zkClient) {
    try {

      if (zkClient == null) {
        return new RateLimiterConfig(SolrRequest.SolrRequestType.QUERY);
      }

      Map<String, Object> clusterPropsJson =
          (Map<String, Object>)
              Utils.fromJSON(zkClient.getData(ZkStateReader.CLUSTER_PROPS, null, new Stat(), true));
      byte[] configInput = Utils.toJSON(clusterPropsJson.get(RL_CONFIG_KEY));

      if (configInput.length == 0) {
        // No Rate Limiter configuration defined in clusterprops.json. Return default configuration
        // values
        return new RateLimiterConfig(SolrRequest.SolrRequestType.QUERY);
      }

      RateLimiterPayload rateLimiterMeta = mapper.readValue(configInput, RateLimiterPayload.class);

      return new RateLimiterConfig(SolrRequest.SolrRequestType.QUERY, rateLimiterMeta);
    } catch (KeeperException.NoNodeException e) {
      return new RateLimiterConfig(SolrRequest.SolrRequestType.QUERY);
    } catch (KeeperException | InterruptedException e) {
      throw new RuntimeException(
          "Error reading cluster property", SolrZkClient.checkInterrupted(e));
    } catch (IOException e) {
      throw new RuntimeException("Encountered an IOException " + e.getMessage());
    }
  }
}
