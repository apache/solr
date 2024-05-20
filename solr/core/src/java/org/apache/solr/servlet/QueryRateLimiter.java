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
import java.util.ArrayList;
import java.util.List;
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

  public QueryRateLimiter(RateLimiterConfig rateLimiterConfig) {
    super(rateLimiterConfig);
  }

  public void processConfigChange(Map<String, Object> properties) throws IOException {
    RateLimiterConfig rateLimiterConfig = getRateLimiterConfig();
    byte[] configInput = Utils.toJSON(properties.get(RL_CONFIG_KEY));

    if (configInput == null || configInput.length == 0) {
      return;
    }

    RateLimiterPayload rateLimiterMeta = mapper.readValue(configInput, RateLimiterPayload.class);

    constructQueryRateLimiterConfigInternal(rateLimiterMeta, rateLimiterConfig);
  }

  // To be used in initialization
  @SuppressWarnings({"unchecked"})
  static List<RateLimiterConfig> constructQueryRateLimiterConfig(SolrZkClient zkClient) {
    List<RateLimiterConfig> cfgs = new ArrayList<>();
    try {
      if (zkClient == null) {
        cfgs.add(new RateLimiterConfig());
        return cfgs;
      }

      Map<String, Object> clusterPropsJson =
          (Map<String, Object>)
              Utils.fromJSON(zkClient.getData(ZkStateReader.CLUSTER_PROPS, null, new Stat(), true));
      Object o = clusterPropsJson.get(RL_CONFIG_KEY);
      if (o == null) {
        cfgs.add(new RateLimiterConfig());
        return cfgs;
      }
      if (o instanceof List) {
        for (Object c : ((List<Object>) o)) {
          cfgs.add(parseConfig(c));
        }
      } else {
        cfgs.add(parseConfig(o));
      }
      return cfgs;
    } catch (KeeperException.NoNodeException e) {
      cfgs.add(new RateLimiterConfig());
      return cfgs;
    } catch (KeeperException | InterruptedException e) {
      throw new RuntimeException(
          "Error reading cluster property", SolrZkClient.checkInterrupted(e));
    } catch (IOException e) {
      throw new RuntimeException("Encountered an IOException " + e.getMessage());
    }
  }

  private static RateLimiterConfig parseConfig(Object o) throws IOException {
    RateLimiterConfig rateLimiterConfig = new RateLimiterConfig();
    RateLimiterPayload rateLimiterMeta =
        mapper.readValue(Utils.toJSON(o), RateLimiterPayload.class);

    constructQueryRateLimiterConfigInternal(rateLimiterMeta, rateLimiterConfig);
    return rateLimiterConfig;
  }

  private static void constructQueryRateLimiterConfigInternal(
      RateLimiterPayload rateLimiterMeta, RateLimiterConfig rateLimiterConfig) {

    if (rateLimiterMeta == null) {
      // No Rate limiter configuration defined in clusterprops.json
      return;
    }

    if (rateLimiterMeta.allowedRequests != null) {
      rateLimiterConfig.allowedRequests = rateLimiterMeta.allowedRequests.intValue();
    }

    if (rateLimiterMeta.enabled != null) {
      rateLimiterConfig.isEnabled = rateLimiterMeta.enabled;
    }

    if (rateLimiterMeta.guaranteedSlots != null) {
      rateLimiterConfig.guaranteedSlotsThreshold = rateLimiterMeta.guaranteedSlots;
    }

    if (rateLimiterMeta.slotBorrowingEnabled != null) {
      rateLimiterConfig.isSlotBorrowingEnabled = rateLimiterMeta.slotBorrowingEnabled;
    }

    if (rateLimiterMeta.slotAcquisitionTimeoutInMS != null) {
      rateLimiterConfig.waitForSlotAcquisition =
          rateLimiterMeta.slotAcquisitionTimeoutInMS.longValue();
    }
    rateLimiterConfig.requestType = SolrRequest.SolrRequestType.parse(rateLimiterMeta.type);
  }
}
