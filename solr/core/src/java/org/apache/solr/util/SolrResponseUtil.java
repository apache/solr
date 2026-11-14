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
package org.apache.solr.util;

import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.util.Objects;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.handler.component.HttpShardHandler;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.handler.component.ShardResponse;
import org.apache.solr.response.SolrQueryResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SolrResponseUtil {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private SolrResponseUtil() {}

  public static Object getSubsectionFromShardResponse(
      ResponseBuilder rb, ShardResponse srsp, String shardResponseKey, boolean subSectionOptional) {
    Object shardResponseSubsection;
    try {
      SolrResponse solrResponse = srsp.getSolrResponse();
      NamedList<Object> response = solrResponse.getResponse();
      shardResponseSubsection = response.get(shardResponseKey);
      if (shardResponseSubsection != null) {
        return shardResponseSubsection;
      } else {
        NamedList<?> responseHeader =
            Objects.requireNonNull(
                (NamedList<?>) response.get(SolrQueryResponse.RESPONSE_HEADER_KEY));
        if (subSectionOptional
            || Boolean.TRUE.equals(
                responseHeader.getBooleanArg(
                    SolrQueryResponse.RESPONSE_HEADER_PARTIAL_RESULTS_KEY))) {
          return null;
        } else {
          String shardIdentifier = getShardIdentifierForLogging(srsp);
          log.warn(
              "corrupted response on {} : missing response section {}, response has {} top-level entries, shard request purpose {}",
              shardIdentifier,
              shardResponseKey,
              response.size(),
              srsp.getShardRequest().purpose);
          throw new SolrException(
              SolrException.ErrorCode.SERVER_ERROR,
              shardResponseKey
                  + " is absent in response from "
                  + shardIdentifier
                  + ", but "
                  + SolrQueryResponse.RESPONSE_HEADER_PARTIAL_RESULTS_KEY
                  + " isn't set in the response.");
        }
      }
    } catch (Exception ex) {
      if (rb != null && HttpShardHandler.getShardsTolerantAsBool(rb.req)) {
        return null;
      } else {
        throw new SolrException(
            SolrException.ErrorCode.SERVER_ERROR,
            "Unable to read "
                + shardResponseKey
                + " info for shard: "
                + getShardIdentifierForLogging(srsp),
            ex);
      }
    }
  }

  private static String getShardIdentifierForLogging(ShardResponse srsp) {
    String nodeName = srsp.getNodeName();
    if (nodeName != null) {
      return nodeName;
    }
    String shardAddress = srsp.getShardAddress();
    if (shardAddress == null) {
      return "unknown";
    }
    try {
      URI uri = URI.create(shardAddress);
      String host = uri.getHost();
      if (host == null) {
        return "unknown";
      }
      int port = uri.getPort();
      return port == -1 ? host : host + ":" + port;
    } catch (IllegalArgumentException e) {
      return "unknown";
    }
  }
}
