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
import java.util.Objects;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.util.NamedList;
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
          log.warn("corrupted response on {} : {}", srsp.getShardRequest(), solrResponse);
          throw new SolrException(
              SolrException.ErrorCode.SERVER_ERROR,
              shardResponseKey
                  + " is absent in response from "
                  + srsp.getNodeName()
                  + ", but "
                  + SolrQueryResponse.RESPONSE_HEADER_PARTIAL_RESULTS_KEY
                  + " isn't set in the response.");
        }
      }
    } catch (Exception ex) {
      if (rb != null && ShardParams.getShardsTolerantAsBool(rb.req.getParams())) {
        return null;
      } else {
        throw new SolrException(
            SolrException.ErrorCode.SERVER_ERROR,
            "Unable to read " + shardResponseKey + " info for shard: " + srsp.getShard(),
            ex);
      }
    }
  }
}
