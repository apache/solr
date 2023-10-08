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

package org.apache.solr.client.solrj.response;

import java.util.Map;
import org.apache.solr.common.SolrException;

/** Delegation Token responses */
public abstract class DelegationTokenResponse extends SolrResponseBase {

  public static class Get extends DelegationTokenResponse {

    /** Get the urlString to be used as the delegation token */
    public String getDelegationToken() {
      try {
        Map<?, ?> map = (Map<?, ?>) getResponse().get("Token");
        if (map != null) {
          return (String) map.get("urlString");
        }
      } catch (ClassCastException e) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "parsing error", e);
      }
      return null;
    }
  }

  public static class Renew extends DelegationTokenResponse {
    public Long getExpirationTime() {
      try {
        return (Long) getResponse().get("long");
      } catch (ClassCastException e) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "parsing error", e);
      }
    }
  }

  public static class Cancel extends DelegationTokenResponse {}
}
