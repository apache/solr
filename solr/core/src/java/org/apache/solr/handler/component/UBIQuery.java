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
package org.apache.solr.handler.component;

/**
 * Handles all the data required for tracking a query using User Behavior Insights.
 *
 * <p>Compatible with the
 * https://github.com/o19s/ubi/blob/main/schema/1.0.0/query.request.schema.json.
 */
public class UBIQuery {

  private String queryId;
  private String userQuery;
  private Object queryAttributes;

  public UBIQuery(String queryId) {

    if (queryId == null) {
      queryId = "1234";
    }
    this.queryId = queryId;
  }

  public String getQueryId() {
    return queryId;
  }

  public void setQueryId(String queryId) {
    this.queryId = queryId;
  }

  public String getUserQuery() {
    return userQuery;
  }

  public void setUserQuery(String userQuery) {
    this.userQuery = userQuery;
  }

  public Object getQueryAttributes() {
    return queryAttributes;
  }

  public void setQueryAttributes(Object queryAttributes) {
    this.queryAttributes = queryAttributes;
  }

  /**
   * Convert the UBIQuery into the format consumed by a streaming expression tuple()
   *
   * @return String The tuple specific formatted data similar to "query_id=123,user_query=foo"
   */
  public String toTuple() {
    return UBIComponent.QUERY_ID
        + "="
        + this.queryId
        + ","
        + UBIComponent.USER_QUERY
        + "="
        + this.userQuery;
  }
}
