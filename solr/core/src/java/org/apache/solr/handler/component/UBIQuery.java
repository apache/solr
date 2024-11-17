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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;

/**
 * Handles all the data required for tracking a query using User Behavior Insights.
 *
 * <p>Compatible with the
 * https://github.com/o19s/ubi/blob/main/schema/1.2.0/query.request.schema.json.
 */
public class UBIQuery {

  private String application;
  private String queryId;
  private String userQuery;
  private Object queryAttributes;
  private String docIds;

  public UBIQuery(String queryId) {

    if (queryId == null) {
      queryId = UUID.randomUUID().toString().toLowerCase(Locale.ROOT);
    }
    this.queryId = queryId;
  }

  public void setApplication(String application) {
    this.application = application;
  }

  public String getApplication() {
    return this.application;
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

  public String getDocIds() {
    return docIds;
  }

  public void setDocIds(String docIds) {
    this.docIds = docIds;
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  public Map toMap() {
    @SuppressWarnings({"rawtypes", "unchecked"})
    Map map = new HashMap();
    map.put(UBIComponent.QUERY_ID, this.queryId);
    map.put(UBIComponent.APPLICATION, this.application);
    map.put(UBIComponent.USER_QUERY, this.userQuery);
    if (this.queryAttributes != null) {
      ObjectMapper objectMapper = new ObjectMapper();
      try {
        map.put(
            UBIComponent.QUERY_ATTRIBUTES, objectMapper.writeValueAsString(this.queryAttributes));
      } catch (JsonProcessingException e) {
        // eat it.
      }
    }

    return map;
  }
}
