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
package org.apache.solr.client.api.model;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;
import io.swagger.v3.oas.annotations.media.Schema;
import java.util.HashMap;
import java.util.Map;
import org.apache.solr.client.api.util.ReflectWritable;

/**
 * Response body for {@code GET /api/cluster/commands/{requestId}}.
 *
 * <p>Completed and failed commands also flatten the original command response (for example {@code
 * success} / {@code failure} sub-responses) into this object as additional properties.
 */
public class GetClusterCommandStatusResponse extends SolrJerseyResponse {

  @JsonProperty("status")
  @Schema(description = "The current state of the asynchronous request and a descriptive message.")
  public CommandStatus status;

  private Map<String, Object> unknownFields = new HashMap<>();

  @JsonAnyGetter
  public Map<String, Object> unknownProperties() {
    return unknownFields;
  }

  @JsonAnySetter
  public void setUnknownProperty(String field, Object value) {
    unknownFields.put(field, value);
  }

  /** Nested {@code status} object returned by REQUESTSTATUS. */
  public static class CommandStatus implements ReflectWritable {
    @JsonProperty("state")
    @Schema(description = "Request state: submitted, running, completed, failed, or notfound.")
    public State state;

    @JsonProperty("msg")
    @Schema(description = "A message describing where the request was found, if at all.")
    public String msg;

    /**
     * The state of an asynchronous request. Mirrors {@code
     * org.apache.solr.client.solrj.response.RequestStatusState}'s constants and wire keys; kept as
     * a separate type here since this module (solr:api) cannot depend on solrj.
     */
    public enum State {
      SUBMITTED("submitted"),
      RUNNING("running"),
      COMPLETED("completed"),
      FAILED("failed"),
      NOT_FOUND("notfound");

      private final String key;

      State(String key) {
        this.key = key;
      }

      @JsonValue
      public String getKey() {
        return key;
      }

      @JsonCreator
      public static State fromKey(String key) {
        for (State state : values()) {
          if (state.key.equalsIgnoreCase(key)) {
            return state;
          }
        }
        throw new IllegalArgumentException("Unknown request status state: " + key);
      }
    }
  }
}
