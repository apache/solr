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

package org.apache.solr.model.api.response;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Map;

/**
 * A value type representing an error.
 *
 * <p>Based on the fields exposed in responses from Solr's v1/requestHandler API.
 */
public class ErrorInfo {

  // TODO These are used by some classes in 'solrj' - should we just duplicate, or should we have solrj rely on solrj-model directly?
  // (though if so, how do we avoid pulling in jackson-annotations - is a dependency exclude sufficient for that?)
  public static final String ROOT_ERROR_CLASS = "root-error-class";
  public static final String ERROR_CLASS = "error-class";

  @JsonProperty("metadata")
  public ErrorMetadata metadata;

  @JsonProperty("details")
  public List<Map<String, Object>> details;

  @JsonProperty("msg")
  public String msg;

  @JsonProperty("trace")
  public String trace;

  @JsonProperty("code")
  public Integer code;

  public static class ErrorMetadata {
    @JsonProperty(ERROR_CLASS)
    public String errorClass;

    @JsonProperty(ROOT_ERROR_CLASS)
    public String rootErrorClass;
  }
}
