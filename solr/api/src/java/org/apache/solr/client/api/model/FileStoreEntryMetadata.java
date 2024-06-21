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
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import org.apache.solr.client.api.util.ReflectWritable;

public class FileStoreEntryMetadata implements ReflectWritable {
  @JsonProperty public String name;
  @JsonProperty public Boolean dir;
  @JsonProperty public Long size;
  @JsonProperty public Date timestamp;

  private Map<String, Object> additionalMetadata = new HashMap<>();

  @JsonAnyGetter
  public Map<String, Object> unknownProperties() {
    return additionalMetadata;
  }

  @JsonAnySetter
  public void setUnknownProperty(String field, Object value) {
    additionalMetadata.put(field, value);
  }
}
