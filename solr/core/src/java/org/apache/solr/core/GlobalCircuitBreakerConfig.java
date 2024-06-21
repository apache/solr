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

package org.apache.solr.core;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.solr.common.annotation.JsonProperty;
import org.apache.solr.common.util.ReflectMapWriter;

public class GlobalCircuitBreakerConfig implements ReflectMapWriter {
  public static final String CIRCUIT_BREAKER_CLUSTER_PROPS_KEY = "circuit-breakers";
  @JsonProperty public Map<String, CircuitBreakerConfig> configs = new HashMap<>();

  @JsonProperty
  public Map<String, Map<String, CircuitBreakerConfig>> hostOverrides = new HashMap<>();

  public static class CircuitBreakerConfig implements ReflectMapWriter {
    @JsonProperty public Boolean enabled = false;
    @JsonProperty public Boolean warnOnly = false;
    @JsonProperty public Double updateThreshold;
    @JsonProperty public Double queryThreshold;

    @Override
    public int hashCode() {
      return Objects.hash(enabled, warnOnly, updateThreshold, queryThreshold);
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof CircuitBreakerConfig) {
        CircuitBreakerConfig that = (CircuitBreakerConfig) obj;
        return that.enabled.equals(this.enabled)
            && that.warnOnly.equals(this.warnOnly)
            && that.updateThreshold.equals(this.updateThreshold)
            && that.queryThreshold.equals(this.queryThreshold);
      }
      return false;
    }
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof GlobalCircuitBreakerConfig) {
      GlobalCircuitBreakerConfig that = (GlobalCircuitBreakerConfig) o;
      return that.configs.equals(this.configs) && that.hostOverrides.equals(this.hostOverrides);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(configs, hostOverrides);
  }
}
