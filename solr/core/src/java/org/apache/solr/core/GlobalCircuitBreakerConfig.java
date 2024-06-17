package org.apache.solr.core;

import org.apache.solr.common.annotation.JsonProperty;
import org.apache.solr.common.util.ReflectMapWriter;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class GlobalCircuitBreakerConfig implements ReflectMapWriter {
    public static final String CIRCUIT_BREAKER_CLUSTER_PROPS_KEY = "circuit-breakers";
    @JsonProperty
    public Map<String, CircuitBreakerConfig> configs = new HashMap<>();

    @JsonProperty
    public Map<String, Map<String, CircuitBreakerConfig>> hostOverrides = new HashMap<>();

    public static class CircuitBreakerConfig implements ReflectMapWriter {
        @JsonProperty
        public Boolean enabled = false;
        @JsonProperty
        public Boolean warnOnly = false;
        @JsonProperty
        public Double updateThreshold;
        @JsonProperty
        public Double queryThreshold;

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