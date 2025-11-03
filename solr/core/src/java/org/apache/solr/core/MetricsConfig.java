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

/**
 * Configuration for metrics collection in Solr. Currently only supports both enabled and disabled
 * states. TODO: Extend to support more configuration options in the future.
 */
public class MetricsConfig {

  private final boolean enabled;

  private MetricsConfig(boolean enabled) {
    this.enabled = enabled;
  }

  public boolean isEnabled() {
    return enabled;
  }

  public static class MetricsConfigBuilder {
    private boolean enabled = true;

    public MetricsConfigBuilder() {}

    public MetricsConfigBuilder setEnabled(boolean enabled) {
      this.enabled = enabled;
      return this;
    }

    public MetricsConfig build() {
      return new MetricsConfig(enabled);
    }
  }
}
