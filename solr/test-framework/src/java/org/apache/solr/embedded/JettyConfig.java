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
package org.apache.solr.embedded;

import jakarta.servlet.Filter;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.jetty.SSLConfig;
import org.eclipse.jetty.ee10.servlet.ServletHolder;

public class JettyConfig {

  public final boolean onlyHttp1;
  public final int port;
  public final int portRetryTime;
  public final boolean stopAtShutdown;
  public final Long waitForLoadingCoresToFinishMs;
  public final Map<ServletHolder, String> extraServlets;
  public final Map<Class<? extends Filter>, String> extraFilters;
  public final SSLConfig sslConfig;
  public final boolean enableV2;
  public final boolean enableGracefulShutdown;

  /** Snapshot of the builder that built this config; enables {@link #builder(JettyConfig)}. */
  private final Builder builder;

  private JettyConfig(Builder builder) {
    this.builder = builder;
    this.onlyHttp1 = builder.onlyHttp1;
    this.port = builder.port;
    this.portRetryTime = builder.portRetryTime;
    this.stopAtShutdown = builder.stopAtShutdown;
    this.waitForLoadingCoresToFinishMs = builder.waitForLoadingCoresToFinishMs;
    this.extraServlets = Collections.unmodifiableMap(builder.extraServlets);
    this.extraFilters = Collections.unmodifiableMap(builder.extraFilters);
    this.sslConfig = builder.sslConfig;
    this.enableV2 = builder.enableV2;
    this.enableGracefulShutdown = builder.enableGracefulShutdown;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static Builder builder(JettyConfig other) {
    return other.builder.clone();
  }

  public static class Builder implements Cloneable {

    boolean onlyHttp1 = false;
    int port = 0;
    boolean enableV2 = true;
    boolean enableGracefulShutdown = false;
    boolean stopAtShutdown = true;
    Long waitForLoadingCoresToFinishMs = 300000L;
    Map<ServletHolder, String> extraServlets = new TreeMap<>();
    Map<Class<? extends Filter>, String> extraFilters = new LinkedHashMap<>();
    SSLConfig sslConfig =
        SolrTestCaseJ4.sslConfig != null ? SolrTestCaseJ4.sslConfig.buildServerSSLConfig() : null;
    int portRetryTime = 60;

    public Builder useOnlyHttp1(boolean useOnlyHttp1) {
      this.onlyHttp1 = useOnlyHttp1;
      return this;
    }

    public Builder enableV2(boolean flag) {
      this.enableV2 = flag;
      return this;
    }

    public Builder enableGracefulShutdown(boolean flag) {
      this.enableGracefulShutdown = flag;
      return this;
    }

    public Builder setPort(int port) {
      this.port = port;
      return this;
    }

    public Builder stopAtShutdown(boolean stopAtShutdown) {
      this.stopAtShutdown = stopAtShutdown;
      return this;
    }

    public Builder waitForLoadingCoresToFinish(Long waitForLoadingCoresToFinishMs) {
      this.waitForLoadingCoresToFinishMs = waitForLoadingCoresToFinishMs;
      return this;
    }

    public Builder withServlet(ServletHolder servlet, String pathSpec) {
      extraServlets.put(servlet, pathSpec);
      return this;
    }

    public Builder withServlets(Map<ServletHolder, String> servlets) {
      if (servlets != null) extraServlets.putAll(servlets);
      return this;
    }

    public Builder withFilter(Class<? extends Filter> filterClass, String pathSpec) {
      extraFilters.put(filterClass, pathSpec);
      return this;
    }

    public Builder withFilters(Map<Class<? extends Filter>, String> filters) {
      if (filters != null) extraFilters.putAll(filters);
      return this;
    }

    public Builder withSSLConfig(SSLConfig sslConfig) {
      this.sslConfig = sslConfig;
      return this;
    }

    public Builder withPortRetryTime(int portRetryTime) {
      this.portRetryTime = portRetryTime;
      return this;
    }

    /** Copies the maps too, so the clone is fully independent; the SSLConfig is shared. */
    @Override
    public Builder clone() {
      try {
        Builder clone = (Builder) super.clone();
        clone.extraServlets = new TreeMap<>(extraServlets);
        clone.extraFilters = new LinkedHashMap<>(extraFilters);
        return clone;
      } catch (CloneNotSupportedException e) {
        throw new AssertionError(e);
      }
    }

    public JettyConfig build() {
      // clone so later mutations of this builder don't leak into the built config's snapshot
      return new JettyConfig(clone());
    }
  }
}
