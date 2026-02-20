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

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Response from /node/info/system */
public class NodeSystemResponse extends SolrJerseyResponse {

  @JsonProperty public NodeSystemInfo nodeInfo;

  /** wrapper around the node info */
  public static class NodeSystemInfo {
    @JsonProperty public String host;
    @JsonProperty public String node;
    @JsonProperty public String mode;
    @JsonProperty public String zkHost;

    @JsonProperty("solr_home")
    public String solrHome;

    @JsonProperty("core_root")
    public String coreRoot;

    @JsonProperty public String environment;

    @JsonProperty(value = "environment_label")
    public String environmentLabel;

    @JsonProperty(value = "environment_color")
    public String environmentColor;

    @JsonProperty public Lucene lucene;
    @JsonProperty public JVM jvm;
    @JsonProperty public Security security;
    @JsonProperty public GPU gpu;
    @JsonProperty public Map<String, String> system;
  }

  /** /node/system/security */
  public static class Security {
    @JsonProperty public boolean tls;
    @JsonProperty public String authenticationPlugin;
    @JsonProperty public String authorizationPlugin;
    @JsonProperty public String username;
    @JsonProperty public Set<String> roles;
    @JsonProperty public Set<String> permissions;
  }

  /** /node/system/lucene */
  public static class Lucene {
    @JsonProperty("solr-spec-version")
    public String solrSpecVersion;

    @JsonProperty("solr-impl-version")
    public String solrImplVersion;

    @JsonProperty("lucene-spec-version")
    public String luceneSpecVersion;

    @JsonProperty("lucene-impl-version")
    public String luceneImplVersion;
  }

  /** /node/system/jvm */
  public static class JVM extends Vendor {
    @JsonProperty public int processors;
    @JsonProperty public Vendor jre;
    @JsonProperty public Vendor spec;
    @JsonProperty public Vendor vm;
    @JsonProperty public JvmJmx jmx;
    @JsonProperty public JvmMemory memory;
  }

  public static class JvmMemory {
    @JsonProperty public String free;
    @JsonProperty public String total;
    @JsonProperty public String max;
    @JsonProperty public String used;
    @JsonProperty public JvmMemoryRaw raw;
  }

  public static class JvmMemoryRaw extends MemoryRaw {
    @JsonProperty public long max;

    @JsonProperty("used%")
    public double usedPercent;
  }

  public static class MemoryRaw {
    @JsonProperty public long free;
    @JsonProperty public long total;
    @JsonProperty public long used;
  }

  public static class Vendor {
    @JsonProperty public String name;
    @JsonProperty public String vendor;
    @JsonProperty public String version;
  }

  public static class JvmJmx {
    @JsonProperty public String classpath;
    @JsonProperty public Date startTime;
    @JsonProperty public long upTimeMS;
    @JsonProperty public List<String> commandLineArgs;
  }

  public static class GPU {
    @JsonProperty public boolean available;
    @JsonProperty public long count;
    @JsonProperty public MemoryRaw memory;
    @JsonProperty public Map<String, Object> devices;
  }
}
