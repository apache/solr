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
package org.apache.solr.client.solrj.response;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.client.api.model.SolrJerseyResponse;
import org.apache.solr.common.util.NamedList;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.OptBoolean;

/**
 * This class represents a response from "/admin/info/system"
 */
public class SystemInfoResponse extends SolrJerseyResponse {

  private static final long serialVersionUID = 1L;

  @JsonProperty("mode")
  public String mode;

  @JsonProperty("zkHost")
  public String zkHost;
  
  @JsonProperty("solr_home")
  public String solrHome;
  
  @JsonProperty("core_root")
  public String coreRoot;

  @JsonProperty("node")
  public String node;
  
  @JsonProperty("lucene")
  public Lucene lucene;

  @JsonProperty("jvm")
  public JVM jvm;

  @JsonProperty("security")
  public Security security;
  
  private Map<String,String> system = new HashMap<>();;
  
  @JsonAnyGetter
  public Map<String, String> getSystem() {
    return system;
  }
  
  @JsonAnySetter
  public void setSystemProperty(String key, String value) {
    this.system.put(key, value);
  }
  
  /** /admin/info/system/security */
  public class Security {
    @JsonProperty("tls")
    public boolean tls;
  }

  /** /admin/info/system/lucene */
  public class Lucene {
    @JsonProperty("solr-spec-version")
    public String solrSpecVersion;
    
    @JsonProperty("solr-impl-version")
    public String solrImplVersion;
    
    @JsonProperty("lucene-spec-version")
    public String luceneSpecVersion;
    
    @JsonProperty("lucene-impl-version")
    public String luceneImplVersion;
    
  }
  
  /** /admin/info/system/jvm */
  public class JVM extends Vendor {
    @JsonProperty("processors")
    public int processors;

    @JsonProperty("jre")
    public Vendor jre;
    
    @JsonProperty("spec")
    public Vendor spec;
    
    @JsonProperty("vm")
    public Vendor vm;
  }
  
  public class JvmMemory {

    @JsonProperty("free")
    public String free;
    @JsonProperty("total")
    public String total;
    @JsonProperty("max")
    public String max;
    @JsonProperty("used")
    public String used;

    @JsonProperty("raw")
    public JvmMemoryRaw raw;
  }

  public class JvmMemoryRaw {
    @JsonProperty("free")
    public long free;
    @JsonProperty("total")
    public long total;
    @JsonProperty("max")
    public long max;
    @JsonProperty("used")
    public long used;
    @JsonProperty("used%")
    public double usedPercent;
  }
  
  
  public class Vendor {
    @JsonProperty(value="name", isRequired=OptBoolean.FALSE)
    public String name;
    @JsonProperty(value="vendor", isRequired=OptBoolean.FALSE)
    public String vendor;
    @JsonProperty(value="version")
    public String version;
  }
  
  public class JvmJmx {
    @JsonProperty(value="classpath")
    public String classpath;
    @JsonProperty(value="startTime")
    public String startTime;
    @JsonProperty(value="upTimeMS")
    public long upTimeMS;

    @JsonProperty(value="commandLineArgs")
    public List<String> commandLineArgs;
  }
}
