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

package org.apache.solr.util;

import java.util.HashMap;
import java.util.Map;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.core.GlobalCircuitBreakerConfig;
import org.apache.solr.util.circuitbreaker.CircuitBreakerRegistry;
import org.apache.solr.util.circuitbreaker.GlobalCircuitBreakerManager;
import org.apache.solr.util.circuitbreaker.LoadAverageCircuitBreaker;
import org.apache.solr.util.circuitbreaker.MemoryCircuitBreaker;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestGlobalCircuitBreakerManager extends SolrTestCaseJ4 {
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml", "schema.xml");
  }

  @AfterClass
  public static void afterClass() throws Exception {
    // make sure there are no global breakers left in order to not interfere with other test suites
    CircuitBreakerRegistry.deregisterGlobal();
  }

  @After
  public void after() throws Exception {
    // deregister all global breakers so tests do not interfere with each other
    CircuitBreakerRegistry.deregisterGlobal();
  }

  @Test
  public void testEnableDisable() {
    GlobalCircuitBreakerManager gm = new GlobalCircuitBreakerManager(h.getCoreContainer());
    CircuitBreakerRegistry cbr = h.getCore().getCircuitBreakerRegistry();
    assertTrue(CircuitBreakerRegistry.listGlobal().isEmpty());

    GlobalCircuitBreakerConfig globalConfig = new GlobalCircuitBreakerConfig();
    GlobalCircuitBreakerConfig.CircuitBreakerConfig loadConfig =
        new GlobalCircuitBreakerConfig.CircuitBreakerConfig();
    loadConfig.enabled = true;
    loadConfig.queryThreshold = 1.0;
    globalConfig.configs.put(LoadAverageCircuitBreaker.class.getCanonicalName(), loadConfig);

    gm.onChange(getClusterProperties(globalConfig));
    assertTrue(cbr.isEnabled(SolrRequest.SolrRequestType.QUERY));
    assertTrue(containsCircuitBreakerOfClass(LoadAverageCircuitBreaker.class));

    globalConfig = new GlobalCircuitBreakerConfig();
    loadConfig = new GlobalCircuitBreakerConfig.CircuitBreakerConfig();
    loadConfig.enabled = false;
    globalConfig.configs.put(LoadAverageCircuitBreaker.class.getCanonicalName(), loadConfig);

    gm.onChange(getClusterProperties(globalConfig));
    assertTrue(CircuitBreakerRegistry.listGlobal().isEmpty());
    assertFalse(cbr.isEnabled(SolrRequest.SolrRequestType.QUERY));
  }

  @Test
  public void testDisableOne() {
    GlobalCircuitBreakerManager gm = new GlobalCircuitBreakerManager(h.getCoreContainer());
    CircuitBreakerRegistry cbr = h.getCore().getCircuitBreakerRegistry();
    assertTrue(CircuitBreakerRegistry.listGlobal().isEmpty());

    GlobalCircuitBreakerConfig globalConfig = new GlobalCircuitBreakerConfig();
    GlobalCircuitBreakerConfig.CircuitBreakerConfig loadConfig =
        new GlobalCircuitBreakerConfig.CircuitBreakerConfig();
    loadConfig.enabled = true;
    loadConfig.queryThreshold = 1.0;
    GlobalCircuitBreakerConfig.CircuitBreakerConfig memConfig =
        new GlobalCircuitBreakerConfig.CircuitBreakerConfig();
    memConfig.enabled = true;
    memConfig.updateThreshold = 1.0;
    globalConfig.configs.put(LoadAverageCircuitBreaker.class.getCanonicalName(), loadConfig);
    globalConfig.configs.put(MemoryCircuitBreaker.class.getCanonicalName(), memConfig);

    gm.onChange(getClusterProperties(globalConfig));
    assertTrue(cbr.isEnabled(SolrRequest.SolrRequestType.QUERY));
    assertTrue(cbr.isEnabled(SolrRequest.SolrRequestType.UPDATE));
    assertTrue(containsCircuitBreakerOfClass(LoadAverageCircuitBreaker.class));
    assertTrue(containsCircuitBreakerOfClass(MemoryCircuitBreaker.class));

    loadConfig = new GlobalCircuitBreakerConfig.CircuitBreakerConfig();
    loadConfig.enabled = false;
    globalConfig.configs.put(LoadAverageCircuitBreaker.class.getCanonicalName(), loadConfig);

    gm.onChange(getClusterProperties(globalConfig));
    assertFalse(cbr.isEnabled(SolrRequest.SolrRequestType.QUERY));
    assertTrue(cbr.isEnabled(SolrRequest.SolrRequestType.UPDATE));
    assertFalse(containsCircuitBreakerOfClass(LoadAverageCircuitBreaker.class));
    assertTrue(containsCircuitBreakerOfClass(MemoryCircuitBreaker.class));
  }

  @Test
  public void testBothThresholds() {
    GlobalCircuitBreakerManager gm = new GlobalCircuitBreakerManager(h.getCoreContainer());
    CircuitBreakerRegistry cbr = h.getCore().getCircuitBreakerRegistry();
    assertTrue(CircuitBreakerRegistry.listGlobal().isEmpty());

    GlobalCircuitBreakerConfig globalConfig = new GlobalCircuitBreakerConfig();
    GlobalCircuitBreakerConfig.CircuitBreakerConfig loadConfig =
        new GlobalCircuitBreakerConfig.CircuitBreakerConfig();
    loadConfig.enabled = true;
    loadConfig.queryThreshold = 1.0;
    loadConfig.updateThreshold = 1.0;
    globalConfig.configs.put(LoadAverageCircuitBreaker.class.getCanonicalName(), loadConfig);

    gm.onChange(getClusterProperties(globalConfig));
    assertTrue(cbr.isEnabled(SolrRequest.SolrRequestType.QUERY));
    assertTrue(cbr.isEnabled(SolrRequest.SolrRequestType.UPDATE));
    assertTrue(containsCircuitBreakerOfClass(LoadAverageCircuitBreaker.class));
  }

  @Test
  public void testEmptyConfig() {
    GlobalCircuitBreakerManager gm = new GlobalCircuitBreakerManager(h.getCoreContainer());
    CircuitBreakerRegistry cbr = h.getCore().getCircuitBreakerRegistry();
    assertTrue(CircuitBreakerRegistry.listGlobal().isEmpty());

    GlobalCircuitBreakerConfig globalConfig = new GlobalCircuitBreakerConfig();
    GlobalCircuitBreakerConfig.CircuitBreakerConfig loadConfig =
        new GlobalCircuitBreakerConfig.CircuitBreakerConfig();
    loadConfig.enabled = true;
    loadConfig.queryThreshold = 1.0;
    GlobalCircuitBreakerConfig.CircuitBreakerConfig memConfig =
        new GlobalCircuitBreakerConfig.CircuitBreakerConfig();
    memConfig.enabled = true;
    memConfig.updateThreshold = 1.0;
    globalConfig.configs.put(LoadAverageCircuitBreaker.class.getCanonicalName(), loadConfig);
    globalConfig.configs.put(MemoryCircuitBreaker.class.getCanonicalName(), memConfig);

    gm.onChange(getClusterProperties(globalConfig));
    assertTrue(cbr.isEnabled(SolrRequest.SolrRequestType.QUERY));
    assertTrue(cbr.isEnabled(SolrRequest.SolrRequestType.UPDATE));
    assertTrue(containsCircuitBreakerOfClass(LoadAverageCircuitBreaker.class));
    assertTrue(containsCircuitBreakerOfClass(MemoryCircuitBreaker.class));

    globalConfig = new GlobalCircuitBreakerConfig();

    gm.onChange(getClusterProperties(globalConfig));
    assertTrue(CircuitBreakerRegistry.listGlobal().isEmpty());
    assertFalse(cbr.isEnabled(SolrRequest.SolrRequestType.QUERY));
    assertFalse(cbr.isEnabled(SolrRequest.SolrRequestType.UPDATE));
  }

  @Test
  public void testHostOverride() {
    GlobalCircuitBreakerManager gm = new GlobalCircuitBreakerManager(h.getCoreContainer());
    CircuitBreakerRegistry cbr = h.getCore().getCircuitBreakerRegistry();
    assertTrue(CircuitBreakerRegistry.listGlobal().isEmpty());

    GlobalCircuitBreakerConfig globalConfig = new GlobalCircuitBreakerConfig();
    GlobalCircuitBreakerConfig.CircuitBreakerConfig loadConfig =
        new GlobalCircuitBreakerConfig.CircuitBreakerConfig();
    loadConfig.enabled = true;
    loadConfig.queryThreshold = 1.0;
    globalConfig.configs.put(LoadAverageCircuitBreaker.class.getCanonicalName(), loadConfig);

    gm.onChange(getClusterProperties(globalConfig));
    assertTrue(cbr.isEnabled(SolrRequest.SolrRequestType.QUERY));
    assertTrue(containsCircuitBreakerOfClass(LoadAverageCircuitBreaker.class));

    GlobalCircuitBreakerConfig.CircuitBreakerConfig overrideLoadConfig =
        new GlobalCircuitBreakerConfig.CircuitBreakerConfig();
    overrideLoadConfig.enabled = false;
    putHostOverride(globalConfig, "testNode", LoadAverageCircuitBreaker.class, overrideLoadConfig);

    gm.onChange(getClusterProperties(globalConfig));
    assertTrue(CircuitBreakerRegistry.listGlobal().isEmpty());
    assertFalse(cbr.isEnabled(SolrRequest.SolrRequestType.QUERY));

    overrideLoadConfig = new GlobalCircuitBreakerConfig.CircuitBreakerConfig();
    overrideLoadConfig.enabled = true;
    overrideLoadConfig.updateThreshold = 1.0;
    putHostOverride(globalConfig, "testNode", LoadAverageCircuitBreaker.class, overrideLoadConfig);

    gm.onChange(getClusterProperties(globalConfig));
    assertTrue(cbr.isEnabled(SolrRequest.SolrRequestType.UPDATE));
    assertFalse(cbr.isEnabled(SolrRequest.SolrRequestType.QUERY));
  }

  @Test
  public void testHostOverrideInvalidNode() {
    GlobalCircuitBreakerManager gm = new GlobalCircuitBreakerManager(h.getCoreContainer());
    CircuitBreakerRegistry cbr = h.getCore().getCircuitBreakerRegistry();
    assertTrue(CircuitBreakerRegistry.listGlobal().isEmpty());

    GlobalCircuitBreakerConfig globalConfig = new GlobalCircuitBreakerConfig();
    GlobalCircuitBreakerConfig.CircuitBreakerConfig loadConfig =
        new GlobalCircuitBreakerConfig.CircuitBreakerConfig();
    loadConfig.enabled = true;
    loadConfig.queryThreshold = 1.0;
    loadConfig.updateThreshold = 1.0;
    globalConfig.configs.put(LoadAverageCircuitBreaker.class.getCanonicalName(), loadConfig);

    gm.onChange(getClusterProperties(globalConfig));
    assertTrue(cbr.isEnabled(SolrRequest.SolrRequestType.QUERY));
    assertTrue(cbr.isEnabled(SolrRequest.SolrRequestType.UPDATE));
    assertTrue(containsCircuitBreakerOfClass(LoadAverageCircuitBreaker.class));

    GlobalCircuitBreakerConfig.CircuitBreakerConfig overrideLoadConfig =
        new GlobalCircuitBreakerConfig.CircuitBreakerConfig();
    overrideLoadConfig.enabled = false;
    globalConfig.configs.put(LoadAverageCircuitBreaker.class.getCanonicalName(), loadConfig);

    putHostOverride(
        globalConfig, "notANode-thatExists", LoadAverageCircuitBreaker.class, overrideLoadConfig);

    gm.onChange(getClusterProperties(globalConfig));
    assertTrue(cbr.isEnabled(SolrRequest.SolrRequestType.QUERY));
    assertTrue(cbr.isEnabled(SolrRequest.SolrRequestType.UPDATE));
    assertTrue(containsCircuitBreakerOfClass(LoadAverageCircuitBreaker.class));
  }

  private Map<String, Object> getClusterProperties(GlobalCircuitBreakerConfig config) {
    Map<String, Object> ret = new HashMap<>();
    ret.put(
        GlobalCircuitBreakerConfig.CIRCUIT_BREAKER_CLUSTER_PROPS_KEY,
        config.toMap(new HashMap<>()));
    return ret;
  }

  private boolean containsCircuitBreakerOfClass(Class<?> c) {
    return CircuitBreakerRegistry.listGlobal().stream().anyMatch(c::isInstance);
  }

  private void putHostOverride(
      GlobalCircuitBreakerConfig globalConfig,
      String hostname,
      Class<?> cbClass,
      GlobalCircuitBreakerConfig.CircuitBreakerConfig cbConfig) {
    Map<String, GlobalCircuitBreakerConfig.CircuitBreakerConfig> overrideMap = new HashMap<>();
    overrideMap.put(cbClass.getCanonicalName(), cbConfig);
    globalConfig.hostOverrides.put(hostname, overrideMap);
  }
}
