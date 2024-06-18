package org.apache.solr.util.circuitbreaker;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.Map;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.common.cloud.ClusterPropertiesListener;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.GlobalCircuitBreakerConfig;
import org.apache.solr.util.SolrJacksonAnnotationInspector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GlobalCircuitBreakerManager implements ClusterPropertiesListener {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final ObjectMapper mapper = SolrJacksonAnnotationInspector.createObjectMapper();
  private final GlobalCircuitBreakerFactory factory;
  private final CoreContainer coreContainer;
  private GlobalCircuitBreakerConfig currentConfig;

  private static volatile GlobalCircuitBreakerManager instance;

  public static void init(CoreContainer coreContainer) {
    if (instance == null) {
      synchronized (GlobalCircuitBreakerManager.class) {
        instance = new GlobalCircuitBreakerManager(coreContainer);
        coreContainer
            .getZkController()
            .getZkStateReader()
            .registerClusterPropertiesListener(instance);
      }
    }
  }

  public GlobalCircuitBreakerManager(CoreContainer coreContainer) {
    super();
    this.factory = new GlobalCircuitBreakerFactory(coreContainer);
    this.coreContainer = coreContainer;
  }

  // for registering global circuit breakers set in clusterprops
  @Override
  public boolean onChange(Map<String, Object> properties) {
    try {
      GlobalCircuitBreakerConfig nextConfig = processConfigChange(properties);
      if (nextConfig != null && !nextConfig.equals(this.currentConfig)) {
        this.currentConfig = nextConfig;
        registerCircuitBreakers(nextConfig);
      }
    } catch (Exception e) {
      if (log.isWarnEnabled()) {
        // don't break when things are misconfigured
        log.warn("error parsing global circuit breaker configuration {}", e);
      }
    }
    return false;
  }

  private GlobalCircuitBreakerConfig processConfigChange(Map<String, Object> properties)
      throws IOException {
    Object cbConfig = properties.get(GlobalCircuitBreakerConfig.CIRCUIT_BREAKER_CLUSTER_PROPS_KEY);
    GlobalCircuitBreakerConfig globalCBConfig = null;
    if (cbConfig != null) {
      byte[] configInput = Utils.toJSON(cbConfig);
      if (configInput != null && configInput.length > 0) {
        globalCBConfig = mapper.readValue(configInput, GlobalCircuitBreakerConfig.class);
      }
    }
    return globalCBConfig;
  }

  private void registerCircuitBreakers(GlobalCircuitBreakerConfig gbConfig) {
    CircuitBreakerRegistry.deregisterGlobal();
    for (Map.Entry<String, GlobalCircuitBreakerConfig.CircuitBreakerConfig> entry :
        gbConfig.configs.entrySet()) {
      GlobalCircuitBreakerConfig.CircuitBreakerConfig config =
          getConfig(gbConfig, entry.getKey(), entry.getValue());
      try {
        if (config.enabled) {
          if (config.queryThreshold != null && config.queryThreshold != Double.MAX_VALUE) {
            registerGlobalCircuitBreaker(
                this.factory.create(entry.getKey()),
                config.queryThreshold,
                SolrRequest.SolrRequestType.QUERY,
                config.warnOnly);
          }
          if (config.updateThreshold != null && config.updateThreshold != Double.MAX_VALUE) {
            registerGlobalCircuitBreaker(
                this.factory.create(entry.getKey()),
                config.updateThreshold,
                SolrRequest.SolrRequestType.UPDATE,
                config.warnOnly);
          }
        }
      } catch (Exception e) {
        if (log.isWarnEnabled()) {
          log.warn("error while registering global circuit breaker {}: {}", entry.getKey(), e);
        }
      }
    }
  }

  private GlobalCircuitBreakerConfig.CircuitBreakerConfig getConfig(
      GlobalCircuitBreakerConfig gbConfig,
      String className,
      GlobalCircuitBreakerConfig.CircuitBreakerConfig globalConfig) {
    Map<String, GlobalCircuitBreakerConfig.CircuitBreakerConfig> thisHostOverrides =
        gbConfig.hostOverrides.get(getHostName());
    if (thisHostOverrides != null && thisHostOverrides.get(className) != null) {
      if (log.isInfoEnabled()) {
        log.info("overriding circuit breaker {} for host {}", className, getHostName());
      }
      return thisHostOverrides.get(className);
    }
    return globalConfig;
  }

  private String getHostName() {
    // hostname can be null if host is offline, so default to localhost
    return this.coreContainer.getHostName() != null
        ? this.coreContainer.getHostName()
        : "localhost";
  }

  private void registerGlobalCircuitBreaker(
      CircuitBreaker globalCb,
      double threshold,
      SolrRequest.SolrRequestType type,
      boolean warnOnly) {
    globalCb.setThreshold(threshold);
    globalCb.setRequestTypes(List.of(type.name()));
    globalCb.setWarnOnly(warnOnly);
    CircuitBreakerRegistry.registerGlobal(globalCb);
    if (log.isInfoEnabled()) {
      log.info("registered global circuit breaker {}", globalCb);
    }
  }
}
