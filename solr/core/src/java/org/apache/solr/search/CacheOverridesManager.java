package org.apache.solr.search;

import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.Utils;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A manager that gets and subscribes to ZK clusterprops.json on field "cacheOverrides" <br>
 * <br>
 * The value of the field defines a list of cache overrides, each override is a map with cache name
 * as key and a map of properties as value. An extra "collections" key can be used to apply the
 * overrides only to specific collections. <br>
 * <br>
 * The override will only be effective if the cache is re-instantiated. Therefore, some backing
 * shared caches that only instantiate on startup might only see the overrides after process
 * restart, while some transient caches such as local core cache will see the new values on searcher
 * reopening without restart. <br>
 * <br>
 * Take note that overrides do NOT work on below properties: <br>
 *
 * <ol>
 *   <li>class
 *   <li>regenerator
 * </ol>
 *
 * <br>
 * Example of the override config in clusterprops.json: <br>
 *
 * <pre>
 *   {
 * ...
 *  cacheOverrides : [
 *    {
 *      filterCache :  {
 *        size: 9999
 *      }
 *      documentCache : {
 *        size: 9999
 *      }
 *      fs-cache : {
 *        maxRamMB: 9999
 *      }
 *    },
 *    {
 *      filterCache :  {
 *        size: 12345
 *      }
 *      collections: [ "104H4B" ]
 *    }
 *  ]
 *
 * }
 * </pre>
 *
 * Entries will be applied as overrides according to the order declared in the array. Hence, later
 * entry overrides earlier for overlaps
 */
@SuppressWarnings("unchecked")
public class CacheOverridesManager {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private volatile Map<String, List<CacheOverrides>> overridesByCacheName = Map.of();

  public CacheOverridesManager(SolrZkClient zkClient) {
    byte[] clusterPropsBytes = null;

    final Watcher watcher =
        new Watcher() {
          @Override
          public void process(WatchedEvent event) {
            try {
              if (event.getType() == Event.EventType.NodeDataChanged
                  || event.getType() == Event.EventType.NodeCreated) {
                // Fetch the updated cluster properties
                byte[] data = zkClient.getData(event.getPath(), this, null, true);
                if (data != null) {
                  Map<String, Object> clusterPropsJson = (Map<String, Object>) Utils.fromJSON(data);
                  // Process the cache overrides from cluster properties
                  processCacheOverrides(clusterPropsJson.get("cacheOverrides"));
                } else {
                  processCacheOverrides(null);
                }
              } else if (event.getType() == Event.EventType.NodeDeleted) {
                zkClient.exists(event.getPath(), this, true);
                processCacheOverrides(null);
              } else {
                // just re-install watcher
                zkClient.exists(event.getPath(), this, true);
              }
            } catch (Exception e) {
              log.warn("Error processing cache overrides from ZooKeeper", e);
            }
          }
        };

    try {
      clusterPropsBytes = zkClient.getData(ZkStateReader.CLUSTER_PROPS, watcher, null, true);
    } catch (KeeperException.NoNodeException e) {
      try {
        zkClient.exists(ZkStateReader.CLUSTER_PROPS, watcher, true); // still install a watcher
      } catch (Exception ex) {
        log.warn("Error installing exist watcher on cluster properties from ZooKeeper", e);
      }
    } catch (Exception e) {
      log.warn("Error fetching cluster properties from ZooKeeper", e);
    }

    if (clusterPropsBytes != null) {
      Map<String, String> clusterPropsJson =
          (Map<String, String>) Utils.fromJSON(clusterPropsBytes);
      processCacheOverrides(clusterPropsJson.get("cacheOverrides"));
    }
  }

  private void processCacheOverrides(Object cacheOverridesContents) {
    if (cacheOverridesContents instanceof List) {
      @SuppressWarnings("unchecked")
      List<Map<String, Object>> entries = (List<Map<String, Object>>) cacheOverridesContents;
      Map<String, List<CacheOverrides>> newOverridesByCacheName = new HashMap<>();
      for (Map<String, Object> overridesMap : entries) {
        List<String> collectionsFilter = (List<String>) overridesMap.get("collections");

        for (Map.Entry<String, Object> entry : overridesMap.entrySet()) {
          if ("collections".equals(entry.getKey())) { // a special case for collection filter
            continue;
          }
          String cacheName = entry.getKey();
          Map<String, String> overrides = new HashMap<>();
          for (Map.Entry<String, Object> propertyKv :
              ((Map<String, Object>) entry.getValue()).entrySet()) {
            overrides.put(propertyKv.getKey(), String.valueOf(propertyKv.getValue()));
          }
          CacheOverrides cacheOverrides =
              new CacheOverrides(
                  overrides, collectionsFilter == null ? null : Set.copyOf(collectionsFilter));
          newOverridesByCacheName
              .computeIfAbsent(cacheName, k -> new java.util.ArrayList<>())
              .add(cacheOverrides);
        }
      }

      overridesByCacheName = Map.copyOf(newOverridesByCacheName);
      log.info("Cache overrides updated to {}", overridesByCacheName);

    } else if (cacheOverridesContents == null) { // overrides removal
      overridesByCacheName = Map.of();
      log.info("Cleared cache overrides");
    } else {
      log.warn(
          "Unexpected format for cacheOverrides in cluster properties: {}", cacheOverridesContents);
    }
  }

  /**
   * Applies the overrides to the given cache configuration.
   *
   * @param cacheConfig the existing cache config
   * @param cacheName the name of the cache to apply overrides for (filterCache, documentCache etc)
   * @param collection the collection name of this cache
   * @return existing config if no overrides otherwise a new config with overrides applied
   */
  public CacheConfig applyOverrides(CacheConfig cacheConfig, String cacheName, String collection) {
    List<Map<String, String>> overridesEntries = getOverrides(cacheName, collection);
    if (overridesEntries != null) {
      for (Map<String, String> overrides : overridesEntries) {
        cacheConfig = cacheConfig.withArgs(overrides);
      }
    }
    return cacheConfig;
  }

  List<Map<String, String>> getOverrides(String cacheName, String collection) {
    List<CacheOverrides> overrides = overridesByCacheName.get(cacheName);
    if (overrides == null) {
      return null;
    }
    List<Map<String, String>> result =
        overrides.stream()
            .map(override -> override.getOverrides(collection))
            .filter(overridesMap -> overridesMap != null && !overridesMap.isEmpty())
            .collect(Collectors.toList());
    return result.isEmpty() ? null : result;
  }

  static class CacheOverrides {
    private final Map<String, String> overrides;
    private final Set<String> matchCollections;

    public CacheOverrides(Map<String, String> overrides, Set<String> matchCollections) {
      this.overrides = new HashMap<>(overrides);
      this.matchCollections = matchCollections;
    }

    public Map<String, String> getOverrides(String collection) {
      if (matchCollections == null || matchCollections.contains(collection)) {
        return overrides;
      } else {
        return null;
      }
    }

    @Override
    public String toString() {
      return "CacheOverrides{"
          + "overrides="
          + overrides
          + ", matchCollections="
          + matchCollections
          + '}';
    }
  }
}
