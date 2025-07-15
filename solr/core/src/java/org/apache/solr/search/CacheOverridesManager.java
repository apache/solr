package org.apache.solr.search;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.core.SolrCore;
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
 *  "cacheOverrides" : [
 *    {
 *      "filterCache" :  {
 *        "size": 9999
 *      },
 *      "documentCache" : {
 *        "size": 9999
 *      }
 *      "fs-cache" : {
 *        "maxRamMB": 9999
 *      }
 *    },
 *    {
 *      "filterCache" :  {
 *        "size": 12345
 *      },
 *      "collections": [ "104H4B" ]
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

  private enum FilterEnum {
    COLLECTIONS("collections"),
    NODES("nodes");

    private final String key;
    private static final Map<String, FilterEnum> keyToFilter;

    static {
      keyToFilter = new HashMap<>();
      for (FilterEnum filter : FilterEnum.values()) {
        keyToFilter.put(filter.key, filter);
      }
    }

    FilterEnum(String key) {
      this.key = key;
    }

    static FilterEnum fromKey(String key) {
      return keyToFilter.get(key);
    }
  }

  interface Filter extends Function<SolrCore, Boolean> {}

  public CacheOverridesManager(ZkStateReader zkStateReader) {
    zkStateReader.registerClusterPropertiesListener(
        (Map<String, Object> properties) -> {
          processCacheOverrides(properties.get("cacheOverrides"));
          return false;
        });
  }

  private void processCacheOverrides(Object cacheOverridesContents) {
    if (cacheOverridesContents instanceof List) {
      @SuppressWarnings("unchecked")
      List<Map<String, Object>> entries = (List<Map<String, Object>>) cacheOverridesContents;
      Map<String, List<CacheOverrides>> newOverridesByCacheName = new HashMap<>();

      for (Map<String, Object> overridesMap : entries) {
        List<Filter> filters = new ArrayList<>();
        Map<String, Map<String, String>> kvOverridesByCacheName = new HashMap<>();

        // iterate entries to collect filters and override entries
        for (Map.Entry<String, Object> entry : overridesMap.entrySet()) {
          FilterEnum filterEnum = FilterEnum.fromKey(entry.getKey());
          if (filterEnum != null) { // a filter key, not an override
            Filter filter = getFilter(filterEnum, entry.getValue());
            if (filter == null) {
              log.warn(
                  "Ignoring cache override updates. Invalid filter configuration for key '{}': {}",
                  entry.getKey(),
                  entry.getValue());
              // do not proceed with any invalid filter, reject cacheOverridesContents for any
              // invalid input
              return;
            }
            filters.add(filter);
          } else {
            Map<String, String> kvOverrides = new HashMap<>();
            String cacheName = entry.getKey();
            for (Map.Entry<String, Object> propertyKv :
                ((Map<String, Object>) entry.getValue()).entrySet()) {
              kvOverrides.put(propertyKv.getKey(), String.valueOf(propertyKv.getValue()));
            }
            kvOverridesByCacheName.put(cacheName, kvOverrides);
          }
        }

        for (Map.Entry<String, Map<String, String>> kvOverridesWithCacheName :
            kvOverridesByCacheName.entrySet()) {
          String cacheName = kvOverridesWithCacheName.getKey();
          CacheOverrides cacheOverrides =
              new CacheOverrides(kvOverridesWithCacheName.getValue(), filters);
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

  private static Filter getFilter(FilterEnum filterEnum, Object filterValue) {
    switch (filterEnum) {
      case COLLECTIONS:
        if (filterValue instanceof List) {
          @SuppressWarnings("unchecked")
          List<String> collectionsFilter = (List<String>) filterValue;
          return new CollectionFilter(collectionsFilter);
        } else {
          log.warn("Expected a list for collections filter, got: {}", filterValue);
          return null; // invalid filter value
        }
      case NODES:
        if (filterValue instanceof List) {
          @SuppressWarnings("unchecked")
          List<String> nodesFilter = (List<String>) filterValue;
          return new NodeFilter(nodesFilter);
        } else {
          log.warn("Expected a list for nodes filter, got: {}", filterValue);
          return null; // invalid filter value
        }
      default:
        log.warn("Unknown filter type: {}", filterEnum);
        return null; // unknown filter type
    }
  }

  /**
   * Applies the overrides to the given cache configuration.
   *
   * @param cacheConfig the existing cache config
   * @param cacheName the name of the cache to apply overrides for (filterCache, documentCache etc)
   * @param core the solr core that holds the cache
   * @return existing config if no overrides otherwise a new config with overrides applied
   */
  public CacheConfig applyOverrides(CacheConfig cacheConfig, String cacheName, SolrCore core) {
    List<Map<String, String>> overridesEntries = getOverrides(cacheName, core);
    for (Map<String, String> overrides : overridesEntries) {
      cacheConfig = cacheConfig.withArgs(overrides);
    }
    return cacheConfig;
  }

  List<Map<String, String>> getOverrides(String cacheName, SolrCore core) {
    List<CacheOverrides> overrides = overridesByCacheName.get(cacheName);
    if (overrides == null) {
      return List.of();
    }
    return overrides.stream()
        .map(cacheOverrides -> cacheOverrides.getOverridesKvsByCore(core))
        .filter(overridesMap -> overridesMap != null && !overridesMap.isEmpty())
        .collect(Collectors.toList());
  }

  static class CacheOverrides {
    private final Map<String, String> overrides;
    private final List<Filter> filters;

    public CacheOverrides(Map<String, String> overrides, List<Filter> filters) {
      this.overrides = new HashMap<>(overrides);
      this.filters = filters;
    }

    public Map<String, String> getOverridesKvsByCore(SolrCore core) {
      for (Filter filter : filters) {
        if (!filter.apply(core)) {
          return null; // filter did not match
        }
      }
      return overrides;
    }

    @Override
    public String toString() {
      return "CacheOverrides{" + "overrides=" + overrides + ", filters=" + filters + '}';
    }
  }

  private static class CollectionFilter implements Filter {
    private final List<String> collections;

    private CollectionFilter(List<String> collections) {
      this.collections = collections;
    }

    @Override
    public Boolean apply(SolrCore core) {
      return collections.contains(core.getCoreDescriptor().getCollectionName());
    }

    @Override
    public String toString() {
      return "CollectionFilter{" + "collections=" + collections + '}';
    }
  }

  private static class NodeFilter implements Filter {
    private final List<String> nodes;

    private NodeFilter(List<String> nodes) {
      this.nodes = nodes;
    }

    @Override
    public Boolean apply(SolrCore core) {
      if (core.getCoreContainer().getZkController() == null) {
        return false;
      }
      String node = core.getCoreContainer().getZkController().getNodeName();
      if (node == null) {
        return false; // no node name, cannot apply filter
      }
      return nodes.contains(node);
    }

    @Override
    public String toString() {
      return "NodeFilter{" + "nodes=" + nodes + '}';
    }
  }
}
