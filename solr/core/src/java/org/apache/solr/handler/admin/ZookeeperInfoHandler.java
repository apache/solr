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
package org.apache.solr.handler.admin;

import static org.apache.solr.common.params.CommonParams.OMIT_HEADER;
import static org.apache.solr.common.params.CommonParams.PATH;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.DocCollection.CollectionStateProps;
import org.apache.solr.common.cloud.OnReconnect;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice.SliceStateProps;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.common.util.SuppressForbidden;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.security.AuthorizationContext;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Zookeeper Info
 *
 * @since solr 4.0
 */
public final class ZookeeperInfoHandler extends RequestHandlerBase {
  private static final String PARAM_DETAIL = "detail";
  private final CoreContainer cores;

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  // used for custom sorting collection names looking like prefix##
  // only go out to 7 digits (which safely fits in an int)
  private static final Pattern endsWithDigits = Pattern.compile("^(\\D*)(\\d{1,7}?)$");

  public ZookeeperInfoHandler(CoreContainer cc) {
    this.cores = cc;
  }

  @Override
  public String getDescription() {
    return "Fetch Zookeeper contents";
  }

  @Override
  public Category getCategory() {
    return Category.ADMIN;
  }

  @Override
  public Name getPermissionName(AuthorizationContext request) {
    var params = request.getParams();
    String path = normalizePath(params.get(PATH, ""));
    String detail = params.get(PARAM_DETAIL, "false");
    if ("/security.json".equalsIgnoreCase(path) && "true".equalsIgnoreCase(detail)) {
      return Name.SECURITY_READ_PERM;
    } else {
      return Name.ZK_READ_PERM;
    }
  }

  /** Enumeration of ways to filter collections on the graph panel. */
  enum FilterType {
    none,
    name,
    status
  }

  /** Holds state of a single page of collections requested from the cloud panel. */
  static final class PageOfCollections {
    List<String> selected;
    int numFound = 0; // total number of matches (across all pages)
    int start;
    int rows;
    FilterType filterType;
    String filter;

    PageOfCollections(int start, int rows, FilterType filterType, String filter) {
      this.start = start;
      this.rows = rows;
      this.filterType = filterType;
      this.filter = filter;
    }

    void selectPage(List<String> collections) {
      numFound = collections.size();
      // start with full set and then find the sublist for the desired selected
      selected = collections;

      if (rows > 0) { // paging desired
        if (start > numFound) start = 0; // this might happen if they applied a new filter

        int lastIndex = Math.min(start + rows, numFound);
        if (start > 0 || lastIndex < numFound) selected = collections.subList(start, lastIndex);
      }
    }

    /** Filters a list of collections by name if applicable. */
    List<String> applyNameFilter(List<String> collections) {

      if (filterType != FilterType.name || filter == null)
        return collections; // name filter doesn't apply

      // typically, a user will type a prefix and then *, e.g. tj*
      // when they really mean tj.*
      String regexFilter =
          (!filter.endsWith(".*") && filter.endsWith("*"))
              ? filter.substring(0, filter.length() - 1) + ".*"
              : filter;

      // case-insensitive
      if (!regexFilter.startsWith("(?i)")) regexFilter = "(?i)" + regexFilter;

      Pattern filterRegex = Pattern.compile(regexFilter);
      List<String> filtered = new ArrayList<>();
      for (String next : collections) {
        if (matches(filterRegex, next)) filtered.add(next);
      }

      return filtered;
    }

    /**
     * Walk the collection state JSON object to see if it has any replicas that match the state the
     * user is filtering by.
     */
    @SuppressWarnings("unchecked")
    boolean matchesStatusFilter(Map<String, Object> collectionState, Set<String> liveNodes) {

      if (filterType != FilterType.status || filter == null || filter.isEmpty())
        return true; // no status filter, so all match

      boolean isHealthy = true; // means all replicas for all shards active
      boolean hasDownedShard = false; // means one or more shards is down
      boolean replicaInRecovery = false;

      Map<String, Object> shards =
          (Map<String, Object>) collectionState.get(CollectionStateProps.SHARDS);
      for (Object o : shards.values()) {
        boolean hasActive = false;
        Map<String, Object> shard = (Map<String, Object>) o;
        Map<String, Object> replicas = (Map<String, Object>) shard.get(SliceStateProps.REPLICAS);
        for (Object value : replicas.values()) {
          Map<String, Object> replicaState = (Map<String, Object>) value;
          Replica.State coreState =
              Replica.State.getState((String) replicaState.get(ZkStateReader.STATE_PROP));
          String nodeName = (String) replicaState.get(ZkStateReader.NODE_NAME_PROP);

          // state can lie to you if the node is offline, so need to reconcile with live_nodes too
          if (!liveNodes.contains(nodeName))
            coreState = Replica.State.DOWN; // not on a live node, so must be down

          if (coreState == Replica.State.ACTIVE) {
            hasActive = true; // assumed no replicas active and found one that is for this shard
          } else {
            if (coreState == Replica.State.RECOVERING) {
              replicaInRecovery = true;
            }
            isHealthy = false; // assumed healthy and found one replica that is not
          }
        }

        if (!hasActive) hasDownedShard = true; // this is bad
      }

      if ("healthy".equals(filter)) {
        return isHealthy;
      } else if ("degraded".equals(filter)) {
        return !hasDownedShard && !isHealthy; // means no shards offline but not 100% healthy either
      } else if ("downed_shard".equals(filter)) {
        return hasDownedShard;
      } else if (Replica.State.getState(filter) == Replica.State.RECOVERING) {
        return !isHealthy && replicaInRecovery;
      }

      return true;
    }

    boolean matches(final Pattern filter, final String collName) {
      return filter.matcher(collName).matches();
    }

    String getPagingHeader() {
      return start
          + "|"
          + rows
          + "|"
          + numFound
          + "|"
          + (filterType != null ? filterType.toString() : "")
          + "|"
          + (filter != null ? filter : "");
    }

    @Override
    public String toString() {
      return getPagingHeader();
    }
  }

  /**
   * Supports paged navigation of collections on the cloud panel. To avoid serving stale collection
   * data, this object watches the /collections znode, which will change if a collection is added or
   * removed.
   */
  static final class PagedCollectionSupport implements Watcher, Comparator<String>, OnReconnect {

    // this is the full merged list of collections from ZooKeeper
    private List<String> cachedCollections;

    /** If the list of collections changes, mark the cache as stale. */
    @Override
    public void process(WatchedEvent event) {
      // session events are not change events, and do not remove the watcher
      if (Event.EventType.None.equals(event.getType())) {
        return;
      }
      synchronized (this) {
        cachedCollections = null;
      }
    }

    /** Create a merged view of all collections from /collections/?/state.json */
    private synchronized List<String> getCollections(SolrZkClient zkClient)
        throws KeeperException, InterruptedException {
      if (cachedCollections == null) {
        // cache is stale, rebuild the full list ...
        cachedCollections = new ArrayList<>();

        List<String> fromZk = zkClient.getChildren("/collections", this);
        if (fromZk != null) cachedCollections.addAll(fromZk);

        // sort the final merged set of collections
        cachedCollections.sort(this);
      }

      return cachedCollections;
    }

    /** Gets the requested page of collections after applying filters and offsets. */
    public void fetchPage(PageOfCollections page, SolrZkClient zkClient)
        throws KeeperException, InterruptedException {

      List<String> children = getCollections(zkClient);
      page.selected = children; // start with the page being the full list

      // activate paging (if disabled) for large collection sets
      if (page.start == 0 && page.rows == -1 && page.filter == null && children.size() > 10) {
        page.rows = 20;
      }

      // apply the name filter if supplied (we don't need to pull state
      // data from ZK to do name filtering
      if (page.filterType == FilterType.name && page.filter != null)
        children = page.applyNameFilter(children);

      // a little hacky ... we can't select the page when filtering by
      // status until reading all status objects from ZK
      if (page.filterType != FilterType.status) page.selectPage(children);
    }

    @Override
    public int compare(String left, String right) {
      if (left == null) return -1;

      if (left.equals(right)) return 0;

      // sort lexically unless the two collection names start with the same base prefix
      // and end in a number (which is a common enough naming scheme to have direct
      // support for it)
      Matcher leftMatcher = endsWithDigits.matcher(left);
      if (leftMatcher.matches()) {
        Matcher rightMatcher = endsWithDigits.matcher(right);
        if (rightMatcher.matches()) {
          String leftGroup1 = leftMatcher.group(1);
          String rightGroup1 = rightMatcher.group(1);
          if (leftGroup1.equals(rightGroup1)) {
            // both start with the same prefix ... compare indexes
            // using longs here as we don't know how long the 2nd group is
            int leftGroup2 = Integer.parseInt(leftMatcher.group(2));
            int rightGroup2 = Integer.parseInt(rightMatcher.group(2));
            return Integer.compare(leftGroup2, rightGroup2);
          }
        }
      }
      return left.compareTo(right);
    }

    /** Called after a ZooKeeper session expiration occurs */
    @Override
    public void onReconnect() {
      // we need to re-establish the watcher on the collections list after session expires
      synchronized (this) {
        cachedCollections = null;
      }
    }
  }

  private PagedCollectionSupport pagingSupport;

  @Override
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    final SolrParams params = req.getParams();

    // Force JSON response and omit header for cleaner output
    // Map<String, String> map = Map.of(WT, "json", OMIT_HEADER, "true");
    Map<String, String> map = Map.of(OMIT_HEADER, "true");
    req.setParams(SolrParams.wrapDefaults(new MapSolrParams(map), params));

    // Ensure paging support is initialized
    ensurePagingSupportInitialized();

    // Validate parameters
    validateParameters(params);

    // Determine request type and handle accordingly
    boolean isGraphView = "graph".equals(params.get("view"));
    ZkBaseResponseBuilder builder =
        isGraphView ? handleGraphViewRequest(params) : handlePathViewRequest(params);

    builder.build();

    addMapToResponse(builder.getDataMap(), rsp);
  }

  /** Ensures the paging support is initialized (thread-safe lazy initialization). */
  private void ensurePagingSupportInitialized() {
    synchronized (this) {
      if (pagingSupport == null) {
        pagingSupport = new PagedCollectionSupport();
        ZkController zkController = cores.getZkController();
        if (zkController != null) {
          // Get notified when the ZK session expires (so we can clear cached collections)
          zkController.addOnReconnectListener(pagingSupport);
        }
      }
    }
  }

  /**
   * Validates request parameters to prevent illegal operations.
   *
   * @param params Request parameters to validate
   */
  private void validateParameters(SolrParams params) {
    if (params.get("addr") != null) {
      throw new SolrException(ErrorCode.BAD_REQUEST, "Illegal parameter \"addr\"");
    }
  }

  /**
   * Handles the graph view request with paginated collections.
   *
   * @param params Request parameters including pagination settings
   * @return JSON string representing paginated collection data
   */
  private ZkBaseResponseBuilder handleGraphViewRequest(SolrParams params) {
    // Extract pagination parameters
    int start = params.getInt("start", 0);
    int rows = params.getInt("rows", -1);

    // Extract filter parameters
    FilterType filterType = extractFilterType(params);
    String filter = extractFilter(params, filterType);

    // Extract display options (applicable to graph view)
    boolean detail = params.getBool(PARAM_DETAIL, false);
    boolean dump = params.getBool("dump", false);

    // Create response builder for paginated collections
    return new ZkGraphResponseBuilder(
        cores.getZkController(),
        new PageOfCollections(start, rows, filterType, filter),
        pagingSupport,
        detail,
        dump);
  }

  /**
   * Handles the path view request for a specific ZooKeeper path.
   *
   * @param params Request parameters including the path to display
   * @return JSON string representing the ZooKeeper path data
   */
  private ZkBaseResponseBuilder handlePathViewRequest(SolrParams params) {
    // Extract path parameter
    String path = params.get(PATH);

    // Extract display options
    boolean detail = params.getBool(PARAM_DETAIL, false);
    boolean dump = params.getBool("dump", false);

    // Create response builder for specific path
    return new ZkPathResponseBuilder(cores.getZkController(), path, detail, dump);
  }

  /**
   * Extracts and normalizes the filter type from request parameters.
   *
   * @param params Request parameters
   * @return The filter type (defaults to FilterType.none if not specified)
   */
  private FilterType extractFilterType(SolrParams params) {
    String filterType = params.get("filterType");
    if (filterType != null) {
      filterType = filterType.trim().toLowerCase(Locale.ROOT);
      if (filterType.isEmpty()) {
        return FilterType.none;
      }
      return switch (filterType) {
        case "none" -> FilterType.none;
        case "name" -> FilterType.name;
        case "status" -> FilterType.status;
        default -> throw new SolrException(
            ErrorCode.BAD_REQUEST,
            "Invalid filterType '" + filterType + "'. Allowed values are: none, name, status");
      };
    }
    return FilterType.none;
  }

  /**
   * Extracts and normalizes the filter value from request parameters.
   *
   * @param params Request parameters
   * @param filterType The filter type being used
   * @return The filter string, or null if not applicable
   */
  private String extractFilter(SolrParams params, FilterType filterType) {
    if (filterType == FilterType.none) {
      return null;
    }

    String filter = params.get("filter");
    if (filter != null) {
      filter = filter.trim();
      if (!filter.isEmpty()) {
        return filter;
      }
    }
    return null;
  }

  /**
   * Adds Map data to SolrQueryResponse.
   *
   * @param dataMap The data map to add
   * @param rsp The response object to populate
   */
  private void addMapToResponse(Map<String, Object> dataMap, SolrQueryResponse rsp) {
    // Add the structured data directly to the response
    // This allows any response writer (json, xml, etc.) to serialize it properly
    for (Map.Entry<String, Object> entry : dataMap.entrySet()) {
      rsp.add(entry.getKey(), entry.getValue());
    }
  }

  @SuppressForbidden(reason = "JDK String class doesn't offer a stripEnd equivalent")
  private String normalizePath(String path) {
    return StringUtils.stripEnd(path, "/");
  }

  // --------------------------------------------------------------------------------------
  //
  // --------------------------------------------------------------------------------------

  /**
   * Base class for ZooKeeper response builders. Provides common functionality for building
   * structured response data from ZooKeeper.
   */
  abstract static class ZkBaseResponseBuilder {
    protected boolean detail;
    protected boolean dump;

    protected final Map<String, Object> dataMap = new LinkedHashMap<>();
    protected final SolrZkClient zkClient;
    protected final ZkController zkController;
    protected final String keeperAddr;

    public ZkBaseResponseBuilder(ZkController controller, boolean detail, boolean dump) {
      this.zkController = controller;
      this.detail = detail;
      this.dump = dump;
      this.keeperAddr = controller.getZkServerAddress();
      this.zkClient = controller.getZkClient();
    }

    public abstract void build() throws IOException;

    /** Returns the data as a Map for proper serialization by response writers. */
    public Map<String, Object> getDataMap() {
      return dataMap;
    }

    protected void writeError(int code, String msg) {
      throw new SolrException(ErrorCode.getErrorCode(code), msg);
    }

    protected String time(long ms) {
      return (new Date(ms)) + " (" + ms + ")";
    }
  }

  /**
   * Response builder implementation for a specific ZooKeeper path and its data. Used by Solr Admin
   * UI.
   */
  static class ZkPathResponseBuilder extends ZkBaseResponseBuilder {

    private String path;

    public ZkPathResponseBuilder(
        ZkController controller, String path, boolean detail, boolean dump) {
      super(controller, detail, dump);
      this.path = path;
    }

    @Override
    public void build() throws IOException {
      if (zkClient == null) {
        return;
      }

      // normalize path
      if (path == null) {
        path = "/";
      } else {
        path = path.trim();
        if (path.isEmpty()) {
          path = "/";
        }
      }

      if (path.endsWith("/") && path.length() > 1) {
        path = path.substring(0, path.length() - 1);
      }

      int idx = path.lastIndexOf('/');
      String parent = idx >= 0 ? path.substring(0, idx) : path;
      if (parent.isEmpty()) {
        parent = "/";
      }

      if (detail) {
        Map<String, Object> znodeData = buildZnodeData(path);
        if (znodeData == null) {
          return;
        }
        dataMap.putAll(znodeData);
      }

      List<Object> treeList = new ArrayList<>();
      if (!buildTree(treeList, path)) {
        return; // there was an error
      }
      dataMap.put("tree", treeList);
    }

    private boolean buildTree(List<Object> treeList, String path) {
      int idx = path.lastIndexOf('/');
      String label = idx > 0 ? path.substring(idx + 1) : path;

      Map<String, Object> node = new LinkedHashMap<>();
      node.put("text", label);

      Map<String, Object> aAttr = new LinkedHashMap<>();
      String href =
          "admin/zookeeper?detail=true&path=" + URLEncoder.encode(path, StandardCharsets.UTF_8);
      aAttr.put("href", href);
      node.put("a_attr", aAttr);

      Stat stat = new Stat();
      try {
        // Trickily, the call to zkClient.getData fills in the stat variable
        byte[] data = zkClient.getData(path, null, stat);

        if (stat.getEphemeralOwner() != 0) {
          node.put("ephemeral", true);
          node.put("version", stat.getVersion());
        }

        if (dump) {
          Map<String, Object> znodeData = buildZnodeData(path);
          if (znodeData != null) {
            node.putAll(znodeData);
          }
        }

      } catch (IllegalArgumentException e) {
        // path doesn't exist (must have been removed)
        node.put("warning", "(path gone)");
      } catch (KeeperException e) {
        node.put("warning", e.toString());
        log.warn("Keeper Exception", e);
      } catch (InterruptedException e) {
        node.put("warning", e.toString());
        log.warn("InterruptedException", e);
      }

      if (stat.getNumChildren() > 0) {
        List<Object> childrenList = new ArrayList<>();

        try {
          List<String> children = zkClient.getChildren(path, null);
          java.util.Collections.sort(children);

          for (String child : children) {
            String childPath = path + (path.endsWith("/") ? "" : "/") + child;
            if (!buildTree(childrenList, childPath)) {
              return false;
            }
          }
        } catch (KeeperException | InterruptedException e) {
          writeError(500, e.toString());
          return false;
        } catch (IllegalArgumentException e) {
          // path doesn't exist (must have been removed)
          childrenList.add("(children gone)");
        }

        node.put("children", childrenList);
      }

      treeList.add(node);
      return true;
    }

    private Map<String, Object> buildZnodeData(String path) {
      try {
        String dataStr = null;
        String dataStrErr = null;
        Stat stat = new Stat();
        // Trickily, the call to zkClient.getData fills in the stat variable
        byte[] data = zkClient.getData(path, null, stat);
        if (null != data) {
          try {
            dataStr = (new BytesRef(data)).utf8ToString();
          } catch (Exception e) {
            dataStrErr = "data is not parsable as a utf8 String: " + e;
          }
        }

        SimpleOrderedMap<Object> znodeMap = new SimpleOrderedMap<>();
        SimpleOrderedMap<Object> znodeContent = new SimpleOrderedMap<>();

        znodeContent.add(PATH, path);

        SimpleOrderedMap<Object> prop = new SimpleOrderedMap<>();
        prop.add("version", stat.getVersion());
        prop.add("aversion", stat.getAversion());
        prop.add("children_count", stat.getNumChildren());
        prop.add("ctime", time(stat.getCtime()));
        prop.add("cversion", stat.getCversion());
        prop.add("czxid", stat.getCzxid());
        prop.add("ephemeralOwner", stat.getEphemeralOwner());
        prop.add("mtime", time(stat.getMtime()));
        prop.add("mzxid", stat.getMzxid());
        prop.add("pzxid", stat.getPzxid());
        prop.add("dataLength", stat.getDataLength());
        if (null != dataStrErr) {
          prop.add("dataNote", dataStrErr);
        }
        znodeContent.add("prop", prop);

        if (null != dataStr) {
          znodeContent.add("data", dataStr);
        }

        znodeMap.add("znode", znodeContent);
        return znodeMap;
      } catch (KeeperException | InterruptedException e) {
        writeError(500, e.toString());
        return null;
      }
    }
  }

  /**
   * Response builder implementation for a paginated, filtered view of collections. Supports
   * pagination, and collection state retrieval.
   */
  static class ZkGraphResponseBuilder extends ZkBaseResponseBuilder {
    private final PageOfCollections page;
    private final PagedCollectionSupport pagingSupport;

    public ZkGraphResponseBuilder(
        ZkController controller,
        PageOfCollections page,
        PagedCollectionSupport pagingSupport,
        boolean detail,
        boolean dump) {
      super(controller, detail, dump);
      this.page = page;
      this.pagingSupport = pagingSupport;
    }

    @Override
    public void build() throws IOException {
      SortedMap<String, Object> collectionStates;
      try {
        // support paging of the collections graph view (in case there are many collections)
        // fetch the requested page of collections and then retrieve the state for each
        pagingSupport.fetchPage(page, zkClient);
        // keep track of how many collections match the filter
        boolean applyStatusFilter = (page.filterType == FilterType.status && page.filter != null);
        List<String> matchesStatusFilter = applyStatusFilter ? new ArrayList<>() : null;
        ClusterState cs = zkController.getZkStateReader().getClusterState();
        Set<String> liveNodes = applyStatusFilter ? cs.getLiveNodes() : null;

        collectionStates = new TreeMap<>(pagingSupport);
        for (String collection : page.selected) {
          DocCollection dc = cs.getCollectionOrNull(collection);
          if (dc != null) {
            Map<String, Object> collectionState = dc.toMap(new LinkedHashMap<>());
            if (applyStatusFilter) {
              // verify this collection matches the filtered state
              if (page.matchesStatusFilter(collectionState, liveNodes)) {
                matchesStatusFilter.add(collection);
                collectionStates.put(
                    collection, ClusterStatus.postProcessCollectionJSON(collectionState));
              }
            } else {
              collectionStates.put(
                  collection, ClusterStatus.postProcessCollectionJSON(collectionState));
            }
          }
        }

        if (applyStatusFilter) {
          // update the paged navigation info after applying the status filter
          page.selectPage(matchesStatusFilter);

          // rebuild the Map of state data
          SortedMap<String, Object> map = new TreeMap<>(pagingSupport);
          for (String next : page.selected) map.put(next, collectionStates.get(next));
          collectionStates = map;
        }
      } catch (KeeperException | InterruptedException e) {
        writeError(500, e.toString());
        return;
      }

      Map<String, Object> znodeContent = new LinkedHashMap<>();
      znodeContent.put(PATH, ZkStateReader.COLLECTIONS_ZKNODE);
      znodeContent.put("data", collectionStates);
      znodeContent.put("paging", page.getPagingHeader());

      dataMap.put("znode", znodeContent);
    }
  }
}
