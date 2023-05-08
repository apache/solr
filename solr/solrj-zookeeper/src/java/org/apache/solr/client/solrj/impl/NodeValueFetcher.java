package org.apache.solr.client.solrj.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.solr.client.solrj.response.SimpleSolrResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.util.Utils;
import org.apache.zookeeper.KeeperException;

/**
 * This class is responsible for fetching mtrics and other attributes from a given node in Solr
 * cluster
 */
public class NodeValueFetcher {
  // well known tags
  public static final String NODE = "node";
  public static final String PORT = "port";
  public static final String HOST = "host";
  public static final String CORES = "cores";
  public static final String ROLE = "role";
  public static final String NODEROLE = "nodeRole";
  public static final String SYSPROP = "sysprop.";
  public static final Set<String> tags =
      Set.of(NODE, PORT, HOST, CORES, Tags.FREEDISK.NAME, ROLE, Tags.HEAPUSAGE.NAME);
  public static final Pattern hostAndPortPattern = Pattern.compile("(?:https?://)?([^:]+):(\\d+)");
  public static final String METRICS_PREFIX = "metrics:";

  public enum Tags {
    FREEDISK(
        "freedisk", "solr.node", "CONTAINER.fs.usableSpace", "solr.node/CONTAINER.fs.usableSpace"),
    TOTALDISK(
        "totaldisk", "solr.node", "CONTAINER.fs.totalSpace", "solr.node/CONTAINER.fs.totalSpace"),
    CORES("cores", "solr.node", "CONTAINER.cores", null) {
      @Override
      public Object extractResult(Object root) {
        NamedList<?> node = (NamedList<?>) Utils.getObjectByPath(root, false, "solr.node");
        int count = 0;
        for (String leafCoreMetricName : new String[] {"lazy", "loaded", "unloaded"}) {
          Number n = (Number) node.get("CONTAINER.cores." + leafCoreMetricName);
          if (n != null) count += n.intValue();
        }
        return count;
      }
    },
    SYSLOADAVG("sysLoadAvg", "solr.jvm", "os.systemLoadAverage", "solr.jvm/os.systemLoadAverage"),
    HEAPUSAGE("heapUsage", "solr.jvm", "memory.heap.usage", "solr.jvm/memory.heap.usage");
    public final String group, prefix, NAME, path;

    Tags(String name, String group, String prefix, String path) {
      this.group = group;
      this.prefix = prefix;
      this.NAME = name;
      this.path = path;
    }

    public Object extractResult(Object root) {
      Object v = Utils.getObjectByPath(root, true, path);
      return v == null ? null : convertVal(v);
    }

    public Object convertVal(Object val) {
      if (val instanceof String) {
        return Double.valueOf((String) val);
      } else if (val instanceof Number) {
        Number num = (Number) val;
        return num.doubleValue();

      } else {
        throw new IllegalArgumentException("Unknown type : " + val);
      }
    }
  }

  protected void getRemoteInfo(
      String solrNode, Set<String> requestedTags, SolrClientNodeStateProvider.RemoteCallCtx ctx) {
    if (!(ctx).isNodeAlive(solrNode)) return;
    Map<String, Set<Object>> metricsKeyVsTag = new HashMap<>();
    for (String tag : requestedTags) {
      if (tag.startsWith(SYSPROP)) {
        metricsKeyVsTag
            .computeIfAbsent(
                "solr.jvm:system.properties:" + tag.substring(SYSPROP.length()),
                k -> new HashSet<>())
            .add(tag);
      } else if (tag.startsWith(METRICS_PREFIX)) {
        metricsKeyVsTag
            .computeIfAbsent(tag.substring(METRICS_PREFIX.length()), k -> new HashSet<>())
            .add(tag);
      }
    }
    if (!metricsKeyVsTag.isEmpty()) {
      SolrClientNodeStateProvider.fetchReplicaMetrics(solrNode, ctx, metricsKeyVsTag);
    }

    Set<String> groups = new HashSet<>();
    List<String> prefixes = new ArrayList<>();
    for (Tags t : Tags.values()) {
      if (requestedTags.contains(t.NAME)) {
        groups.add(t.group);
        prefixes.add(t.prefix);
      }
    }
    if (groups.isEmpty() || prefixes.isEmpty()) return;

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("group", StrUtils.join(groups, ','));
    params.add("prefix", StrUtils.join(prefixes, ','));

    try {
      SimpleSolrResponse rsp = ctx.invokeWithRetry(solrNode, CommonParams.METRICS_PATH, params);
      NamedList<?> metrics = (NamedList<?>) rsp.nl.get("metrics");
      if (metrics != null) {
        for (Tags t : Tags.values()) {
          ctx.tags.put(t.NAME, t.extractResult(metrics));
        }
      }
    } catch (Exception e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Error getting remote info", e);
    }
  }

  public void getTags(
      String solrNode, Set<String> requestedTags, SolrClientNodeStateProvider.RemoteCallCtx ctx) {
    try {
      if (requestedTags.contains(NODE)) ctx.tags.put(NODE, solrNode);
      if (requestedTags.contains(HOST)) {
        Matcher hostAndPortMatcher = hostAndPortPattern.matcher(solrNode);
        if (hostAndPortMatcher.find()) ctx.tags.put(HOST, hostAndPortMatcher.group(1));
      }
      if (requestedTags.contains(PORT)) {
        Matcher hostAndPortMatcher = hostAndPortPattern.matcher(solrNode);
        if (hostAndPortMatcher.find()) ctx.tags.put(PORT, hostAndPortMatcher.group(2));
      }
      if (requestedTags.contains(ROLE)) fillRole(solrNode, ctx, ROLE);
      if (requestedTags.contains(NODEROLE))
        fillRole(solrNode, ctx, NODEROLE); // for new policy framework

      getRemoteInfo(solrNode, requestedTags, ctx);
    } catch (Exception e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }
  }

  private void fillRole(String solrNode, SolrClientNodeStateProvider.RemoteCallCtx ctx, String key)
      throws KeeperException, InterruptedException {
    Map<?, ?> roles =
        (Map<?, ?>)
            (ctx.session != null
                ? ctx.session.get(ZkStateReader.ROLES)
                : null); // we don't want to hit the ZK for each node
    // so cache and reuse
    try {
      if (roles == null) roles = ctx.getZkJson(ZkStateReader.ROLES);
      cacheRoles(solrNode, ctx, key, roles);
    } catch (KeeperException.NoNodeException e) {
      cacheRoles(solrNode, ctx, key, Collections.emptyMap());
    }
  }

  private void cacheRoles(
      String solrNode, SolrClientNodeStateProvider.RemoteCallCtx ctx, String key, Map<?, ?> roles) {
    if (ctx.session != null) ctx.session.put(ZkStateReader.ROLES, roles);
    if (roles != null) {
      for (Map.Entry<?, ?> e : roles.entrySet()) {
        if (e.getValue() instanceof List) {
          if (((List<?>) e.getValue()).contains(solrNode)) {
            ctx.tags.put(key, e.getKey());
            break;
          }
        }
      }
    }
  }
}
