package org.apache.solr.handler.component;

import com.carrotsearch.hppc.ObjectIntHashMap;
import com.carrotsearch.hppc.ObjectIntMap;
import com.carrotsearch.hppc.cursors.ObjectIntCursor;
import com.github.benmanes.caffeine.cache.Caffeine;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.solr.core.SolrInfoBean;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.metrics.SolrMetricProducer;
import org.apache.solr.metrics.SolrMetricsContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class SlowNodeDetector implements SolrMetricProducer {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private final ConcurrentMap<String, Object> slowNodes;
  private static final double DEFAULT_LATENCY_DROP_RATIO_THRESHOLD = 0.5;
  private static final int DEFAULT_MAX_SLOW_NODE_PERCENTAGE = 10;
  private static final int DEFAULT_MIN_CORE_PER_REQUEST = 512;
  private static final int DEFAULT_SLOW_LATENCY_THRESHOLD = 10000;
  private static final int DEFAULT_SLOW_NODE_TTL = 3600000;

  private final double latencyDropRatioThreshold;
  private final int maxSlowResponsePercentage;
  private final int minShardCountPerRequest;
  private final int slowLatencyThreshold;

  /**
   * @param latencyDropRatioThreshold identify as a latency drop point when current latency is < 0.5
   *     of previous
   * @param maxSlowResponsePercentage Only up to this percentage of responses can be considered
   *     "slow". The rest of the responses are considered "normal"
   * @param minShardCountPerRequest minimum number of shards per Shard Request to be considered for
   *     slow node detection
   * @param slowLatencyThreshold minimum latency in millisec to be considered as slow node
   * @param slowNodeTtl slow node list entry expire on write(detection) in millisec
   */
  private SlowNodeDetector(
      double latencyDropRatioThreshold,
      int maxSlowResponsePercentage,
      int minShardCountPerRequest,
      int slowLatencyThreshold,
      long slowNodeTtl) {
    this.latencyDropRatioThreshold = latencyDropRatioThreshold;
    this.maxSlowResponsePercentage = maxSlowResponsePercentage;
    this.minShardCountPerRequest = minShardCountPerRequest;
    this.slowLatencyThreshold = slowLatencyThreshold;

    Caffeine<Object, Object> builder = Caffeine.newBuilder();

    if (slowNodeTtl >= 0) {
      builder.expireAfterWrite(slowNodeTtl, java.util.concurrent.TimeUnit.MILLISECONDS);
    }
    slowNodes = builder.<String, Object>build().asMap();
  }

  Set<String> getSlowNodes() {
    return new HashSet<>(slowNodes.keySet());
  }

  /** For test only */
  void setSlowNodes(Set<String> slowNodes) {
    this.slowNodes.clear();
    for (String slowNode : slowNodes) {
      this.slowNodes.put(slowNode, Boolean.TRUE);
    }
  }

  void notifyRequestStats(RequestStats stats) {
    ComputeResult computeResult = computeSlowNodes(stats);
    ;

    if (computeResult != null) {
      for (String slowNode : computeResult.newSlowNodes) {
        slowNodes.put(slowNode, Boolean.TRUE);
      }
      for (String purgingSlowNode : computeResult.recoveredSlowNodes) {
        slowNodes.remove(purgingSlowNode);
      }
    }
  }

  private ComputeResult computeSlowNodes(RequestStats stats) {
    if (stats.responseLatencies.size() < minShardCountPerRequest) {
      return null; // not enough responses to make a decision
    }
    int maxSlowResponseCount = stats.responseLatencies.size() * maxSlowResponsePercentage / 100;
    if (maxSlowResponseCount < 1) {
      return null; // not enough responses to make a decision
    }

    Collections.sort(stats.responseLatencies);

    if (stats.responseLatencies.get(0).latency < slowLatencyThreshold) {
      return null; // fastest response is not slow enough to consider any node as slow
    }

    Long previousLatency = null;
    boolean foundLatencyDrop = false;
    ObjectIntMap<String> iteratedResponseCountByNode = new ObjectIntHashMap<>();

    // iterate response to find slow nodes
    int index = 0;
    for (RequestStats.NodeLatency current : stats.responseLatencies) {
      if (index++ > maxSlowResponseCount) {
        // too many potential slow responses, not a good data as we assume they are minority
        break;
      }
      if (previousLatency != null
          && (double) current.latency / previousLatency < latencyDropRatioThreshold) {
        // found the drop in latencies, all the iterated nodes are potentially slow
        foundLatencyDrop = true;
        log.info(
            "Found latency drop point found. Previous latency {} vs current latency {}",
            previousLatency,
            current.latency);
        break;
      }

      // no latency drop point found so far and the rest latencies would not be significant enough
      // to form a drop
      if (current.latency < slowLatencyThreshold) {
        break;
      }

      previousLatency = current.latency;
      iteratedResponseCountByNode.putOrAdd(current.node, 1, 1);
    }

    Set<String> slowNodes = new HashSet<>();
    if (foundLatencyDrop) {
      // then that means there are some nodes that are significantly slower than others
      for (ObjectIntCursor<String> nodeWithSlowResponseCount : iteratedResponseCountByNode) {
        String potentialSlowNode = nodeWithSlowResponseCount.key;

        if (nodeWithSlowResponseCount.value
            == stats.responseCountByNode.getOrDefault(potentialSlowNode, 0)) {
          // all responses of this node is slow, it is a slow node
          slowNodes.add(potentialSlowNode);
        }
      }
    }

    // find previous slow nodes that might have recovered
    Set<String> recoveredNodes = new HashSet<>();
    Set<String> recoveredSlowNodeCandidates = new HashSet<>(this.slowNodes.keySet());
    recoveredSlowNodeCandidates.removeAll(slowNodes);

    if (!recoveredSlowNodeCandidates.isEmpty()) {
      // the threshold for sorted response to be considered as normal latency
      int normalNodeResponseCount = stats.responseLatencies.size() - maxSlowResponseCount;
      iteratedResponseCountByNode.clear();
      for (int i = stats.responseLatencies.size() - 1; i >= normalNodeResponseCount; i--) {
        RequestStats.NodeLatency latency = stats.responseLatencies.get(i);
        if (!recoveredSlowNodeCandidates.contains(latency.node)) {
          continue;
        }
        iteratedResponseCountByNode.putOrAdd(latency.node, 1, 1);
      }

      // if the all responses of such node are considered normal, then consider the node as
      // recovered
      for (ObjectIntCursor<String> nodeWithNormalResponseCount : iteratedResponseCountByNode) {
        String potentialRecoveredNode = nodeWithNormalResponseCount.key;

        if (nodeWithNormalResponseCount.value
            == stats.responseCountByNode.getOrDefault(potentialRecoveredNode, 0)) {
          recoveredNodes.add(potentialRecoveredNode);
        }
      }
    }

    ComputeResult result = null;
    if (!slowNodes.isEmpty() || !recoveredNodes.isEmpty()) {
      result = new ComputeResult(slowNodes, recoveredNodes);
    }

    if (log.isInfoEnabled() && result != null) {
      log.info("Slow nodes compute result: {}", result);
    }

    return result;
  }

  private static class ComputeResult {
    Set<String> newSlowNodes;
    Set<String> recoveredSlowNodes; // previously slow nodes that are no longer slow

    ComputeResult(Set<String> newSlowNodes, Set<String> recoveredSlowNodes) {
      this.newSlowNodes = newSlowNodes;
      this.recoveredSlowNodes = recoveredSlowNodes;
    }

    @Override
    public String toString() {
      return "ComputeResult{"
          + "newSlowNodes="
          + newSlowNodes
          + ", recoveredSlowNodes="
          + recoveredSlowNodes
          + '}';
    }
  }

  @Override
  public void initializeMetrics(SolrMetricsContext parentContext, String scope) {
    String nodeRegistry = SolrMetricManager.getRegistryName(SolrInfoBean.Group.node);
    SolrMetricManager manager = parentContext.getMetricManager();
    manager.registerGauge(
        parentContext,
        nodeRegistry,
        slowNodes::keySet,
        parentContext.getTag(),
        SolrMetricManager.ResolutionStrategy.REPLACE,
        "slowNodes",
        scope);
    manager.registerGauge(
        parentContext,
        nodeRegistry,
        slowNodes::size,
        parentContext.getTag(),
        SolrMetricManager.ResolutionStrategy.REPLACE,
        "slowNodeCount",
        scope);
  }

  @Override
  public SolrMetricsContext getSolrMetricsContext() { // using the same context as parent
    return null;
  }

  static class Builder {
    private double latencyDropRatioThreshold =
        DEFAULT_LATENCY_DROP_RATIO_THRESHOLD; // identify as a latency drop point when current
    // latency is < 0.5 of previous
    private int maxSlowResponsePercentage =
        DEFAULT_MAX_SLOW_NODE_PERCENTAGE; // can only find up to this percentage of slow node. If
    // more than this percentage of potential slow nodes
    // detected, do not return any slow node at all
    private int minShardCountPerRequest =
        DEFAULT_MIN_CORE_PER_REQUEST; // minimum number of cores per Shard Request to be considered
    // for slow node detection
    private int slowLatencyThreshold =
        DEFAULT_SLOW_LATENCY_THRESHOLD; // minimum latency to be considered as slow node
    private long slowNodeTtl = DEFAULT_SLOW_NODE_TTL;

    public Builder withLatencyDropRatioThreshold(double latencyDropRatioThreshold) {
      this.latencyDropRatioThreshold = latencyDropRatioThreshold;
      return this;
    }

    public Builder withMaxSlowResponsePercentage(int maxSlowResponsePercentage) {
      this.maxSlowResponsePercentage = maxSlowResponsePercentage;
      return this;
    }

    public Builder withMinShardCountPerRequest(int minShardCountPerRequest) {
      this.minShardCountPerRequest = minShardCountPerRequest;
      return this;
    }

    public Builder withSlowLatencyThreshold(int slowLatencyThreshold) {
      this.slowLatencyThreshold = slowLatencyThreshold;
      return this;
    }

    public Builder withSlowNodeTtl(long slowNodeTtl) {
      this.slowNodeTtl = slowNodeTtl;
      return this;
    }

    public SlowNodeDetector build() {
      return new SlowNodeDetector(
          latencyDropRatioThreshold,
          maxSlowResponsePercentage,
          minShardCountPerRequest,
          slowLatencyThreshold,
          slowNodeTtl);
    }

    @Override
    public String toString() {
      return "Builder{"
          + "latencyDropRatioThreshold="
          + latencyDropRatioThreshold
          + ", maxSlowResponsePercentage="
          + maxSlowResponsePercentage
          + ", minShardCountPerRequest="
          + minShardCountPerRequest
          + ", slowLatencyThreshold="
          + slowLatencyThreshold
          + ", slowNodeTtl="
          + slowNodeTtl
          + '}';
    }
  }
}

class RequestStats {
  final List<NodeLatency> responseLatencies = new ArrayList<>();
  final Map<String, Integer> responseCountByNode = new ConcurrentHashMap<>();

  RequestStats() {}

  static class NodeLatency implements Comparable<NodeLatency> {
    final String node;
    final long latency;

    NodeLatency(String node, long latency) {
      this.node = node;
      this.latency = latency;
    }

    @Override
    public int compareTo(NodeLatency other) {
      int timeComparison = Long.compare(other.latency, this.latency); // reverse order
      if (timeComparison != 0) {
        return timeComparison;
      }
      return this.node.compareTo(other.node);
    }
  }

  public synchronized void recordLatency(String node, long latency) {
    responseLatencies.add(new NodeLatency(node, latency));
    responseCountByNode.compute(node, (k, c) -> c != null ? c + 1 : 1);
  }

  void clear() {
    responseLatencies.clear();
    responseCountByNode.clear();
  }
}
