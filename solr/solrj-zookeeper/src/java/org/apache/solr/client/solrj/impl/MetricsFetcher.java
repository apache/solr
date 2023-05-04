package org.apache.solr.client.solrj.impl;

import org.apache.solr.client.solrj.response.SimpleSolrResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.cloud.rule.SnitchContext;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.util.Utils;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

// uses metrics API to get node information
public class MetricsFetcher {
    // well known tags
    public static final String NODE = "node";
    public static final String PORT = "port";
    public static final String HOST = "host";
    public static final String CORES = "cores";
    public static final String DISK = "freedisk";
    public static final String ROLE = "role";
    public static final String NODEROLE = "nodeRole";
    public static final String SYSPROP = "sysprop.";
    public static final String SYSLOADAVG = "sysLoadAvg";
    public static final String HEAPUSAGE = "heapUsage";
    public static final Set<String> tags =
        Set.of(NODE, PORT, HOST, CORES, DISK, ROLE, HEAPUSAGE, "ip_1", "ip_2", "ip_3", "ip_4");
    public static final List<String> IP_SNITCHES =
        Collections.unmodifiableList(Arrays.asList("ip_1", "ip_2", "ip_3", "ip_4"));
    public static final Pattern hostAndPortPattern = Pattern.compile("(?:https?://)?([^:]+):(\\d+)");
    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    protected void getRemoteInfo(String solrNode, Set<String> requestedTags, SnitchContext ctx) {
        if (!((SolrClientNodeStateProvider.ClientSnitchCtx) ctx).isNodeAlive(solrNode)) return;
        SolrClientNodeStateProvider.ClientSnitchCtx snitchContext = (SolrClientNodeStateProvider.ClientSnitchCtx) ctx;
        Map<String, Set<Object>> metricsKeyVsTag = new HashMap<>();
        for (String tag : requestedTags) {
            if (tag.startsWith(SYSPROP)) {
                metricsKeyVsTag
                        .computeIfAbsent(
                                "solr.jvm:system.properties:" + tag.substring(SYSPROP.length()),
                                k -> new HashSet<>())
                        .add(tag);
            } else if (tag.startsWith(SolrClientNodeStateProvider.METRICS_PREFIX)) {
                metricsKeyVsTag
                        .computeIfAbsent(tag.substring(SolrClientNodeStateProvider.METRICS_PREFIX.length()), k -> new HashSet<>())
                        .add(tag);
            }
        }
        if (!metricsKeyVsTag.isEmpty()) {
            SolrClientNodeStateProvider.fetchReplicaMetrics(solrNode, snitchContext, metricsKeyVsTag);
        }

        Set<String> groups = new HashSet<>();
        List<String> prefixes = new ArrayList<>();
        if (requestedTags.contains(DISK)) {
            groups.add("solr.node");
            prefixes.add("CONTAINER.fs.usableSpace");
        }
        if (requestedTags.contains(SolrClientNodeStateProvider.Variable.TOTALDISK.tagName)) {
            groups.add("solr.node");
            prefixes.add("CONTAINER.fs.totalSpace");
        }
        if (requestedTags.contains(CORES)) {
            groups.add("solr.node");
            prefixes.add("CONTAINER.cores");
        }
        if (requestedTags.contains(SYSLOADAVG)) {
            groups.add("solr.jvm");
            prefixes.add("os.systemLoadAverage");
        }
        if (requestedTags.contains(HEAPUSAGE)) {
            groups.add("solr.jvm");
            prefixes.add("memory.heap.usage");
        }
        if (groups.isEmpty() || prefixes.isEmpty()) return;

        ModifiableSolrParams params = new ModifiableSolrParams();
        params.add("group", StrUtils.join(groups, ','));
        params.add("prefix", StrUtils.join(prefixes, ','));

        try {
            SimpleSolrResponse rsp =
                    snitchContext.invokeWithRetry(solrNode, CommonParams.METRICS_PATH, params);
            NamedList<?> metrics = (NamedList<?>) rsp.nl.get("metrics");
            if (metrics != null) {
                // metrics enabled
                if (requestedTags.contains(SolrClientNodeStateProvider.Variable.FREEDISK.tagName)) {
                    Object n = Utils.getObjectByPath(metrics, true, "solr.node/CONTAINER.fs.usableSpace");
                    if (n != null)
                        ctx.getTags().put(SolrClientNodeStateProvider.Variable.FREEDISK.tagName, SolrClientNodeStateProvider.Variable.FREEDISK.convertVal(n));
                }
                if (requestedTags.contains(SolrClientNodeStateProvider.Variable.TOTALDISK.tagName)) {
                    Object n = Utils.getObjectByPath(metrics, true, "solr.node/CONTAINER.fs.totalSpace");
                    if (n != null)
                        ctx.getTags().put(SolrClientNodeStateProvider.Variable.TOTALDISK.tagName, SolrClientNodeStateProvider.Variable.TOTALDISK.convertVal(n));
                }
                if (requestedTags.contains(CORES)) {
                    NamedList<?> node = (NamedList<?>) metrics.get("solr.node");
                    int count = 0;
                    for (String leafCoreMetricName : new String[]{"lazy", "loaded", "unloaded"}) {
                        Number n = (Number) node.get("CONTAINER.cores." + leafCoreMetricName);
                        if (n != null) count += n.intValue();
                    }
                    ctx.getTags().put(CORES, count);
                }
                if (requestedTags.contains(SYSLOADAVG)) {
                    Number n =
                            (Number) Utils.getObjectByPath(metrics, true, "solr.jvm/os.systemLoadAverage");
                    if (n != null) ctx.getTags().put(SYSLOADAVG, n.doubleValue());
                }
                if (requestedTags.contains(HEAPUSAGE)) {
                    Number n = (Number) Utils.getObjectByPath(metrics, true, "solr.jvm/memory.heap.usage");
                    if (n != null) ctx.getTags().put(HEAPUSAGE, n.doubleValue() * 100.0d);
                }
            }
        } catch (Exception e) {
            throw new SolrException(
                    SolrException.ErrorCode.SERVER_ERROR, "Error getting remote info", e);
        }
    }

    public void getTags(String solrNode, Set<String> requestedTags, SnitchContext ctx) {
        try {
            if (requestedTags.contains(NODE))
                ctx.getTags().put(NODE, solrNode);
            if (requestedTags.contains(HOST)) {
                Matcher hostAndPortMatcher = hostAndPortPattern.matcher(solrNode);
                if (hostAndPortMatcher.find())
                    ctx.getTags().put(HOST, hostAndPortMatcher.group(1));
            }
            if (requestedTags.contains(PORT)) {
                Matcher hostAndPortMatcher = hostAndPortPattern.matcher(solrNode);
                if (hostAndPortMatcher.find())
                    ctx.getTags().put(PORT, hostAndPortMatcher.group(2));
            }
            if (requestedTags.contains(ROLE))
                fillRole(solrNode, ctx, ROLE);
            if (requestedTags.contains(NODEROLE))
                fillRole(solrNode, ctx, NODEROLE); // for new policy framework

            addIpTags(solrNode, requestedTags, ctx);

            getRemoteInfo(solrNode, requestedTags, ctx);
        } catch (Exception e) {
            throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
        }
    }

    private void addIpTags(String solrNode, Set<String> requestedTags, SnitchContext context) {

        List<String> requestedHostTags = new ArrayList<>();
        for (String tag : requestedTags) {
            if (IP_SNITCHES.contains(tag)) {
                requestedHostTags.add(tag);
            }
        }

        if (requestedHostTags.isEmpty()) {
            return;
        }

        String[] ipFragments = getIpFragments(solrNode);

        if (ipFragments == null) {
            return;
        }

        int ipSnitchCount = IP_SNITCHES.size();
        for (int i = 0; i < ipSnitchCount; i++) {
            String currentTagValue = ipFragments[i];
            String currentTagKey = IP_SNITCHES.get(ipSnitchCount - i - 1);

            if (requestedHostTags.contains(currentTagKey)) {
                context.getTags().put(currentTagKey, currentTagValue);
            }
        }
    }

    private String[] getIpFragments(String solrNode) {
        Matcher hostAndPortMatcher = hostAndPortPattern.matcher(solrNode);
        if (hostAndPortMatcher.find()) {
            String host = hostAndPortMatcher.group(1);
            if (host != null) {
                String ip = getHostIp(host);
                if (ip != null) {
                    return ip.split(SolrClientNodeStateProvider.HOST_FRAG_SEPARATOR_REGEX); // IPv6 support will be provided by SOLR-8523
                }
            }
        }

        log.warn(
                "Failed to match host IP address from node URL [{}] using regex [{}]",
                solrNode,
                hostAndPortPattern.pattern());
        return null;
    }

    public String getHostIp(String host) {
        try {
            InetAddress address = InetAddress.getByName(host);
            return address.getHostAddress();
        } catch (Exception e) {
            log.warn("Failed to get IP address from host [{}], with exception [{}] ", host, e);
            return null;
        }
    }

    private void fillRole(String solrNode, SnitchContext ctx, String key)
            throws KeeperException, InterruptedException {
        Map<?, ?> roles =
                (Map<?, ?>) ctx.retrieve(ZkStateReader.ROLES); // we don't want to hit the ZK for each node
        // so cache and reuse
        try {
            if (roles == null) roles = ctx.getZkJson(ZkStateReader.ROLES);
            cacheRoles(solrNode, ctx, key, roles);
        } catch (KeeperException.NoNodeException e) {
            cacheRoles(solrNode, ctx, key, Collections.emptyMap());
        }
    }

    private void cacheRoles(String solrNode, SnitchContext ctx, String key, Map<?, ?> roles) {
        ctx.store(ZkStateReader.ROLES, roles);
        if (roles != null) {
            for (Map.Entry<?, ?> e : roles.entrySet()) {
                if (e.getValue() instanceof List) {
                    if (((List<?>) e.getValue()).contains(solrNode)) {
                        ctx.getTags().put(key, e.getKey());
                        break;
                    }
                }
            }
        }
    }


}
