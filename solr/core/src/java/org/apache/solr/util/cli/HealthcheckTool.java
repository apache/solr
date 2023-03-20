package org.apache.solr.util.cli;

import org.apache.commons.cli.CommandLine;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.CloudLegacySolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.util.CLIO;
import org.apache.solr.util.SolrCLI;
import org.noggit.CharArr;
import org.noggit.JSONWriter;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.solr.common.params.CommonParams.DISTRIB;

/**
 * Requests health information about a specific collection in SolrCloud.
 */
public class HealthcheckTool extends SolrCloudTool {

    public HealthcheckTool() {
        this(CLIO.getOutStream());
    }

    public HealthcheckTool(PrintStream stdout) {
        super(stdout);
    }

    @Override
    public String getName() {
        return "healthcheck";
    }

    @Override
    protected void runCloudTool(CloudLegacySolrClient cloudSolrClient, CommandLine cli)
            throws Exception {
        SolrCLI.raiseLogLevelUnlessVerbose(cli);
        String collection = cli.getOptionValue("collection");
        if (collection == null)
            throw new IllegalArgumentException(
                    "Must provide a collection to run a healthcheck against!");

        SolrCLI.log.debug("Running healthcheck for {}", collection);

        ZkStateReader zkStateReader = ZkStateReader.from(cloudSolrClient);

        ClusterState clusterState = zkStateReader.getClusterState();
        Set<String> liveNodes = clusterState.getLiveNodes();
        final DocCollection docCollection = clusterState.getCollectionOrNull(collection);
        if (docCollection == null || docCollection.getSlices() == null)
            throw new IllegalArgumentException("Collection " + collection + " not found!");

        Collection<Slice> slices = docCollection.getSlices();
        // Test http code using a HEAD request first, fail fast if authentication failure
        String urlForColl =
                zkStateReader.getLeaderUrl(collection, slices.stream().findFirst().get().getName(), 1000);
        SolrCLI.attemptHttpHead(urlForColl, cloudSolrClient.getHttpClient());

        SolrQuery q = new SolrQuery("*:*");
        q.setRows(0);
        QueryResponse qr = cloudSolrClient.query(collection, q);
        String collErr = null;
        long docCount = -1;
        try {
            docCount = qr.getResults().getNumFound();
        } catch (Exception exc) {
            collErr = String.valueOf(exc);
        }

        List<Object> shardList = new ArrayList<>();
        boolean collectionIsHealthy = (docCount != -1);

        for (Slice slice : slices) {
            String shardName = slice.getName();
            // since we're reporting health of this shard, there's no guarantee of a leader
            String leaderUrl = null;
            try {
                leaderUrl = zkStateReader.getLeaderUrl(collection, shardName, 1000);
            } catch (Exception exc) {
                SolrCLI.log.warn("Failed to get leader for shard {} due to: {}", shardName, exc);
            }

            List<SolrCLI.ReplicaHealth> replicaList = new ArrayList<>();
            for (Replica r : slice.getReplicas()) {

                String uptime = null;
                String memory = null;
                String replicaStatus = null;
                long numDocs = -1L;

                ZkCoreNodeProps replicaCoreProps = new ZkCoreNodeProps(r);
                String coreUrl = replicaCoreProps.getCoreUrl();
                boolean isLeader = coreUrl.equals(leaderUrl);

                // if replica's node is not live, its status is DOWN
                String nodeName = replicaCoreProps.getNodeName();
                if (nodeName == null || !liveNodes.contains(nodeName)) {
                    replicaStatus = Replica.State.DOWN.toString();
                } else {
                    // query this replica directly to get doc count and assess health
                    q = new SolrQuery("*:*");
                    q.setRows(0);
                    q.set(DISTRIB, "false");
                    try (HttpSolrClient solr = new HttpSolrClient.Builder(coreUrl).build()) {

                        String solrUrl = solr.getBaseURL();

                        qr = solr.query(q);
                        numDocs = qr.getResults().getNumFound();

                        int lastSlash = solrUrl.lastIndexOf('/');
                        String systemInfoUrl = solrUrl.substring(0, lastSlash) + "/admin/info/system";
                        Map<String, Object> info = SolrCLI.getJson(solr.getHttpClient(), systemInfoUrl, 2, true);
                        uptime = SolrCLI.uptime(SolrCLI.asLong("/jvm/jmx/upTimeMS", info));
                        String usedMemory = SolrCLI.asString("/jvm/memory/used", info);
                        String totalMemory = SolrCLI.asString("/jvm/memory/total", info);
                        memory = usedMemory + " of " + totalMemory;

                        // if we get here, we can trust the state
                        replicaStatus = replicaCoreProps.getState();
                    } catch (Exception exc) {
                        SolrCLI.log.error("ERROR: {} when trying to reach: {}", exc, coreUrl);

                        if (SolrCLI.checkCommunicationError(exc)) {
                            replicaStatus = Replica.State.DOWN.toString();
                        } else {
                            replicaStatus = "error: " + exc;
                        }
                    }
                }

                replicaList.add(
                        new SolrCLI.ReplicaHealth(
                                shardName,
                                r.getName(),
                                coreUrl,
                                replicaStatus,
                                numDocs,
                                isLeader,
                                uptime,
                                memory));
            }

            SolrCLI.ShardHealth shardHealth = new SolrCLI.ShardHealth(shardName, replicaList);
            if (SolrCLI.ShardState.healthy != shardHealth.getShardState())
                collectionIsHealthy = false; // at least one shard is un-healthy

            shardList.add(shardHealth.asMap());
        }

        Map<String, Object> report = new LinkedHashMap<>();
        report.put("collection", collection);
        report.put("status", collectionIsHealthy ? "healthy" : "degraded");
        if (collErr != null) {
            report.put("error", collErr);
        }
        report.put("numDocs", docCount);
        report.put("numShards", slices.size());
        report.put("shards", shardList);

        CharArr arr = new CharArr();
        new JSONWriter(arr, 2).write(report);
        echo(arr.toString());
    }
} // end HealthcheckTool
