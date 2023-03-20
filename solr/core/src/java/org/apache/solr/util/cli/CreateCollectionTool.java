package org.apache.solr.util.cli;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudLegacySolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.common.cloud.ZkMaintenanceUtils;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionAdminParams;
import org.apache.solr.core.ConfigSetService;
import org.apache.solr.util.CLIO;
import org.apache.solr.util.SolrCLI;
import org.noggit.CharArr;
import org.noggit.JSONWriter;

import java.io.PrintStream;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.solr.common.params.CommonParams.NAME;

/**
 * Supports create_collection command in the bin/solr script.
 */
public class CreateCollectionTool extends ToolBase {

    public CreateCollectionTool() {
        this(CLIO.getOutStream());
    }

    public CreateCollectionTool(PrintStream stdout) {
        super(stdout);
    }

    @Override
    public String getName() {
        return "create_collection";
    }

    @Override
    public Option[] getOptions() {
        return SolrCLI.CREATE_COLLECTION_OPTIONS;
    }

    @Override
    public void runImpl(CommandLine cli) throws Exception {
        SolrCLI.raiseLogLevelUnlessVerbose(cli);
        String zkHost = SolrCLI.getZkHost(cli);
        if (zkHost == null) {
            throw new IllegalStateException(
                    "Solr at "
                            + cli.getOptionValue("solrUrl")
                            + " is running in standalone server mode, please use the create_core command instead;\n"
                            + "create_collection can only be used when running in SolrCloud mode.\n");
        }

        try (CloudSolrClient cloudSolrClient =
                     new CloudLegacySolrClient.Builder(Collections.singletonList(zkHost), Optional.empty())
                             .build()) {
            echoIfVerbose("\nConnecting to ZooKeeper at " + zkHost + " ...", cli);
            cloudSolrClient.connect();
            runCloudTool(cloudSolrClient, cli);
        }
    }

    protected void runCloudTool(CloudSolrClient cloudSolrClient, CommandLine cli) throws Exception {

        Set<String> liveNodes = cloudSolrClient.getClusterState().getLiveNodes();
        if (liveNodes.isEmpty())
            throw new IllegalStateException(
                    "No live nodes found! Cannot create a collection until "
                            + "there is at least 1 live node in the cluster.");

        String baseUrl = cli.getOptionValue("solrUrl");
        if (baseUrl == null) {
            String firstLiveNode = liveNodes.iterator().next();
            baseUrl = ZkStateReader.from(cloudSolrClient).getBaseUrlForNodeName(firstLiveNode);
        }

        String collectionName = cli.getOptionValue(NAME);

        // build a URL to create the collection
        int numShards = optionAsInt(cli, "shards", 1);
        int replicationFactor = optionAsInt(cli, "replicationFactor", 1);

        String confname = cli.getOptionValue("confname");
        String confdir = cli.getOptionValue("confdir");
        String configsetsDir = cli.getOptionValue("configsetsDir");

        boolean configExistsInZk =
                confname != null
                        && !"".equals(confname.trim())
                        && ZkStateReader.from(cloudSolrClient)
                        .getZkClient()
                        .exists("/configs/" + confname, true);

        if (CollectionAdminParams.SYSTEM_COLL.equals(collectionName)) {
            // do nothing
        } else if (configExistsInZk) {
            echo("Re-using existing configuration directory " + confname);
        } else if (confdir != null && !"".equals(confdir.trim())) {
            if (confname == null || "".equals(confname.trim())) {
                confname = collectionName;
            }
            Path confPath = ConfigSetService.getConfigsetPath(confdir, configsetsDir);

            echoIfVerbose(
                    "Uploading "
                            + confPath.toAbsolutePath()
                            + " for config "
                            + confname
                            + " to ZooKeeper at "
                            + cloudSolrClient.getClusterStateProvider().getQuorumHosts(),
                    cli);
            ZkMaintenanceUtils.uploadToZK(
                    ZkStateReader.from(cloudSolrClient).getZkClient(),
                    confPath,
                    ZkMaintenanceUtils.CONFIGS_ZKNODE + "/" + confname,
                    ZkMaintenanceUtils.UPLOAD_FILENAME_EXCLUDE_PATTERN);
        }

        // since creating a collection is a heavy-weight operation, check for existence first
        String collectionListUrl = baseUrl + "/admin/collections?action=list";
        if (SolrCLI.safeCheckCollectionExists(collectionListUrl, collectionName)) {
            throw new IllegalStateException(
                    "\nCollection '"
                            + collectionName
                            + "' already exists!\nChecked collection existence using Collections API command:\n"
                            + collectionListUrl);
        }

        // doesn't seem to exist ... try to create
        String createCollectionUrl =
                String.format(
                        Locale.ROOT,
                        "%s/admin/collections?action=CREATE&name=%s&numShards=%d&replicationFactor=%d",
                        baseUrl,
                        collectionName,
                        numShards,
                        replicationFactor);
        if (confname != null && !"".equals(confname.trim())) {
            createCollectionUrl =
                    createCollectionUrl + String.format(Locale.ROOT, "&collection.configName=%s", confname);
        }

        echoIfVerbose(
                "\nCreating new collection '"
                        + collectionName
                        + "' using command:\n"
                        + createCollectionUrl
                        + "\n",
                cli);

        Map<String, Object> json = null;
        try {
            json = SolrCLI.getJson(createCollectionUrl);
        } catch (SolrServerException sse) {
            throw new Exception(
                    "Failed to create collection '" + collectionName + "' due to: " + sse.getMessage());
        }

        if (cli.hasOption(SolrCLI.OPTION_VERBOSE.getOpt())) {
            CharArr arr = new CharArr();
            new JSONWriter(arr, 2).write(json);
            echo(arr.toString());
        } else {
            String endMessage =
                    String.format(
                            Locale.ROOT,
                            "Created collection '%s' with %d shard(s), %d replica(s)",
                            collectionName,
                            numShards,
                            replicationFactor);
            if (confname != null && !"".equals(confname.trim())) {
                endMessage += String.format(Locale.ROOT, " with config-set '%s'", confname);
            }

            echo(endMessage);
        }
    }

    protected int optionAsInt(CommandLine cli, String option, int defaultVal) {
        return Integer.parseInt(cli.getOptionValue(option, String.valueOf(defaultVal)));
    }
} // end CreateCollectionTool class
