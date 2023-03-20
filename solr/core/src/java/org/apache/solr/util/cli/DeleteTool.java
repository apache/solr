package org.apache.solr.util.cli;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudLegacySolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.util.CLIO;
import org.apache.solr.util.SolrCLI;
import org.noggit.CharArr;
import org.noggit.JSONWriter;

import java.io.PrintStream;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.solr.common.params.CommonParams.NAME;

public class DeleteTool extends ToolBase {

    public DeleteTool() {
        this(CLIO.getOutStream());
    }

    public DeleteTool(PrintStream stdout) {
        super(stdout);
    }

    @Override
    public String getName() {
        return "delete";
    }

    @Override
    public Option[] getOptions() {
        return new Option[]{
                SolrCLI.OPTION_SOLRURL,
                Option.builder(NAME)
                        .argName("NAME")
                        .hasArg()
                        .required(true)
                        .desc("Name of the core / collection to delete.")
                        .build(),
                Option.builder("deleteConfig")
                        .argName("true|false")
                        .hasArg()
                        .required(false)
                        .desc(
                                "Flag to indicate if the underlying configuration directory for a collection should also be deleted; default is true.")
                        .build(),
                Option.builder("forceDeleteConfig")
                        .required(false)
                        .desc(
                                "Skip safety checks when deleting the configuration directory used by a collection.")
                        .build(),
                SolrCLI.OPTION_ZKHOST,
                SolrCLI.OPTION_VERBOSE
        };
    }

    @Override
    public void runImpl(CommandLine cli) throws Exception {
        SolrCLI.raiseLogLevelUnlessVerbose(cli);
        String solrUrl = cli.getOptionValue("solrUrl", SolrCLI.DEFAULT_SOLR_URL);
        if (!solrUrl.endsWith("/")) solrUrl += "/";

        String systemInfoUrl = solrUrl + "admin/info/system";
        CloseableHttpClient httpClient = SolrCLI.getHttpClient();
        try {
            Map<String, Object> systemInfo = SolrCLI.getJson(httpClient, systemInfoUrl, 2, true);
            if ("solrcloud".equals(systemInfo.get("mode"))) {
                deleteCollection(cli);
            } else {
                deleteCore(cli, solrUrl);
            }
        } finally {
            SolrCLI.closeHttpClient(httpClient);
        }
    }

    protected void deleteCollection(CommandLine cli) throws Exception {
        String zkHost = SolrCLI.getZkHost(cli);
        try (CloudSolrClient cloudSolrClient =
                     new CloudLegacySolrClient.Builder(Collections.singletonList(zkHost), Optional.empty())
                             .withSocketTimeout(30000)
                             .withConnectionTimeout(15000)
                             .build()) {
            echoIfVerbose("Connecting to ZooKeeper at " + zkHost, cli);
            cloudSolrClient.connect();
            deleteCollection(cloudSolrClient, cli);
        }
    }

    protected void deleteCollection(CloudSolrClient cloudSolrClient, CommandLine cli)
            throws Exception {
        Set<String> liveNodes = cloudSolrClient.getClusterState().getLiveNodes();
        if (liveNodes.isEmpty())
            throw new IllegalStateException(
                    "No live nodes found! Cannot delete a collection until "
                            + "there is at least 1 live node in the cluster.");

        String firstLiveNode = liveNodes.iterator().next();
        ZkStateReader zkStateReader = ZkStateReader.from(cloudSolrClient);
        String baseUrl = zkStateReader.getBaseUrlForNodeName(firstLiveNode);
        String collectionName = cli.getOptionValue(NAME);
        if (!zkStateReader.getClusterState().hasCollection(collectionName)) {
            throw new IllegalArgumentException("Collection " + collectionName + " not found!");
        }

        String configName =
                zkStateReader.getClusterState().getCollection(collectionName).getConfigName();
        boolean deleteConfig = "true".equals(cli.getOptionValue("deleteConfig", "true"));
        if (deleteConfig && configName != null) {
            if (cli.hasOption("forceDeleteConfig")) {
                SolrCLI.log.warn(
                        "Skipping safety checks, configuration directory {} will be deleted with impunity.",
                        configName);
            } else {
                // need to scan all Collections to see if any are using the config
                Set<String> collections = zkStateReader.getClusterState().getCollectionsMap().keySet();

                // give a little note to the user if there are many collections in case it takes a while
                if (collections.size() > 50)
                    if (SolrCLI.log.isInfoEnabled()) {
                        SolrCLI.log.info(
                                "Scanning {} to ensure no other collections are using config {}",
                                collections.size(),
                                configName);
                    }

                Optional<String> inUse =
                        collections.stream()
                                .filter(name -> !name.equals(collectionName)) // ignore this collection
                                .filter(
                                        name ->
                                                configName.equals(
                                                        zkStateReader.getClusterState().getCollection(name).getConfigName()))
                                .findFirst();
                if (inUse.isPresent()) {
                    deleteConfig = false;
                    SolrCLI.log.warn(
                            "Configuration directory {} is also being used by {}{}",
                            configName,
                            inUse.get(),
                            "; configuration will not be deleted from ZooKeeper. You can pass the -forceDeleteConfig flag to force delete.");
                }
            }
        }

        String deleteCollectionUrl =
                String.format(
                        Locale.ROOT, "%s/admin/collections?action=DELETE&name=%s", baseUrl, collectionName);

        echoIfVerbose(
                "\nDeleting collection '"
                        + collectionName
                        + "' using command:\n"
                        + deleteCollectionUrl
                        + "\n",
                cli);

        Map<String, Object> json;
        try {
            json = SolrCLI.getJson(deleteCollectionUrl);
        } catch (SolrServerException sse) {
            throw new Exception(
                    "Failed to delete collection '" + collectionName + "' due to: " + sse.getMessage());
        }

        if (deleteConfig) {
            String configZnode = "/configs/" + configName;
            try {
                zkStateReader.getZkClient().clean(configZnode);
            } catch (Exception exc) {
                echo(
                        "\nWARNING: Failed to delete configuration directory "
                                + configZnode
                                + " in ZooKeeper due to: "
                                + exc.getMessage()
                                + "\nYou'll need to manually delete this znode using the zkcli script.");
            }
        }

        if (json != null) {
            CharArr arr = new CharArr();
            new JSONWriter(arr, 2).write(json);
            echo(arr.toString());
            echo("\n");
        }

        echo("Deleted collection '" + collectionName + "' using command:\n" + deleteCollectionUrl);
    }

    protected void deleteCore(CommandLine cli, String solrUrl)
            throws Exception {
        String coreName = cli.getOptionValue(NAME);
        String deleteCoreUrl =
                String.format(
                        Locale.ROOT,
                        "%sadmin/cores?action=UNLOAD&core=%s&deleteIndex=true&deleteDataDir=true&deleteInstanceDir=true",
                        solrUrl,
                        coreName);

        echo("\nDeleting core '" + coreName + "' using command:\n" + deleteCoreUrl + "\n");

        Map<String, Object> json;
        try {
            json = SolrCLI.getJson(deleteCoreUrl);
        } catch (SolrServerException sse) {
            throw new Exception("Failed to delete core '" + coreName + "' due to: " + sse.getMessage());
        }

        if (json != null) {
            CharArr arr = new CharArr();
            new JSONWriter(arr, 2).write(json);
            echoIfVerbose(arr.toString(), cli);
            echoIfVerbose("\n", cli);
        }
    }
}
