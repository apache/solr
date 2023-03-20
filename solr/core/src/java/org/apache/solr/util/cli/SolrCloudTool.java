package org.apache.solr.util.cli;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.solr.client.solrj.impl.CloudLegacySolrClient;
import org.apache.solr.util.SolrCLI;

import java.io.PrintStream;
import java.util.Collections;
import java.util.Optional;

/**
 * Helps build SolrCloud aware tools by initializing a CloudSolrClient instance before running the
 * tool.
 */
public abstract class SolrCloudTool extends ToolBase {

    protected SolrCloudTool(PrintStream stdout) {
        super(stdout);
    }

    @Override
    public Option[] getOptions() {
        return SolrCLI.cloudOptions;
    }

    @Override
    public void runImpl(CommandLine cli) throws Exception {
        SolrCLI.raiseLogLevelUnlessVerbose(cli);
        String zkHost = cli.getOptionValue(SolrCLI.OPTION_ZKHOST.getOpt(), SolrCLI.ZK_HOST);

        SolrCLI.log.debug("Connecting to Solr cluster: {}", zkHost);
        try (var cloudSolrClient =
                     new CloudLegacySolrClient.Builder(Collections.singletonList(zkHost), Optional.empty())
                             .build()) {

            cloudSolrClient.connect();
            runCloudTool(cloudSolrClient, cli);
        }
    }

    /**
     * Runs a SolrCloud tool with CloudSolrClient initialized
     */
    protected abstract void runCloudTool(CloudLegacySolrClient cloudSolrClient, CommandLine cli)
            throws Exception;
}
