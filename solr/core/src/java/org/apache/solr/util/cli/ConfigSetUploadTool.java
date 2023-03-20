package org.apache.solr.util.cli;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkMaintenanceUtils;
import org.apache.solr.core.ConfigSetService;
import org.apache.solr.util.CLIO;
import org.apache.solr.util.SolrCLI;

import java.io.PrintStream;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

public class ConfigSetUploadTool extends ToolBase {

    public ConfigSetUploadTool() {
        this(CLIO.getOutStream());
    }

    public ConfigSetUploadTool(PrintStream stdout) {
        super(stdout);
    }

    @Override
    public Option[] getOptions() {
        return new Option[]{
                Option.builder("confname")
                        .argName("confname") // Comes out in help message
                        .hasArg() // Has one sub-argument
                        .required(true) // confname argument must be present
                        .desc("Configset name in ZooKeeper.")
                        .build(), // passed as -confname value
                Option.builder("confdir")
                        .argName("confdir")
                        .hasArg()
                        .required(true)
                        .desc("Local directory with configs.")
                        .build(),
                Option.builder("configsetsDir")
                        .argName("configsetsDir")
                        .hasArg()
                        .required(false)
                        .desc("Parent directory of example configsets.")
                        .build(),
                SolrCLI.OPTION_ZKHOST,
                SolrCLI.OPTION_VERBOSE
        };
    }

    @Override
    public String getName() {
        return "upconfig";
    }

    @Override
    public void runImpl(CommandLine cli) throws Exception {
        SolrCLI.raiseLogLevelUnlessVerbose(cli);
        String zkHost = SolrCLI.getZkHost(cli);
        if (zkHost == null) {
            throw new IllegalStateException(
                    "Solr at "
                            + cli.getOptionValue("solrUrl")
                            + " is running in standalone server mode, upconfig can only be used when running in SolrCloud mode.\n");
        }

        String confName = cli.getOptionValue("confname");
        try (SolrZkClient zkClient =
                     new SolrZkClient.Builder()
                             .withUrl(zkHost)
                             .withTimeout(30000, TimeUnit.MILLISECONDS)
                             .build()) {
            echoIfVerbose("\nConnecting to ZooKeeper at " + zkHost + " ...", cli);
            Path confPath =
                    ConfigSetService.getConfigsetPath(
                            cli.getOptionValue("confdir"), cli.getOptionValue("configsetsDir"));

            echo(
                    "Uploading "
                            + confPath.toAbsolutePath().toString()
                            + " for config "
                            + cli.getOptionValue("confname")
                            + " to ZooKeeper at "
                            + zkHost);
            ZkMaintenanceUtils.uploadToZK(
                    zkClient,
                    confPath,
                    ZkMaintenanceUtils.CONFIGS_ZKNODE + "/" + confName,
                    ZkMaintenanceUtils.UPLOAD_FILENAME_EXCLUDE_PATTERN);

        } catch (Exception e) {
            SolrCLI.log.error("Could not complete upconfig operation for reason: ", e);
            throw (e);
        }
    }
}
