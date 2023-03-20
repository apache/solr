package org.apache.solr.util.cli;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.util.CLIO;
import org.apache.solr.util.SolrCLI;

import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

public class ConfigSetDownloadTool extends ToolBase {

    public ConfigSetDownloadTool() {
        this(CLIO.getOutStream());
    }

    public ConfigSetDownloadTool(PrintStream stdout) {
        super(stdout);
    }

    @Override
    public Option[] getOptions() {
        return new Option[]{
                Option.builder("confname")
                        .argName("confname")
                        .hasArg()
                        .required(true)
                        .desc("Configset name in ZooKeeper.")
                        .build(),
                Option.builder("confdir")
                        .argName("confdir")
                        .hasArg()
                        .required(true)
                        .desc("Local directory with configs.")
                        .build(),
                SolrCLI.OPTION_ZKHOST,
                SolrCLI.OPTION_VERBOSE
        };
    }

    @Override
    public String getName() {
        return "downconfig";
    }

    @Override
    public void runImpl(CommandLine cli) throws Exception {
        SolrCLI.raiseLogLevelUnlessVerbose(cli);
        String zkHost = SolrCLI.getZkHost(cli);
        if (zkHost == null) {
            throw new IllegalStateException(
                    "Solr at "
                            + cli.getOptionValue("solrUrl")
                            + " is running in standalone server mode, downconfig can only be used when running in SolrCloud mode.\n");
        }

        try (SolrZkClient zkClient =
                     new SolrZkClient.Builder()
                             .withUrl(zkHost)
                             .withTimeout(30000, TimeUnit.MILLISECONDS)
                             .build()) {
            echoIfVerbose("\nConnecting to ZooKeeper at " + zkHost + " ...", cli);
            String confName = cli.getOptionValue("confname");
            String confDir = cli.getOptionValue("confdir");
            Path configSetPath = Paths.get(confDir);
            // we try to be nice about having the "conf" in the directory, and we create it if it's not
            // there.
            if (configSetPath.endsWith("/conf") == false) {
                configSetPath = Paths.get(configSetPath.toString(), "conf");
            }
            Files.createDirectories(configSetPath);
            echo(
                    "Downloading configset "
                            + confName
                            + " from ZooKeeper at "
                            + zkHost
                            + " to directory "
                            + configSetPath.toAbsolutePath());

            zkClient.downConfig(confName, configSetPath);
        } catch (Exception e) {
            SolrCLI.log.error("Could not complete downconfig operation for reason: ", e);
            throw (e);
        }
    }
} // End ConfigSetDownloadTool class
