package org.apache.solr.util.cli;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.util.CLIO;
import org.apache.solr.util.SolrCLI;

import java.io.PrintStream;
import java.util.concurrent.TimeUnit;

public class ZkMkrootTool extends ToolBase {

    public ZkMkrootTool() {
        this(CLIO.getOutStream());
    }

    public ZkMkrootTool(PrintStream stdout) {
        super(stdout);
    }

    @Override
    public Option[] getOptions() {
        return new Option[]{
                Option.builder("path")
                        .argName("path")
                        .hasArg()
                        .required(true)
                        .desc("Path to create.")
                        .build(),
                SolrCLI.OPTION_ZKHOST,
                SolrCLI.OPTION_VERBOSE
        };
    }

    @Override
    public String getName() {
        return "mkroot";
    }

    @Override
    public void runImpl(CommandLine cli) throws Exception {
        SolrCLI.raiseLogLevelUnlessVerbose(cli);
        String zkHost = SolrCLI.getZkHost(cli);

        if (zkHost == null) {
            throw new IllegalStateException(
                    "Solr at "
                            + cli.getOptionValue("zkHost")
                            + " is running in standalone server mode, 'zk mkroot' can only be used when running in SolrCloud mode.\n");
        }

        try (SolrZkClient zkClient =
                     new SolrZkClient.Builder()
                             .withUrl(zkHost)
                             .withTimeout(30000, TimeUnit.MILLISECONDS)
                             .build()) {
            echoIfVerbose("\nConnecting to ZooKeeper at " + zkHost + " ...", cli);

            String znode = cli.getOptionValue("path");
            echo("Creating ZooKeeper path " + znode + " on ZooKeeper at " + zkHost);
            zkClient.makePath(znode, true);
        } catch (Exception e) {
            SolrCLI.log.error("Could not complete mkroot operation for reason: ", e);
            throw (e);
        }
    }
} // End zkMkrootTool class
