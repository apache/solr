package org.apache.solr.util.cli;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.util.CLIO;
import org.apache.solr.util.SolrCLI;

import java.io.PrintStream;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

public class ZkMvTool extends ToolBase {

    public ZkMvTool() {
        this(CLIO.getOutStream());
    }

    public ZkMvTool(PrintStream stdout) {
        super(stdout);
    }

    @Override
    public Option[] getOptions() {
        return new Option[]{
                Option.builder("src")
                        .argName("src")
                        .hasArg()
                        .required(true)
                        .desc("Source Znode to move from.")
                        .build(),
                Option.builder("dst")
                        .argName("dst")
                        .hasArg()
                        .required(true)
                        .desc("Destination Znode to move to.")
                        .build(),
                SolrCLI.OPTION_ZKHOST,
                SolrCLI.OPTION_VERBOSE
        };
    }

    @Override
    public String getName() {
        return "mv";
    }

    @Override
    public void runImpl(CommandLine cli) throws Exception {
        SolrCLI.raiseLogLevelUnlessVerbose(cli);
        String zkHost = SolrCLI.getZkHost(cli);
        if (zkHost == null) {
            throw new IllegalStateException(
                    "Solr at "
                            + cli.getOptionValue("solrUrl")
                            + " is running in standalone server mode, 'zk rm' can only be used when running in SolrCloud mode.\n");
        }

        try (SolrZkClient zkClient =
                     new SolrZkClient.Builder()
                             .withUrl(zkHost)
                             .withTimeout(30000, TimeUnit.MILLISECONDS)
                             .build()) {
            echoIfVerbose("\nConnecting to ZooKeeper at " + zkHost + " ...", cli);
            String src = cli.getOptionValue("src");
            String dst = cli.getOptionValue("dst");

            if (src.toLowerCase(Locale.ROOT).startsWith("file:")
                    || dst.toLowerCase(Locale.ROOT).startsWith("file:")) {
                throw new SolrServerException(
                        "mv command operates on znodes and 'file:' has been specified.");
            }
            String source = src;
            if (src.toLowerCase(Locale.ROOT).startsWith("zk")) {
                source = src.substring(3);
            }

            String dest = dst;
            if (dst.toLowerCase(Locale.ROOT).startsWith("zk")) {
                dest = dst.substring(3);
            }

            echo("Moving Znode " + source + " to " + dest + " on ZooKeeper at " + zkHost);
            zkClient.moveZnode(source, dest);
        } catch (Exception e) {
            SolrCLI.log.error("Could not complete mv operation for reason: ", e);
            throw (e);
        }
    }
}
