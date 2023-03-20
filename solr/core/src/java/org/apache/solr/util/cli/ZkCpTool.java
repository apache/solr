package org.apache.solr.util.cli;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.util.CLIO;
import org.apache.solr.util.SolrCLI;

import java.io.PrintStream;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

public class ZkCpTool extends ToolBase {

    public ZkCpTool() {
        this(CLIO.getOutStream());
    }

    public ZkCpTool(PrintStream stdout) {
        super(stdout);
    }

    @Override
    public Option[] getOptions() {
        return new Option[]{
                Option.builder("src")
                        .argName("src")
                        .hasArg()
                        .required(true)
                        .desc("Source file or directory, may be local or a Znode.")
                        .build(),
                Option.builder("dst")
                        .argName("dst")
                        .hasArg()
                        .required(true)
                        .desc("Destination of copy, may be local or a Znode.")
                        .build(),
                SolrCLI.OPTION_RECURSE,
                SolrCLI.OPTION_ZKHOST,
                SolrCLI.OPTION_VERBOSE
        };
    }

    @Override
    public String getName() {
        return "cp";
    }

    @Override
    public void runImpl(CommandLine cli) throws Exception {
        SolrCLI.raiseLogLevelUnlessVerbose(cli);
        String zkHost = SolrCLI.getZkHost(cli);
        if (zkHost == null) {
            throw new IllegalStateException(
                    "Solr at "
                            + cli.getOptionValue("solrUrl")
                            + " is running in standalone server mode, cp can only be used when running in SolrCloud mode.\n");
        }

        try (SolrZkClient zkClient =
                     new SolrZkClient.Builder()
                             .withUrl(zkHost)
                             .withTimeout(30000, TimeUnit.MILLISECONDS)
                             .build()) {
            echoIfVerbose("\nConnecting to ZooKeeper at " + zkHost + " ...", cli);
            String src = cli.getOptionValue("src");
            String dst = cli.getOptionValue("dst");
            Boolean recurse = Boolean.parseBoolean(cli.getOptionValue("recurse"));
            echo("Copying from '" + src + "' to '" + dst + "'. ZooKeeper at " + zkHost);

            boolean srcIsZk = src.toLowerCase(Locale.ROOT).startsWith("zk:");
            boolean dstIsZk = dst.toLowerCase(Locale.ROOT).startsWith("zk:");

            String srcName = src;
            if (srcIsZk) {
                srcName = src.substring(3);
            } else if (srcName.toLowerCase(Locale.ROOT).startsWith("file:")) {
                srcName = srcName.substring(5);
            }

            String dstName = dst;
            if (dstIsZk) {
                dstName = dst.substring(3);
            } else {
                if (dstName.toLowerCase(Locale.ROOT).startsWith("file:")) {
                    dstName = dstName.substring(5);
                }
            }
            zkClient.zkTransfer(srcName, srcIsZk, dstName, dstIsZk, recurse);
        } catch (Exception e) {
            SolrCLI.log.error("Could not complete the zk operation for reason: ", e);
            throw (e);
        }
    }
}
