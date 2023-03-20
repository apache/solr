package org.apache.solr.util.cli;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.solr.util.CLIO;
import org.apache.solr.util.SolrCLI;
import org.noggit.CharArr;
import org.noggit.JSONWriter;

import javax.net.ssl.SSLPeerUnverifiedException;
import java.io.PrintStream;
import java.net.URL;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Get the status of a Solr server.
 */
public class StatusTool extends ToolBase {

    public StatusTool() {
        this(CLIO.getOutStream());
    }

    public StatusTool(PrintStream stdout) {
        super(stdout);
    }

    @Override
    public String getName() {
        return "status";
    }

    @Override
    public Option[] getOptions() {
        return new Option[]{
                Option.builder("solr")
                        .argName("URL")
                        .hasArg()
                        .required(false)
                        .desc("Address of the Solr Web application, defaults to: " + SolrCLI.DEFAULT_SOLR_URL + '.')
                        .build(),
                Option.builder("maxWaitSecs")
                        .argName("SECS")
                        .hasArg()
                        .required(false)
                        .desc("Wait up to the specified number of seconds to see Solr running.")
                        .build()
        };
    }

    @Override
    public void runImpl(CommandLine cli) throws Exception {
        int maxWaitSecs = Integer.parseInt(cli.getOptionValue("maxWaitSecs", "0"));
        String solrUrl = cli.getOptionValue("solr", SolrCLI.DEFAULT_SOLR_URL);
        if (maxWaitSecs > 0) {
            int solrPort = (new URL(solrUrl)).getPort();
            echo("Waiting up to " + maxWaitSecs + " seconds to see Solr running on port " + solrPort);
            try {
                waitToSeeSolrUp(solrUrl, maxWaitSecs);
                echo("Started Solr server on port " + solrPort + ". Happy searching!");
            } catch (TimeoutException timeout) {
                throw new Exception(
                        "Solr at " + solrUrl + " did not come online within " + maxWaitSecs + " seconds!");
            }
        } else {
            try {
                CharArr arr = new CharArr();
                new JSONWriter(arr, 2).write(getStatus(solrUrl));
                echo(arr.toString());
            } catch (Exception exc) {
                if (SolrCLI.exceptionIsAuthRelated(exc)) {
                    throw exc;
                }
                if (SolrCLI.checkCommunicationError(exc)) {
                    // this is not actually an error from the tool as it's ok if Solr is not online.
                    CLIO.err("Solr at " + solrUrl + " not online.");
                } else {
                    throw new Exception(
                            "Failed to get system information from " + solrUrl + " due to: " + exc);
                }
            }
        }
    }

    public Map<String, Object> waitToSeeSolrUp(String solrUrl, int maxWaitSecs) throws Exception {
        long timeout =
                System.nanoTime() + TimeUnit.NANOSECONDS.convert(maxWaitSecs, TimeUnit.SECONDS);
        while (System.nanoTime() < timeout) {
            try {
                return getStatus(solrUrl);
            } catch (SSLPeerUnverifiedException exc) {
                throw exc;
            } catch (Exception exc) {
                if (SolrCLI.exceptionIsAuthRelated(exc)) {
                    throw exc;
                }
                try {
                    Thread.sleep(2000L);
                } catch (InterruptedException interrupted) {
                    timeout = 0; // stop looping
                }
            }
        }
        throw new TimeoutException(
                "Did not see Solr at " + solrUrl + " come online within " + maxWaitSecs + " seconds!");
    }

    public Map<String, Object> getStatus(String solrUrl) throws Exception {
        Map<String, Object> status = null;

        if (!solrUrl.endsWith("/")) solrUrl += "/";

        String systemInfoUrl = solrUrl + "admin/info/system";
        CloseableHttpClient httpClient = SolrCLI.getHttpClient();
        try {
            // hit Solr to get system info
            Map<String, Object> systemInfo = SolrCLI.getJson(httpClient, systemInfoUrl, 2, true);
            // convert raw JSON into user-friendly output
            status = reportStatus(solrUrl, systemInfo, httpClient);
        } finally {
            SolrCLI.closeHttpClient(httpClient);
        }

        return status;
    }

    public Map<String, Object> reportStatus(
            String solrUrl, Map<String, Object> info, HttpClient httpClient) throws Exception {
        Map<String, Object> status = new LinkedHashMap<>();

        String solrHome = (String) info.get("solr_home");
        status.put("solr_home", solrHome != null ? solrHome : "?");
        status.put("version", SolrCLI.asString("/lucene/solr-impl-version", info));
        status.put("startTime", SolrCLI.asString("/jvm/jmx/startTime", info));
        status.put("uptime", SolrCLI.uptime(SolrCLI.asLong("/jvm/jmx/upTimeMS", info)));

        String usedMemory = SolrCLI.asString("/jvm/memory/used", info);
        String totalMemory = SolrCLI.asString("/jvm/memory/total", info);
        status.put("memory", usedMemory + " of " + totalMemory);

        // if this is a Solr in solrcloud mode, gather some basic cluster info
        if ("solrcloud".equals(info.get("mode"))) {
            String zkHost = (String) info.get("zkHost");
            status.put("cloud", getCloudStatus(httpClient, solrUrl, zkHost));
        }

        return status;
    }

    /**
     * Calls the CLUSTERSTATUS endpoint in Solr to get basic status information about the SolrCloud
     * cluster.
     */
    protected Map<String, String> getCloudStatus(
            HttpClient httpClient, String solrUrl, String zkHost) throws Exception {
        Map<String, String> cloudStatus = new LinkedHashMap<>();
        cloudStatus.put("ZooKeeper", (zkHost != null) ? zkHost : "?");

        String clusterStatusUrl = solrUrl + "admin/collections?action=CLUSTERSTATUS";
        Map<String, Object> json = SolrCLI.getJson(httpClient, clusterStatusUrl, 2, true);

        List<String> liveNodes = SolrCLI.asList("/cluster/live_nodes", json);
        cloudStatus.put("liveNodes", String.valueOf(liveNodes.size()));

        Map<String, Object> collections = SolrCLI.asMap("/cluster/collections", json);
        cloudStatus.put("collections", String.valueOf(collections.size()));

        return cloudStatus;
    }
} // end StatusTool class
