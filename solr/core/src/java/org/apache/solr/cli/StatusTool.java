/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.solr.cli;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.solr.cli.SolrProcessManager.SolrProcess;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.request.ClusterApi;
import org.apache.solr.client.solrj.request.CollectionsApi;
import org.apache.solr.client.solrj.request.SystemInfoRequest;
import org.apache.solr.client.solrj.response.SystemInfoResponse;
import org.apache.solr.common.util.URLUtil;
import org.noggit.CharArr;
import org.noggit.JSONWriter;

/**
 * Supports status command in the bin/solr script.
 *
 * <p>Get the status of a Solr server.
 */
@SuppressWarnings("UnnecessarilyFullyQualified")
@picocli.CommandLine.Command(
    name = "status",
    description = "Get the status of a Solr server.",
    exitCodeListHeading = "%nExit Codes:%n",
    exitCodeList = {
      "0:Solr is running.",
      "1:Solr is not running, or the URL/port could not be reached."
    },
    footerHeading = "%nExamples:%n",
    footer = {
      "  # Check status of Solr running on the default port",
      "  bin/solr status",
      "",
      "  # Check status of a specific Solr instance by URL",
      "  bin/solr status -s http://localhost:8983",
      "",
      "  # Wait up to 30 seconds for Solr to start",
      "  bin/solr status --max-wait-secs 30 -s http://localhost:8983"
    })
public class StatusTool extends ToolBase {
  @picocli.CommandLine.Option(
      names = {"--max-wait-secs"},
      description = "Wait up to the specified number of seconds to see Solr running.")
  private Integer maxWaitSecs;

  @picocli.CommandLine.Option(
      names = {"-p", "--port"},
      description = "Port on localhost to check status for")
  private Integer port;

  @picocli.CommandLine.Option(
      names = {"-s", "--solr-url"},
      description = "Base Solr URL, which can be used to determine the zk-host if that's not known")
  private String solrUrl;

  @picocli.CommandLine.Option(
      names = {"--short"},
      paramLabel = "short",
      description = "Short format. Prints one URL per line for running instances")
  private boolean shortFormat;

  @picocli.CommandLine.Option(
      names = {"-u", "--credentials"},
      description =
          "Credentials in the format username:password. Example: --credentials solr:SolrRocks")
  private String credentials;

  @Deprecated
  private static final Option MAX_WAIT_SECS_OPTION =
      Option.builder()
          .longOpt("max-wait-secs")
          .hasArg()
          .argName("SECS")
          .type(Integer.class)
          .deprecated() // Will make it a stealth option, not printed or complained about
          .desc("Wait up to the specified number of seconds to see Solr running.")
          .get();

  @Deprecated
  public static final Option PORT_OPTION =
      Option.builder("p")
          .longOpt("port")
          .hasArg()
          .argName("PORT")
          .type(Integer.class)
          .desc("Port on localhost to check status for")
          .get();

  @Deprecated
  public static final Option SHORT_OPTION =
      Option.builder()
          .longOpt("short")
          .argName("SHORT")
          .desc("Short format. Prints one URL per line for running instances")
          .get();

  private final SolrProcessManager processMgr;

  public StatusTool() {
    this(new DefaultToolRuntime());
  }

  public StatusTool(ToolRuntime runtime) {
    super(runtime);
    processMgr = new SolrProcessManager();
  }

  @Override
  public String getName() {
    return "status";
  }

  @Override
  public Options getOptions() {
    OptionGroup optionGroup = new OptionGroup();
    optionGroup.addOption(PORT_OPTION);
    optionGroup.addOption(CommonCLIOptions.SOLR_URL_OPTION);
    return super.getOptions()
        .addOption(MAX_WAIT_SECS_OPTION)
        .addOption(SHORT_OPTION)
        .addOption(CommonCLIOptions.CREDENTIALS_OPTION)
        .addOptionGroup(optionGroup);
  }

  @Override
  public void runImpl(CommandLine cli) throws Exception {
    solrUrl = cli.getOptionValue(CommonCLIOptions.SOLR_URL_OPTION);
    port = cli.hasOption(PORT_OPTION) ? cli.getParsedOptionValue(PORT_OPTION) : null;
    shortFormat = cli.hasOption(SHORT_OPTION);
    maxWaitSecs = cli.getParsedOptionValue(MAX_WAIT_SECS_OPTION, 0);

    runTool();
  }

  public int runTool() throws Exception {
    if (solrUrl != null) {
      if (!URLUtil.hasScheme(solrUrl)) {
        CLIO.err("Invalid URL provided: " + solrUrl);
        runtime.exit(1);
      }

      // URL provided, do not consult local processes, as the URL may be remote
      if (maxWaitSecs > 0) {
        // Used by Windows start script when starting Solr
        try {
          waitForSolrUpAndPrintStatus(solrUrl);
          runtime.exit(0);
        } catch (Exception e) {
          CLIO.err(e.getMessage());
          runtime.exit(1);
        }
      } else {
        boolean running = printStatusFromRunningSolr(solrUrl);
        runtime.exit(running ? 0 : 1);
      }
    }

    if (port != null) {
      Optional<SolrProcess> proc = processMgr.processForPort(port);
      if (proc.isEmpty()) {
        CLIO.err("Could not find a running Solr on port " + port);
        runtime.exit(1);
      } else {
        solrUrl = proc.get().getLocalUrl();
        if (shortFormat) {
          CLIO.out(solrUrl);
        } else {
          printProcessStatus(proc.get());
        }
        runtime.exit(0);
      }
    }

    // No URL or port, scan for running processes
    Collection<SolrProcess> procs = processMgr.scanSolrPidFiles();
    if (!procs.isEmpty()) {
      for (SolrProcess process : procs) {
        if (shortFormat) {
          CLIO.out(process.getLocalUrl());
        } else {
          printProcessStatus(process);
        }
      }
    } else {
      if (!shortFormat) {
        CLIO.out("\nNo Solr nodes are running.\n");
      }
    }
    return 0;
  }

  private void printProcessStatus(SolrProcess process) throws Exception {
    String pidUrl = process.getLocalUrl();
    if (shortFormat) {
      CLIO.out(pidUrl);
    } else {
      if (maxWaitSecs > 0) {
        waitForSolrUpAndPrintStatus(pidUrl);
      } else {
        CLIO.out(
            String.format(
                Locale.ROOT,
                "\nSolr process %s running on port %s",
                process.pid(),
                process.port()));
        printStatusFromRunningSolr(pidUrl);
      }
    }
    CLIO.out("");
  }

  public void waitForSolrUpAndPrintStatus(String pidUrl) throws Exception {
    int solrPort = -1;
    try {
      solrPort = CLIUtils.portFromUrl(pidUrl);
    } catch (Exception e) {
      CLIO.err("Invalid URL provided, does not contain port");
      runtime.exit(1);
    }
    echo("Waiting up to " + maxWaitSecs + " seconds to see Solr running on port " + solrPort);
    boolean solrUp = waitForSolrUp(pidUrl);
    if (solrUp) {
      echo("Started Solr server on port " + solrPort + ". Happy searching!");
    } else {
      throw new Exception(
          "Solr at " + solrUrl + " did not come online within " + maxWaitSecs + " seconds!");
    }
  }

  /**
   * Wait for Solr to come online and return true if it does, false otherwise.
   *
   * @return true if Solr comes online, false otherwise
   */
  public boolean waitForSolrUp(String pidUrl) throws Exception {
    try {
      waitToSeeSolrUp(pidUrl, credentials, maxWaitSecs, TimeUnit.SECONDS);
      return true;
    } catch (TimeoutException timeout) {
      return false;
    }
  }

  public boolean printStatusFromRunningSolr(String pidUrl) {
    String statusJson = null;
    try {
      statusJson = statusFromRunningSolr(pidUrl);
    } catch (Exception e) {
      /* ignore */
    }
    if (statusJson != null) {
      runtime.println(statusJson);
    } else {
      CLIO.err("Solr at " + pidUrl + " not online.");
    }
    return statusJson != null;
  }

  /**
   * Get the status of a Solr server and responds with a JSON status string.
   *
   * @return the status of the Solr server or null if the server is not online
   * @throws Exception if there is an error getting the status
   */
  public String statusFromRunningSolr(String pidUrl) throws Exception {
    try {
      CharArr arr = new CharArr();
      new JSONWriter(arr, 2).write(getStatus(pidUrl));
      return arr.toString();
    } catch (Exception exc) {
      if (CLIUtils.exceptionIsAuthRelated(exc)) {
        throw exc;
      }
      if (CLIUtils.checkCommunicationError(exc)) {
        // this is not actually an error from the tool as it's ok if Solr is not online.
        return null;
      } else {
        throw new Exception("Failed to get system information from " + pidUrl + " due to: " + exc);
      }
    }
  }

  public Map<String, Object> waitToSeeSolrUp(String pidUrl) throws Exception {
    return waitToSeeSolrUp(pidUrl, credentials, maxWaitSecs, TimeUnit.SECONDS);
  }

  @SuppressWarnings("BusyWait")
  public Map<String, Object> waitToSeeSolrUp(
      String pidUrl, String credentials, long maxWait, TimeUnit unit) throws Exception {
    long timeout = System.nanoTime() + TimeUnit.NANOSECONDS.convert(maxWait, unit);
    while (System.nanoTime() < timeout) {

      try {
        return getStatus(pidUrl);
      } catch (Exception exc) {
        if (CLIUtils.exceptionIsAuthRelated(exc)) {
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
        "Did not see Solr at "
            + solrUrl
            + " come online within "
            + TimeUnit.SECONDS.convert(maxWait, unit)
            + " seconds!");
  }

  public Map<String, Object> getStatus(String pidUrl) throws Exception {
    return getStatus(pidUrl, credentials);
  }

  public Map<String, Object> getStatus(String pidUrl, String credentials) throws Exception {
    try (var solrClient = CLIUtils.getSolrClient(pidUrl, credentials)) {
      return reportStatus(solrClient);
    }
  }

  public static Map<String, Object> reportStatus(SolrClient solrClient) throws Exception {
    Map<String, Object> status = new LinkedHashMap<>();
    SystemInfoResponse sysResponse = (new SystemInfoRequest()).process(solrClient);
    status.put("solr_home", sysResponse.getSolrHome() != null ? sysResponse.getSolrHome() : "?");
    status.put("version", sysResponse.getSolrImplVersion());

    status.put("startTime", sysResponse.getJVMStartTime());
    status.put("uptime", sysResponse.getJVMUpTimeMillis());

    status.put(
        "memory",
        sysResponse.getHumanReadableJVMMemoryUsed()
            + " of "
            + sysResponse.getHumanReadableJVMMemoryTotal());

    // if this is a Solr in solrcloud mode, gather some basic cluster info
    if ("solrcloud".equals(sysResponse.getMode())) {
      status.put("cloud", getCloudStatus(solrClient, sysResponse.getZkHost()));
    }
    return status;
  }

  /**
   * Calls V2 API endpoints to get basic status information about the SolrCloud cluster.
   *
   * <p>Uses GET /cluster/nodes for live node count and GET /collections for collection count.
   */
  private static Map<String, String> getCloudStatus(SolrClient solrClient, String zkHost)
      throws Exception {
    Map<String, String> cloudStatus = new LinkedHashMap<>();
    cloudStatus.put("ZooKeeper", (zkHost != null) ? zkHost : "?");

    var nodesResponse = new ClusterApi.ListClusterNodes().process(solrClient);
    var liveNodes = nodesResponse != null ? nodesResponse.nodes : null;
    cloudStatus.put("liveNodes", String.valueOf(liveNodes != null ? liveNodes.size() : 0));

    var collectionsResponse = new CollectionsApi.ListCollections().process(solrClient);
    var collections =
        collectionsResponse != null && collectionsResponse.collections != null
            ? collectionsResponse.collections
            : List.of();
    cloudStatus.put("collections", String.valueOf(collections.size()));

    return cloudStatus;
  }

  @Override
  public int callTool() throws Exception {
    return runTool();
  }
}
