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

import static org.apache.solr.cli.SolrCLI.OPTION_SOLRURL;

import java.io.PrintStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DeprecatedAttributes;
import org.apache.commons.cli.Option;
import org.apache.solr.cli.SolrProcessManager.SolrProcess;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.URLUtil;
import org.noggit.CharArr;
import org.noggit.JSONWriter;

public class StatusTool extends ToolBase {
  private final SolrProcessManager processMgr;

  public StatusTool() {
    this(CLIO.getOutStream());
  }

  public StatusTool(PrintStream stdout) {
    super(stdout);
    processMgr = new SolrProcessManager();
  }

  @Override
  public String getName() {
    return "status";
  }

  private static final Option OPTION_MAXWAITSECS =
      Option.builder()
          .longOpt("max-wait-secs")
          .argName("SECS")
          .hasArg()
          .required(false)
          .deprecated() // Will make it a stealth option, not printed or complained about
          .desc("Wait up to the specified number of seconds to see Solr running.")
          .build();

  private static final Option OPTION_MAXWAITSECS_DEPRECATED =
      Option.builder("maxWaitSecs")
          .argName("SECS")
          .hasArg()
          .required(false)
          .desc("Wait up to the specified number of seconds to see Solr running.")
          .deprecated(
              DeprecatedAttributes.builder()
                  .setForRemoval(true)
                  .setSince("9.7")
                  .setDescription("Use --max-wait-secs instead")
                  .get())
          .build();

  public static final Option OPTION_PORT =
      Option.builder("p")
          .longOpt("port")
          .argName("PORT")
          .required(false)
          .hasArg()
          .desc("Port on localhost to check status for")
          .build();

  public static final Option OPTION_SHORT =
      Option.builder()
          .longOpt("short")
          .argName("SHORT")
          .required(false)
          .desc("Short format. Prints one URL per line for running instances")
          .build();

  @Override
  public List<Option> getOptions() {
    return List.of(
        OPTION_SOLRURL,
        OPTION_MAXWAITSECS,
        OPTION_MAXWAITSECS_DEPRECATED,
        OPTION_PORT,
        OPTION_SHORT);
  }

  @Override
  public void runImpl(CommandLine cli) throws Exception {
    String solrUrl = cli.getOptionValue(OPTION_SOLRURL);
    Integer port =
        cli.hasOption(OPTION_PORT) ? Integer.parseInt(cli.getOptionValue(OPTION_PORT)) : null;
    boolean shortFormat = cli.hasOption(OPTION_SHORT);
    int maxWaitSecs = Integer.parseInt(cli.getOptionValue("max-wait-secs", "0"));

    if (port != null && solrUrl != null) {
      throw new IllegalArgumentException("Only one of port or url can be specified");
    }

    if (solrUrl != null) {
      if (!URLUtil.hasScheme(solrUrl)) {
        CLIO.err("Invalid URL provided: " + solrUrl);
        System.exit(1);
      }

      // URL provided, do not consult local processes, as the URL may be remote
      if (maxWaitSecs > 0) {
        // Used by Windows start script when starting Solr
        try {
          waitForSolrUpAndPrintStatus(solrUrl, cli, maxWaitSecs);
          System.exit(0);
        } catch (Exception e) {
          CLIO.err(e.getMessage());
          System.exit(1);
        }
      } else {
        boolean running = printStatusFromRunningSolr(solrUrl, cli);
        System.exit(running ? 0 : 1);
      }
    }

    if (port != null) {
      Optional<SolrProcess> proc = processMgr.processForPort(port);
      if (proc.isEmpty()) {
        CLIO.err("Could not find a running Solr on port " + port);
        System.exit(1);
      } else {
        solrUrl = proc.get().getLocalUrl();
        if (shortFormat) {
          CLIO.out(solrUrl);
        } else {
          printProcessStatus(proc.get(), cli);
        }
        System.exit(0);
      }
    }

    // No URL or port, scan for running processes
    Collection<SolrProcess> procs = processMgr.scanSolrPidFiles();
    if (!procs.isEmpty()) {
      for (SolrProcess process : procs) {
        if (shortFormat) {
          CLIO.out(process.getLocalUrl());
        } else {
          printProcessStatus(process, cli);
        }
      }
    } else {
      if (!shortFormat) {
        CLIO.out("\nNo Solr nodes are running.\n");
      }
    }
  }

  private void printProcessStatus(SolrProcess process, CommandLine cli) throws Exception {
    int maxWaitSecs =
        Integer.parseInt(
            SolrCLI.getOptionWithDeprecatedAndDefault(cli, "max-wait-secs", "maxWaitSecs", "0"));
    boolean shortFormat = cli.hasOption(OPTION_SHORT);
    String pidUrl = process.getLocalUrl();
    if (shortFormat) {
      CLIO.out(pidUrl);
    } else {
      if (maxWaitSecs > 0) {
        waitForSolrUpAndPrintStatus(pidUrl, cli, maxWaitSecs);
      } else {
        CLIO.out(
            String.format(
                Locale.ROOT,
                "\nSolr process %s running on port %s",
                process.getPid(),
                process.getPort()));
        printStatusFromRunningSolr(pidUrl, cli);
      }
    }
    CLIO.out("");
  }

  private Integer portFromUrl(String solrUrl) {
    try {
      URI uri = new URI(solrUrl);
      int port = uri.getPort();
      if (port == -1) {
        return uri.getScheme().equals("https") ? 443 : 80;
      } else {
        return port;
      }
    } catch (URISyntaxException e) {
      CLIO.err("Invalid URL provided, does not contain port");
      System.exit(1);
      return null;
    }
  }

  public void waitForSolrUpAndPrintStatus(String solrUrl, CommandLine cli, int maxWaitSecs)
      throws Exception {
    int solrPort = portFromUrl(solrUrl);
    echo("Waiting up to " + maxWaitSecs + " seconds to see Solr running on port " + solrPort);
    boolean solrUp = waitForSolrUp(solrUrl, cli, maxWaitSecs);
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
   * @param solrUrl the URL of the Solr server
   * @param cli the command line options
   * @param maxWaitSecs the maximum number of seconds to wait
   * @return true if Solr comes online, false otherwise
   */
  public boolean waitForSolrUp(String solrUrl, CommandLine cli, int maxWaitSecs) throws Exception {
    try {
      waitToSeeSolrUp(solrUrl, maxWaitSecs, TimeUnit.SECONDS);
      return true;
    } catch (TimeoutException timeout) {
      return false;
    }
  }

  public boolean printStatusFromRunningSolr(String solrUrl, CommandLine cli) throws Exception {
    String statusJson = null;
    try {
      statusJson = statusFromRunningSolr(solrUrl, cli);
    } catch (Exception e) {
      /* ignore */
    }
    if (statusJson != null) {
      CLIO.out(statusJson);
    } else {
      CLIO.err("Solr at " + solrUrl + " not online.");
    }
    return statusJson != null;
  }

  /**
   * Get the status of a Solr server and responds with a JSON status string.
   *
   * @param solrUrl the URL of the Solr server
   * @param cli the command line options
   * @return the status of the Solr server or null if the server is not online
   * @throws Exception if there is an error getting the status
   */
  public String statusFromRunningSolr(String solrUrl, CommandLine cli) throws Exception {
    try {
      CharArr arr = new CharArr();
      new JSONWriter(arr, 2).write(getStatus(solrUrl));
      return arr.toString();
    } catch (Exception exc) {
      if (SolrCLI.exceptionIsAuthRelated(exc)) {
        throw exc;
      }
      if (SolrCLI.checkCommunicationError(exc)) {
        // this is not actually an error from the tool as it's ok if Solr is not online.
        return null;
      } else {
        throw new Exception("Failed to get system information from " + solrUrl + " due to: " + exc);
      }
    }
  }

  @SuppressWarnings("BusyWait")
  public Map<String, Object> waitToSeeSolrUp(String solrUrl, long maxWait, TimeUnit unit)
      throws Exception {
    long timeout = System.nanoTime() + TimeUnit.NANOSECONDS.convert(maxWait, unit);
    while (System.nanoTime() < timeout) {
      try {
        return getStatus(solrUrl);
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
        "Did not see Solr at "
            + solrUrl
            + " come online within "
            + TimeUnit.SECONDS.convert(maxWait, unit)
            + " seconds!");
  }

  public Map<String, Object> getStatus(String solrUrl) throws Exception {
    Map<String, Object> status;

    if (!solrUrl.endsWith("/")) solrUrl += "/";

    try (var solrClient = SolrCLI.getSolrClient(solrUrl)) {
      NamedList<Object> systemInfo =
          solrClient.request(
              new GenericSolrRequest(SolrRequest.METHOD.GET, CommonParams.SYSTEM_INFO_PATH));
      // convert raw JSON into user-friendly output
      status = reportStatus(systemInfo, solrClient);
    }

    return status;
  }

  public Map<String, Object> reportStatus(NamedList<Object> info, SolrClient solrClient)
      throws Exception {
    Map<String, Object> status = new LinkedHashMap<>();

    String solrHome = (String) info.get("solr_home");
    status.put("solr_home", solrHome != null ? solrHome : "?");
    status.put("version", info.findRecursive("lucene", "solr-impl-version"));
    status.put("startTime", info.findRecursive("jvm", "jmx", "startTime").toString());
    status.put("uptime", SolrCLI.uptime((Long) info.findRecursive("jvm", "jmx", "upTimeMS")));

    String usedMemory = (String) info.findRecursive("jvm", "memory", "used");
    String totalMemory = (String) info.findRecursive("jvm", "memory", "total");
    status.put("memory", usedMemory + " of " + totalMemory);

    // if this is a Solr in solrcloud mode, gather some basic cluster info
    if ("solrcloud".equals(info.get("mode"))) {
      String zkHost = (String) info.get("zkHost");
      status.put("cloud", getCloudStatus(solrClient, zkHost));
    }

    return status;
  }

  /**
   * Calls the CLUSTERSTATUS endpoint in Solr to get basic status information about the SolrCloud
   * cluster.
   */
  @SuppressWarnings("unchecked")
  protected Map<String, String> getCloudStatus(SolrClient solrClient, String zkHost)
      throws Exception {
    Map<String, String> cloudStatus = new LinkedHashMap<>();
    cloudStatus.put("ZooKeeper", (zkHost != null) ? zkHost : "?");

    // TODO add booleans to request just what we want; not everything
    NamedList<Object> json = solrClient.request(new CollectionAdminRequest.ClusterStatus());

    List<String> liveNodes = (List<String>) json.findRecursive("cluster", "live_nodes");
    cloudStatus.put("liveNodes", String.valueOf(liveNodes.size()));

    // TODO get this as a metric from the metrics API instead, or something else.
    var collections = (NamedList<Object>) json.findRecursive("cluster", "collections");
    cloudStatus.put("collections", String.valueOf(collections.size()));

    return cloudStatus;
  }
}
