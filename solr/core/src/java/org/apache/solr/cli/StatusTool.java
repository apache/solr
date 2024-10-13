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
import org.apache.commons.cli.Option;
import org.apache.solr.cli.SolrProcessManager.SolrProcess;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.util.NamedList;
import org.noggit.CharArr;
import org.noggit.JSONWriter;

/**
 * Supports status command in the bin/solr script.
 *
 * <p>Get the status of a Solr server.
 */
public class StatusTool extends ToolBase {
  private static Map<Long, ProcessHandle> processes;
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
          .desc("Wait up to the specified number of seconds to see Solr running.")
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
    return List.of(OPTION_SOLRURL, OPTION_MAXWAITSECS, OPTION_PORT, OPTION_SHORT);
  }

  @Override
  public void runImpl(CommandLine cli) throws Exception {
    String solrUrl = cli.getOptionValue(OPTION_SOLRURL);
    Integer port =
        cli.hasOption(OPTION_PORT) ? Integer.parseInt(cli.getOptionValue(OPTION_PORT)) : null;
    boolean shortFormat = cli.hasOption(OPTION_SHORT);

    if (port != null && solrUrl != null) {
      throw new IllegalArgumentException("Only one of port or url can be specified");
    }

    if (port != null) {
      Optional<SolrProcess> proc = processMgr.processForPort(port);
      if (proc.isEmpty()) {
        CLIO.err("Could not find a running Solr on port " + port);
        System.exit(1);
      } else {
        solrUrl = proc.get().getLocalUrl();
      }
    }

    if (solrUrl == null) {
      if (!shortFormat) {
        CLIO.out("Looking for running processes...");
      }
      Collection<SolrProcess> procs = processMgr.getAllRunning();
      if (!shortFormat) {
        CLIO.out(String.format(Locale.ROOT, "\nFound %s Solr nodes: ", procs.size()));
      }
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
    } else {
      // We have a solrUrl
      int urlPort = portFromUrl(solrUrl);
      if (shortFormat) {
        if (processMgr.isRunningWithPort(urlPort)) {
          CLIO.out(solrUrl);
        }
      } else {
        Optional<SolrProcess> process = processMgr.processForPort(urlPort);
        if (process.isPresent()) {
          CLIO.out(String.format(Locale.ROOT, "\nFound %s Solr nodes: ", 1));
          printProcessStatus(process.get(), cli);
        } else {
          CLIO.out("\nNo Solr running on port " + urlPort + ".");
        }
      }
    }
  }

  private void printProcessStatus(SolrProcess process, CommandLine cli) throws Exception {
    int maxWaitSecs = Integer.parseInt(cli.getOptionValue("max-wait-secs", "0"));
    boolean shortFormat = cli.hasOption(OPTION_SHORT);
    String pidUrl = process.getLocalUrl();
    CLIO.out(
        String.format(
            Locale.ROOT,
            "\nSolr process %s running on port %s",
            process.getPid(),
            process.getPort()));
    if (shortFormat) {
      CLIO.out(pidUrl);
    } else {
      statusFromRunningSolr(pidUrl, cli, maxWaitSecs);
    }
    CLIO.out("");
  }

  private Integer portFromUrl(String solrUrl) {
    try {
      return new URI(solrUrl).getPort();
    } catch (URISyntaxException e) {
      CLIO.err("Invalid URL provided, does not contain port");
      System.exit(1);
      return null;
    }
  }

  public void statusFromRunningSolr(String solrUrl, CommandLine cli, int maxWaitSecs)
      throws Exception {
    if (maxWaitSecs > 0) {
      int solrPort = new URI(solrUrl).getPort();
      echo("Waiting up to " + maxWaitSecs + " seconds to see Solr running on port " + solrPort);
      try {
        waitToSeeSolrUp(
            solrUrl,
            cli.getOptionValue(SolrCLI.OPTION_CREDENTIALS.getLongOpt()),
            maxWaitSecs,
            TimeUnit.SECONDS);
        echo("Started Solr server on port " + solrPort + ". Happy searching!");
      } catch (TimeoutException timeout) {
        throw new Exception(
            "Solr at " + solrUrl + " did not come online within " + maxWaitSecs + " seconds!");
      }
    } else {
      try {
        CharArr arr = new CharArr();
        new JSONWriter(arr, 2)
            .write(getStatus(solrUrl, cli.getOptionValue(SolrCLI.OPTION_CREDENTIALS.getLongOpt())));
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

  public Map<String, Object> waitToSeeSolrUp(
      String solrUrl, String credentials, long maxWait, TimeUnit unit) throws Exception {
    long timeout = System.nanoTime() + TimeUnit.NANOSECONDS.convert(maxWait, unit);
    while (System.nanoTime() < timeout) {

      try {
        return getStatus(solrUrl, credentials);
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

  public Map<String, Object> getStatus(String solrUrl, String credentials) throws Exception {
    try (var solrClient = SolrCLI.getSolrClient(solrUrl, credentials)) {
      return getStatus(solrClient);
    }
  }

  public Map<String, Object> getStatus(SolrClient solrClient) throws Exception {
    Map<String, Object> status;

    NamedList<Object> systemInfo =
        solrClient.request(
            new GenericSolrRequest(SolrRequest.METHOD.GET, CommonParams.SYSTEM_INFO_PATH));
    // convert raw JSON into user-friendly output
    status = reportStatus(systemInfo, solrClient);

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

    NamedList<Object> json = solrClient.request(new CollectionAdminRequest.ClusterStatus());

    List<String> liveNodes = (List<String>) json.findRecursive("cluster", "live_nodes");
    cloudStatus.put("liveNodes", String.valueOf(liveNodes.size()));

    Map<String, Object> collections =
        ((NamedList<Object>) json.findRecursive("cluster", "collections")).asMap();
    cloudStatus.put("collections", String.valueOf(collections.size()));

    return cloudStatus;
  }
}
