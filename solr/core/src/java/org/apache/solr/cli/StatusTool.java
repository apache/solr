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

import java.io.PrintStream;
import java.net.URL;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.util.NamedList;
import org.noggit.CharArr;
import org.noggit.JSONWriter;

public class StatusTool extends ToolBase {
  /** Get the status of a Solr server. */
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

  // These options are not exposed to the end user, and are
  // used directly by the bin/solr status CLI.
  @Override
  public List<Option> getOptions() {
    return List.of(
        Option.builder("solr")
            .argName("URL")
            .hasArg()
            .required(false)
            .desc(
                "Address of the Solr Web application, defaults to: "
                    + SolrCLI.getDefaultSolrUrl()
                    + '.')
            .build(),
        Option.builder("maxWaitSecs")
            .argName("SECS")
            .hasArg()
            .required(false)
            .desc("Wait up to the specified number of seconds to see Solr running.")
            .build());
  }

  @Override
  public void runImpl(CommandLine cli) throws Exception {
    int maxWaitSecs = Integer.parseInt(cli.getOptionValue("maxWaitSecs", "0"));
    String solrUrl = cli.getOptionValue("solr", SolrCLI.getDefaultSolrUrl());
    if (maxWaitSecs > 0) {
      int solrPort = (new URL(solrUrl)).getPort();
      echo("Waiting up to " + maxWaitSecs + " seconds to see Solr running on port " + solrPort);
      try {
        waitToSeeSolrUp(solrUrl, maxWaitSecs, TimeUnit.SECONDS);
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

    NamedList<Object> json = solrClient.request(new CollectionAdminRequest.ClusterStatus());

    List<String> liveNodes = (List<String>) json.findRecursive("cluster", "live_nodes");
    cloudStatus.put("liveNodes", String.valueOf(liveNodes.size()));

    Map<String, Object> collections =
        ((NamedList<Object>) json.findRecursive("cluster", "collections")).asMap();
    cloudStatus.put("collections", String.valueOf(collections.size()));

    return cloudStatus;
  }
}
