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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.common.util.NamedList;
import org.noggit.CharArr;
import org.noggit.JSONWriter;

/** Sends a POST to the Config API to perform a specified action. */
public class ConfigTool extends ToolBase {

  public ConfigTool() {
    this(CLIO.getOutStream());
  }

  public ConfigTool(PrintStream stdout) {
    super(stdout);
  }

  @Override
  public String getName() {
    return "config";
  }

  @Override
  public List<Option> getOptions() {
    List<Option> configOptions =
        List.of(
            Option.builder("action")
                .argName("ACTION")
                .hasArg()
                .required(false)
                .desc(
                    "Config API action, one of: set-property, unset-property; set-user-property, unset-user-property; default is 'set-property'.")
                .build(),
            Option.builder("property")
                .argName("PROP")
                .hasArg()
                .required(true)
                .desc(
                    "Name of the Config API property to apply the action to, such as: 'updateHandler.autoSoftCommit.maxTime'.")
                .build(),
            Option.builder("value")
                .argName("VALUE")
                .hasArg()
                .required(false)
                .desc("Set the property to this value; accepts JSON objects and strings.")
                .build(),
            SolrCLI.OPTION_SOLRURL,
            SolrCLI.OPTION_ZKHOST,
            Option.builder("p")
                .argName("PORT")
                .hasArg()
                .required(false)
                .desc("The port of the Solr node to use when applying configuration change.")
                .longOpt("port")
                .build(),
            Option.builder("s")
                .argName("SCHEME")
                .hasArg()
                .required(false)
                .desc(
                    "The scheme for accessing Solr.  Accepted values: http or https.  Default is 'http'")
                .longOpt("scheme")
                .build());
    return SolrCLI.joinOptions(configOptions, SolrCLI.cloudOptions);
  }

  @Override
  public void runImpl(CommandLine cli) throws Exception {
    String solrUrl;
    try {
      solrUrl = SolrCLI.resolveSolrUrl(cli);
    } catch (IllegalStateException e) {
      // Fallback to using the provided scheme and port
      final String scheme = cli.getOptionValue("scheme", "http");
      if (cli.hasOption("port")) {
        solrUrl = scheme + "://localhost:" + cli.getOptionValue("port", "8983") + "/solr";
      } else {
        throw e;
      }
    }

    String action = cli.getOptionValue("action", "set-property");
    String collection = cli.getOptionValue("collection");
    String property = cli.getOptionValue("property");
    String value = cli.getOptionValue("value");

    Map<String, Object> jsonObj = new HashMap<>();
    if (value != null) {
      Map<String, String> setMap = new HashMap<>();
      setMap.put(property, value);
      jsonObj.put(action, setMap);
    } else {
      jsonObj.put(action, property);
    }

    CharArr arr = new CharArr();
    (new JSONWriter(arr, 0)).write(jsonObj);
    String jsonBody = arr.toString();

    String updatePath = "/" + collection + "/config";

    echo("\nPOSTing request to Config API: " + solrUrl + updatePath);
    echo(jsonBody);

    try (SolrClient solrClient = new Http2SolrClient.Builder(solrUrl).build()) {
      NamedList<Object> result = SolrCLI.postJsonToSolr(solrClient, updatePath, jsonBody);
      Integer statusCode = (Integer) result.findRecursive("responseHeader", "status");
      if (statusCode == 0) {
        if (value != null) {
          echo("Successfully " + action + " " + property + " to " + value);
        } else {
          echo("Successfully " + action + " " + property);
        }
      } else {
        throw new Exception("Failed to " + action + " property due to:\n" + result);
      }
    }
  }
}
