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
import org.apache.solr.common.util.NamedList;
import org.noggit.CharArr;
import org.noggit.JSONWriter;

/**
 * Supports config command in the bin/solr script.
 *
 * <p>Sends a POST to the Config API to perform a specified action.
 */
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
    return List.of(
        Option.builder("c")
            .longOpt("name")
            .argName("NAME")
            .hasArg()
            .required(true)
            .desc("Name of the collection.")
            .build(),
        Option.builder("action")
            .argName("ACTION")
            .hasArg()
            .required(false)
            .desc(
                "Config API action, one of: set-property, unset-property, set-user-property, unset-user-property; default is 'set-property'.")
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
        SolrCLI.OPTION_CREDENTIALS);
  }

  @Override
  public void runImpl(CommandLine cli) throws Exception {
    String solrUrl = SolrCLI.normalizeSolrUrl(cli);
    String action = cli.getOptionValue("action", "set-property");
    String collection = cli.getOptionValue("name");
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

    try (SolrClient solrClient =
        SolrCLI.getSolrClient(solrUrl, cli.getOptionValue(SolrCLI.OPTION_CREDENTIALS.getLongOpt()))) {
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
