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
import org.apache.commons.cli.DeprecatedAttributes;
import org.apache.commons.cli.MissingArgumentException;
import org.apache.commons.cli.Option;
import org.apache.solr.client.solrj.SolrClient;
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
    return List.of(
        Option.builder("c")
            .longOpt("name")
            .argName("NAME")
            .hasArg()
            .required(true)
            .desc("Name of the collection.")
            .build(),
        Option.builder("a")
            .longOpt("action")
            .argName("ACTION")
            .hasArg()
            .required(false)
            .desc(
                "Config API action, one of: set-property, unset-property, set-user-property, unset-user-property; default is 'set-property'.")
            .build(),
        Option.builder()
            .longOpt("property")
            .argName("PROP")
            .hasArg()
            .required(
                false) // Should be TRUE but have a deprecated option to deal with first, so we
            // enforce in code
            .desc(
                "Name of the Config API property to apply the action to, such as: 'updateHandler.autoSoftCommit.maxTime'.")
            .build(),
        Option.builder("p")
            .deprecated(
                DeprecatedAttributes.builder()
                    .setForRemoval(true)
                    .setSince("9.8")
                    .setDescription("Use --property instead")
                    .get())
            .hasArg()
            .argName("PROP")
            .required(false)
            .desc(
                "Name of the Config API property to apply the action to, such as: 'updateHandler.autoSoftCommit.maxTime'.")
            .build(),
        Option.builder("v")
            .longOpt("value")
            .argName("VALUE")
            .hasArg()
            .required(false)
            .desc("Set the property to this value; accepts JSON objects and strings.")
            .build(),
        SolrCLI.OPTION_SOLRURL,
        SolrCLI.OPTION_SOLRURL_DEPRECATED,
        SolrCLI.OPTION_ZKHOST,
        SolrCLI.OPTION_ZKHOST_DEPRECATED);
  }

  @Override
  public void runImpl(CommandLine cli) throws Exception {
    String solrUrl = SolrCLI.normalizeSolrUrl(cli);
    String action = cli.getOptionValue("action", "set-property");
    String collection = cli.getOptionValue("name");
    String property = SolrCLI.getOptionWithDeprecatedAndDefault(cli, "property", "p", null);
    String value = cli.getOptionValue("value");

    if (property == null) {
      throw new MissingArgumentException("'property' is a required option.");
    }

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

    try (SolrClient solrClient = SolrCLI.getSolrClient(solrUrl)) {
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
