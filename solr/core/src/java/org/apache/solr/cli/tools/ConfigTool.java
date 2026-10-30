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

package org.apache.solr.cli.tools;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.MissingArgumentException;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.solr.cli.CLIO;
import org.apache.solr.cli.CLIUtils;
import org.apache.solr.cli.CommonCLIOptions;
import org.apache.solr.cli.ConnectionOptions;
import org.apache.solr.cli.CredentialsOptions;
import org.apache.solr.cli.DefaultToolRuntime;
import org.apache.solr.cli.SolrCLI;
import org.apache.solr.cli.ToolBase;
import org.apache.solr.cli.ToolRuntime;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.common.util.EnvUtils;
import org.apache.solr.common.util.NamedList;
import org.noggit.CharArr;
import org.noggit.JSONWriter;

/**
 * Supports config command in the bin/solr script.
 *
 * <p>Sends a POST to the Config API to perform a specified action.
 */
@SuppressWarnings("UnnecessarilyFullyQualified")
@picocli.CommandLine.Command(
    name = "config",
    description = "Sends a POST to the Config API to perform a specified action.",
    footerHeading = "%nExamples:%n",
    footer = {
      "  # Set a config property",
      "  bin/solr config -c mycollection --property updateHandler.autoSoftCommit.maxTime --value"
          + " 10000"
    })
public class ConfigTool extends ToolBase {

  /**
   * @deprecated Only used by the commons-cli parser; the picocli path declares this as an annotated
   *     field.
   */
  @Deprecated
  private static final Option COLLECTION_NAME_OPTION =
      Option.builder("c")
          .longOpt("name")
          .hasArg()
          .argName("NAME")
          .required()
          .desc("Name of the collection.")
          .get();

  /**
   * @deprecated Only used by the commons-cli parser; the picocli path declares this as an annotated
   *     field.
   */
  @Deprecated
  private static final Option ACTION_OPTION =
      Option.builder("a")
          .longOpt("action")
          .hasArg()
          .argName("ACTION")
          .desc(
              "Config API action, one of: set-property, unset-property, set-user-property, unset-user-property; default is 'set-property'.")
          .get();

  /**
   * @deprecated Only used by the commons-cli parser; the picocli path declares this as an annotated
   *     field.
   */
  @Deprecated
  private static final Option PROPERTY_OPTION =
      Option.builder()
          .longOpt("property")
          .hasArg()
          .argName("PROP")
          .required()
          .desc(
              "Name of the Config API property to apply the action to, such as: 'updateHandler.autoSoftCommit.maxTime'.")
          .get();

  /**
   * @deprecated Only used by the commons-cli parser; the picocli path declares this as an annotated
   *     field.
   */
  @Deprecated
  private static final Option VALUE_OPTION =
      Option.builder("v")
          .longOpt("value")
          .hasArg()
          .argName("VALUE")
          .desc("Set the property to this value; accepts JSON objects and strings.")
          .get();

  /** Parameters for the config command, independent of the command line parser. */
  record ConfigParams(
      String solrUrl,
      String action,
      String collection,
      String property,
      String value,
      String credentials) {}

  // --- picocli fields ---

  @picocli.CommandLine.ArgGroup(exclusive = true, multiplicity = "0..1")
  private ConnectionOptions connectionOptions;

  @picocli.CommandLine.Mixin private CredentialsOptions credentialsOptions;

  @picocli.CommandLine.Option(
      names = {"-c", "--name"},
      required = true,
      paramLabel = "NAME",
      description = "Name of the collection.")
  private String nameOpt;

  /** The values of {@code --action}, spelled on the command line as {@link #toString()} says. */
  enum Action {
    SET_PROPERTY("set-property"),
    UNSET_PROPERTY("unset-property"),
    SET_USER_PROPERTY("set-user-property"),
    UNSET_USER_PROPERTY("unset-user-property");

    private final String id;

    Action(String id) {
      this.id = id;
    }

    @Override
    public String toString() {
      return id;
    }
  }

  @picocli.CommandLine.Option(
      names = {"-a", "--action"},
      defaultValue = "set-property",
      paramLabel = "ACTION",
      description =
          "Config API action, one of: ${COMPLETION-CANDIDATES}; default is '${DEFAULT-VALUE}'.")
  private Action actionOpt;

  @picocli.CommandLine.Option(
      names = "--property",
      required = true,
      paramLabel = "PROP",
      description =
          "Name of the Config API property to apply the action to, such as:"
              + " 'updateHandler.autoSoftCommit.maxTime'.")
  private String propertyOpt;

  // Long-only: "-v" is ToolBase's --verbose, and picocli rejects a duplicate short name. Under
  // commons-cli the later-added VALUE_OPTION wins, so there "-v" still means --value.
  @picocli.CommandLine.Option(
      names = "--value",
      paramLabel = "VALUE",
      description = "Set the property to this value; accepts JSON objects and strings.")
  private String valueOpt;

  public ConfigTool() {
    this(new DefaultToolRuntime());
  }

  public ConfigTool(ToolRuntime runtime) {
    super(runtime);
  }

  @Override
  public String getName() {
    return "config";
  }

  @Override
  public Options getOptions() {
    return super.getOptions()
        .addOption(COLLECTION_NAME_OPTION)
        .addOption(ACTION_OPTION)
        .addOption(PROPERTY_OPTION)
        .addOption(VALUE_OPTION)
        .addOption(CommonCLIOptions.CREDENTIALS_OPTION)
        .addOptionGroup(getConnectionOptions());
  }

  @Override
  public void runImpl(CommandLine cli) throws Exception {
    String solrUrl = CLIUtils.normalizeSolrUrl(cli);
    String action = cli.getOptionValue(ACTION_OPTION, "set-property");
    String value = cli.getOptionValue(VALUE_OPTION);

    // value is required unless the property is one of the "unset-" type.
    if (!action.contains("unset-") && value == null) {
      throw new MissingArgumentException("'value' is a required option.");
    }

    ConfigParams params =
        new ConfigParams(
            solrUrl,
            action,
            cli.getOptionValue(COLLECTION_NAME_OPTION),
            cli.getOptionValue(PROPERTY_OPTION),
            value,
            cli.getOptionValue(CommonCLIOptions.CREDENTIALS_OPTION));
    updateConfig(params);
  }

  void updateConfig(ConfigParams params) throws Exception {
    String solrUrl = params.solrUrl();
    String action = params.action();
    String collection = params.collection();
    String property = params.property();
    String value = params.value();

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
    echoIfVerbose(jsonBody);

    try (SolrClient solrClient = CLIUtils.getSolrClient(solrUrl, params.credentials())) {
      NamedList<Object> result = SolrCLI.postJsonToSolr(solrClient, updatePath, jsonBody);
      Integer statusCode = (Integer) result._get(List.of("responseHeader", "status"), null);
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

  @Override
  public int callTool() throws Exception {
    String solrUrl = resolveSolrUrl(credentialsOptions.credentials);

    // value is required unless the property is one of the "unset-" type.
    String action = actionOpt.toString();
    if (!action.contains("unset-") && valueOpt == null) {
      throw new MissingArgumentException("'value' is a required option.");
    }

    ConfigParams params =
        new ConfigParams(
            solrUrl, action, nameOpt, propertyOpt, valueOpt, credentialsOptions.credentials);
    updateConfig(params);
    return 0;
  }

  private String resolveSolrUrl(String credentials) throws Exception {
    String solrUrlArg = (connectionOptions != null) ? connectionOptions.effectiveSolrUrl() : null;
    if (solrUrlArg != null) {
      return CLIUtils.normalizeSolrUrl(solrUrlArg);
    }
    String zkHostArg =
        (connectionOptions != null)
            ? connectionOptions.effectiveZkHost()
            : EnvUtils.getProperty("zkHost");
    if (zkHostArg != null) {
      return CLIUtils.solrUrlFromConnection(
          CloudSolrClient.CloudSolrClientConnection.parse(zkHostArg), credentials);
    }
    String defaultSolrUrl = CLIUtils.getDefaultSolrUrl();
    CLIO.err(
        "Neither --zk-host or --solr-url parameters, nor ZK_HOST env var provided, so assuming solr url is "
            + defaultSolrUrl
            + ".");
    return defaultSolrUrl;
  }
}
