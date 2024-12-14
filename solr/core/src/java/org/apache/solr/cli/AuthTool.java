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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.Console;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.lucene.util.Constants;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.core.SolrCore;
import org.apache.solr.security.Sha256AuthenticationProvider;
import org.apache.zookeeper.KeeperException;

/** Supports auth command in the bin/solr script. */
public class AuthTool extends ToolBase {

  private static final Option TYPE_OPTION =
      Option.builder()
          .longOpt("type")
          .hasArg()
          .desc(
              "The authentication mechanism to enable (currently only basicAuth). Defaults to 'basicAuth'.")
          .build();

  private static final Option PROMPT_OPTION =
      Option.builder()
          .longOpt("prompt")
          .hasArg()
          .type(Boolean.class)
          .desc(
              "Prompts the user to provide the credentials. Use either --credentials or --prompt, not both.")
          .build();

  private static final Option BLOCK_UNKNOWN_OPTION =
      Option.builder()
          .longOpt("block-unknown")
          .desc("Blocks all access for unknown users (requires authentication for all endpoints).")
          .hasArg()
          .argName("true|false")
          .type(Boolean.class)
          .build();

  private static final Option SOLR_INCLUDE_FILE_OPTION =
      Option.builder()
          .longOpt("solr-include-file")
          .hasArg()
          .argName("FILE")
          .desc(
              "The Solr include file which contains overridable environment variables for configuring Solr configurations.")
          .build();

  private static final Option UPDATE_INCLUDE_FILE_OPTION =
      Option.builder()
          .longOpt("update-include-file-only")
          .desc(
              "Only update the solr.in.sh or solr.in.cmd file, and skip actual enabling/disabling"
                  + " authentication (i.e. don't update security.json).")
          .hasArg()
          .type(Boolean.class)
          .build();

  private static final Option AUTH_CONF_DIR_OPTION =
      Option.builder()
          .longOpt("auth-conf-dir")
          .hasArg()
          .argName("FILE")
          .required()
          .desc(
              "This is where any authentication related configuration files, if any, would be placed.")
          .build();

  public AuthTool() {
    this(CLIO.getOutStream());
  }

  public AuthTool(PrintStream stdout) {
    super(stdout);
  }

  @Override
  public String getName() {
    return "auth";
  }

  @Override
  public String getUsage() {
    return "\n  bin/solr auth enable [--type basicAuth] --credentials user:pass [--block-unknown <true|false>] [--update-include-file-only <true|false>] [-v]\n"
        + "  bin/solr auth enable [--type basicAuth] --prompt <true|false> [--block-unknown <true|false>] [--update-include-file-only <true|false>] [-v]\n"
        + "  bin/solr auth disable [--update-include-file-only <true|false>] [-v]\n";
  }

  @Override
  public String getHeader() {
    return "Updates or enables/disables authentication.  Must be run on the Solr server itself.\n"
        + "\n"
        + "List of options:";
  }

  List<String> authenticationVariables =
      Arrays.asList(
          "SOLR_AUTHENTICATION_CLIENT_BUILDER", "SOLR_AUTH_TYPE", "SOLR_AUTHENTICATION_OPTS");

  @Override
  public Options getOptions() {
    return super.getOptions()
        .addOption(TYPE_OPTION)
        .addOption(PROMPT_OPTION)
        .addOption(BLOCK_UNKNOWN_OPTION)
        .addOption(SOLR_INCLUDE_FILE_OPTION)
        .addOption(UPDATE_INCLUDE_FILE_OPTION)
        .addOption(AUTH_CONF_DIR_OPTION)
        .addOption(CommonCLIOptions.CREDENTIALS_OPTION)
        .addOptionGroup(getConnectionOptions());
  }

  private void ensureArgumentIsValidBooleanIfPresent(CommandLine cli, Option option) {
    if (cli.hasOption(option)) {
      final String value = cli.getOptionValue(option);
      if (!"true".equalsIgnoreCase(value) && !"false".equalsIgnoreCase(value)) {
        echo(
            "Argument ["
                + option.getLongOpt()
                + "] must be either true or false, but was ["
                + value
                + "]");
        SolrCLI.exit(1);
      }
    }
  }

  private void handleBasicAuth(CommandLine cli) throws Exception {
    String cmd = cli.getArgs()[0];
    boolean prompt = Boolean.parseBoolean(cli.getOptionValue(PROMPT_OPTION, "false"));
    boolean updateIncludeFileOnly =
        Boolean.parseBoolean(cli.getOptionValue(UPDATE_INCLUDE_FILE_OPTION, "false"));
    switch (cmd) {
      case "enable":
        if (!prompt && !cli.hasOption(CommonCLIOptions.CREDENTIALS_OPTION)) {
          CLIO.out("Option --credentials or --prompt is required with enable.");
          SolrCLI.exit(1);
        } else if (!prompt
            && (cli.getOptionValue(CommonCLIOptions.CREDENTIALS_OPTION) == null
                || !cli.getOptionValue(CommonCLIOptions.CREDENTIALS_OPTION).contains(":"))) {
          CLIO.out("Option --credentials is not in correct format.");
          SolrCLI.exit(1);
        }

        String zkHost = null;

        if (!updateIncludeFileOnly) {
          try {
            zkHost = CLIUtils.getZkHost(cli);
          } catch (Exception ex) {
            if (cli.hasOption(CommonCLIOptions.ZK_HOST_OPTION)) {
              CLIO.out(
                  "Couldn't get ZooKeeper host. Please make sure that ZooKeeper is running and the correct zk-host has been passed in.");
            } else {
              CLIO.out(
                  "Couldn't get ZooKeeper host. Please make sure Solr is running in cloud mode, or a zk-host has been passed in.");
            }
            SolrCLI.exit(1);
          }
          if (zkHost == null) {
            if (cli.hasOption(CommonCLIOptions.ZK_HOST_OPTION)) {
              CLIO.out(
                  "Couldn't get ZooKeeper host. Please make sure that ZooKeeper is running and the correct zk-host has been passed in.");
            } else {
              CLIO.out(
                  "Couldn't get ZooKeeper host. Please make sure Solr is running in cloud mode, or a zk-host has been passed in.");
            }
            SolrCLI.exit(1);
          }

          // check if security is already enabled or not
          try (SolrZkClient zkClient = CLIUtils.getSolrZkClient(cli, zkHost)) {
            checkSecurityJsonExists(zkClient);
          }
        }

        String username, password;
        if (cli.hasOption(CommonCLIOptions.CREDENTIALS_OPTION)) {
          String credentials = cli.getOptionValue(CommonCLIOptions.CREDENTIALS_OPTION);
          username = credentials.split(":")[0];
          password = credentials.split(":")[1];
        } else {
          Console console = System.console();
          // keep prompting until they've entered a non-empty username & password
          do {
            username = console.readLine("Enter username: ");
          } while (username == null || username.trim().length() == 0);
          username = username.trim();

          do {
            password = new String(console.readPassword("Enter password: "));
          } while (password.length() == 0);
        }

        boolean blockUnknown =
            Boolean.parseBoolean(cli.getOptionValue(BLOCK_UNKNOWN_OPTION, "true"));

        String resourceName = "security.json";
        final URL resource = SolrCore.class.getClassLoader().getResource(resourceName);
        if (null == resource) {
          throw new IllegalArgumentException("invalid resource name: " + resourceName);
        }

        ObjectMapper mapper = new ObjectMapper();
        JsonNode securityJson1 = mapper.readTree(resource.openStream());
        ((ObjectNode) securityJson1).put("blockUnknown", blockUnknown);
        JsonNode credentialsNode = securityJson1.get("authentication").get("credentials");
        ((ObjectNode) credentialsNode)
            .put(username, Sha256AuthenticationProvider.getSaltedHashedValue(password));
        JsonNode userRoleNode = securityJson1.get("authorization").get("user-role");
        String[] predefinedRoles = {"superadmin", "admin", "search", "index"};
        ArrayNode rolesNode = mapper.createArrayNode();
        for (String role : predefinedRoles) {
          rolesNode.add(role);
        }
        ((ObjectNode) userRoleNode).set(username, rolesNode);
        String securityJson = securityJson1.toPrettyString();

        if (!updateIncludeFileOnly) {
          echoIfVerbose("Uploading following security.json: " + securityJson);
          try (SolrZkClient zkClient = CLIUtils.getSolrZkClient(cli, zkHost)) {
            zkClient.setData("/security.json", securityJson.getBytes(StandardCharsets.UTF_8), true);
          }
        }

        String solrIncludeFilename = cli.getOptionValue(SOLR_INCLUDE_FILE_OPTION);
        Path includeFile = Path.of(solrIncludeFilename);
        if (Files.notExists(includeFile) || !Files.isWritable(includeFile)) {
          CLIO.out(
              "Solr include file " + solrIncludeFilename + " doesn't exist or is not writeable.");
          printAuthEnablingInstructions(username, password);
          System.exit(0);
        }
        String authConfDir = cli.getOptionValue(AUTH_CONF_DIR_OPTION);
        Path basicAuthConfFile = Path.of(authConfDir, "basicAuth.conf");

        if (!Files.isWritable(basicAuthConfFile.getParent())) {
          CLIO.out("Cannot write to file: " + basicAuthConfFile.toAbsolutePath());
          printAuthEnablingInstructions(username, password);
          System.exit(0);
        }

        Files.writeString(
            basicAuthConfFile,
            "httpBasicAuthUser=" + username + "\nhttpBasicAuthPassword=" + password,
            StandardCharsets.UTF_8);

        // update the solr.in.sh file to contain the necessary authentication lines
        updateIncludeFileEnableAuth(includeFile, basicAuthConfFile);
        final String successMessage =
            String.format(
                Locale.ROOT, "Successfully enabled basic auth with username [%s].", username);
        echo(successMessage);
        return;
      case "disable":
        clearSecurityJson(cli, updateIncludeFileOnly);

        solrIncludeFilename = cli.getOptionValue(SOLR_INCLUDE_FILE_OPTION);
        includeFile = Path.of(solrIncludeFilename);
        if (Files.notExists(includeFile) || !Files.isWritable(includeFile)) {
          CLIO.out(
              "Solr include file " + solrIncludeFilename + " doesn't exist or is not writeable.");
          CLIO.out(
              "Security has been disabled. Please remove any SOLR_AUTH_TYPE or SOLR_AUTHENTICATION_OPTS configuration from solr.in.sh/solr.in.cmd.\n");
          System.exit(0);
        }

        // update the solr.in.sh file to comment out the necessary authentication lines
        updateIncludeFileDisableAuth(includeFile);
        return;
      default:
        CLIO.out("Valid auth commands are: enable, disable.");
        SolrCLI.exit(1);
    }

    CLIO.out("Options not understood.");
    SolrCLI.exit(1);
  }

  private void checkSecurityJsonExists(SolrZkClient zkClient)
      throws KeeperException, InterruptedException {
    if (zkClient.exists("/security.json", true)) {
      byte[] oldSecurityBytes = zkClient.getData("/security.json", null, null, true);
      if (!"{}".equals(new String(oldSecurityBytes, StandardCharsets.UTF_8).trim())) {
        CLIO.out(
            "Security is already enabled. You can disable it with 'bin/solr auth disable'. Existing security.json: \n"
                + new String(oldSecurityBytes, StandardCharsets.UTF_8));
        SolrCLI.exit(1);
      }
    }
  }

  private void clearSecurityJson(CommandLine cli, boolean updateIncludeFileOnly) throws Exception {
    String zkHost;
    if (!updateIncludeFileOnly) {
      zkHost = CLIUtils.getZkHost(cli);
      if (zkHost == null) {
        stdout.print("ZK Host not found. Solr should be running in cloud mode.");
        SolrCLI.exit(1);
      }

      echoIfVerbose("Uploading following security.json: {}");

      try (SolrZkClient zkClient = CLIUtils.getSolrZkClient(cli, zkHost)) {
        zkClient.setData("/security.json", "{}".getBytes(StandardCharsets.UTF_8), true);
      }
    }
  }

  private void printAuthEnablingInstructions(String username, String password) {
    if (Constants.WINDOWS) {
      CLIO.out(
          "\nAdd the following lines to the solr.in.cmd file so that the solr.cmd script can use subsequently.\n");
      CLIO.out(
          "set SOLR_AUTH_TYPE=basic\n"
              + "set SOLR_AUTHENTICATION_OPTS=\"-Dbasicauth="
              + username
              + ":"
              + password
              + "\"\n");
    } else {
      CLIO.out(
          "\nAdd the following lines to the solr.in.sh file so that the ./solr script can use subsequently.\n");
      CLIO.out(
          "SOLR_AUTH_TYPE=\"basic\"\n"
              + "SOLR_AUTHENTICATION_OPTS=\"-Dbasicauth="
              + username
              + ":"
              + password
              + "\"\n");
    }
  }

  /**
   * This will update the include file (e.g. solr.in.sh / solr.in.cmd) with the authentication
   * parameters.
   *
   * @param includeFile The include file
   * @param basicAuthConfFile If basicAuth, the path of the file containing credentials. If not,
   *     null.
   */
  private void updateIncludeFileEnableAuth(Path includeFile, Path basicAuthConfFile)
      throws IOException {
    List<String> includeFileLines = Files.readAllLines(includeFile, StandardCharsets.UTF_8);
    for (int i = 0; i < includeFileLines.size(); i++) {
      String line = includeFileLines.get(i);
      if (authenticationVariables.contains(line.trim().split("=")[0].trim())) { // Non-Windows
        includeFileLines.set(i, "# " + line);
      }
      if (line.trim().split("=")[0].trim().startsWith("set ")
          && authenticationVariables.contains(
              line.trim().split("=")[0].trim().substring(4))) { // Windows
        includeFileLines.set(i, "REM " + line);
      }
    }
    includeFileLines.add(""); // blank line

    if (basicAuthConfFile != null) { // for basicAuth
      if (Constants.WINDOWS) {
        includeFileLines.add("REM The following lines added by solr.cmd for enabling BasicAuth");
        includeFileLines.add("set SOLR_AUTH_TYPE=basic");
        includeFileLines.add(
            "set SOLR_AUTHENTICATION_OPTS=\"-Dsolr.httpclient.config=" + basicAuthConfFile + "\"");
      } else {
        includeFileLines.add("# The following lines added by ./solr for enabling BasicAuth");
        includeFileLines.add("SOLR_AUTH_TYPE=\"basic\"");
        includeFileLines.add(
            "SOLR_AUTHENTICATION_OPTS=\"-Dsolr.httpclient.config=" + basicAuthConfFile + "\"");
      }
    }

    String lines = includeFileLines.stream().collect(Collectors.joining(System.lineSeparator()));
    Files.writeString(includeFile, lines, StandardCharsets.UTF_8);

    if (basicAuthConfFile != null) {
      echoIfVerbose("Written out credentials file: " + basicAuthConfFile);
    }
    echoIfVerbose("Updated Solr include file: " + includeFile.toAbsolutePath());
  }

  private void updateIncludeFileDisableAuth(Path includeFile) throws IOException {
    List<String> includeFileLines = Files.readAllLines(includeFile, StandardCharsets.UTF_8);
    boolean hasChanged = false;
    for (int i = 0; i < includeFileLines.size(); i++) {
      String line = includeFileLines.get(i);
      if (authenticationVariables.contains(line.trim().split("=")[0].trim())) { // Non-Windows
        includeFileLines.set(i, "# " + line);
        hasChanged = true;
      }
      if (line.trim().split("=")[0].trim().startsWith("set ")
          && authenticationVariables.contains(
              line.trim().split("=")[0].trim().substring(4))) { // Windows
        includeFileLines.set(i, "REM " + line);
        hasChanged = true;
      }
    }
    if (hasChanged) {
      String lines = includeFileLines.stream().collect(Collectors.joining(System.lineSeparator()));
      Files.writeString(includeFile, lines, StandardCharsets.UTF_8);
      echoIfVerbose("Commented out necessary lines from " + includeFile.toAbsolutePath());
    }
  }

  @Override
  public void runImpl(CommandLine cli) throws Exception {
    ensureArgumentIsValidBooleanIfPresent(cli, BLOCK_UNKNOWN_OPTION);
    ensureArgumentIsValidBooleanIfPresent(cli, UPDATE_INCLUDE_FILE_OPTION);

    String type = cli.getOptionValue(TYPE_OPTION, "basicAuth");
    // switch structure is here to support future auth options like oAuth
    switch (type) {
      case "basicAuth":
        handleBasicAuth(cli);
        break;
      default:
        throw new IllegalStateException("Only type=basicAuth supported at the moment.");
    }
  }
}
