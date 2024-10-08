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
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.lucene.util.Constants;
import org.apache.solr.client.solrj.impl.SolrZkClientTimeout;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.core.SolrCore;
import org.apache.solr.security.Sha256AuthenticationProvider;
import org.apache.zookeeper.KeeperException;

// Authentication tool
public class AuthTool extends ToolBase {
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

  List<String> authenticationVariables =
      Arrays.asList(
          "SOLR_AUTHENTICATION_CLIENT_BUILDER", "SOLR_AUTH_TYPE", "SOLR_AUTHENTICATION_OPTS");

  @Override
  public List<Option> getOptions() {
    return List.of(
        Option.builder("type")
            .argName("type")
            .hasArg()
            .desc(
                "The authentication mechanism to enable (basicAuth or kerberos). Defaults to 'basicAuth'.")
            .build(),
        Option.builder("credentials")
            .argName("credentials")
            .hasArg()
            .desc(
                "Credentials in the format username:password. Example: -credentials solr:SolrRocks")
            .build(),
        Option.builder("prompt")
            .argName("prompt")
            .hasArg()
            .desc(
                "Prompts the user to provide the credentials. Use either -credentials or -prompt, not both.")
            .build(),
        Option.builder("config")
            .argName("config")
            .hasArgs()
            .desc(
                "Configuration parameters (Solr startup parameters). Required for Kerberos authentication.")
            .build(),
        Option.builder("blockUnknown")
            .argName("blockUnknown")
            .desc(
                "Blocks all access for unknown users (requires authentication for all endpoints).")
            .hasArg()
            .build(),
        Option.builder("solrIncludeFile")
            .argName("solrIncludeFile")
            .hasArg()
            .desc(
                "The Solr include file which contains overridable environment variables for configuring Solr configurations.")
            .build(),
        Option.builder("updateIncludeFileOnly")
            .argName("updateIncludeFileOnly")
            .desc(
                "Only update the solr.in.sh or solr.in.cmd file, and skip actual enabling/disabling"
                    + " authentication (i.e. don't update security.json).")
            .hasArg()
            .build(),
        Option.builder("authConfDir")
            .argName("authConfDir")
            .hasArg()
            .required()
            .desc(
                "This is where any authentication related configuration files, if any, would be placed.")
            .build(),
        Option.builder("solrUrl").argName("solrUrl").hasArg().desc("Solr URL.").build(),
        Option.builder("zkHost")
            .argName("zkHost")
            .hasArg()
            .desc("ZooKeeper host to connect to.")
            .build(),
        SolrCLI.OPTION_VERBOSE);
  }

  private void ensureArgumentIsValidBooleanIfPresent(CommandLine cli, String argName) {
    if (cli.hasOption(argName)) {
      final String value = cli.getOptionValue(argName);
      if (!"true".equalsIgnoreCase(value) && !"false".equalsIgnoreCase(value)) {
        echo("Argument [" + argName + "] must be either true or false, but was [" + value + "]");
        SolrCLI.exit(1);
      }
    }
  }

  @Override
  public int runTool(CommandLine cli) throws Exception {
    SolrCLI.raiseLogLevelUnlessVerbose(cli);
    if (cli.getOptions().length == 0
        || cli.getArgs().length == 0
        || cli.getArgs().length > 1
        || cli.hasOption("h")) {
      new HelpFormatter()
          .printHelp("bin/solr auth <enable|disable> [OPTIONS]", SolrCLI.getToolOptions(this));
      return 1;
    }

    ensureArgumentIsValidBooleanIfPresent(cli, "blockUnknown");
    ensureArgumentIsValidBooleanIfPresent(cli, "updateIncludeFileOnly");

    String type = cli.getOptionValue("type", "basicAuth");
    switch (type) {
      case "basicAuth":
        return handleBasicAuth(cli);
      case "kerberos":
        return handleKerberos(cli);
      default:
        CLIO.out("Only type=basicAuth or kerberos supported at the moment.");
        SolrCLI.exit(1);
    }
    return 1;
  }

  private int handleKerberos(CommandLine cli) throws Exception {
    String cmd = cli.getArgs()[0];
    boolean updateIncludeFileOnly =
        Boolean.parseBoolean(cli.getOptionValue("updateIncludeFileOnly", "false"));
    String securityJson =
        "{"
            + "\n  \"authentication\":{"
            + "\n   \"class\":\"solr.KerberosPlugin\""
            + "\n  }"
            + "\n}";

    switch (cmd) {
      case "enable":
        String zkHost = null;
        boolean zkInaccessible = false;

        if (!updateIncludeFileOnly) {
          try {
            zkHost = SolrCLI.getZkHost(cli);
          } catch (Exception ex) {
            CLIO.out(
                "Unable to access ZooKeeper. Please add the following security.json to ZooKeeper (in case of SolrCloud):\n"
                    + securityJson
                    + "\n");
            zkInaccessible = true;
          }
          if (zkHost == null) {
            if (!zkInaccessible) {
              CLIO.out(
                  "Unable to access ZooKeeper. Please add the following security.json to ZooKeeper (in case of SolrCloud):\n"
                      + securityJson
                      + "\n");
              zkInaccessible = true;
            }
          }

          // check if security is already enabled or not
          if (!zkInaccessible) {
            try (SolrZkClient zkClient =
                new SolrZkClient.Builder()
                    .withUrl(zkHost)
                    .withTimeout(
                        SolrZkClientTimeout.DEFAULT_ZK_CLIENT_TIMEOUT, TimeUnit.MILLISECONDS)
                    .build()) {
              checkSecurityJsonExists(zkClient);
            } catch (Exception ex) {
              CLIO.out(
                  "Unable to access ZooKeeper. Please add the following security.json to ZooKeeper (in case of SolrCloud):\n"
                      + securityJson
                      + "\n");
              zkInaccessible = true;
            }
          }
        }

        if (!updateIncludeFileOnly) {
          if (!zkInaccessible) {
            echoIfVerbose("Uploading following security.json: " + securityJson, cli);
            try (SolrZkClient zkClient =
                new SolrZkClient.Builder()
                    .withUrl(zkHost)
                    .withTimeout(
                        SolrZkClientTimeout.DEFAULT_ZK_CLIENT_TIMEOUT, TimeUnit.MILLISECONDS)
                    .build()) {
              zkClient.setData(
                  "/security.json", securityJson.getBytes(StandardCharsets.UTF_8), true);
            } catch (Exception ex) {
              CLIO.out(
                  "Unable to access ZooKeeper. Please add the following security.json to ZooKeeper (in case of SolrCloud):\n"
                      + securityJson);
            }
          }
        }

        String config = StrUtils.join(Arrays.asList(cli.getOptionValues("config")), ' ');
        // config is base64 encoded (to get around parsing problems), decode it
        config = config.replace(" ", "");
        config =
            new String(
                Base64.getDecoder().decode(config.getBytes(StandardCharsets.UTF_8)),
                StandardCharsets.UTF_8);
        config = config.replace("\n", "").replace("\r", "");

        String solrIncludeFilename = cli.getOptionValue("solrIncludeFile");
        File includeFile = new File(solrIncludeFilename);
        if (!includeFile.exists() || !includeFile.canWrite()) {
          CLIO.out(
              "Solr include file " + solrIncludeFilename + " doesn't exist or is not writeable.");
          printAuthEnablingInstructions(config);
          System.exit(0);
        }

        // update the solr.in.sh file to contain the necessary authentication lines
        updateIncludeFileEnableAuth(includeFile.toPath(), null, config, cli);
        echo(
            "Successfully enabled Kerberos authentication; please restart any running Solr nodes.");
        return 0;

      case "disable":
        clearSecurityJson(cli, updateIncludeFileOnly);

        solrIncludeFilename = cli.getOptionValue("solrIncludeFile");
        includeFile = new File(solrIncludeFilename);
        if (!includeFile.exists() || !includeFile.canWrite()) {
          CLIO.out(
              "Solr include file " + solrIncludeFilename + " doesn't exist or is not writeable.");
          CLIO.out(
              "Security has been disabled. Please remove any SOLR_AUTH_TYPE or SOLR_AUTHENTICATION_OPTS configuration from solr.in.sh/solr.in.cmd.\n");
          System.exit(0);
        }

        // update the solr.in.sh file to comment out the necessary authentication lines
        updateIncludeFileDisableAuth(includeFile.toPath(), cli);
        return 0;

      default:
        CLIO.out("Valid auth commands are: enable, disable.");
        SolrCLI.exit(1);
    }

    CLIO.out("Options not understood.");
    new HelpFormatter()
        .printHelp("bin/solr auth <enable|disable> [OPTIONS]", SolrCLI.getToolOptions(this));
    return 1;
  }

  private int handleBasicAuth(CommandLine cli) throws Exception {
    String cmd = cli.getArgs()[0];
    boolean prompt = Boolean.parseBoolean(cli.getOptionValue("prompt", "false"));
    boolean updateIncludeFileOnly =
        Boolean.parseBoolean(cli.getOptionValue("updateIncludeFileOnly", "false"));
    switch (cmd) {
      case "enable":
        if (!prompt && !cli.hasOption("credentials")) {
          CLIO.out("Option -credentials or -prompt is required with enable.");
          new HelpFormatter()
              .printHelp("bin/solr auth <enable|disable> [OPTIONS]", SolrCLI.getToolOptions(this));
          SolrCLI.exit(1);
        } else if (!prompt
            && (cli.getOptionValue("credentials") == null
                || !cli.getOptionValue("credentials").contains(":"))) {
          CLIO.out("Option -credentials is not in correct format.");
          new HelpFormatter()
              .printHelp("bin/solr auth <enable|disable> [OPTIONS]", SolrCLI.getToolOptions(this));
          SolrCLI.exit(1);
        }

        String zkHost = null;

        if (!updateIncludeFileOnly) {
          try {
            zkHost = SolrCLI.getZkHost(cli);
          } catch (Exception ex) {
            if (cli.hasOption("zkHost")) {
              CLIO.out(
                  "Couldn't get ZooKeeper host. Please make sure that ZooKeeper is running and the correct zkHost has been passed in.");
            } else {
              CLIO.out(
                  "Couldn't get ZooKeeper host. Please make sure Solr is running in cloud mode, or a zkHost has been passed in.");
            }
            SolrCLI.exit(1);
          }
          if (zkHost == null) {
            if (cli.hasOption("zkHost")) {
              CLIO.out(
                  "Couldn't get ZooKeeper host. Please make sure that ZooKeeper is running and the correct zkHost has been passed in.");
            } else {
              CLIO.out(
                  "Couldn't get ZooKeeper host. Please make sure Solr is running in cloud mode, or a zkHost has been passed in.");
            }
            SolrCLI.exit(1);
          }

          // check if security is already enabled or not
          try (SolrZkClient zkClient =
              new SolrZkClient.Builder()
                  .withUrl(zkHost)
                  .withTimeout(SolrZkClientTimeout.DEFAULT_ZK_CLIENT_TIMEOUT, TimeUnit.MILLISECONDS)
                  .build()) {
            checkSecurityJsonExists(zkClient);
          }
        }

        String username, password;
        if (cli.hasOption("credentials")) {
          String credentials = cli.getOptionValue("credentials");
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

        boolean blockUnknown = Boolean.parseBoolean(cli.getOptionValue("blockUnknown", "true"));

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
          echoIfVerbose("Uploading following security.json: " + securityJson, cli);
          try (SolrZkClient zkClient =
              new SolrZkClient.Builder()
                  .withUrl(zkHost)
                  .withTimeout(SolrZkClientTimeout.DEFAULT_ZK_CLIENT_TIMEOUT, TimeUnit.MILLISECONDS)
                  .build()) {
            zkClient.setData("/security.json", securityJson.getBytes(StandardCharsets.UTF_8), true);
          }
        }

        String solrIncludeFilename = cli.getOptionValue("solrIncludeFile");
        File includeFile = new File(solrIncludeFilename);
        if (!includeFile.exists() || !includeFile.canWrite()) {
          CLIO.out(
              "Solr include file " + solrIncludeFilename + " doesn't exist or is not writeable.");
          printAuthEnablingInstructions(username, password);
          System.exit(0);
        }
        String authConfDir = cli.getOptionValue("authConfDir");
        File basicAuthConfFile = new File(authConfDir + File.separator + "basicAuth.conf");

        if (!basicAuthConfFile.getParentFile().canWrite()) {
          CLIO.out("Cannot write to file: " + basicAuthConfFile.getAbsolutePath());
          printAuthEnablingInstructions(username, password);
          System.exit(0);
        }

        Files.writeString(
            basicAuthConfFile.toPath(),
            "httpBasicAuthUser=" + username + "\nhttpBasicAuthPassword=" + password,
            StandardCharsets.UTF_8);

        // update the solr.in.sh file to contain the necessary authentication lines
        updateIncludeFileEnableAuth(
            includeFile.toPath(), basicAuthConfFile.getAbsolutePath(), null, cli);
        final String successMessage =
            String.format(
                Locale.ROOT,
                "Successfully enabled basic auth with username [%s] and password [%s].",
                username,
                password);
        echo(successMessage);
        return 0;

      case "disable":
        clearSecurityJson(cli, updateIncludeFileOnly);

        solrIncludeFilename = cli.getOptionValue("solrIncludeFile");
        includeFile = new File(solrIncludeFilename);
        if (!includeFile.exists() || !includeFile.canWrite()) {
          CLIO.out(
              "Solr include file " + solrIncludeFilename + " doesn't exist or is not writeable.");
          CLIO.out(
              "Security has been disabled. Please remove any SOLR_AUTH_TYPE or SOLR_AUTHENTICATION_OPTS configuration from solr.in.sh/solr.in.cmd.\n");
          System.exit(0);
        }

        // update the solr.in.sh file to comment out the necessary authentication lines
        updateIncludeFileDisableAuth(includeFile.toPath(), cli);
        return 0;

      default:
        CLIO.out("Valid auth commands are: enable, disable.");
        SolrCLI.exit(1);
    }

    CLIO.out("Options not understood.");
    new HelpFormatter()
        .printHelp("bin/solr auth <enable|disable> [OPTIONS]", SolrCLI.getToolOptions(this));
    return 1;
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
      zkHost = SolrCLI.getZkHost(cli);
      if (zkHost == null) {
        stdout.print("ZK Host not found. Solr should be running in cloud mode.");
        SolrCLI.exit(1);
      }

      echoIfVerbose("Uploading following security.json: {}", cli);

      try (SolrZkClient zkClient =
          new SolrZkClient.Builder()
              .withUrl(zkHost)
              .withTimeout(SolrZkClientTimeout.DEFAULT_ZK_CLIENT_TIMEOUT, TimeUnit.MILLISECONDS)
              .build()) {
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

  private void printAuthEnablingInstructions(String kerberosConfig) {
    if (Constants.WINDOWS) {
      CLIO.out(
          "\nAdd the following lines to the solr.in.cmd file so that the solr.cmd script can use subsequently.\n");
      CLIO.out(
          "set SOLR_AUTH_TYPE=kerberos\n"
              + "set SOLR_AUTHENTICATION_OPTS=\""
              + kerberosConfig
              + "\"\n");
    } else {
      CLIO.out(
          "\nAdd the following lines to the solr.in.sh file so that the ./solr script can use subsequently.\n");
      CLIO.out(
          "SOLR_AUTH_TYPE=\"kerberos\"\n"
              + "SOLR_AUTHENTICATION_OPTS=\""
              + kerberosConfig
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
   * @param kerberosConfig If kerberos, the config string containing startup parameters. If not,
   *     null.
   */
  private void updateIncludeFileEnableAuth(
      Path includeFile, String basicAuthConfFile, String kerberosConfig, CommandLine cli)
      throws IOException {
    assert !(basicAuthConfFile != null
        && kerberosConfig != null); // only one of the two needs to be populated
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
    } else { // for kerberos
      if (Constants.WINDOWS) {
        includeFileLines.add("REM The following lines added by solr.cmd for enabling BasicAuth");
        includeFileLines.add("set SOLR_AUTH_TYPE=kerberos");
        includeFileLines.add(
            "set SOLR_AUTHENTICATION_OPTS=\"-Dsolr.httpclient.config=" + basicAuthConfFile + "\"");
      } else {
        includeFileLines.add("# The following lines added by ./solr for enabling BasicAuth");
        includeFileLines.add("SOLR_AUTH_TYPE=\"kerberos\"");
        includeFileLines.add("SOLR_AUTHENTICATION_OPTS=\"" + kerberosConfig + "\"");
      }
    }

    String lines = includeFileLines.stream().collect(Collectors.joining(System.lineSeparator()));
    Files.writeString(includeFile, lines, StandardCharsets.UTF_8);

    if (basicAuthConfFile != null) {
      echoIfVerbose("Written out credentials file: " + basicAuthConfFile, cli);
    }
    echoIfVerbose("Updated Solr include file: " + includeFile.toAbsolutePath(), cli);
  }

  private void updateIncludeFileDisableAuth(Path includeFile, CommandLine cli) throws IOException {
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
      echoIfVerbose("Commented out necessary lines from " + includeFile.toAbsolutePath(), cli);
    }
  }

  @Override
  public void runImpl(CommandLine cli) throws Exception {}
}
