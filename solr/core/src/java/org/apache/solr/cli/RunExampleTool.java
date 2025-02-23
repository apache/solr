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

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.net.Socket;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.exec.DefaultExecuteResultHandler;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteException;
import org.apache.commons.exec.Executor;
import org.apache.commons.exec.OS;
import org.apache.commons.exec.environment.EnvironmentUtils;
import org.apache.commons.io.file.PathUtils;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.EnvUtils;
import org.noggit.CharArr;
import org.noggit.JSONWriter;

/**
 * Supports start command in the bin/solr script.
 *
 * <p>Enhances start command by providing an interactive session with the user to launch (or
 * relaunch the -e cloud example)
 */
public class RunExampleTool extends ToolBase {

  private static final String PROMPT_FOR_NUMBER = "Please enter %s [%d]: ";
  private static final String PROMPT_FOR_NUMBER_IN_RANGE =
      "Please enter %s between %d and %d [%d]: ";
  private static final String PROMPT_NUMBER_TOO_SMALL =
      "%d is too small! " + PROMPT_FOR_NUMBER_IN_RANGE;
  private static final String PROMPT_NUMBER_TOO_LARGE =
      "%d is too large! " + PROMPT_FOR_NUMBER_IN_RANGE;

  private static final Option NO_PROMPT_OPTION =
      Option.builder("y")
          .longOpt("no-prompt")
          .desc(
              "Don't prompt for input; accept all defaults when running examples that accept user input.")
          .build();

  private static final Option EXAMPLE_OPTION =
      Option.builder("e")
          .longOpt("example")
          .hasArg()
          .argName("NAME")
          .required()
          .desc("Name of the example to launch, one of: cloud, techproducts, schemaless, films.")
          .build();

  private static final Option SCRIPT_OPTION =
      Option.builder()
          .longOpt("script")
          .hasArg()
          .argName("PATH")
          .desc("Path to the bin/solr script.")
          .build();

  private static final Option SERVER_DIR_OPTION =
      Option.builder("d")
          .longOpt("server-dir")
          .hasArg()
          .argName("DIR")
          .required()
          .desc("Path to the Solr server directory.")
          .build();

  private static final Option FORCE_OPTION =
      Option.builder("f")
          .longOpt("force")
          .argName("FORCE")
          .desc("Force option in case Solr is run as root.")
          .build();

  private static final Option EXAMPLE_DIR_OPTION =
      Option.builder()
          .longOpt("example-dir")
          .hasArg()
          .argName("DIR")
          .desc(
              "Path to the Solr example directory; if not provided, ${serverDir}/../example is expected to exist.")
          .build();

  private static final Option SOLR_HOME_OPTION =
      Option.builder()
          .longOpt("solr-home")
          .hasArg()
          .argName("SOLR_HOME_DIR")
          .required(false)
          .desc(
              "Path to the Solr home directory; if not provided, ${serverDir}/solr is expected to exist.")
          .build();

  private static final Option URL_SCHEME_OPTION =
      Option.builder()
          .longOpt("url-scheme")
          .hasArg()
          .argName("SCHEME")
          .desc("Solr URL scheme: http or https, defaults to http if not specified.")
          .build();

  private static final Option PORT_OPTION =
      Option.builder("p")
          .longOpt("port")
          .hasArg()
          .argName("PORT")
          .desc("Specify the port to start the Solr HTTP listener on; default is 8983.")
          .build();

  private static final Option HOST_OPTION =
      Option.builder()
          .longOpt("host")
          .hasArg()
          .argName("HOSTNAME")
          .desc("Specify the hostname for this Solr instance.")
          .build();

  private static final Option USER_MANAGED_OPTION =
      Option.builder().longOpt("user-managed").desc("Start Solr in User Managed mode.").build();

  private static final Option MEMORY_OPTION =
      Option.builder("m")
          .longOpt("memory")
          .hasArg()
          .argName("MEM")
          .desc(
              "Sets the min (-Xms) and max (-Xmx) heap size for the JVM, such as: -m 4g results in: -Xms4g -Xmx4g; by default, this script sets the heap size to 512m.")
          .build();

  private static final Option JVM_OPTS_OPTION =
      Option.builder()
          .longOpt("jvm-opts")
          .hasArg()
          .argName("OPTS")
          .desc("Additional options to be passed to the JVM when starting example Solr server(s).")
          .build();

  protected InputStream userInput;
  protected Executor executor;
  protected String script;
  protected Path serverDir;
  protected Path exampleDir;
  protected Path solrHomeDir;
  protected String urlScheme;

  /** Default constructor used by the framework when running as a command-line application. */
  public RunExampleTool() {
    this(null, System.in, CLIO.getOutStream());
  }

  public RunExampleTool(Executor executor, InputStream userInput, PrintStream stdout) {
    super(stdout);
    this.executor = (executor != null) ? executor : new DefaultExecutor();
    this.userInput = userInput;
  }

  @Override
  public String getName() {
    return "run_example";
  }

  @Override
  public Options getOptions() {
    return super.getOptions()
        .addOption(NO_PROMPT_OPTION)
        .addOption(EXAMPLE_OPTION)
        .addOption(SCRIPT_OPTION)
        .addOption(SERVER_DIR_OPTION)
        .addOption(SOLR_HOME_OPTION)
        .addOption(FORCE_OPTION)
        .addOption(EXAMPLE_DIR_OPTION)
        .addOption(URL_SCHEME_OPTION)
        .addOption(PORT_OPTION)
        .addOption(HOST_OPTION)
        .addOption(USER_MANAGED_OPTION)
        .addOption(MEMORY_OPTION)
        .addOption(JVM_OPTS_OPTION)
        .addOption(CommonCLIOptions.ZK_HOST_OPTION);
  }

  @Override
  public void runImpl(CommandLine cli) throws Exception {
    this.urlScheme = cli.getOptionValue(URL_SCHEME_OPTION, "http");
    String exampleType = cli.getOptionValue(EXAMPLE_OPTION);

    serverDir = Path.of(cli.getOptionValue(SERVER_DIR_OPTION));
    if (!Files.isDirectory(serverDir))
      throw new IllegalArgumentException(
          "Value of --server-dir option is invalid! "
              + serverDir.toAbsolutePath()
              + " is not a directory!");

    script = cli.getOptionValue(SCRIPT_OPTION);
    if (script != null) {
      if (!Files.isRegularFile(Path.of(script)))
        throw new IllegalArgumentException(
            "Value of --script option is invalid! " + script + " not found");
    } else {
      Path scriptFile = serverDir.getParent().resolve("bin").resolve("solr");
      if (Files.isRegularFile(scriptFile)) {
        script = scriptFile.toAbsolutePath().toString();
      } else {
        scriptFile = serverDir.getParent().resolve("bin").resolve("solr.cmd");
        if (Files.isRegularFile(scriptFile)) {
          script = scriptFile.toAbsolutePath().toString();
        } else {
          throw new IllegalArgumentException(
              "Cannot locate the bin/solr script! Please pass --script to this application.");
        }
      }
    }

    exampleDir =
        (cli.hasOption(EXAMPLE_DIR_OPTION))
            ? Path.of(cli.getOptionValue(EXAMPLE_DIR_OPTION))
            : serverDir.getParent().resolve("example");
    if (!Files.isDirectory(exampleDir))
      throw new IllegalArgumentException(
          "Value of --example-dir option is invalid! "
              + exampleDir.toAbsolutePath()
              + " is not a directory!");

    if (cli.hasOption(SOLR_HOME_OPTION)) {
      solrHomeDir = Path.of(cli.getOptionValue(SOLR_HOME_OPTION));
    } else {
      String solrHomeProp = EnvUtils.getProperty("solr.home");
      if (solrHomeProp != null && !solrHomeProp.isEmpty()) {
        solrHomeDir = Path.of(solrHomeProp);
      } else if ("cloud".equals(exampleType)) {
        solrHomeDir = exampleDir.resolve("cloud");
        if (!Files.isDirectory(solrHomeDir)) Files.createDirectory(solrHomeDir);
      } else {
        solrHomeDir = serverDir.resolve("solr");
      }
    }
    if (!Files.isDirectory(solrHomeDir))
      throw new IllegalArgumentException(
          "Value of --solr-home option is invalid! "
              + solrHomeDir.toAbsolutePath()
              + " is not a directory!");

    echoIfVerbose(
        "Running with\nserverDir="
            + serverDir.toAbsolutePath()
            + ",\nexampleDir="
            + exampleDir.toAbsolutePath()
            + ",\nsolrHomeDir="
            + solrHomeDir.toAbsolutePath()
            + "\nscript="
            + script);

    if ("cloud".equals(exampleType)) {
      runCloudExample(cli);
    } else if ("techproducts".equals(exampleType)
        || "schemaless".equals(exampleType)
        || "films".equals(exampleType)) {
      runExample(cli, exampleType);
    } else {
      throw new IllegalArgumentException(
          "Unsupported example "
              + exampleType
              + "! Please choose one of: cloud, schemaless, techproducts, or films");
    }
  }

  protected void runExample(CommandLine cli, String exampleName) throws Exception {
    String collectionName = "schemaless".equals(exampleName) ? "gettingstarted" : exampleName;
    String configSet =
        "techproducts".equals(exampleName) ? "sample_techproducts_configs" : "_default";

    boolean isCloudMode = !cli.hasOption(USER_MANAGED_OPTION);
    String zkHost = cli.getOptionValue(CommonCLIOptions.ZK_HOST_OPTION);
    int port =
        Integer.parseInt(
            cli.getOptionValue(PORT_OPTION, System.getenv().getOrDefault("SOLR_PORT", "8983")));
    Map<String, Object> nodeStatus = startSolr(solrHomeDir, isCloudMode, cli, port, zkHost, 30);

    String solrUrl = CLIUtils.normalizeSolrUrl((String) nodeStatus.get("baseUrl"));

    // If the example already exists then let the user know they should delete it, or
    // they may get unusual behaviors.
    boolean alreadyExists = false;
    boolean cloudMode = nodeStatus.get("cloud") != null;
    if (cloudMode) {
      if (CLIUtils.safeCheckCollectionExists(
          solrUrl, collectionName, cli.getOptionValue(CommonCLIOptions.CREDENTIALS_OPTION))) {
        alreadyExists = true;
        echo(
            "\nWARNING: Collection '"
                + collectionName
                + "' already exists, which may make starting this example not work well!");
      }
    } else {
      String coreName = collectionName;
      if (CLIUtils.safeCheckCoreExists(
          solrUrl, coreName, cli.getOptionValue(CommonCLIOptions.CREDENTIALS_OPTION))) {
        alreadyExists = true;
        echo(
            "\nWARNING: Core '"
                + coreName
                + "' already exists, which may make starting this example not work well!");
      }
    }

    if (alreadyExists) {
      echo(
          "You may want to run 'bin/solr delete -c "
              + collectionName
              + " --delete-config' first before running the example to ensure a fresh state.");
    }

    if (!alreadyExists) {
      // invoke the CreateTool
      String[] createArgs =
          new String[] {
            "--name", collectionName,
            "--shards", "1",
            "--replication-factor", "1",
            "--conf-name", collectionName,
            "--conf-dir", configSet,
            "--solr-url", solrUrl
          };
      CreateTool createTool = new CreateTool(stdout);
      int createCode = createTool.runTool(SolrCLI.processCommandLineArgs(createTool, createArgs));
      if (createCode != 0)
        throw new Exception(
            "Failed to create " + collectionName + " using command: " + Arrays.asList(createArgs));
    }

    if ("techproducts".equals(exampleName) && !alreadyExists) {

      Path exampledocsDir = this.exampleDir.resolve("exampledocs");
      if (!Files.isDirectory(exampledocsDir)) {
        Path readOnlyExampleDir = serverDir.resolveSibling("example");
        if (Files.isDirectory(readOnlyExampleDir)) {
          exampledocsDir = readOnlyExampleDir.resolve("exampledocs");
        }
      }

      if (Files.isDirectory(exampledocsDir)) {
        echo("Indexing tech product example docs from " + exampledocsDir.toAbsolutePath());

        String[] args =
            new String[] {
              "post",
              "--solr-url",
              solrUrl,
              "--name",
              collectionName,
              "--type",
              "application/xml",
              "--filetypes",
              "xml",
              exampledocsDir.toAbsolutePath().toString()
            };
        PostTool postTool = new PostTool();
        CommandLine postToolCli = SolrCLI.parseCmdLine(postTool, args);
        postTool.runTool(postToolCli);

      } else {
        echo(
            "exampledocs directory not found, skipping indexing step for the techproducts example");
      }
    } else if ("films".equals(exampleName) && !alreadyExists) {
      try (SolrClient solrClient =
          CLIUtils.getSolrClient(
              solrUrl, cli.getOptionValue(CommonCLIOptions.CREDENTIALS_OPTION))) {
        echo("Adding dense vector field type to films schema");
        SolrCLI.postJsonToSolr(
            solrClient,
            "/" + collectionName + "/schema",
            "{\n"
                + "        \"add-field-type\" : {\n"
                + "          \"name\":\"knn_vector_10\",\n"
                + "          \"class\":\"solr.DenseVectorField\",\n"
                + "          \"vectorDimension\":10,\n"
                + "          \"similarityFunction\":cosine\n"
                + "          \"knnAlgorithm\":hnsw\n"
                + "        }\n"
                + "      }");

        echo(
            "Adding name, genre, directed_by, initial_release_date, and film_vector fields to films schema");
        SolrCLI.postJsonToSolr(
            solrClient,
            "/" + collectionName + "/schema",
            "{\n"
                + "        \"add-field\" : {\n"
                + "          \"name\":\"name\",\n"
                + "          \"type\":\"text_general\",\n"
                + "          \"multiValued\":false,\n"
                + "          \"stored\":true\n"
                + "        },\n"
                + "        \"add-field\" : {\n"
                + "          \"name\":\"genre\",\n"
                + "          \"type\":\"text_general\",\n"
                + "          \"multiValued\":true,\n"
                + "          \"stored\":true\n"
                + "        },\n"
                + "        \"add-field\" : {\n"
                + "          \"name\":\"directed_by\",\n"
                + "          \"type\":\"text_general\",\n"
                + "          \"multiValued\":true,\n"
                + "          \"stored\":true\n"
                + "        },\n"
                + "        \"add-field\" : {\n"
                + "          \"name\":\"initial_release_date\",\n"
                + "          \"type\":\"pdate\",\n"
                + "          \"stored\":true\n"
                + "        },\n"
                + "        \"add-field\" : {\n"
                + "          \"name\":\"film_vector\",\n"
                + "          \"type\":\"knn_vector_10\",\n"
                + "          \"indexed\":true\n"
                + "          \"stored\":true\n"
                + "        },\n"
                + "        \"add-copy-field\" : {\n"
                + "          \"source\":\"genre\",\n"
                + "          \"dest\":\"_text_\"\n"
                + "        },\n"
                + "        \"add-copy-field\" : {\n"
                + "          \"source\":\"name\",\n"
                + "          \"dest\":\"_text_\"\n"
                + "        },\n"
                + "        \"add-copy-field\" : {\n"
                + "          \"source\":\"directed_by\",\n"
                + "          \"dest\":\"_text_\"\n"
                + "        }\n"
                + "      }");

        echo(
            "Adding paramsets \"algo\" and \"algo_b\" to films configuration for relevancy tuning");
        SolrCLI.postJsonToSolr(
            solrClient,
            "/" + collectionName + "/config/params",
            "{\n"
                + "        \"set\": {\n"
                + "        \"algo_a\":{\n"
                + "               \"defType\":\"dismax\",\n"
                + "               \"qf\":\"name\"\n"
                + "             }\n"
                + "           },\n"
                + "           \"set\": {\n"
                + "             \"algo_b\":{\n"
                + "               \"defType\":\"dismax\",\n"
                + "               \"qf\":\"name\",\n"
                + "               \"mm\":\"100%\"\n"
                + "             }\n"
                + "            }\n"
                + "        }\n");

        Path filmsJsonFile = this.exampleDir.resolve("films").resolve("films.json");
        echo("Indexing films example docs from " + filmsJsonFile.toAbsolutePath());
        String[] args =
            new String[] {
              "post",
              "--solr-url",
              solrUrl,
              "--name",
              collectionName,
              "--type",
              "application/json",
              filmsJsonFile.toAbsolutePath().toString()
            };
        PostTool postTool = new PostTool();
        CommandLine postToolCli = SolrCLI.parseCmdLine(postTool, args);
        postTool.runTool(postToolCli);

      } catch (Exception ex) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, ex);
      }

      echo(
          "\nSolr "
              + exampleName
              + " example launched successfully. Direct your Web browser to "
              + solrUrl
              + " to visit the Solr Admin UI");
    }
  }

  protected void runCloudExample(CommandLine cli) throws Exception {

    boolean prompt = !cli.hasOption(NO_PROMPT_OPTION);
    int numNodes = 2;
    int[] cloudPorts = new int[] {8983, 7574, 8984, 7575};
    int defaultPort =
        Integer.parseInt(
            cli.getOptionValue(PORT_OPTION, System.getenv().getOrDefault("SOLR_PORT", "8983")));
    if (defaultPort != 8983) {
      // Override the old default port numbers if user has started the example overriding SOLR_PORT
      cloudPorts = new int[] {defaultPort, defaultPort + 1, defaultPort + 2, defaultPort + 3};
    }

    echo("\nWelcome to the SolrCloud example!\n");

    Scanner readInput = prompt ? new Scanner(userInput, StandardCharsets.UTF_8) : null;
    if (prompt) {
      echo(
          "This interactive session will help you launch a SolrCloud cluster on your local workstation.");

      // get the number of nodes to start
      numNodes =
          promptForInt(
              readInput,
              "To begin, how many Solr nodes would you like to run in your local cluster? (specify 1-4 nodes) [2]: ",
              "a number",
              numNodes,
              1,
              4);

      echo("Ok, let's start up " + numNodes + " Solr nodes for your example SolrCloud cluster.");

      // get the ports for each port
      for (int n = 0; n < numNodes; n++) {
        String promptMsg =
            String.format(
                Locale.ROOT, "Please enter the port for node%d [%d]: ", (n + 1), cloudPorts[n]);
        int port = promptForPort(readInput, n + 1, promptMsg, cloudPorts[n]);
        while (!isPortAvailable(port)) {
          port =
              promptForPort(
                  readInput,
                  n + 1,
                  "Oops! Looks like port "
                      + port
                      + " is already being used by another process. Please choose a different port.",
                  cloudPorts[n]);
        }

        cloudPorts[n] = port;
        echoIfVerbose("Using port " + port + " for node " + (n + 1));
      }
    } else {
      echo("Starting up " + numNodes + " Solr nodes for your example SolrCloud cluster.\n");
    }

    // setup a unique solr.solr.home directory for each node
    Path node1Dir = setupSolrHomeDir(serverDir, solrHomeDir, "node1");
    for (int n = 2; n <= numNodes; n++) {
      Path nodeNDir = solrHomeDir.resolve("node" + n);
      if (!Files.isDirectory(nodeNDir)) {
        echo("Cloning " + node1Dir.toAbsolutePath() + " into\n   " + nodeNDir.toAbsolutePath());
        PathUtils.copyDirectory(node1Dir, nodeNDir, StandardCopyOption.REPLACE_EXISTING);
      } else {
        echo(nodeNDir.toAbsolutePath() + " already exists.");
      }
    }

    // deal with extra args passed to the script to run the example
    String zkHost = cli.getOptionValue(CommonCLIOptions.ZK_HOST_OPTION);

    // start the first node (most likely with embedded ZK)
    Map<String, Object> nodeStatus =
        startSolr(node1Dir.resolve("solr"), true, cli, cloudPorts[0], zkHost, 30);

    if (zkHost == null) {
      @SuppressWarnings("unchecked")
      Map<String, Object> cloudStatus = (Map<String, Object>) nodeStatus.get("cloud");
      if (cloudStatus != null) {
        String zookeeper = (String) cloudStatus.get("ZooKeeper");
        if (zookeeper != null) zkHost = zookeeper;
      }
      if (zkHost == null)
        throw new Exception("Could not get the ZooKeeper connection string for node1!");
    }

    if (numNodes > 1) {
      // start the other nodes
      for (int n = 1; n < numNodes; n++)
        startSolr(
            solrHomeDir.resolve("node" + (n + 1)).resolve("solr"),
            true,
            cli,
            cloudPorts[n],
            zkHost,
            30);
    }

    String solrUrl = CLIUtils.normalizeSolrUrl((String) nodeStatus.get("baseUrl"), false);

    // wait until live nodes == numNodes
    waitToSeeLiveNodes(zkHost, numNodes);

    // create the collection
    String collectionName =
        createCloudExampleCollection(
            numNodes,
            readInput,
            prompt,
            solrUrl,
            cli.getOptionValue(CommonCLIOptions.CREDENTIALS_OPTION));

    echo("\n\nSolrCloud example running, please visit: " + solrUrl + " \n");
  }

  /** wait until the number of live nodes == numNodes. */
  protected void waitToSeeLiveNodes(String zkHost, int numNodes) {
    try (CloudSolrClient cloudClient =
        new CloudSolrClient.Builder(Collections.singletonList(zkHost), Optional.empty()).build()) {
      cloudClient.connect();
      Set<String> liveNodes = cloudClient.getClusterState().getLiveNodes();
      int numLiveNodes = (liveNodes != null) ? liveNodes.size() : 0;
      long timeout = System.nanoTime() + TimeUnit.NANOSECONDS.convert(10, TimeUnit.SECONDS);
      while (System.nanoTime() < timeout && numLiveNodes < numNodes) {
        echo(
            "\nWaiting up to "
                + 10
                + " seconds to see "
                + (numNodes - numLiveNodes)
                + " more nodes join the SolrCloud cluster ...");
        try {
          Thread.sleep(2000);
        } catch (InterruptedException ie) {
          Thread.interrupted();
        }
        liveNodes = cloudClient.getClusterState().getLiveNodes();
        numLiveNodes = (liveNodes != null) ? liveNodes.size() : 0;
      }
      if (numLiveNodes < numNodes) {
        echo(
            "\nWARNING: Only "
                + numLiveNodes
                + " of "
                + numNodes
                + " are active in the cluster after "
                + 10
                + " seconds! Please check the solr.log for each node to look for errors.\n");
      }
    } catch (Exception exc) {
      CLIO.err("Failed to see if " + numNodes + " joined the SolrCloud cluster due to: " + exc);
    }
  }

  protected Map<String, Object> startSolr(
      Path solrHomeDir,
      boolean cloudMode,
      CommandLine cli,
      int port,
      String zkHost,
      int maxWaitSecs)
      throws Exception {

    String extraArgs = readExtraArgs(cli.getArgs());

    String host = cli.getOptionValue(HOST_OPTION);
    String memory = cli.getOptionValue(MEMORY_OPTION);

    String hostArg = (host != null && !"localhost".equals(host)) ? " --host " + host : "";
    String zkHostArg = (zkHost != null) ? " -z " + zkHost : "";
    String memArg = (memory != null) ? " -m " + memory : "";
    String cloudModeArg = cloudMode ? "" : "--user-managed";
    String forceArg = cli.hasOption(FORCE_OPTION) ? " --force" : "";
    String verboseArg = isVerbose() ? "--verbose" : "";

    String jvmOpts = cli.getOptionValue(JVM_OPTS_OPTION);
    String jvmOptsArg = (jvmOpts != null) ? " --jvm-opts \"" + jvmOpts + "\"" : "";

    Path cwd = Path.of(System.getProperty("user.dir"));
    Path binDir = Path.of(script).getParent();

    boolean isWindows = (OS.isFamilyDOS() || OS.isFamilyWin9x() || OS.isFamilyWindows());
    String callScript = (!isWindows && cwd.equals(binDir.getParent())) ? "bin/solr" : script;

    String cwdPath = cwd.toAbsolutePath().toString();
    String solrHome = solrHomeDir.toAbsolutePath().toRealPath().toString();

    // don't display a huge path for solr home if it is relative to the cwd
    if (!isWindows && cwdPath.length() > 1 && solrHome.startsWith(cwdPath))
      solrHome = solrHome.substring(cwdPath.length() + 1);

    final var syspropArg =
        ("techproducts".equals(cli.getOptionValue(EXAMPLE_OPTION)))
            ? "-Dsolr.modules=clustering,extraction,langid,ltr,scripting -Dsolr.ltr.enabled=true -Dsolr.clustering.enabled=true"
            : "";

    String startCmd =
        String.format(
            Locale.ROOT,
            "\"%s\" start %s -p %d --solr-home \"%s\" --server-dir \"%s\" %s %s %s %s %s %s %s %s",
            callScript,
            cloudModeArg,
            port,
            solrHome,
            serverDir.toAbsolutePath(),
            hostArg,
            zkHostArg,
            memArg,
            forceArg,
            verboseArg,
            extraArgs,
            jvmOptsArg,
            syspropArg);
    startCmd = startCmd.replaceAll("\\s+", " ").trim(); // for pretty printing

    echo("\nStarting up Solr on port " + port + " using command:");
    echo(startCmd + "\n");

    String solrUrl =
        String.format(
            Locale.ROOT, "%s://%s:%d/solr", urlScheme, (host != null ? host : "localhost"), port);

    String credentials = null; // for now we don't need it for example tool.  But we should.

    Map<String, Object> nodeStatus = checkPortConflict(solrUrl, credentials, solrHomeDir, port);
    if (nodeStatus != null)
      return nodeStatus; // the server they are trying to start is already running

    int code = 0;
    if (isWindows) {
      // On Windows, the execution doesn't return, so we have to execute async
      // and when calling the script, it seems to be inheriting the environment that launched this
      // app, so we have to prune out env vars that may cause issues
      Map<String, String> startEnv = new HashMap<>();
      Map<String, String> procEnv = EnvironmentUtils.getProcEnvironment();
      if (procEnv != null) {
        for (Map.Entry<String, String> entry : procEnv.entrySet()) {
          String envVar = entry.getKey();
          String envVarVal = entry.getValue();
          if (envVarVal != null && !"EXAMPLE".equals(envVar) && !envVar.startsWith("SOLR_")) {
            startEnv.put(envVar, envVarVal);
          }
        }
      }
      DefaultExecuteResultHandler handler = new DefaultExecuteResultHandler();
      executor.execute(org.apache.commons.exec.CommandLine.parse(startCmd), startEnv, handler);

      // wait for execution.
      try {
        handler.waitFor(3000);
      } catch (InterruptedException ie) {
        // safe to ignore ...
        Thread.interrupted();
      }
      if (handler.hasResult() && handler.getExitValue() != 0) {
        throw new Exception(
            "Failed to start Solr using command: "
                + startCmd
                + " Exception : "
                + handler.getException());
      }
    } else {
      try {
        code = executor.execute(org.apache.commons.exec.CommandLine.parse(startCmd));
      } catch (ExecuteException e) {
        throw new Exception(
            "Failed to start Solr using command: " + startCmd + " Exception : " + e);
      }
    }
    if (code != 0) throw new Exception("Failed to start Solr using command: " + startCmd);

    return getNodeStatus(
        solrUrl, cli.getOptionValue(CommonCLIOptions.CREDENTIALS_OPTION), maxWaitSecs);
  }

  protected Map<String, Object> checkPortConflict(
      String solrUrl, String credentials, Path solrHomeDir, int port) {
    // quickly check if the port is in use
    if (isPortAvailable(port)) return null; // not in use ... try to start

    Map<String, Object> nodeStatus = null;
    try {
      nodeStatus = (new StatusTool()).getStatus(solrUrl, credentials);
    } catch (Exception ignore) {
      /* just trying to determine if this example is already running. */
    }

    if (nodeStatus != null) {
      String solr_home = (String) nodeStatus.get("solr_home");
      if (solr_home != null) {
        String solrHomePath = solrHomeDir.toAbsolutePath().toString();
        if (!solrHomePath.endsWith("/")) solrHomePath += "/";
        if (!solr_home.endsWith("/")) solr_home += "/";

        if (solrHomePath.equals(solr_home)) {
          CharArr arr = new CharArr();
          new JSONWriter(arr, 2).write(nodeStatus);
          echo("Solr is already setup and running on port " + port + " with status:\n" + arr);
          echo(
              "\nIf this is not the example node you are trying to start, please choose a different port.");
          nodeStatus.put("baseUrl", solrUrl);
          return nodeStatus;
        }
      }
    }

    throw new IllegalStateException("Port " + port + " is already being used by another process.");
  }

  protected String readExtraArgs(String[] extraArgsArr) {
    String extraArgs = "";
    if (extraArgsArr != null && extraArgsArr.length > 0) {
      StringBuilder sb = new StringBuilder();
      int app = 0;
      for (int e = 0; e < extraArgsArr.length; e++) {
        String arg = extraArgsArr[e];
        if ("e".equals(arg) || "example".equals(arg)) {
          e++; // skip over the example arg
          continue;
        }

        if (app > 0) sb.append(" ");
        sb.append(arg);
        ++app;
      }
      extraArgs = sb.toString().trim();
    }
    return extraArgs;
  }

  protected String createCloudExampleCollection(
      int numNodes, Scanner readInput, boolean prompt, String solrUrl, String credentials)
      throws Exception {
    // yay! numNodes SolrCloud nodes running
    int numShards = 2;
    int replicationFactor = 2;
    String cloudConfig = "_default";
    String collectionName = "gettingstarted";

    Path configsetsDir = serverDir.resolve("solr").resolve("configsets");

    if (prompt) {
      echo(
          "\nNow let's create a new collection for indexing documents in your "
              + numNodes
              + "-node cluster.");

      while (true) {
        collectionName =
            prompt(
                readInput,
                "Please provide a name for your new collection: [" + collectionName + "] ",
                collectionName);

        // Test for existence and then prompt to either create another collection or skip the
        // creation step
        if (CLIUtils.safeCheckCollectionExists(solrUrl, credentials, collectionName)) {
          echo("\nCollection '" + collectionName + "' already exists!");
          int oneOrTwo =
              promptForInt(
                  readInput,
                  "Do you want to re-use the existing collection or create a new one? Enter 1 to reuse, 2 to create new [1]: ",
                  "a 1 or 2",
                  1,
                  1,
                  2);
          if (oneOrTwo == 1) {
            return collectionName;
          } else {
            continue;
          }
        } else {
          break; // user selected a collection that doesn't exist ... proceed on
        }
      }

      numShards =
          promptForInt(
              readInput,
              "How many shards would you like to split " + collectionName + " into? [2]",
              "a shard count",
              2,
              1,
              4);

      replicationFactor =
          promptForInt(
              readInput,
              "How many replicas per shard would you like to create? [2] ",
              "a replication factor",
              2,
              1,
              4);

      echo(
          "Please choose a configuration for the "
              + collectionName
              + " collection, available options are:");
      String validConfigs = "_default or sample_techproducts_configs [" + cloudConfig + "] ";
      cloudConfig = prompt(readInput, validConfigs, cloudConfig);

      // validate the cloudConfig name
      while (!isValidConfig(configsetsDir, cloudConfig)) {
        echo(
            cloudConfig
                + " is not a valid configuration directory! Please choose a configuration for the "
                + collectionName
                + " collection, available options are:");
        cloudConfig = prompt(readInput, validConfigs, cloudConfig);
      }
    } else {
      // must verify if default collection exists
      if (CLIUtils.safeCheckCollectionExists(solrUrl, collectionName, credentials)) {
        echo(
            "\nCollection '"
                + collectionName
                + "' already exists! Skipping collection creation step.");
        return collectionName;
      }
    }

    // invoke the CreateTool
    String[] createArgs =
        new String[] {
          "--name", collectionName,
          "--shards", String.valueOf(numShards),
          "--replication-factor", String.valueOf(replicationFactor),
          "--conf-name", collectionName,
          "--conf-dir", cloudConfig,
          "--solr-url", solrUrl
        };

    CreateTool createTool = new CreateTool(stdout);
    int createCode = createTool.runTool(SolrCLI.processCommandLineArgs(createTool, createArgs));

    if (createCode != 0)
      throw new Exception(
          "Failed to create collection using command: " + Arrays.asList(createArgs));

    return collectionName;
  }

  protected boolean isValidConfig(Path configsetsDir, String config) {
    Path configDir = configsetsDir.resolve(config);
    if (Files.isDirectory(configDir)) return true;

    // not a built-in configset ... maybe it's a custom directory?
    configDir = Path.of(config);
    return Files.isDirectory(configDir);
  }

  protected Map<String, Object> getNodeStatus(String solrUrl, String credentials, int maxWaitSecs)
      throws Exception {
    StatusTool statusTool = new StatusTool();
    echoIfVerbose("\nChecking status of Solr at " + solrUrl + " ...");

    URI solrURI = new URI(solrUrl);
    Map<String, Object> nodeStatus =
        statusTool.waitToSeeSolrUp(solrUrl, credentials, maxWaitSecs, TimeUnit.SECONDS);
    nodeStatus.put("baseUrl", solrUrl);
    CharArr arr = new CharArr();
    new JSONWriter(arr, 2).write(nodeStatus);
    String mode = (nodeStatus.get("cloud") != null) ? "cloud" : "standalone";

    echoIfVerbose(
        "\nSolr is running on " + solrURI.getPort() + " in " + mode + " mode with status:\n" + arr);

    return nodeStatus;
  }

  protected Path setupSolrHomeDir(Path serverDir, Path solrHomeParentDir, String dirName)
      throws IOException {
    Path solrXml = serverDir.resolve("solr").resolve("solr.xml");
    if (!Files.isRegularFile(solrXml))
      throw new IllegalArgumentException(
          "Value of --server-dir option is invalid! " + solrXml.toAbsolutePath() + " not found!");

    Path zooCfg = serverDir.resolve("solr").resolve("zoo.cfg");
    if (!Files.isRegularFile(zooCfg))
      throw new IllegalArgumentException(
          "Value of --server-dir option is invalid! " + zooCfg.toAbsolutePath() + " not found!");

    Path solrHomeDir = solrHomeParentDir.resolve(dirName).resolve("solr");
    if (!Files.isDirectory(solrHomeDir)) {
      echo("Creating Solr home directory " + solrHomeDir);
      Files.createDirectories(solrHomeDir);
    } else {
      echo("Solr home directory " + solrHomeDir.toAbsolutePath() + " already exists.");
    }

    copyIfNeeded(solrXml, solrHomeDir.resolve("solr.xml"));
    copyIfNeeded(zooCfg, solrHomeDir.resolve("zoo.cfg"));

    return solrHomeDir.getParent();
  }

  protected void copyIfNeeded(Path src, Path dest) throws IOException {
    if (!Files.isRegularFile(dest)) Files.copy(src, dest);

    if (!Files.isRegularFile(dest))
      throw new IllegalStateException("Required file " + dest.toAbsolutePath() + " not found!");
  }

  protected boolean isPortAvailable(int port) {
    try (Socket s = new Socket("localhost", port)) {
      assert s != null; // To allow compilation..
      return false;
    } catch (IOException e) {
      return true;
    }
  }

  protected Integer promptForPort(Scanner s, int node, String prompt, Integer defVal) {
    return promptForInt(s, prompt, "a port for node " + node, defVal, null, null);
  }

  protected Integer promptForInt(
      Scanner s, String prompt, String label, Integer defVal, Integer min, Integer max) {
    Integer inputAsInt = null;

    String value = prompt(s, prompt, null /* default is null since we handle that here */);
    if (value != null) {
      int attempts = 3;
      while (value != null && --attempts > 0) {
        try {
          inputAsInt = Integer.valueOf(value);

          if (min != null) {
            if (inputAsInt < min) {
              value =
                  prompt(
                      s,
                      String.format(
                          Locale.ROOT,
                          PROMPT_NUMBER_TOO_SMALL,
                          inputAsInt,
                          label,
                          min,
                          max,
                          defVal));
              inputAsInt = null;
              continue;
            }
          }

          if (max != null) {
            if (inputAsInt > max) {
              value =
                  prompt(
                      s,
                      String.format(
                          Locale.ROOT,
                          PROMPT_NUMBER_TOO_LARGE,
                          inputAsInt,
                          label,
                          min,
                          max,
                          defVal));
              inputAsInt = null;
            }
          }

        } catch (NumberFormatException nfe) {
          if (isVerbose()) echo(value + " is not a number!");

          if (min != null && max != null) {
            value =
                prompt(
                    s,
                    String.format(
                        Locale.ROOT, PROMPT_FOR_NUMBER_IN_RANGE, label, min, max, defVal));
          } else {
            value = prompt(s, String.format(Locale.ROOT, PROMPT_FOR_NUMBER, label, defVal));
          }
        }
      }
      if (attempts == 0 && inputAsInt == null)
        echo("Too many failed attempts! Going with default value " + defVal);
    }

    return (inputAsInt != null) ? inputAsInt : defVal;
  }

  protected String prompt(Scanner s, String prompt) {
    return prompt(s, prompt, null);
  }

  protected String prompt(Scanner s, String prompt, String defaultValue) {
    echo(prompt);
    String nextInput = s.nextLine();
    if (nextInput != null) {
      nextInput = nextInput.trim();
      if (nextInput.isEmpty()) nextInput = null;
    }
    return (nextInput != null) ? nextInput : defaultValue;
  }
}
