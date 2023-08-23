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

import static org.apache.solr.common.SolrException.ErrorCode.FORBIDDEN;
import static org.apache.solr.common.SolrException.ErrorCode.UNAUTHORIZED;
import static org.apache.solr.common.params.CommonParams.NAME;

import com.google.common.annotations.VisibleForTesting;
import java.io.File;
import java.lang.invoke.MethodHandles;
import java.net.SocketException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudHttp2SolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.ContentStreamUpdateRequest;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.util.ContentStreamBase;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.util.StartupLoggingUtils;
import org.apache.solr.util.configuration.SSLConfigurationsFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Command-line utility for working with Solr. */
public class SolrCLI implements CLIO {
  private static final long MAX_WAIT_FOR_CORE_LOAD_NANOS =
      TimeUnit.NANOSECONDS.convert(1, TimeUnit.MINUTES);

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final String ZK_HOST = "localhost:9983";

  public static final Option OPTION_ZKHOST =
      Option.builder("z")
          .argName("HOST")
          .hasArg()
          .required(false)
          .desc("Address of the ZooKeeper ensemble; defaults to: " + ZK_HOST)
          .longOpt("zkHost")
          .build();
  public static final Option OPTION_SOLRURL =
      Option.builder("solrUrl")
          .argName("HOST")
          .hasArg()
          .required(false)
          .desc(
              "Base Solr URL, which can be used to determine the zkHost if that's not known; defaults to: "
                  + getDefaultSolrUrl())
          .build();
  public static final Option OPTION_VERBOSE =
      Option.builder("verbose").required(false).desc("Enable more verbose command output.").build();

  public static final Option OPTION_RECURSE =
      Option.builder("recurse")
          .argName("recurse")
          .hasArg()
          .required(false)
          .desc("Recurse (true|false), default is false.")
          // .type(Boolean.class)
          .build();

  public static final List<Option> cloudOptions =
      List.of(
          OPTION_ZKHOST,
          Option.builder("c")
              .argName("COLLECTION")
              .hasArg()
              .required(false)
              .desc("Name of collection")
              .longOpt("collection")
              .build(),
          OPTION_VERBOSE);

  public static void exit(int exitStatus) {
    try {
      System.exit(exitStatus);
    } catch (java.lang.SecurityException secExc) {
      if (exitStatus != 0)
        throw new RuntimeException("SolrCLI failed to exit with status " + exitStatus);
    }
  }

  /** Runs a tool. */
  public static void main(String[] args) throws Exception {
    if (args == null || args.length == 0 || args[0] == null || args[0].trim().length() == 0) {
      CLIO.err(
          "Invalid command-line args! Must pass the name of a tool to run.\n"
              + "Supported tools:\n");
      displayToolOptions();
      exit(1);
    }

    if (Arrays.asList("-v", "-version", "version").contains(args[0])) {
      // select the version tool to be run
      args[0] = "version";
    }

    SSLConfigurationsFactory.current().init();

    Tool tool = null;
    try {
      tool = findTool(args);
    } catch (IllegalArgumentException iae) {
      CLIO.err(iae.getMessage());
      System.exit(1);
    }
    CommandLine cli = parseCmdLine(tool.getName(), args, tool.getOptions());
    System.exit(tool.runTool(cli));
  }

  public static Tool findTool(String[] args) throws Exception {
    String toolType = args[0].trim().toLowerCase(Locale.ROOT);
    return newTool(toolType);
  }

  public static CommandLine parseCmdLine(String toolName, String[] args, List<Option> toolOptions) {
    // the parser doesn't like -D props
    List<String> toolArgList = new ArrayList<>();
    List<String> dashDList = new ArrayList<>();
    for (int a = 1; a < args.length; a++) {
      String arg = args[a];
      if (arg.startsWith("-D")) {
        dashDList.add(arg);
      } else {
        toolArgList.add(arg);
      }
    }
    String[] toolArgs = toolArgList.toArray(new String[0]);

    // process command-line args to configure this application
    CommandLine cli = processCommandLineArgs(toolName, toolOptions, toolArgs);

    List<String> argList = cli.getArgList();
    argList.addAll(dashDList);

    // for SSL support, try to accommodate relative paths set for SSL store props
    String solrInstallDir = System.getProperty("solr.install.dir");
    if (solrInstallDir != null) {
      checkSslStoreSysProp(solrInstallDir, "keyStore");
      checkSslStoreSysProp(solrInstallDir, "trustStore");
    }

    return cli;
  }

  public static String getDefaultSolrUrl() {
    String scheme = System.getenv("SOLR_URL_SCHEME");
    if (scheme == null) {
      scheme = "http";
    }
    String host = System.getenv("SOLR_TOOL_HOST");
    if (host == null) {
      host = "localhost";
    }
    String port = System.getenv("SOLR_PORT");
    if (port == null) {
      port = "8983";
    }
    return String.format(Locale.ROOT, "%s://%s:%s", scheme.toLowerCase(Locale.ROOT), host, port);
  }

  protected static void checkSslStoreSysProp(String solrInstallDir, String key) {
    String sysProp = "javax.net.ssl." + key;
    String keyStore = System.getProperty(sysProp);
    if (keyStore == null) return;

    File keyStoreFile = new File(keyStore);
    if (keyStoreFile.isFile()) return; // configured setting is OK

    keyStoreFile = new File(solrInstallDir, "server/" + keyStore);
    if (keyStoreFile.isFile()) {
      System.setProperty(sysProp, keyStoreFile.getAbsolutePath());
    } else {
      CLIO.err(
          "WARNING: "
              + sysProp
              + " file "
              + keyStore
              + " not found! https requests to Solr will likely fail; please update your "
              + sysProp
              + " setting to use an absolute path.");
    }
  }

  public static void raiseLogLevelUnlessVerbose(CommandLine cli) {
    if (!cli.hasOption(OPTION_VERBOSE.getOpt())) {
      StartupLoggingUtils.changeLogLevel("WARN");
    }
  }

  // Creates an instance of the requested tool, using classpath scanning if necessary
  private static Tool newTool(String toolType) throws Exception {
    if ("healthcheck".equals(toolType)) return new HealthcheckTool();
    else if ("status".equals(toolType)) return new StatusTool();
    else if ("api".equals(toolType)) return new ApiTool();
    else if ("create_collection".equals(toolType)) return new CreateCollectionTool();
    else if ("create_core".equals(toolType)) return new CreateCoreTool();
    else if ("create".equals(toolType)) return new CreateTool();
    else if ("delete".equals(toolType)) return new DeleteTool();
    else if ("config".equals(toolType)) return new ConfigTool();
    else if ("run_example".equals(toolType)) return new RunExampleTool();
    else if ("upconfig".equals(toolType)) return new ConfigSetUploadTool();
    else if ("downconfig".equals(toolType)) return new ConfigSetDownloadTool();
    else if ("rm".equals(toolType)) return new ZkRmTool();
    else if ("mv".equals(toolType)) return new ZkMvTool();
    else if ("cp".equals(toolType)) return new ZkCpTool();
    else if ("ls".equals(toolType)) return new ZkLsTool();
    else if ("mkroot".equals(toolType)) return new ZkMkrootTool();
    else if ("assert".equals(toolType)) return new AssertTool();
    else if ("auth".equals(toolType)) return new AuthTool();
    else if ("export".equals(toolType)) return new ExportTool();
    else if ("package".equals(toolType)) return new PackageTool();
    else if ("postlogs".equals(toolType)) return new PostLogsTool();
    else if ("version".equals(toolType)) return new VersionTool();
    else if ("post".equals(toolType)) return new PostTool();

    // If you add a built-in tool to this class, add it here to avoid
    // classpath scanning

    for (Class<? extends Tool> next : findToolClassesInPackage("org.apache.solr.util")) {
      Tool tool = next.getConstructor().newInstance();
      if (toolType.equals(tool.getName())) return tool;
    }

    throw new IllegalArgumentException(toolType + " is not a valid command!");
  }

  private static void displayToolOptions() throws Exception {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp("healthcheck", getToolOptions(new HealthcheckTool()));
    formatter.printHelp("status", getToolOptions(new StatusTool()));
    formatter.printHelp("api", getToolOptions(new ApiTool()));
    formatter.printHelp("create_collection", getToolOptions(new CreateCollectionTool()));
    formatter.printHelp("create_core", getToolOptions(new CreateCoreTool()));
    formatter.printHelp("create", getToolOptions(new CreateTool()));
    formatter.printHelp("delete", getToolOptions(new DeleteTool()));
    formatter.printHelp("config", getToolOptions(new ConfigTool()));
    formatter.printHelp("run_example", getToolOptions(new RunExampleTool()));
    formatter.printHelp("upconfig", getToolOptions(new ConfigSetUploadTool()));
    formatter.printHelp("downconfig", getToolOptions(new ConfigSetDownloadTool()));
    formatter.printHelp("rm", getToolOptions(new ZkRmTool()));
    formatter.printHelp("cp", getToolOptions(new ZkCpTool()));
    formatter.printHelp("mv", getToolOptions(new ZkMvTool()));
    formatter.printHelp("ls", getToolOptions(new ZkLsTool()));
    formatter.printHelp("export", getToolOptions(new ExportTool()));
    formatter.printHelp("package", getToolOptions(new PackageTool()));

    List<Class<? extends Tool>> toolClasses = findToolClassesInPackage("org.apache.solr.util");
    for (Class<? extends Tool> next : toolClasses) {
      Tool tool = next.getConstructor().newInstance();
      formatter.printHelp(tool.getName(), getToolOptions(tool));
    }
  }

  public static Options getToolOptions(Tool tool) {
    Options options = new Options();
    options.addOption("help", false, "Print this message");
    options.addOption(OPTION_VERBOSE);
    List<Option> toolOpts = tool.getOptions();
    for (Option toolOpt : toolOpts) {
      options.addOption(toolOpt);
    }
    return options;
  }

  public static List<Option> joinOptions(List<Option> lhs, List<Option> rhs) {
    if (lhs == null) {
      return rhs == null ? List.of() : rhs;
    }

    if (rhs == null) {
      return lhs;
    }

    return Stream.concat(lhs.stream(), rhs.stream()).collect(Collectors.toUnmodifiableList());
  }

  /** Parses the command-line arguments passed by the user. */
  public static CommandLine processCommandLineArgs(
      String toolName, List<Option> customOptions, String[] args) {
    Options options = new Options();

    options.addOption("help", false, "Print this message");
    options.addOption(OPTION_VERBOSE);

    if (customOptions != null) {
      for (Option customOption : customOptions) {
        options.addOption(customOption);
      }
    }

    CommandLine cli = null;
    try {
      cli = (new GnuParser()).parse(options, args);
    } catch (ParseException exp) {
      // Check if we passed in a help argument with a non parsing set of arguments.
      boolean hasHelpArg = false;
      if (args != null) {
        for (String arg : args) {
          if ("-h".equals(arg) || "-help".equals(arg)) {
            hasHelpArg = true;
            break;
          }
        }
      }
      if (!hasHelpArg) {
        CLIO.err("Failed to parse command-line arguments due to: " + exp.getMessage());
        exit(1);
      } else {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp(toolName, options);
        exit(0);
      }
    }

    if (cli.hasOption("help")) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(toolName, options);
      exit(0);
    }

    return cli;
  }

  /** Scans Jar files on the classpath for Tool implementations to activate. */
  private static List<Class<? extends Tool>> findToolClassesInPackage(String packageName) {
    List<Class<? extends Tool>> toolClasses = new ArrayList<>();
    try {
      ClassLoader classLoader = SolrCLI.class.getClassLoader();
      String path = packageName.replace('.', '/');
      Enumeration<URL> resources = classLoader.getResources(path);
      Set<String> classes = new TreeSet<>();
      while (resources.hasMoreElements()) {
        URL resource = resources.nextElement();
        classes.addAll(findClasses(resource.getFile(), packageName));
      }

      for (String classInPackage : classes) {
        Class<?> theClass = Class.forName(classInPackage);
        if (Tool.class.isAssignableFrom(theClass)) toolClasses.add(theClass.asSubclass(Tool.class));
      }
    } catch (Exception e) {
      // safe to squelch this as it's just looking for tools to run
      log.debug("Failed to find Tool impl classes in {}, due to: ", packageName, e);
    }
    return toolClasses;
  }

  private static Set<String> findClasses(String path, String packageName) throws Exception {
    Set<String> classes = new TreeSet<>();
    if (path.startsWith("file:") && path.contains("!")) {
      String[] split = path.split("!");
      URL jar = new URL(split[0]);
      try (ZipInputStream zip = new ZipInputStream(jar.openStream())) {
        ZipEntry entry;
        while ((entry = zip.getNextEntry()) != null) {
          if (entry.getName().endsWith(".class")) {
            String className =
                entry
                    .getName()
                    .replaceAll("[$].*", "")
                    .replaceAll("[.]class", "")
                    .replace('/', '.');
            if (className.startsWith(packageName)) classes.add(className);
          }
        }
      }
    }
    return classes;
  }

  /**
   * Determine if a request to Solr failed due to a communication error, which is generally
   * retry-able.
   */
  public static boolean checkCommunicationError(Exception exc) {
    Throwable rootCause = SolrException.getRootCause(exc);
    return (rootCause instanceof SolrServerException || rootCause instanceof SocketException);
  }

  public static void checkCodeForAuthError(int code) {
    if (code == UNAUTHORIZED.code || code == FORBIDDEN.code) {
      throw new SolrException(
          SolrException.ErrorCode.getErrorCode(code),
          "Solr requires authentication for request. Please supply valid credentials. HTTP code="
              + code);
    }
  }

  public static boolean exceptionIsAuthRelated(Exception exc) {
    return (exc instanceof SolrException
        && Arrays.asList(UNAUTHORIZED.code, FORBIDDEN.code).contains(((SolrException) exc).code()));
  }

  public static SolrClient getSolrClient(String solrUrl) {
    return new Http2SolrClient.Builder(solrUrl).withMaxConnectionsPerHost(32).build();
  }

  private static final String JSON_CONTENT_TYPE = "application/json";

  public static NamedList<Object> postJsonToSolr(
      SolrClient solrClient, String updatePath, String jsonBody) throws Exception {
    ContentStreamBase.StringStream contentStream = new ContentStreamBase.StringStream(jsonBody);
    contentStream.setContentType(JSON_CONTENT_TYPE);
    ContentStreamUpdateRequest req = new ContentStreamUpdateRequest(updatePath);
    req.addContentStream(contentStream);
    return solrClient.request(req);
  }

  public static final String DEFAULT_CONFIG_SET = "_default";

  private static final long MS_IN_MIN = 60 * 1000L;
  private static final long MS_IN_HOUR = MS_IN_MIN * 60L;
  private static final long MS_IN_DAY = MS_IN_HOUR * 24L;

  @VisibleForTesting
  public static String uptime(long uptimeMs) {
    if (uptimeMs <= 0L) return "?";

    long numDays = (uptimeMs >= MS_IN_DAY) ? (uptimeMs / MS_IN_DAY) : 0L;
    long rem = uptimeMs - (numDays * MS_IN_DAY);
    long numHours = (rem >= MS_IN_HOUR) ? (rem / MS_IN_HOUR) : 0L;
    rem = rem - (numHours * MS_IN_HOUR);
    long numMinutes = (rem >= MS_IN_MIN) ? (rem / MS_IN_MIN) : 0L;
    rem = rem - (numMinutes * MS_IN_MIN);
    long numSeconds = Math.round(rem / 1000.0);
    return String.format(
        Locale.ROOT,
        "%d days, %d hours, %d minutes, %d seconds",
        numDays,
        numHours,
        numMinutes,
        numSeconds);
  }

  public static final List<Option> CREATE_COLLECTION_OPTIONS =
      List.of(
          OPTION_ZKHOST,
          OPTION_SOLRURL,
          Option.builder(NAME)
              .argName("NAME")
              .hasArg()
              .required(true)
              .desc("Name of collection to create.")
              .build(),
          Option.builder("shards")
              .argName("#")
              .hasArg()
              .required(false)
              .desc("Number of shards; default is 1.")
              .build(),
          Option.builder("replicationFactor")
              .argName("#")
              .hasArg()
              .required(false)
              .desc(
                  "Number of copies of each document across the collection (replicas per shard); default is 1.")
              .build(),
          Option.builder("confdir")
              .argName("NAME")
              .hasArg()
              .required(false)
              .desc(
                  "Configuration directory to copy when creating the new collection; default is "
                      + DEFAULT_CONFIG_SET
                      + '.')
              .build(),
          Option.builder("confname")
              .argName("NAME")
              .hasArg()
              .required(false)
              .desc("Configuration name; default is the collection name.")
              .build(),
          Option.builder("configsetsDir")
              .argName("DIR")
              .hasArg()
              .required(true)
              .desc("Path to configsets directory on the local system.")
              .build(),
          OPTION_VERBOSE);

  /**
   * Get the base URL of a live Solr instance from either the solrUrl command-line option from
   * ZooKeeper.
   */
  public static String resolveSolrUrl(CommandLine cli) throws Exception {
    String solrUrl = cli.getOptionValue("solrUrl");
    if (solrUrl == null) {
      String zkHost = cli.getOptionValue("zkHost");
      if (zkHost == null) {
        solrUrl = SolrCLI.getDefaultSolrUrl();
        CLIO.getOutStream()
            .println(
                "Neither -zkHost or -solrUrl parameters provided so assuming solrUrl is "
                    + solrUrl
                    + ".");
      } else {
        try (CloudSolrClient cloudSolrClient =
            new CloudHttp2SolrClient.Builder(Collections.singletonList(zkHost), Optional.empty())
                .build()) {
          cloudSolrClient.connect();
          Set<String> liveNodes = cloudSolrClient.getClusterState().getLiveNodes();
          if (liveNodes.isEmpty())
            throw new IllegalStateException(
                "No live nodes found! Cannot determine 'solrUrl' from ZooKeeper: " + zkHost);

          String firstLiveNode = liveNodes.iterator().next();
          solrUrl = ZkStateReader.from(cloudSolrClient).getBaseUrlForNodeName(firstLiveNode);
        }
      }
    }
    return solrUrl;
  }

  /**
   * Get the ZooKeeper connection string from either the zkHost command-line option or by looking it
   * up from a running Solr instance based on the solrUrl option.
   */
  public static String getZkHost(CommandLine cli) throws Exception {
    String zkHost = cli.getOptionValue("zkHost");
    if (zkHost != null) return zkHost;

    // find it using the localPort
    String solrUrl = cli.getOptionValue("solrUrl");
    if (solrUrl == null) {
      solrUrl = getDefaultSolrUrl();
      CLIO.getOutStream()
          .println(
              "Neither -zkHost or -solrUrl parameters provided so assuming solrUrl is "
                  + solrUrl
                  + ".");
    }

    try (var solrClient = getSolrClient(solrUrl)) {
      // hit Solr to get system info
      NamedList<Object> systemInfo =
          solrClient.request(
              new GenericSolrRequest(SolrRequest.METHOD.GET, CommonParams.SYSTEM_INFO_PATH));

      // convert raw JSON into user-friendly output
      StatusTool statusTool = new StatusTool();
      Map<String, Object> status = statusTool.reportStatus(systemInfo, solrClient);
      @SuppressWarnings("unchecked")
      Map<String, Object> cloud = (Map<String, Object>) status.get("cloud");
      if (cloud != null) {
        String zookeeper = (String) cloud.get("ZooKeeper");
        if (zookeeper.endsWith("(embedded)")) {
          zookeeper = zookeeper.substring(0, zookeeper.length() - "(embedded)".length());
        }
        zkHost = zookeeper;
      }
    }

    return zkHost;
  }

  public static boolean safeCheckCollectionExists(String solrUrl, String collection) {
    boolean exists = false;
    try (var solrClient = getSolrClient(solrUrl)) {
      NamedList<Object> existsCheckResult = solrClient.request(new CollectionAdminRequest.List());
      @SuppressWarnings("unchecked")
      List<String> collections = (List<String>) existsCheckResult.get("collections");
      exists = collections != null && collections.contains(collection);
    } catch (Exception exc) {
      // just ignore it since we're only interested in a positive result here
    }
    return exists;
  }

  @SuppressWarnings("unchecked")
  public static boolean safeCheckCoreExists(String solrUrl, String coreName) {
    boolean exists = false;
    try (var solrClient = getSolrClient(solrUrl)) {
      boolean wait = false;
      final long startWaitAt = System.nanoTime();
      do {
        if (wait) {
          final int clamPeriodForStatusPollMs = 1000;
          Thread.sleep(clamPeriodForStatusPollMs);
        }
        NamedList<Object> existsCheckResult =
            CoreAdminRequest.getStatus(coreName, solrClient).getResponse();
        NamedList<Object> status = (NamedList<Object>) existsCheckResult.get("status");
        NamedList<Object> coreStatus = (NamedList<Object>) status.get(coreName);
        Map<String, Object> failureStatus =
            (Map<String, Object>) existsCheckResult.get("initFailures");
        String errorMsg = (String) failureStatus.get(coreName);
        final boolean hasName = coreStatus != null && coreStatus.asMap().containsKey(NAME);
        exists = hasName || errorMsg != null;
        wait = hasName && errorMsg == null && "true".equals(coreStatus.get("isLoading"));
      } while (wait && System.nanoTime() - startWaitAt < MAX_WAIT_FOR_CORE_LOAD_NANOS);
    } catch (Exception exc) {
      // just ignore it since we're only interested in a positive result here
    }
    return exists;
  }

  public static class AssertionFailureException extends Exception {
    public AssertionFailureException(String message) {
      super(message);
    }
  }
}
