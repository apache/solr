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
import static org.apache.solr.common.params.CommonParams.SYSTEM_INFO_PATH;

import com.google.common.annotations.VisibleForTesting;
import java.io.File;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.net.SocketException;
import java.net.URI;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
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
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.DeprecatedAttributes;
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
import org.apache.solr.client.solrj.impl.SolrZkClientTimeout;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.ContentStreamUpdateRequest;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.util.ContentStreamBase;
import org.apache.solr.common.util.EnvUtils;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.util.StartupLoggingUtils;
import org.apache.solr.util.configuration.SSLConfigurationsFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Command-line utility for working with Solr. */
public class SolrCLI implements CLIO {

  public static String RED = "\u001B[31m";
  public static String GREEN = "\u001B[32m";
  public static String YELLOW = "\u001B[33m";

  private static final long MAX_WAIT_FOR_CORE_LOAD_NANOS =
      TimeUnit.NANOSECONDS.convert(1, TimeUnit.MINUTES);

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final String ZK_HOST = "localhost:9983";

  public static final Option OPTION_ZKHOST_DEPRECATED =
      Option.builder("zkHost")
          .longOpt("zkHost")
          .deprecated(
              DeprecatedAttributes.builder()
                  .setForRemoval(true)
                  .setSince("9.7")
                  .setDescription("Use --zk-host instead")
                  .get())
          .argName("HOST")
          .hasArg()
          .required(false)
          .desc(
              "Zookeeper connection string; unnecessary if ZK_HOST is defined in solr.in.sh; otherwise, defaults to "
                  + ZK_HOST
                  + '.')
          .build();

  public static final Option OPTION_ZKHOST =
      Option.builder("z")
          .longOpt("zk-host")
          .argName("HOST")
          .hasArg()
          .required(false)
          .desc(
              "Zookeeper connection string; unnecessary if ZK_HOST is defined in solr.in.sh; otherwise, defaults to "
                  + ZK_HOST
                  + '.')
          .build();
  public static final Option OPTION_SOLRURL_DEPRECATED =
      Option.builder("solrUrl")
          .longOpt("solrUrl")
          .deprecated(
              DeprecatedAttributes.builder()
                  .setForRemoval(true)
                  .setSince("9.7")
                  .setDescription("Use --solr-url instead")
                  .get())
          .argName("HOST")
          .hasArg()
          .required(false)
          .desc(
              "Base Solr URL, which can be used to determine the zk-host if that's not known; defaults to: "
                  + getDefaultSolrUrl()
                  + '.')
          .build();
  public static final Option OPTION_SOLRURL =
      Option.builder("url")
          .longOpt("solr-url")
          .argName("HOST")
          .hasArg()
          .required(false)
          .desc(
              "Base Solr URL, which can be used to determine the zk-host if that's not known; defaults to: "
                  + getDefaultSolrUrl()
                  + '.')
          .build();
  public static final Option OPTION_VERBOSE =
      Option.builder("v")
          .longOpt("verbose")
          .required(false)
          .desc("Enable verbose command output.")
          .build();
  public static final Option OPTION_HELP =
      Option.builder("h").longOpt("help").required(false).desc("Print this message.").build();

  public static final Option OPTION_RECURSE =
      Option.builder("r")
          .longOpt("recurse")
          .argName("recurse")
          .hasArg()
          .required(false)
          .desc("Recurse (true|false), default is false.")
          // .type(Boolean.class)
          .build();

  public static final Option OPTION_CREDENTIALS =
      Option.builder("u")
          .longOpt("credentials")
          .argName("credentials")
          .hasArg()
          .required(false)
          .desc(
              "Credentials in the format username:password. Example: --credentials solr:SolrRocks")
          .build();

  public static final Option OPTION_CREDENTIALS_DEPRECATED =
      Option.builder("credentials")
          .argName("credentials")
          .hasArg()
          .deprecated(
              DeprecatedAttributes.builder()
                  .setForRemoval(true)
                  .setSince("9.7")
                  .setDescription("Use -u or --credentials instead")
                  .get())
          .required(false)
          .desc(
              "Credentials in the format username:password. Example: --credentials solr:SolrRocks")
          .build();

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
    final boolean hasNoCommand =
        args == null || args.length == 0 || args[0] == null || args[0].trim().isEmpty();
    final boolean isHelpCommand =
        !hasNoCommand && Arrays.asList("-h", "--help", "/?").contains(args[0]);

    if (hasNoCommand || isHelpCommand) {
      printHelp();
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
    CommandLine cli = parseCmdLine(tool, args);
    System.exit(tool.runTool(cli));
  }

  public static Tool findTool(String[] args) throws Exception {
    String toolType = args[0].trim().toLowerCase(Locale.ROOT);
    return newTool(toolType);
  }

  public static CommandLine parseCmdLine(Tool tool, String[] args) {
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
    CommandLine cli = processCommandLineArgs(tool, toolArgs);

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
    // note that ENV_VAR syntax (and the env vars too) are mapped to env.var sys props
    String scheme = EnvUtils.getProperty("solr.url.scheme", "http");
    String host = EnvUtils.getProperty("solr.tool.host", "localhost");
    String port = EnvUtils.getProperty("jetty.port", "8983"); // from SOLR_PORT env
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
    else if ("cluster".equals(toolType)) return new ClusterTool();
    else if ("updateacls".equals(toolType)) return new UpdateACLTool();
    else if ("linkconfig".equals(toolType)) return new LinkConfigTool();
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

  /** Returns tool options for given tool, for usage display purposes. Hides deprecated options. */
  public static Options getToolOptions(Tool tool) {
    Options options = new Options();
    options.addOption(OPTION_HELP);
    options.addOption(OPTION_VERBOSE);
    List<Option> toolOpts = tool.getOptions();
    for (Option toolOpt : toolOpts) {
      if (!toolOpt.isDeprecated()) {
        options.addOption(toolOpt);
      }
    }
    return options;
  }

  /**
   * Returns the value of the option with the given name, or the value of the deprecated option with
   */
  public static String getOptionWithDeprecatedAndDefault(
      CommandLine cli, String opt, String deprecated, String def) {
    String val = cli.getOptionValue(opt);
    if (val == null) {
      val = cli.getOptionValue(deprecated);
    }
    return val == null ? def : val;
  }

  // TODO: SOLR-17429 - remove the custom logic when CommonsCLI is upgraded and
  // makes stderr the default, or makes Option.toDeprecatedString() public.
  private static void deprecatedHandlerStdErr(Option o) {
    if (o.isDeprecated()) {
      final StringBuilder buf =
          new StringBuilder().append("Option '-").append(o.getOpt()).append('\'');
      if (o.getLongOpt() != null) {
        buf.append(",'--").append(o.getLongOpt()).append('\'');
      }
      buf.append(": ").append(o.getDeprecated());
      CLIO.err(buf.toString());
    }
  }

  /** Parses the command-line arguments passed by the user. */
  public static CommandLine processCommandLineArgs(Tool tool, String[] args) {
    List<Option> customOptions = tool.getOptions();
    Options options = new Options();

    options.addOption(OPTION_HELP);
    options.addOption(OPTION_VERBOSE);

    if (customOptions != null) {
      for (Option customOption : customOptions) {
        options.addOption(customOption);
      }
    }

    CommandLine cli = null;
    try {
      cli =
          DefaultParser.builder()
              .setDeprecatedHandler(SolrCLI::deprecatedHandlerStdErr)
              .build()
              .parse(options, args);
    } catch (ParseException exp) {
      // Check if we passed in a help argument with a non parsing set of arguments.
      boolean hasHelpArg = false;
      if (args != null) {
        for (String arg : args) {
          if ("-h".equals(arg) || "--help".equals(arg) || "-help".equals(arg)) {
            hasHelpArg = true;
            break;
          }
        }
      }
      if (!hasHelpArg) {
        CLIO.err("Failed to parse command-line arguments due to: " + exp.getMessage() + "\n");
        printToolHelp(tool);
        exit(1);
      } else {
        printToolHelp(tool);
        exit(0);
      }
    }

    if (cli.hasOption("help")) {
      printToolHelp(tool);
      exit(0);
    }

    return cli;
  }

  /** Prints tool help for a given tool. */
  private static void printToolHelp(Tool tool) {
    HelpFormatter formatter = HelpFormatter.builder().get();
    formatter.setWidth(120);
    Options optionsNoDeprecated = new Options();
    tool.getOptions().stream()
        .filter(option -> !option.isDeprecated())
        .forEach(optionsNoDeprecated::addOption);
    String usageString = tool.getUsage() == null ? "bin/solr " + tool.getName() : tool.getUsage();
    boolean autoGenerateUsage = tool.getUsage() == null;
    formatter.printHelp(
        usageString, tool.getHeader(), optionsNoDeprecated, tool.getFooter(), autoGenerateUsage);
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

  /**
   * Get a SolrClient for the given URL. The URL can be with or without /solr suffix, the method
   * will normalize it and add '/solr'. To support certain unit tests, the hostContext can be
   * overridden via the system property 'hostContext'.
   *
   * <p>Note that from Solr 10.0 Solr will use / as base URL to support /api and other contexts, so
   * handling of hostContext differs between Solr 9.x and 10.x.
   *
   * @param solrUrl the base URL of Solr
   * @return a SolrClient initialized with correct hostContext, default '/solr'
   */
  public static SolrClient getSolrClient(String solrUrl) {
    // SOLR-16824: Workaround for making SolrCloudExampleTest pass with custom host contexts
    String hostContext = System.getProperty("hostContext", "/solr");
    if (hostContext.isBlank()) {
      log.warn("Invalid hostContext {} provided, setting to /solr", hostContext);
      hostContext = "/solr";
    }

    solrUrl = normalizeSolrUrl(solrUrl, true, hostContext) + hostContext;

    Http2SolrClient.Builder builder =
        new Http2SolrClient.Builder(solrUrl)
            .withMaxConnectionsPerHost(32)
            .withKeyStoreReloadInterval(-1, TimeUnit.SECONDS);

    return builder.build();
  }

  public static SolrClient getSolrClient(CommandLine cli) throws Exception {
    String solrUrl = SolrCLI.normalizeSolrUrl(cli);
    return getSolrClient(solrUrl);
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

  private static void printHelp() {

    print("Usage: solr COMMAND OPTIONS");
    print(
        "       where COMMAND is one of: start, stop, restart, status, healthcheck, create, delete, version, zk, auth, assert, config, export, api, package, post");
    print("");
    print("  Standalone server example (start Solr running in the background on port 8984):");
    print("");
    printGreen("    ./solr start -p 8984");
    print("");
    print(
        "  SolrCloud example (start Solr running in SolrCloud mode using localhost:2181 to connect to Zookeeper, with 1g max heap size and remote Java debug options enabled):");
    print("");
    printGreen(
        "    ./solr start -c -m 1g -z localhost:2181 -a \"-Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=1044\"");
    print("");
    print(
        "  Omit '-z localhost:2181' from the above command if you have defined ZK_HOST in solr.in.sh.");
    print("");
    print("Pass --help or -h after any COMMAND to see command-specific usage information,");
    print("such as:    ./solr start --help or ./solr stop -h");
  }

  public static String normalizeSolrUrl(String solrUrl, boolean logUrlFormatWarning) {
    return normalizeSolrUrl(solrUrl, logUrlFormatWarning, "/solr");
  }

  /**
   * Strips off the end of solrUrl any hostContext when a legacy solrUrl like
   * http://localhost:8983/solr is used, and optionally logs a warning. In the future we'll have
   * urls ending with /api as well.
   *
   * @param solrUrl The user supplied url to Solr.
   * @param logUrlFormatWarning If a warning message should be logged about the url format
   * @param hostContext context path for Solr, is normally '/solr' but can be overridden here.
   *     Should not end with '/'
   * @return the solrUrl in the format that Solr expects to see internally.
   */
  public static String normalizeSolrUrl(
      String solrUrl, boolean logUrlFormatWarning, String hostContext) {
    if (solrUrl != null) {
      URI uri = URI.create(solrUrl);
      String urlPath = uri.getRawPath();
      if (hostContext != null && hostContext.endsWith("/")) {
        hostContext = hostContext.substring(0, hostContext.length() - 1);
      }
      // Only consider URI path component when normalizing hostContext
      if (urlPath.contains(hostContext)) {
        String newSolrUrl =
            uri.resolve(urlPath.substring(0, urlPath.indexOf(hostContext)) + "/").toString();
        if (logUrlFormatWarning) {
          CLIO.err(
              "WARNING: URLs provided to this tool needn't include Solr's context-root (e.g. \"/solr\"). Such URLs are deprecated and support for them will be removed in a future release. Correcting from ["
                  + solrUrl
                  + "] to ["
                  + newSolrUrl
                  + "].");
        }
        solrUrl = newSolrUrl;
      }
      if (solrUrl.endsWith("/")) {
        solrUrl = solrUrl.substring(0, solrUrl.length() - 1);
      }
    }
    return solrUrl;
  }

  /**
   * Get the base URL of a live Solr instance from either the --solr-url command-line option or from
   * ZooKeeper.
   */
  public static String normalizeSolrUrl(CommandLine cli) throws Exception {
    String solrUrl =
        cli.hasOption("solr-url") ? cli.getOptionValue("solr-url") : cli.getOptionValue("solrUrl");
    if (solrUrl == null) {
      String zkHost =
          cli.hasOption("zk-host") ? cli.getOptionValue("zk-host") : cli.getOptionValue("zkHost");
      if (zkHost == null) {
        solrUrl = SolrCLI.getDefaultSolrUrl();
        CLIO.err(
            "Neither --zk-host or --solr-url parameters provided so assuming solr url is "
                + solrUrl
                + ".");
      } else {
        try (CloudSolrClient cloudSolrClient = getCloudHttp2SolrClient(zkHost)) {
          cloudSolrClient.connect();
          Set<String> liveNodes = cloudSolrClient.getClusterState().getLiveNodes();
          if (liveNodes.isEmpty())
            throw new IllegalStateException(
                "No live nodes found! Cannot determine 'solrUrl' from ZooKeeper: " + zkHost);

          String firstLiveNode = liveNodes.iterator().next();
          solrUrl = ZkStateReader.from(cloudSolrClient).getBaseUrlForNodeName(firstLiveNode);
          solrUrl = normalizeSolrUrl(solrUrl, false);
        }
      }
    }
    return solrUrl;
  }

  /**
   * Get the ZooKeeper connection string from either the zk-host command-line option or by looking
   * it up from a running Solr instance based on the solr-url option.
   */
  public static String getZkHost(CommandLine cli) throws Exception {

    String zkHost = getOptionWithDeprecatedAndDefault(cli, "zk-host", "zkHost", null);
    if (zkHost != null && !zkHost.isBlank()) {
      return zkHost;
    }

    try (var solrClient = getSolrClient(cli)) {
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

  public static SolrZkClient getSolrZkClient(CommandLine cli, String zkHost) {
    if (zkHost == null) {
      throw new IllegalStateException(
          "Solr at "
              + cli.getOptionValue("solrUrl")
              + " is running in standalone server mode, this command can only be used when running in SolrCloud mode.\n");
    }
    return new SolrZkClient.Builder()
        .withUrl(zkHost)
        .withTimeout(SolrZkClientTimeout.DEFAULT_ZK_CLIENT_TIMEOUT, TimeUnit.MILLISECONDS)
        .build();
  }

  public static CloudHttp2SolrClient getCloudHttp2SolrClient(String zkHost) {
    return getCloudHttp2SolrClient(zkHost, null);
  }

  public static CloudHttp2SolrClient getCloudHttp2SolrClient(
      String zkHost, Http2SolrClient.Builder builder) {
    return new CloudHttp2SolrClient.Builder(Collections.singletonList(zkHost), Optional.empty())
        .withInternalClientBuilder(builder)
        .build();
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

  public static boolean isCloudMode(SolrClient solrClient) throws SolrServerException, IOException {
    NamedList<Object> systemInfo =
        solrClient.request(new GenericSolrRequest(SolrRequest.METHOD.GET, SYSTEM_INFO_PATH));
    return "solrcloud".equals(systemInfo.get("mode"));
  }

  public static class AssertionFailureException extends Exception {
    public AssertionFailureException(String message) {
      super(message);
    }
  }

  public static Path getConfigSetsDir(Path solrInstallDir) {
    Path configSetsPath = Paths.get("server/solr/configsets/");
    return solrInstallDir.resolve(configSetsPath);
  }

  public static void print(Object message) {
    print(null, message);
  }

  /** Console print using green color */
  public static void printGreen(Object message) {
    print(GREEN, message);
  }

  /** Console print using red color */
  public static void printRed(Object message) {
    print(RED, message);
  }

  public static void print(String color, Object message) {
    String RESET = "\u001B[0m";

    if (color != null) {
      CLIO.out(color + String.valueOf(message) + RESET);
    } else {
      CLIO.out(String.valueOf(message));
    }
  }
}
