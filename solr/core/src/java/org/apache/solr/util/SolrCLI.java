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
package org.apache.solr.util;

import static org.apache.solr.common.SolrException.ErrorCode.FORBIDDEN;
import static org.apache.solr.common.SolrException.ErrorCode.UNAUTHORIZED;
import static org.apache.solr.common.params.CommonParams.NAME;
import static org.apache.solr.common.util.Utils.fromJSONString;

import com.google.common.annotations.VisibleForTesting;
import java.io.File;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.net.ConnectException;
import java.net.SocketException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.LinkedHashMap;
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
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.NoHttpResponseException;
import org.apache.http.StatusLine;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.HttpResponseException;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.conn.ConnectTimeoutException;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudLegacySolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.request.ContentStreamUpdateRequest;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.ContentStreamBase;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.util.cli.ApiTool;
import org.apache.solr.util.cli.AssertTool;
import org.apache.solr.util.cli.AuthTool;
import org.apache.solr.util.cli.ConfigSetDownloadTool;
import org.apache.solr.util.cli.ConfigSetUploadTool;
import org.apache.solr.util.cli.ConfigTool;
import org.apache.solr.util.cli.CreateCollectionTool;
import org.apache.solr.util.cli.CreateCoreTool;
import org.apache.solr.util.cli.CreateTool;
import org.apache.solr.util.cli.DeleteTool;
import org.apache.solr.util.cli.HealthcheckTool;
import org.apache.solr.util.cli.RunExampleTool;
import org.apache.solr.util.cli.StatusTool;
import org.apache.solr.util.cli.Tool;
import org.apache.solr.util.cli.ZkCpTool;
import org.apache.solr.util.cli.ZkLsTool;
import org.apache.solr.util.cli.ZkMkrootTool;
import org.apache.solr.util.cli.ZkMvTool;
import org.apache.solr.util.cli.ZkRmTool;
import org.apache.solr.util.configuration.SSLConfigurationsFactory;
import org.noggit.CharArr;
import org.noggit.JSONParser;
import org.noggit.JSONWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Command-line utility for working with Solr. */
public class SolrCLI implements CLIO {
  private static final long MAX_WAIT_FOR_CORE_LOAD_NANOS =
      TimeUnit.NANOSECONDS.convert(1, TimeUnit.MINUTES);

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  public static final String DEFAULT_SOLR_URL = "http://localhost:8983/solr";
  public static final String DEFAULT_ZK_HOST = "localhost:9983";
  public static final String DEFAULT_CONFIG_SET = "_default";

  public static final Option OPTION_ZKHOST =
      Option.builder("z")
          .argName("HOST")
          .hasArg()
          .required(false)
          .desc("Address of the ZooKeeper ensemble; defaults to: " + DEFAULT_ZK_HOST)
          .longOpt("zkHost")
          .build();
  public static final Option OPTION_SOLRURL =
      Option.builder("solrUrl")
          .argName("HOST")
          .hasArg()
          .required(false)
          .desc(
              "Base Solr URL, which can be used to determine the zkHost if that's not known; defaults to: "
                  + DEFAULT_SOLR_URL)
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

  public static final Option[] CLOUD_OPTIONS =
      new Option[] {
        OPTION_ZKHOST,
        Option.builder("c")
            .argName("COLLECTION")
            .hasArg()
            .required(false)
            .desc("Name of collection; no default.")
            .longOpt("collection")
            .build(),
        OPTION_VERBOSE
      };

  public static final Option[] CREATE_COLLECTION_OPTIONS =
      new Option[] {
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
        OPTION_VERBOSE
      };

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

    if (args.length == 1 && Arrays.asList("-v", "-version", "version").contains(args[0])) {
      // Simple version tool, no need for its own class
      CLIO.out(SolrVersion.LATEST.toString());
      exit(0);
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

  public static CommandLine parseCmdLine(String toolName, String[] args, Option[] toolOptions)
      throws Exception {
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

    // If you add a built-in tool to this class, add it here to avoid
    // classpath scanning

    for (Class<? extends Tool> next :
        findToolClassesInPackage(List.of("org.apache.solr.util", "org.apache.solr.util.cli"))) {
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
    formatter.printHelp("mv", getToolOptions(new ZkMvTool()));
    formatter.printHelp("cp", getToolOptions(new ZkCpTool()));
    formatter.printHelp("ls", getToolOptions(new ZkLsTool()));
    formatter.printHelp("mkroot", getToolOptions(new ZkMkrootTool()));
    formatter.printHelp("assert", getToolOptions(new AssertTool()));
    formatter.printHelp("auth", getToolOptions(new AuthTool()));
    formatter.printHelp("export", getToolOptions(new ExportTool()));
    formatter.printHelp("package", getToolOptions(new PackageTool()));

    List<Class<? extends Tool>> toolClasses =
        findToolClassesInPackage((List.of("org.apache.solr.util", "org.apache.solr.util.cli")));
    for (Class<? extends Tool> next : toolClasses) {
      Tool tool = next.getConstructor().newInstance();
      formatter.printHelp(tool.getName(), getToolOptions(tool));
    }
  }

  public static Options getToolOptions(Tool tool) {
    Options options = new Options();
    options.addOption("help", false, "Print this message");
    options.addOption(OPTION_VERBOSE);
    Option[] toolOpts = tool.getOptions();
    for (int i = 0; i < toolOpts.length; i++) {
      options.addOption(toolOpts[i]);
    }
    return options;
  }

  public static Option[] joinOptions(Option[] lhs, Option[] rhs) {
    if (lhs == null) {
      return rhs == null ? new Option[0] : rhs;
    }

    if (rhs == null) {
      return lhs;
    }

    Option[] options = new Option[lhs.length + rhs.length];
    System.arraycopy(lhs, 0, options, 0, lhs.length);
    System.arraycopy(rhs, 0, options, lhs.length, rhs.length);

    return options;
  }

  /** Parses the command-line arguments passed by the user. */
  public static CommandLine processCommandLineArgs(
      String toolName, Option[] customOptions, String[] args) {
    Options options = new Options();

    options.addOption("help", false, "Print this message");
    options.addOption(OPTION_VERBOSE);

    if (customOptions != null) {
      for (int i = 0; i < customOptions.length; i++) options.addOption(customOptions[i]);
    }

    CommandLine cli = null;
    try {
      cli = (new GnuParser()).parse(options, args);
    } catch (ParseException exp) {
      boolean hasHelpArg = false;
      if (args != null && args.length > 0) {
        for (int z = 0; z < args.length; z++) {
          if ("--help".equals(args[z]) || "-help".equals(args[z])) {
            hasHelpArg = true;
            break;
          }
        }
      }
      if (!hasHelpArg) {
        CLIO.err("Failed to parse command-line arguments due to: " + exp.getMessage());
      }
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(toolName, options);
      exit(1);
    }

    if (cli.hasOption("help")) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(toolName, options);
      exit(0);
    }

    return cli;
  }

  /** Scans Jar files on the classpath for Tool implementations to activate. */
  private static List<Class<? extends Tool>> findToolClassesInPackage(List<String> packageNames) {
    List<Class<? extends Tool>> toolClasses = new ArrayList<>();
    for (String packageName : packageNames) {
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
          if (Tool.class.isAssignableFrom(theClass))
            toolClasses.add(theClass.asSubclass(Tool.class));
        }
      } catch (Exception e) {
        // safe to squelch this as it's just looking for tools to run
        log.debug("Failed to find Tool impl classes in {}, due to: ", packageName, e);
      }
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
    boolean wasCommError =
        (rootCause instanceof ConnectException
            || rootCause instanceof ConnectTimeoutException
            || rootCause instanceof NoHttpResponseException
            || rootCause instanceof SocketException);
    return wasCommError;
  }

  /**
   * Tries a simple HEAD request and throws SolrException in case of Authorization error
   *
   * @param url the url to do a HEAD request to
   * @param httpClient the http client to use (make sure it has authentication options set)
   * @return the HTTP response code
   * @throws SolrException if auth/autz problems
   * @throws IOException if connection failure
   */
  public static int attemptHttpHead(String url, HttpClient httpClient)
      throws SolrException, IOException {
    HttpResponse response =
        httpClient.execute(new HttpHead(url), HttpClientUtil.createNewHttpClientRequestContext());
    int code = response.getStatusLine().getStatusCode();
    if (code == UNAUTHORIZED.code || code == FORBIDDEN.code) {
      throw new SolrException(
          SolrException.ErrorCode.getErrorCode(code),
          "Solr requires authentication for "
              + url
              + ". Please supply valid credentials. HTTP code="
              + code);
    }
    return code;
  }

  public static boolean exceptionIsAuthRelated(Exception exc) {
    return (exc instanceof SolrException
        && Arrays.asList(UNAUTHORIZED.code, FORBIDDEN.code).contains(((SolrException) exc).code()));
  }

  public static CloseableHttpClient getHttpClient() {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set(HttpClientUtil.PROP_MAX_CONNECTIONS, 128);
    params.set(HttpClientUtil.PROP_MAX_CONNECTIONS_PER_HOST, 32);
    params.set(HttpClientUtil.PROP_FOLLOW_REDIRECTS, false);
    return HttpClientUtil.createClient(params);
  }

  public static final String JSON_CONTENT_TYPE = "application/json";

  public static NamedList<Object> postJsonToSolr(
      SolrClient solrClient, String updatePath, String jsonBody) throws Exception {
    ContentStreamBase.StringStream contentStream = new ContentStreamBase.StringStream(jsonBody);
    contentStream.setContentType(JSON_CONTENT_TYPE);
    ContentStreamUpdateRequest req = new ContentStreamUpdateRequest(updatePath);
    req.addContentStream(contentStream);
    return solrClient.request(req);
  }

  /** Useful when a tool just needs to send one request to Solr. */
  public static Map<String, Object> getJson(String getUrl) throws Exception {
    Map<String, Object> json = null;
    ;
    try (CloseableHttpClient httpClient = getHttpClient()) {
      json = getJson(httpClient, getUrl, 2, true);
    }
    return json;
  }

  /** Utility function for sending HTTP GET request to Solr with built-in retry support. */
  public static Map<String, Object> getJson(
      HttpClient httpClient, String getUrl, int attempts, boolean isFirstAttempt) throws Exception {
    Map<String, Object> json = null;
    if (attempts >= 1) {
      try {
        json = getJson(httpClient, getUrl);
      } catch (Exception exc) {
        if (exceptionIsAuthRelated(exc)) {
          throw exc;
        }
        if (--attempts > 0 && checkCommunicationError(exc)) {
          if (!isFirstAttempt) // only show the log warning after the second attempt fails
          log.warn(
                "Request to {} failed, sleeping for 5 seconds before re-trying the request ...",
                getUrl,
                exc);
          try {
            Thread.sleep(5000);
          } catch (InterruptedException ie) {
            Thread.interrupted();
          }

          // retry using recursion with one-less attempt available
          json = getJson(httpClient, getUrl, attempts, false);
        } else {
          // no more attempts or error is not retry-able
          throw exc;
        }
      }
    }

    return json;
  }

  @SuppressWarnings("unchecked")
  private static class SolrResponseHandler implements ResponseHandler<Map<String, Object>> {
    @Override
    public Map<String, Object> handleResponse(HttpResponse response)
        throws ClientProtocolException, IOException {
      HttpEntity entity = response.getEntity();
      if (entity != null) {

        String respBody = EntityUtils.toString(entity);
        Object resp = null;
        try {
          resp = fromJSONString(respBody);
        } catch (JSONParser.ParseException pe) {
          throw new ClientProtocolException(
              "Expected JSON response from server but received: "
                  + respBody
                  + "\nTypically, this indicates a problem with the Solr server; check the Solr server logs for more information.");
        }
        if (resp instanceof Map) {
          return (Map<String, Object>) resp;
        } else {
          throw new ClientProtocolException(
              "Expected JSON object in response but received " + resp);
        }
      } else {
        StatusLine statusLine = response.getStatusLine();
        throw new HttpResponseException(statusLine.getStatusCode(), statusLine.getReasonPhrase());
      }
    }
  }

  /**
   * Utility function for sending HTTP GET request to Solr and then doing some validation of the
   * response.
   */
  public static Map<String, Object> getJson(HttpClient httpClient, String getUrl) throws Exception {
    try {
      // ensure we're requesting JSON back from Solr
      HttpGet httpGet =
          new HttpGet(
              new URIBuilder(getUrl).setParameter(CommonParams.WT, CommonParams.JSON).build());

      // make the request and get back a parsed JSON object
      Map<String, Object> json =
          httpClient.execute(
              httpGet,
              new SolrResponseHandler(),
              HttpClientUtil.createNewHttpClientRequestContext());
      // check the response JSON from Solr to see if it is an error
      Long statusCode = asLong("/responseHeader/status", json);
      if (statusCode != null && statusCode == -1) {
        throw new SolrServerException(
            "Unable to determine outcome of GET request to: " + getUrl + "! Response: " + json);
      } else if (statusCode != null && statusCode != 0) {
        String errMsg = asString("/error/msg", json);
        if (errMsg == null) errMsg = String.valueOf(json);
        throw new SolrServerException(errMsg);
      } else {
        // make sure no "failure" object in there either
        Object failureObj = json.get("failure");
        if (failureObj != null) {
          if (failureObj instanceof Map) {
            Object err = ((Map) failureObj).get("");
            if (err != null) throw new SolrServerException(err.toString());
          }
          throw new SolrServerException(failureObj.toString());
        }
      }
      return json;
    } catch (ClientProtocolException cpe) {
      // Currently detecting authentication by string-matching the HTTP response
      // Perhaps SolrClient should have thrown an exception itself??
      if (cpe.getMessage().contains("HTTP ERROR 401")
          || cpe.getMessage().contentEquals("HTTP ERROR 403")) {
        int code = cpe.getMessage().contains("HTTP ERROR 401") ? 401 : 403;
        throw new SolrException(
            SolrException.ErrorCode.getErrorCode(code),
            "Solr requires authentication for "
                + getUrl
                + ". Please supply valid credentials. HTTP code="
                + code);
      } else {
        throw cpe;
      }
    }
  }

  /** Helper function for reading a String value from a JSON Object tree. */
  public static String asString(String jsonPath, Map<String, Object> json) {
    return pathAs(String.class, jsonPath, json);
  }

  /** Helper function for reading a Long value from a JSON Object tree. */
  public static Long asLong(String jsonPath, Map<String, Object> json) {
    return pathAs(Long.class, jsonPath, json);
  }

  /** Helper function for reading a List of Strings from a JSON Object tree. */
  @SuppressWarnings("unchecked")
  public static List<String> asList(String jsonPath, Map<String, Object> json) {
    return pathAs(List.class, jsonPath, json);
  }

  /** Helper function for reading a Map from a JSON Object tree. */
  @SuppressWarnings("unchecked")
  public static Map<String, Object> asMap(String jsonPath, Map<String, Object> json) {
    return pathAs(Map.class, jsonPath, json);
  }

  @SuppressWarnings("unchecked")
  public static <T> T pathAs(Class<T> clazz, String jsonPath, Map<String, Object> json) {
    T val = null;
    Object obj = atPath(jsonPath, json);
    if (obj != null) {
      if (clazz.isAssignableFrom(obj.getClass())) {
        val = (T) obj;
      } else {
        // no ok if it's not null and of a different type
        throw new IllegalStateException(
            "Expected a "
                + clazz.getName()
                + " at path "
                + jsonPath
                + " but found "
                + obj
                + " instead! "
                + json);
      }
    } // it's ok if it is null
    return val;
  }

  /**
   * Helper function for reading an Object of unknown type from a JSON Object tree.
   *
   * <p>To find a path to a child that starts with a slash (e.g. queryHandler named /query) you must
   * escape the slash. For instance /config/requestHandler/\/query/defaults/echoParams would get the
   * echoParams value for the "/query" request handler.
   */
  @SuppressWarnings("unchecked")
  public static Object atPath(String jsonPath, Map<String, Object> json) {
    if ("/".equals(jsonPath)) return json;

    if (!jsonPath.startsWith("/"))
      throw new IllegalArgumentException(
          "Invalid JSON path: " + jsonPath + "! Must start with a /");

    Map<String, Object> parent = json;
    Object result = null;
    String[] path =
        jsonPath.split("(?<![\\\\])/"); // Break on all slashes _not_ preceeded by a backslash
    for (int p = 1; p < path.length; p++) {
      String part = path[p];

      if (part.startsWith("\\")) {
        part = part.substring(1);
      }

      Object child = parent.get(part);
      if (child == null) break;

      if (p == path.length - 1) {
        // success - found the node at the desired path
        result = child;
      } else {
        if (child instanceof Map) {
          // keep walking the path down to the desired node
          parent = (Map<String, Object>) child;
        } else {
          // early termination - hit a leaf before the requested node
          break;
        }
      }
    }
    return result;
  }

  @VisibleForTesting
  public static final String uptime(long uptimeMs) {
    if (uptimeMs <= 0L) return "?";

    long numDays = TimeUnit.MILLISECONDS.toDays(uptimeMs);
    long rem = uptimeMs - TimeUnit.DAYS.toMillis(numDays);
    long numHours = TimeUnit.MILLISECONDS.toHours(rem);
    rem = rem - TimeUnit.HOURS.toMillis(numHours);
    long numMinutes = TimeUnit.MILLISECONDS.toMinutes(rem);
    rem = rem - TimeUnit.MINUTES.toMillis(numMinutes);
    long numSeconds = Math.round(rem / 1000.0);
    return String.format(
        Locale.ROOT,
        "%d days, %d hours, %d minutes, %d seconds",
        numDays,
        numHours,
        numMinutes,
        numSeconds);
  }

  public static class ReplicaHealth implements Comparable<ReplicaHealth> {
    String shard;
    String name;
    String url;
    String status;
    long numDocs;
    boolean isLeader;
    String uptime;
    String memory;

    public ReplicaHealth(
        String shard,
        String name,
        String url,
        String status,
        long numDocs,
        boolean isLeader,
        String uptime,
        String memory) {
      this.shard = shard;
      this.name = name;
      this.url = url;
      this.numDocs = numDocs;
      this.status = status;
      this.isLeader = isLeader;
      this.uptime = uptime;
      this.memory = memory;
    }

    public Map<String, Object> asMap() {
      Map<String, Object> map = new LinkedHashMap<>();
      map.put(NAME, name);
      map.put("url", url);
      map.put("numDocs", numDocs);
      map.put("status", status);
      if (uptime != null) map.put("uptime", uptime);
      if (memory != null) map.put("memory", memory);
      if (isLeader) map.put("leader", true);
      return map;
    }

    @Override
    public String toString() {
      CharArr arr = new CharArr();
      new JSONWriter(arr, 2).write(asMap());
      return arr.toString();
    }

    @Override
    public int hashCode() {
      return this.shard.hashCode() + (isLeader ? 1 : 0);
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) return true;
      if (obj == null) return false;
      if (!(obj instanceof ReplicaHealth)) return true;
      ReplicaHealth that = (ReplicaHealth) obj;
      return this.shard.equals(that.shard) && this.isLeader == that.isLeader;
    }

    @Override
    public int compareTo(ReplicaHealth other) {
      if (this == other) return 0;
      if (other == null) return 1;

      int myShardIndex = Integer.parseInt(this.shard.substring("shard".length()));

      int otherShardIndex = Integer.parseInt(other.shard.substring("shard".length()));

      if (myShardIndex == otherShardIndex) {
        // same shard index, list leaders first
        return this.isLeader ? -1 : 1;
      }

      return myShardIndex - otherShardIndex;
    }
  }

  public enum ShardState {
    healthy,
    degraded,
    down,
    no_leader
  }

  public static class ShardHealth {
    String shard;
    List<ReplicaHealth> replicas;

    public ShardHealth(String shard, List<ReplicaHealth> replicas) {
      this.shard = shard;
      this.replicas = replicas;
    }

    public ShardState getShardState() {
      boolean healthy = true;
      boolean hasLeader = false;
      boolean atLeastOneActive = false;
      for (ReplicaHealth replicaHealth : replicas) {
        if (replicaHealth.isLeader) hasLeader = true;

        if (!Replica.State.ACTIVE.toString().equals(replicaHealth.status)) {
          healthy = false;
        } else {
          atLeastOneActive = true;
        }
      }

      if (!hasLeader) return ShardState.no_leader;

      return healthy
          ? ShardState.healthy
          : (atLeastOneActive ? ShardState.degraded : ShardState.down);
    }

    public Map<String, Object> asMap() {
      Map<String, Object> map = new LinkedHashMap<>();
      map.put("shard", shard);
      map.put("status", getShardState().toString());
      List<Object> replicaList = new ArrayList<>();
      for (ReplicaHealth replica : replicas) replicaList.add(replica.asMap());
      map.put("replicas", replicaList);
      return map;
    }

    @Override
    public String toString() {
      CharArr arr = new CharArr();
      new JSONWriter(arr, 2).write(asMap());
      return arr.toString();
    }
  }

  /**
   * Get the base URL of a live Solr instance from either the solrUrl command-line option from
   * ZooKeeper.
   */
  public static String resolveSolrUrl(CommandLine cli) throws Exception {
    String solrUrl = cli.getOptionValue("solrUrl");
    if (solrUrl == null) {
      String zkHost = cli.getOptionValue("zkHost");
      if (zkHost == null)
        throw new IllegalStateException(
            "Must provide either the '-solrUrl' or '-zkHost' parameters!");

      try (CloudSolrClient cloudSolrClient =
          new CloudLegacySolrClient.Builder(Collections.singletonList(zkHost), Optional.empty())
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
    return solrUrl;
  }

  /**
   * Get the ZooKeeper connection string from either the zkHost command-line option or by looking it
   * up from a running Solr instance based on the solrUrl option.
   */
  public static String getZkHost(CommandLine cli) throws Exception {
    String zkHost = cli.getOptionValue("zkHost");
    if (zkHost != null) {
      return zkHost;
    }

    // find it using the localPort
    String solrUrl = cli.getOptionValue("solrUrl");
    if (solrUrl == null)
      throw new IllegalStateException(
          "Must provide either the -zkHost or -solrUrl parameters to use this command!");

    if (!solrUrl.endsWith("/")) {
      solrUrl += "/";
    }

    String systemInfoUrl = solrUrl + "admin/info/system";
    try (CloseableHttpClient httpClient = getHttpClient()) {
      // hit Solr to get system info
      Map<String, Object> systemInfo = getJson(httpClient, systemInfoUrl, 2, true);

      // convert raw JSON into user-friendly output
      StatusTool statusTool = new StatusTool();
      Map<String, Object> status = statusTool.reportStatus(solrUrl, systemInfo, httpClient);
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

  public static boolean safeCheckCollectionExists(String url, String collection) {
    boolean exists = false;
    try {
      Map<String, Object> existsCheckResult = getJson(url);
      @SuppressWarnings("unchecked")
      List<String> collections = (List<String>) existsCheckResult.get("collections");
      exists = collections != null && collections.contains(collection);
    } catch (Exception exc) {
      // just ignore it since we're only interested in a positive result here
    }
    return exists;
  }

  public static boolean safeCheckCoreExists(String coreStatusUrl, String coreName) {
    boolean exists = false;
    try {
      boolean wait = false;
      final long startWaitAt = System.nanoTime();
      do {
        if (wait) {
          final int clamPeriodForStatusPollMs = 1000;
          Thread.sleep(clamPeriodForStatusPollMs);
        }
        Map<String, Object> existsCheckResult = getJson(coreStatusUrl);
        @SuppressWarnings("unchecked")
        Map<String, Object> status = (Map<String, Object>) existsCheckResult.get("status");
        @SuppressWarnings("unchecked")
        Map<String, Object> coreStatus = (Map<String, Object>) status.get(coreName);
        @SuppressWarnings("unchecked")
        Map<String, Object> failureStatus =
            (Map<String, Object>) existsCheckResult.get("initFailures");
        String errorMsg = (String) failureStatus.get(coreName);
        final boolean hasName = coreStatus != null && coreStatus.containsKey(NAME);
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
