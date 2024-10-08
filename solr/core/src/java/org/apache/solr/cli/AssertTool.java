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
import java.io.PrintStream;
import java.lang.invoke.MethodHandles;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileOwnerAttributeView;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.HealthCheckRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.common.util.NamedList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Asserts various conditions and exists with error code if fails, else continues with no output */
public class AssertTool extends ToolBase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static String message = null;
  private static boolean useExitCode = false;
  private static Long timeoutMs = 1000L;

  public AssertTool() {
    this(CLIO.getOutStream());
  }

  public AssertTool(PrintStream stdout) {
    super(stdout);
  }

  @Override
  public String getName() {
    return "assert";
  }

  @Override
  public List<Option> getOptions() {
    return List.of(
        Option.builder("R")
            .desc("Asserts that we are NOT the root user.")
            .longOpt("not-root")
            .build(),
        Option.builder("r").desc("Asserts that we are the root user.").longOpt("root").build(),
        Option.builder("S")
            .desc("Asserts that Solr is NOT running on a certain URL. Default timeout is 1000ms.")
            .longOpt("not-started")
            .hasArg(true)
            .argName("url")
            .build(),
        Option.builder("s")
            .desc("Asserts that Solr is running on a certain URL. Default timeout is 1000ms.")
            .longOpt("started")
            .hasArg(true)
            .argName("url")
            .build(),
        Option.builder("u")
            .desc("Asserts that we run as same user that owns <directory>.")
            .longOpt("same-user")
            .hasArg(true)
            .argName("directory")
            .build(),
        Option.builder("x")
            .desc("Asserts that directory <directory> exists.")
            .longOpt("exists")
            .hasArg(true)
            .argName("directory")
            .build(),
        Option.builder("X")
            .desc("Asserts that directory <directory> does NOT exist.")
            .longOpt("not-exists")
            .hasArg(true)
            .argName("directory")
            .build(),
        Option.builder("c")
            .desc(
                "Asserts that Solr is running in cloud mode.  Also fails if Solr not running.  URL should be for root Solr path.")
            .longOpt("cloud")
            .hasArg(true)
            .argName("url")
            .build(),
        Option.builder("C")
            .desc(
                "Asserts that Solr is not running in cloud mode.  Also fails if Solr not running.  URL should be for root Solr path.")
            .longOpt("not-cloud")
            .hasArg(true)
            .argName("url")
            .build(),
        Option.builder("m")
            .desc("Exception message to be used in place of the default error message.")
            .longOpt("message")
            .hasArg(true)
            .argName("message")
            .build(),
        Option.builder("t")
            .desc("Timeout in ms for commands supporting a timeout.")
            .longOpt("timeout")
            .hasArg(true)
            .type(Long.class)
            .argName("ms")
            .build(),
        Option.builder("e")
            .desc("Return an exit code instead of printing error message on assert fail.")
            .longOpt("exitcode")
            .build());
  }

  @Override
  public int runTool(CommandLine cli) throws Exception {
    verbose = cli.hasOption(SolrCLI.OPTION_VERBOSE.getOpt());

    int toolExitStatus = 0;
    try {
      toolExitStatus = runAssert(cli);
    } catch (Exception exc) {
      // since this is a CLI, spare the user the stacktrace
      String excMsg = exc.getMessage();
      if (excMsg != null) {
        if (verbose) {
          CLIO.err("\nERROR: " + exc + "\n");
        } else {
          CLIO.err("\nERROR: " + excMsg + "\n");
        }
        toolExitStatus = 100; // Exit >= 100 means error, else means number of tests that failed
      } else {
        throw exc;
      }
    }
    return toolExitStatus;
  }

  @Override
  public void runImpl(CommandLine cli) throws Exception {
    runAssert(cli);
  }

  /**
   * Custom run method which may return exit code
   *
   * @param cli the command line object
   * @return 0 on success, or a number corresponding to number of tests that failed
   * @throws Exception if a tool failed, e.g. authentication failure
   */
  protected int runAssert(CommandLine cli) throws Exception {
    if (cli.getOptions().length == 0 || cli.getArgs().length > 0 || cli.hasOption("h")) {
      new HelpFormatter()
          .printHelp(
              "bin/solr assert [-m <message>] [-e] [-rR] [-s <url>] [-S <url>] [-c <url>] [-C <url>] [-u <dir>] [-x <dir>] [-X <dir>]",
              SolrCLI.getToolOptions(this));
      return 1;
    }
    if (cli.hasOption("m")) {
      message = cli.getOptionValue("m");
    }
    if (cli.hasOption("t")) {
      timeoutMs = Long.parseLong(cli.getOptionValue("t"));
    }
    if (cli.hasOption("e")) {
      useExitCode = true;
    }

    int ret = 0;
    if (cli.hasOption("r")) {
      ret += assertRootUser();
    }
    if (cli.hasOption("R")) {
      ret += assertNotRootUser();
    }
    if (cli.hasOption("x")) {
      ret += assertFileExists(cli.getOptionValue("x"));
    }
    if (cli.hasOption("X")) {
      ret += assertFileNotExists(cli.getOptionValue("X"));
    }
    if (cli.hasOption("u")) {
      ret += sameUser(cli.getOptionValue("u"));
    }
    if (cli.hasOption("s")) {
      ret += assertSolrRunning(cli.getOptionValue("s"));
    }
    if (cli.hasOption("S")) {
      ret += assertSolrNotRunning(cli.getOptionValue("S"));
    }
    if (cli.hasOption("c")) {
      ret += assertSolrRunningInCloudMode(cli.getOptionValue("c"));
    }
    if (cli.hasOption("C")) {
      ret += assertSolrNotRunningInCloudMode(cli.getOptionValue("C"));
    }
    return ret;
  }

  public static int assertSolrRunning(String url) throws Exception {
    StatusTool status = new StatusTool();
    try {
      status.waitToSeeSolrUp(url, timeoutMs, TimeUnit.MILLISECONDS);
    } catch (Exception se) {
      if (SolrCLI.exceptionIsAuthRelated(se)) {
        throw se;
      }
      return exitOrException(
          "Solr is not running on url "
              + url
              + " after "
              + TimeUnit.SECONDS.convert(timeoutMs, TimeUnit.MILLISECONDS)
              + " seconds");
    }
    return 0;
  }

  public static int assertSolrNotRunning(String url) throws Exception {
    StatusTool status = new StatusTool();
    long timeout =
        System.nanoTime() + TimeUnit.NANOSECONDS.convert(timeoutMs, TimeUnit.MILLISECONDS);
    try (SolrClient solrClient = SolrCLI.getSolrClient(url)) {
      NamedList<Object> response = solrClient.request(new HealthCheckRequest());
      Integer statusCode = (Integer) response.findRecursive("responseHeader", "status");
      SolrCLI.checkCodeForAuthError(statusCode);
    } catch (IOException | SolrServerException e) {
      log.debug("Opening connection to {} failed, Solr does not seem to be running", url, e);
      return 0;
    }
    while (System.nanoTime() < timeout) {
      try {
        status.waitToSeeSolrUp(url, 1, TimeUnit.SECONDS);
        try {
          log.debug("Solr still up. Waiting before trying again to see if it was stopped");
          Thread.sleep(1000L);
        } catch (InterruptedException interrupted) {
          timeout = 0; // stop looping
        }
      } catch (Exception se) {
        if (SolrCLI.exceptionIsAuthRelated(se)) {
          throw se;
        }
        return exitOrException(se.getMessage());
      }
    }
    return exitOrException(
        "Solr is still running at "
            + url
            + " after "
            + TimeUnit.SECONDS.convert(timeoutMs, TimeUnit.MILLISECONDS)
            + " seconds");
  }

  public static int assertSolrRunningInCloudMode(String url) throws Exception {
    if (!isSolrRunningOn(url)) {
      return exitOrException(
          "Solr is not running on url "
              + url
              + " after "
              + TimeUnit.SECONDS.convert(timeoutMs, TimeUnit.MILLISECONDS)
              + " seconds");
    }

    if (!runningSolrIsCloud(url)) {
      return exitOrException("Solr is not running in cloud mode on " + url);
    }
    return 0;
  }

  public static int assertSolrNotRunningInCloudMode(String url) throws Exception {
    if (!isSolrRunningOn(url)) {
      return exitOrException(
          "Solr is not running on url "
              + url
              + " after "
              + TimeUnit.SECONDS.convert(timeoutMs, TimeUnit.MILLISECONDS)
              + " seconds");
    }

    if (runningSolrIsCloud(url)) {
      return exitOrException("Solr is not running in standalone mode on " + url);
    }
    return 0;
  }

  public static int sameUser(String directory) throws Exception {
    if (Files.exists(Paths.get(directory))) {
      String userForDir = userForDir(Paths.get(directory));
      if (!currentUser().equals(userForDir)) {
        return exitOrException("Must run as user " + userForDir + ". We are " + currentUser());
      }
    } else {
      return exitOrException("Directory " + directory + " does not exist.");
    }
    return 0;
  }

  public static int assertFileExists(String directory) throws Exception {
    if (!Files.exists(Paths.get(directory))) {
      return exitOrException("Directory " + directory + " does not exist.");
    }
    return 0;
  }

  public static int assertFileNotExists(String directory) throws Exception {
    if (Files.exists(Paths.get(directory))) {
      return exitOrException("Directory " + directory + " should not exist.");
    }
    return 0;
  }

  public static int assertRootUser() throws Exception {
    if (!currentUser().equals("root")) {
      return exitOrException("Must run as root user");
    }
    return 0;
  }

  public static int assertNotRootUser() throws Exception {
    if (currentUser().equals("root")) {
      return exitOrException("Not allowed to run as root user");
    }
    return 0;
  }

  public static String currentUser() {
    return System.getProperty("user.name");
  }

  public static String userForDir(Path pathToDir) {
    try {
      FileOwnerAttributeView ownerAttributeView =
          Files.getFileAttributeView(pathToDir, FileOwnerAttributeView.class);
      return ownerAttributeView.getOwner().getName();
    } catch (IOException e) {
      return "N/A";
    }
  }

  private static int exitOrException(String msg) throws SolrCLI.AssertionFailureException {
    if (useExitCode) {
      return 1;
    } else {
      throw new SolrCLI.AssertionFailureException(message != null ? message : msg);
    }
  }

  private static boolean isSolrRunningOn(String url) throws Exception {
    StatusTool status = new StatusTool();
    try {
      status.waitToSeeSolrUp(url, timeoutMs, TimeUnit.MILLISECONDS);
      return true;
    } catch (Exception se) {
      if (SolrCLI.exceptionIsAuthRelated(se)) {
        throw se;
      }
      return false;
    }
  }

  private static boolean runningSolrIsCloud(String url) throws Exception {
    try (final SolrClient client = new Http2SolrClient.Builder(url).build()) {
      final SolrRequest<CollectionAdminResponse> request =
          new CollectionAdminRequest.ClusterStatus();
      final CollectionAdminResponse response = request.process(client);
      return true; // throws an exception otherwise
    } catch (Exception e) {
      if (SolrCLI.exceptionIsAuthRelated(e)) {
        throw e;
      }
      return false;
    }
  }
}
