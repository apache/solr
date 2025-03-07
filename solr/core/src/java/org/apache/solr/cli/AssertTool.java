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
import java.util.concurrent.TimeUnit;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.HealthCheckRequest;
import org.apache.solr.common.util.NamedList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Supports assert command in the bin/solr script. Asserts various conditions and exists with error
 * code if there are failures, else continues with no output.
 */
public class AssertTool extends ToolBase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static String message = null;
  private static boolean useExitCode = false;
  private static Long timeoutMs = 1000L;

  private static final Option IS_NOT_ROOT_OPTION =
      Option.builder().desc("Asserts that we are NOT the root user.").longOpt("not-root").build();

  private static final Option IS_ROOT_OPTION =
      Option.builder().desc("Asserts that we are the root user.").longOpt("root").build();

  private static final OptionGroup ROOT_OPTION =
      new OptionGroup().addOption(IS_NOT_ROOT_OPTION).addOption(IS_ROOT_OPTION);

  private static final Option IS_NOT_RUNNING_ON_OPTION =
      Option.builder()
          .desc("Asserts that Solr is NOT running on a certain URL. Default timeout is 1000ms.")
          .longOpt("not-started")
          .hasArg()
          .argName("url")
          .build();

  private static final Option IS_RUNNING_ON_OPTION =
      Option.builder()
          .desc("Asserts that Solr is running on a certain URL. Default timeout is 1000ms.")
          .longOpt("started")
          .hasArg()
          .argName("url")
          .build();

  private static final OptionGroup RUNNING_OPTION =
      new OptionGroup().addOption(IS_NOT_RUNNING_ON_OPTION).addOption(IS_RUNNING_ON_OPTION);

  private static final Option SAME_USER_OPTION =
      Option.builder()
          .desc("Asserts that we run as same user that owns <directory>.")
          .longOpt("same-user")
          .hasArg()
          .argName("directory")
          .build();

  private static final Option DIRECTORY_EXISTS_OPTION =
      Option.builder()
          .desc("Asserts that directory <directory> exists.")
          .longOpt("exists")
          .hasArg()
          .argName("directory")
          .build();

  private static final Option DIRECTORY_NOT_EXISTS_OPTION =
      Option.builder()
          .desc("Asserts that directory <directory> does NOT exist.")
          .longOpt("not-exists")
          .hasArg()
          .argName("directory")
          .build();

  private static final OptionGroup DIRECTORY_OPTION =
      new OptionGroup().addOption(DIRECTORY_EXISTS_OPTION).addOption(DIRECTORY_NOT_EXISTS_OPTION);

  private static final Option IS_CLOUD_OPTION =
      Option.builder()
          .desc(
              "Asserts that Solr is running in cloud mode.  Also fails if Solr not running.  URL should be for root Solr path.")
          .longOpt("cloud")
          .hasArg()
          .argName("url")
          .build();

  private static final Option IS_NOT_CLOUD_OPTION =
      Option.builder()
          .desc(
              "Asserts that Solr is not running in cloud mode.  Also fails if Solr not running.  URL should be for root Solr path.")
          .longOpt("not-cloud")
          .hasArg()
          .argName("url")
          .build();

  private static final OptionGroup CLOUD_OPTION =
      new OptionGroup().addOption(IS_CLOUD_OPTION).addOption(IS_NOT_CLOUD_OPTION);

  private static final Option MESSAGE_OPTION =
      Option.builder()
          .desc("Exception message to be used in place of the default error message.")
          .longOpt("message")
          .hasArg()
          .argName("message")
          .build();

  private static final Option TIMEOUT_OPTION =
      Option.builder()
          .desc("Timeout in ms for commands supporting a timeout.")
          .longOpt("timeout")
          .hasArg()
          .type(Long.class)
          .argName("ms")
          .build();

  private static final Option EXIT_CODE_OPTION =
      Option.builder()
          .desc("Return an exit code instead of printing error message on assert fail.")
          .longOpt("exitcode")
          .build();

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
  public Options getOptions() {
    return super.getOptions()
        .addOptionGroup(ROOT_OPTION)
        .addOptionGroup(RUNNING_OPTION)
        .addOption(SAME_USER_OPTION)
        .addOptionGroup(DIRECTORY_OPTION)
        .addOptionGroup(CLOUD_OPTION)
        .addOption(MESSAGE_OPTION)
        .addOption(TIMEOUT_OPTION)
        .addOption(EXIT_CODE_OPTION)
        .addOption(CommonCLIOptions.CREDENTIALS_OPTION);
  }

  /**
   * Returns 100 error code for a true "error", otherwise returns the number of tests that failed.
   * Otherwise, very similar to the parent runTool method.
   *
   * @param cli the command line object
   * @return 0 on success, or a number corresponding to number of tests that failed, or 100 for an
   *     Error
   * @throws Exception if a tool failed, e.g. authentication failure
   */
  @Override
  public int runTool(CommandLine cli) throws Exception {
    int toolExitStatus;
    try {
      toolExitStatus = runAssert(cli);
    } catch (Exception exc) {
      // since this is a CLI, spare the user the stacktrace
      String excMsg = exc.getMessage();
      if (excMsg != null) {
        if (isVerbose()) {
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
    message = cli.getOptionValue(MESSAGE_OPTION);
    timeoutMs = cli.getParsedOptionValue(TIMEOUT_OPTION, timeoutMs);
    useExitCode = cli.hasOption(EXIT_CODE_OPTION);

    int ret = 0;
    if (cli.hasOption(IS_ROOT_OPTION)) {
      ret += assertRootUser();
    }
    if (cli.hasOption(IS_NOT_ROOT_OPTION)) {
      ret += assertNotRootUser();
    }
    if (cli.hasOption(DIRECTORY_EXISTS_OPTION)) {
      ret += assertFileExists(cli.getOptionValue(DIRECTORY_EXISTS_OPTION));
    }
    if (cli.hasOption(DIRECTORY_NOT_EXISTS_OPTION)) {
      ret += assertFileNotExists(cli.getOptionValue(DIRECTORY_NOT_EXISTS_OPTION));
    }
    if (cli.hasOption(SAME_USER_OPTION)) {
      ret += sameUser(cli.getOptionValue(SAME_USER_OPTION));
    }
    if (cli.hasOption(IS_RUNNING_ON_OPTION)) {
      ret +=
          assertSolrRunning(
              cli.getOptionValue(IS_RUNNING_ON_OPTION),
              cli.getOptionValue(CommonCLIOptions.CREDENTIALS_OPTION));
    }
    if (cli.hasOption(IS_NOT_RUNNING_ON_OPTION)) {
      ret +=
          assertSolrNotRunning(
              cli.getOptionValue(IS_NOT_RUNNING_ON_OPTION),
              cli.getOptionValue(CommonCLIOptions.CREDENTIALS_OPTION));
    }
    if (cli.hasOption(IS_CLOUD_OPTION)) {
      ret +=
          assertSolrRunningInCloudMode(
              CLIUtils.normalizeSolrUrl(cli.getOptionValue(IS_CLOUD_OPTION)),
              cli.getOptionValue(CommonCLIOptions.CREDENTIALS_OPTION));
    }
    if (cli.hasOption(IS_NOT_CLOUD_OPTION)) {
      ret +=
          assertSolrNotRunningInCloudMode(
              CLIUtils.normalizeSolrUrl(cli.getOptionValue(IS_NOT_CLOUD_OPTION)),
              cli.getOptionValue(CommonCLIOptions.CREDENTIALS_OPTION));
    }
    return ret;
  }

  public static int assertSolrRunning(String url, String credentials) throws Exception {
    StatusTool status = new StatusTool();
    try {
      status.waitToSeeSolrUp(url, credentials, timeoutMs, TimeUnit.MILLISECONDS);
    } catch (Exception se) {
      if (CLIUtils.exceptionIsAuthRelated(se)) {
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

  public static int assertSolrNotRunning(String url, String credentials) throws Exception {
    StatusTool status = new StatusTool();
    long timeout =
        System.nanoTime() + TimeUnit.NANOSECONDS.convert(timeoutMs, TimeUnit.MILLISECONDS);
    try (SolrClient solrClient = CLIUtils.getSolrClient(url, credentials)) {
      NamedList<Object> response = solrClient.request(new HealthCheckRequest());
      Integer statusCode = (Integer) response.findRecursive("responseHeader", "status");
      CLIUtils.checkCodeForAuthError(statusCode);
    } catch (IOException | SolrServerException e) {
      log.debug("Opening connection to {} failed, Solr does not seem to be running", url, e);
      return 0;
    }
    while (System.nanoTime() < timeout) {
      try {
        status.waitToSeeSolrUp(url, credentials, 1, TimeUnit.SECONDS);
        try {
          log.debug("Solr still up. Waiting before trying again to see if it was stopped");
          Thread.sleep(1000L);
        } catch (InterruptedException interrupted) {
          timeout = 0; // stop looping
        }
      } catch (Exception se) {
        if (CLIUtils.exceptionIsAuthRelated(se)) {
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

  public static int assertSolrRunningInCloudMode(String url, String credentials) throws Exception {
    if (!isSolrRunningOn(url, credentials)) {
      return exitOrException(
          "Solr is not running on url "
              + url
              + " after "
              + TimeUnit.SECONDS.convert(timeoutMs, TimeUnit.MILLISECONDS)
              + " seconds");
    }

    if (!runningSolrIsCloud(url, credentials)) {
      return exitOrException("Solr is not running in cloud mode on " + url);
    }
    return 0;
  }

  public static int assertSolrNotRunningInCloudMode(String url, String credentials)
      throws Exception {
    if (!isSolrRunningOn(url, credentials)) {
      return exitOrException(
          "Solr is not running on url "
              + url
              + " after "
              + TimeUnit.SECONDS.convert(timeoutMs, TimeUnit.MILLISECONDS)
              + " seconds");
    }

    if (runningSolrIsCloud(url, credentials)) {
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

  private static int exitOrException(String msg) throws AssertionFailureException {
    if (useExitCode) {
      return 1;
    } else {
      throw new AssertionFailureException(message != null ? message : msg);
    }
  }

  private static boolean isSolrRunningOn(String url, String credentials) throws Exception {
    StatusTool status = new StatusTool();
    try {
      status.waitToSeeSolrUp(url, credentials, timeoutMs, TimeUnit.MILLISECONDS);
      return true;
    } catch (Exception se) {
      if (CLIUtils.exceptionIsAuthRelated(se)) {
        throw se;
      }
      return false;
    }
  }

  private static boolean runningSolrIsCloud(String url, String credentials) throws Exception {
    try (final SolrClient client = CLIUtils.getSolrClient(url, credentials)) {
      return CLIUtils.isCloudMode(client);
    }
  }

  public static class AssertionFailureException extends Exception {
    public AssertionFailureException(String message) {
      super(message);
    }
  }
}
