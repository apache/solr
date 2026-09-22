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
import java.lang.invoke.MethodHandles;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileOwnerAttributeView;
import java.util.ArrayList;
import java.util.List;
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
@SuppressWarnings("UnnecessarilyFullyQualified")
@picocli.CommandLine.Command(
    name = "assert",
    description =
        "Asserts various conditions and exits with an error code if there are failures, else"
            + " continues with no output.",
    footerHeading = "%nExamples:%n",
    footer = {
      "  # Assert Solr is running before continuing",
      "  bin/solr assert --started http://localhost:8983 --timeout 5000",
      "",
      "  # Assert we are not running as root",
      "  bin/solr assert --not-root"
    })
public class AssertTool extends ToolBase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private String message = null;
  private boolean useExitCode = false;
  private Long timeoutMs = 1000L;

  private static final Option IS_NOT_ROOT_OPTION =
      Option.builder().desc("Asserts that we are NOT the root user.").longOpt("not-root").get();

  private static final Option IS_ROOT_OPTION =
      Option.builder().desc("Asserts that we are the root user.").longOpt("root").get();

  private static final OptionGroup ROOT_OPTION =
      new OptionGroup().addOption(IS_NOT_ROOT_OPTION).addOption(IS_ROOT_OPTION);

  private static final Option IS_NOT_RUNNING_ON_OPTION =
      Option.builder()
          .desc("Asserts that Solr is NOT running on a certain URL. Default timeout is 1000ms.")
          .longOpt("not-started")
          .hasArg()
          .argName("url")
          .get();

  private static final Option IS_RUNNING_ON_OPTION =
      Option.builder()
          .desc("Asserts that Solr is running on a certain URL. Default timeout is 1000ms.")
          .longOpt("started")
          .hasArg()
          .argName("url")
          .get();

  private static final OptionGroup RUNNING_OPTION =
      new OptionGroup().addOption(IS_NOT_RUNNING_ON_OPTION).addOption(IS_RUNNING_ON_OPTION);

  private static final Option SAME_USER_OPTION =
      Option.builder()
          .desc("Asserts that we run as same user that owns <directory>.")
          .longOpt("same-user")
          .hasArg()
          .argName("directory")
          .get();

  private static final Option DIRECTORY_EXISTS_OPTION =
      Option.builder()
          .desc("Asserts that directory <directory> exists.")
          .longOpt("exists")
          .hasArg()
          .argName("directory")
          .get();

  private static final Option DIRECTORY_NOT_EXISTS_OPTION =
      Option.builder()
          .desc("Asserts that directory <directory> does NOT exist.")
          .longOpt("not-exists")
          .hasArg()
          .argName("directory")
          .get();

  private static final OptionGroup DIRECTORY_OPTION =
      new OptionGroup().addOption(DIRECTORY_EXISTS_OPTION).addOption(DIRECTORY_NOT_EXISTS_OPTION);

  private static final Option IS_CLOUD_OPTION =
      Option.builder()
          .desc(
              "Asserts that Solr is running in cloud mode.  Also fails if Solr not running.  URL should be for root Solr path.")
          .longOpt("cloud")
          .hasArg()
          .argName("url")
          .get();

  private static final Option IS_NOT_CLOUD_OPTION =
      Option.builder()
          .desc(
              "Asserts that Solr is not running in cloud mode.  Also fails if Solr not running.  URL should be for root Solr path.")
          .longOpt("not-cloud")
          .hasArg()
          .argName("url")
          .get();

  private static final OptionGroup CLOUD_OPTION =
      new OptionGroup().addOption(IS_CLOUD_OPTION).addOption(IS_NOT_CLOUD_OPTION);

  private static final Option MESSAGE_OPTION =
      Option.builder()
          .desc("Exception message to be used in place of the default error message.")
          .longOpt("message")
          .hasArg()
          .argName("message")
          .get();

  private static final Option TIMEOUT_OPTION =
      Option.builder()
          .desc("Timeout in ms for commands supporting a timeout.")
          .longOpt("timeout")
          .hasArg()
          .type(Long.class)
          .argName("ms")
          .get();

  private static final Option EXIT_CODE_OPTION =
      Option.builder()
          .desc("Return an exit code instead of printing error message on assert fail.")
          .longOpt("exitcode")
          .get();

  /** One requested assertion. Multiple assertions may be requested in a single invocation. */
  sealed interface Assertion {
    /** Asserts that we are the root user. */
    record RootUser() implements Assertion {}

    /** Asserts that we are NOT the root user. */
    record NotRootUser() implements Assertion {}

    /** Asserts that the directory exists. */
    record DirExists(String dir) implements Assertion {}

    /** Asserts that the directory does NOT exist. */
    record DirNotExists(String dir) implements Assertion {}

    /** Asserts that we run as the same user that owns the directory. */
    record SameUser(String dir) implements Assertion {}

    /** Asserts that Solr is running on the given URL. */
    record SolrRunning(String url) implements Assertion {}

    /** Asserts that Solr is NOT running on the given URL. */
    record SolrNotRunning(String url) implements Assertion {}

    /** Asserts that Solr on the given URL is running in cloud mode. */
    record CloudMode(String url) implements Assertion {}

    /** Asserts that Solr on the given URL is NOT running in cloud mode. */
    record NotCloudMode(String url) implements Assertion {}
  }

  /**
   * Parameters for the assert command, independent of the command line parser. URL values are the
   * raw user input; they are normalized when the assertion runs.
   *
   * @param credentials credentials used by the URL-based assertions, or null
   * @param assertions assertions to run, in order
   */
  record AssertParams(
      String message,
      Long timeoutMs,
      boolean useExitCode,
      String credentials,
      List<Assertion> assertions) {}

  // --- picocli fields ---

  static class RootOptions {
    @picocli.CommandLine.Option(
        names = "--root",
        description = "Asserts that we are the root user.")
    boolean isRoot;

    @picocli.CommandLine.Option(
        names = "--not-root",
        description = "Asserts that we are NOT the root user.")
    boolean isNotRoot;
  }

  static class RunningOptions {
    @picocli.CommandLine.Option(
        names = "--started",
        paramLabel = "url",
        description = "Asserts that Solr is running on a certain URL. Default timeout is 1000ms.")
    String startedUrl;

    @picocli.CommandLine.Option(
        names = "--not-started",
        paramLabel = "url",
        description =
            "Asserts that Solr is NOT running on a certain URL. Default timeout is 1000ms.")
    String notStartedUrl;
  }

  static class DirectoryOptions {
    @picocli.CommandLine.Option(
        names = "--exists",
        paramLabel = "directory",
        description = "Asserts that directory <directory> exists.")
    String existsDir;

    @picocli.CommandLine.Option(
        names = "--not-exists",
        paramLabel = "directory",
        description = "Asserts that directory <directory> does NOT exist.")
    String notExistsDir;
  }

  static class CloudOptions {
    @picocli.CommandLine.Option(
        names = "--cloud",
        paramLabel = "url",
        description =
            "Asserts that Solr is running in cloud mode.  Also fails if Solr not running.  URL"
                + " should be for root Solr path.")
    String cloudUrl;

    @picocli.CommandLine.Option(
        names = "--not-cloud",
        paramLabel = "url",
        description =
            "Asserts that Solr is not running in cloud mode.  Also fails if Solr not running. "
                + " URL should be for root Solr path.")
    String notCloudUrl;
  }

  @picocli.CommandLine.ArgGroup(exclusive = true, multiplicity = "0..1")
  private RootOptions rootOptions;

  @picocli.CommandLine.ArgGroup(exclusive = true, multiplicity = "0..1")
  private RunningOptions runningOptions;

  @picocli.CommandLine.Option(
      names = "--same-user",
      paramLabel = "directory",
      description = "Asserts that we run as same user that owns <directory>.")
  private String sameUserOpt;

  @picocli.CommandLine.ArgGroup(exclusive = true, multiplicity = "0..1")
  private DirectoryOptions directoryOptions;

  @picocli.CommandLine.ArgGroup(exclusive = true, multiplicity = "0..1")
  private CloudOptions cloudOptions;

  @picocli.CommandLine.Option(
      names = "--message",
      paramLabel = "message",
      description = "Exception message to be used in place of the default error message.")
  private String messageOpt;

  @picocli.CommandLine.Option(
      names = "--timeout",
      paramLabel = "ms",
      defaultValue = "1000",
      description = "Timeout in ms for commands supporting a timeout.")
  private long timeoutOpt;

  @picocli.CommandLine.Option(
      names = "--exitcode",
      description = "Return an exit code instead of printing error message on assert fail.")
  private boolean exitCodeOpt;

  @picocli.CommandLine.Mixin private CredentialsOptions credentialsOptions;

  public AssertTool() {
    this(new DefaultToolRuntime());
  }

  public AssertTool(ToolRuntime runtime) {
    super(runtime);
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
    List<Assertion> assertions = new ArrayList<>();
    if (cli.hasOption(IS_ROOT_OPTION)) {
      assertions.add(new Assertion.RootUser());
    }
    if (cli.hasOption(IS_NOT_ROOT_OPTION)) {
      assertions.add(new Assertion.NotRootUser());
    }
    if (cli.hasOption(DIRECTORY_EXISTS_OPTION)) {
      assertions.add(new Assertion.DirExists(cli.getOptionValue(DIRECTORY_EXISTS_OPTION)));
    }
    if (cli.hasOption(DIRECTORY_NOT_EXISTS_OPTION)) {
      assertions.add(new Assertion.DirNotExists(cli.getOptionValue(DIRECTORY_NOT_EXISTS_OPTION)));
    }
    if (cli.hasOption(SAME_USER_OPTION)) {
      assertions.add(new Assertion.SameUser(cli.getOptionValue(SAME_USER_OPTION)));
    }
    if (cli.hasOption(IS_RUNNING_ON_OPTION)) {
      assertions.add(new Assertion.SolrRunning(cli.getOptionValue(IS_RUNNING_ON_OPTION)));
    }
    if (cli.hasOption(IS_NOT_RUNNING_ON_OPTION)) {
      assertions.add(new Assertion.SolrNotRunning(cli.getOptionValue(IS_NOT_RUNNING_ON_OPTION)));
    }
    if (cli.hasOption(IS_CLOUD_OPTION)) {
      assertions.add(new Assertion.CloudMode(cli.getOptionValue(IS_CLOUD_OPTION)));
    }
    if (cli.hasOption(IS_NOT_CLOUD_OPTION)) {
      assertions.add(new Assertion.NotCloudMode(cli.getOptionValue(IS_NOT_CLOUD_OPTION)));
    }
    return runAssert(
        new AssertParams(
            cli.getOptionValue(MESSAGE_OPTION),
            cli.getParsedOptionValue(TIMEOUT_OPTION, timeoutMs),
            cli.hasOption(EXIT_CODE_OPTION),
            cli.getOptionValue(CommonCLIOptions.CREDENTIALS_OPTION),
            List.copyOf(assertions)));
  }

  /**
   * Runs the requested assertions.
   *
   * @return 0 on success, or the number of assertions that failed
   * @throws Exception if an assertion failed and exit codes are not used, e.g. authentication
   *     failure
   */
  int runAssert(AssertParams params) throws Exception {
    message = params.message();
    timeoutMs = params.timeoutMs();
    useExitCode = params.useExitCode();
    String credentials = params.credentials();

    int ret = 0;
    for (Assertion assertion : params.assertions()) {
      ret +=
          switch (assertion) {
            case Assertion.RootUser() -> assertRootUser();
            case Assertion.NotRootUser() -> assertNotRootUser();
            case Assertion.DirExists(String dir) -> assertFileExists(dir);
            case Assertion.DirNotExists(String dir) -> assertFileNotExists(dir);
            case Assertion.SameUser(String dir) -> sameUser(dir);
            case Assertion.SolrRunning(String url) -> assertSolrRunning(url, credentials);
            case Assertion.SolrNotRunning(String url) -> assertSolrNotRunning(url, credentials);
            case Assertion.CloudMode(String url) ->
                assertSolrRunningInCloudMode(CLIUtils.normalizeSolrUrl(url), credentials);
            case Assertion.NotCloudMode(String url) ->
                assertSolrNotRunningInCloudMode(CLIUtils.normalizeSolrUrl(url), credentials);
          };
    }
    return ret;
  }

  public int assertSolrRunning(String url, String credentials) throws Exception {
    StatusTool status = new StatusTool(runtime);
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

  public int assertSolrNotRunning(String url, String credentials) throws Exception {
    StatusTool status = new StatusTool(runtime);
    long timeout =
        System.nanoTime() + TimeUnit.NANOSECONDS.convert(timeoutMs, TimeUnit.MILLISECONDS);
    try (SolrClient solrClient = CLIUtils.getSolrClient(url, credentials)) {
      NamedList<Object> response = solrClient.request(new HealthCheckRequest());
      Integer statusCode = (Integer) response._get(List.of("responseHeader", "status"), null);
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
          TimeUnit.MILLISECONDS.sleep(1000L);
        } catch (InterruptedException interrupted) {
          Thread.currentThread().interrupt();
          break;
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

  public int assertSolrRunningInCloudMode(String url, String credentials) throws Exception {
    if (isSolrStoppedOn(url, credentials)) {
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

  public int assertSolrNotRunningInCloudMode(String url, String credentials) throws Exception {
    if (isSolrStoppedOn(url, credentials)) {
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

  public int sameUser(String directory) throws Exception {
    Path path = Path.of(directory);
    if (Files.exists(path)) {
      String userForDir = userForDir(path);
      if (!currentUser().equals(userForDir)) {
        return exitOrException("Must run as user " + userForDir + ". We are " + currentUser());
      }
    } else {
      return exitOrException("Directory " + directory + " does not exist.");
    }
    return 0;
  }

  public int assertFileExists(String directory) throws Exception {
    if (!Files.exists(Path.of(directory))) {
      return exitOrException("Directory " + directory + " does not exist.");
    }
    return 0;
  }

  public int assertFileNotExists(String directory) throws Exception {
    if (Files.exists(Path.of(directory))) {
      return exitOrException("Directory " + directory + " should not exist.");
    }
    return 0;
  }

  public int assertRootUser() throws Exception {
    if (!currentUser().equals("root")) {
      return exitOrException("Must run as root user");
    }
    return 0;
  }

  public int assertNotRootUser() throws Exception {
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

  private int exitOrException(String msg) throws AssertionFailureException {
    if (useExitCode) {
      return 1;
    } else {
      throw new AssertionFailureException(message != null ? message : msg);
    }
  }

  private boolean isSolrStoppedOn(String url, String credentials) throws Exception {
    StatusTool status = new StatusTool(runtime);
    try {
      status.waitToSeeSolrUp(url, credentials, timeoutMs, TimeUnit.MILLISECONDS);
      return false;
    } catch (Exception se) {
      if (CLIUtils.exceptionIsAuthRelated(se)) {
        throw se;
      }
      return true;
    }
  }

  private static boolean runningSolrIsCloud(String url, String credentials) throws Exception {
    try (final SolrClient client = CLIUtils.getSolrClient(url, credentials)) {
      return CLIUtils.isCloudMode(client);
    }
  }

  @Override
  public int callTool() throws Exception {
    List<Assertion> assertions = new ArrayList<>();
    if (rootOptions != null && rootOptions.isRoot) {
      assertions.add(new Assertion.RootUser());
    }
    if (rootOptions != null && rootOptions.isNotRoot) {
      assertions.add(new Assertion.NotRootUser());
    }
    if (directoryOptions != null && directoryOptions.existsDir != null) {
      assertions.add(new Assertion.DirExists(directoryOptions.existsDir));
    }
    if (directoryOptions != null && directoryOptions.notExistsDir != null) {
      assertions.add(new Assertion.DirNotExists(directoryOptions.notExistsDir));
    }
    if (sameUserOpt != null) {
      assertions.add(new Assertion.SameUser(sameUserOpt));
    }
    if (runningOptions != null && runningOptions.startedUrl != null) {
      assertions.add(new Assertion.SolrRunning(runningOptions.startedUrl));
    }
    if (runningOptions != null && runningOptions.notStartedUrl != null) {
      assertions.add(new Assertion.SolrNotRunning(runningOptions.notStartedUrl));
    }
    if (cloudOptions != null && cloudOptions.cloudUrl != null) {
      assertions.add(new Assertion.CloudMode(cloudOptions.cloudUrl));
    }
    if (cloudOptions != null && cloudOptions.notCloudUrl != null) {
      assertions.add(new Assertion.NotCloudMode(cloudOptions.notCloudUrl));
    }

    try {
      return runAssert(
          new AssertParams(
              messageOpt,
              timeoutOpt,
              exitCodeOpt,
              credentialsOptions.credentials,
              List.copyOf(assertions)));
    } catch (Exception exc) {
      // Mirrors the commons-cli path's runTool() override: an assertion failure or other error
      // with a message becomes exit code 100, not the ToolBase default of 1.
      String excMsg = exc.getMessage();
      if (excMsg == null) {
        throw exc;
      }
      if (isVerbose()) {
        CLIO.err("\nERROR: " + exc + "\n");
      } else {
        CLIO.err("\nERROR: " + excMsg + "\n");
      }
      return 100;
    }
  }

  public static class AssertionFailureException extends Exception {
    public AssertionFailureException(String message) {
      super(message);
    }
  }
}
