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
package org.apache.solr.webapp;

import com.carrotsearch.randomizedtesting.ThreadFilter;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakLingering;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.logging.Level;
import org.apache.lucene.tests.util.QuickPatchThreadsFilter;
import org.apache.lucene.util.SuppressForbidden;
import org.apache.solr.SolrIgnoredThreadsFilter;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.EnvUtils;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.embedded.JettyConfig;
import org.apache.solr.embedded.JettySolrRunner;
import org.apache.solr.util.ExternalPaths;
import org.apache.solr.util.SeleniumTest;
import org.eclipse.jetty.ee10.servlet.ServletHolder;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.openqa.selenium.By;
import org.openqa.selenium.NoSuchElementException;
import org.openqa.selenium.OutputType;
import org.openqa.selenium.StaleElementReferenceException;
import org.openqa.selenium.TakesScreenshot;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebDriverException;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.chrome.ChromeOptions;
import org.openqa.selenium.logging.LogEntry;
import org.openqa.selenium.logging.LogType;
import org.openqa.selenium.logging.LoggingPreferences;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for browser-based tests of the AngularJS Admin UI.
 *
 * <p>Starts a {@link SolrCloudTestCase} mini-cluster whose Jetty nodes also serve the Admin UI
 * static files (see {@code JettyConfig.Builder#enableAdminUi(boolean)}), then drives the UI with a
 * headless Chrome via Selenium WebDriver.
 *
 * <p>The tests are opt-in via {@code -Ptests.selenium=true} (see {@link SeleniumTest}) and require
 * a locally installed Chrome/Chromium browser. Discovery order: the {@code
 * tests.selenium.chrome.binary} system property, the {@code CHROME_BIN} environment variable, then
 * a list of well-known install locations. Since the tests only run when explicitly enabled, a
 * missing browser is a test failure, not a skip. The matching chromedriver is provisioned by
 * Selenium Manager, which may download it on first use (cached under {@code ~/.cache/selenium}).
 */
@SolrTestCaseJ4.SuppressSSL(bugUrl = "Admin UI browser tests drive plain http")
@ThreadLeakFilters(
    defaultFilters = true,
    filters = {
      SolrIgnoredThreadsFilter.class,
      QuickPatchThreadsFilter.class,
      AdminUiTestBase.WebDriverThreadsFilter.class
    })
@ThreadLeakLingering(linger = 5000)
@SeleniumTest
public abstract class AdminUiTestBase extends SolrCloudTestCase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /** Generous to accommodate loaded CI machines; successful waits return as soon as satisfied. */
  protected static final Duration WAIT_TIMEOUT = Duration.ofSeconds(30);

  protected static WebDriver driver;

  /** Base url of the first node, e.g. {@code http://127.0.0.1:PORT/solr} */
  protected static String baseUrl;

  /**
   * Optional security.json for the cluster. Subclasses assign this in their {@code @BeforeClass}
   * (which runs after this class's browser-starting one, but before the cluster starts lazily on
   * first use). Never assign it in a {@code static} block: test runners may load all test classes
   * up front, so static initializers of one class can run long before its suite executes.
   */
  protected static String securityJson;

  /**
   * When true (set by {@code AdminUiStandaloneTestBase}), no cloud cluster is started; the test
   * class starts its own standalone {@link JettySolrRunner}(s), assigns {@link #standaloneJetty}
   * and {@link #baseUrl}, and stops them again.
   */
  protected static boolean standaloneMode = false;

  /** The standalone node backing {@link #adminApi} when {@link #standaloneMode} is set. */
  protected static JettySolrRunner standaloneJetty;

  /**
   * Serves the generated js-client bundle the AngularJS UI expects at {@code libs/solr/index.js}:
   * its {@code CollectionsV2} service fails to instantiate without the {@code solrApi} global,
   * taking the whole Collections screen down with it. The bundle is built by {@code
   * :solr:webapp:js-client} and its location handed to the test JVM in {@code
   * tests.ui.jsclient.bundle}; it only exists inside the built webapp, not in the source tree tests
   * serve from.
   */
  public static class JsClientServlet extends HttpServlet {
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
      resp.setContentType("text/javascript");
      Files.copy(jsClientBundlePath(), resp.getOutputStream());
    }
  }

  /**
   * The generated js-client bundle: handed to us by the build in {@code tests.ui.jsclient.bundle},
   * with the js-client build's output location as a fallback so tests can also run from an IDE
   * (after a Gradle build has produced the bundle). Null when unavailable.
   */
  private static Path jsClientBundlePath() {
    String path = EnvUtils.getProperty("tests.ui.jsclient.bundle");
    if (path == null && ExternalPaths.SOURCE_HOME == null) {
      return null;
    }
    Path bundle =
        path != null
            ? Path.of(path)
            : ExternalPaths.SOURCE_HOME.resolve("webapp/js-client/build/jsClientBundle/index.js");
    return Files.isReadable(bundle) ? bundle : null;
  }

  /** Ignores threads spawned by Selenium and the JDK http client it uses. */
  public static class WebDriverThreadsFilter implements ThreadFilter {
    @Override
    public boolean reject(Thread t) {
      String name = t.getName();
      // JDK java.net.http client worker/selector threads (used by Selenium) are daemon
      // threads in a shared pool that outlive WebDriver.quit()
      return name.startsWith("HttpClient-")
          // reaps the external chromedriver/chrome processes
          || name.startsWith("process reaper")
          // selenium driver-service startup checker pool, terminates on its own
          || name.startsWith("UrlChecker-")
          // selenium's chromedriver stdout/stderr pump, stops when the process exits
          || name.startsWith("External Process Output Forwarder")
          // JDK-internal scheduler backing CompletableFuture timeouts, lives forever
          || name.equals("CompletableFutureDelayScheduler");
    }
  }

  @BeforeClass
  @SuppressForbidden(reason = "Selenium's logging preferences API uses java.util.logging levels")
  public static void startClusterAndBrowser() throws Exception {
    Path chrome = findChromeBinary();
    if (chrome == null) {
      fail(
          "Selenium tests are enabled (-Ptests.selenium=true) but no Chrome/Chromium binary was"
              + " found; install one or point -Dtests.selenium.chrome.binary at it");
    }
    if (jsClientBundlePath() == null) {
      fail(
          "No generated js-client bundle available; the Gradle build wires it via"
              + " tests.ui.jsclient.bundle (is -PdisableJsClient=true set?)");
    }

    // metrics are off by default in test clusters, but UI screens (e.g. Plugins) need them;
    // restored after the class by SolrTestCase's SystemPropertiesRestoreRule
    System.setProperty("metricsEnabled", "true");
    // the cluster starts lazily via ensureCloudCluster(), after subclass @BeforeClass
    // methods have had the chance to configure securityJson or standalone mode

    ChromeOptions options = new ChromeOptions();
    options.setBinary(chrome.toString());
    options.addArguments(
        "--headless=new",
        "--window-size=1440,1024",
        "--disable-gpu",
        "--no-sandbox",
        "--disable-dev-shm-usage");
    LoggingPreferences logPrefs = new LoggingPreferences();
    logPrefs.enable(LogType.BROWSER, Level.ALL);
    options.setCapability("goog:loggingPrefs", logPrefs);
    try {
      driver = new ChromeDriver(options);
    } catch (WebDriverException e) {
      throw new AssertionError(
          "Selenium tests are enabled (-Ptests.selenium=true) but ChromeDriver could not start"
              + " (chromedriver missing and not downloadable?)",
          e);
    }
    driver.manage().timeouts().pageLoadTimeout(Duration.ofSeconds(30));
  }

  /** Starts the 2-node cloud cluster serving the UI, unless already started. */
  protected static void ensureCloudCluster() {
    if (standaloneMode || cluster != null) {
      return;
    }
    try {
      var clusterBuilder =
          configureCluster(2).withJettyConfig(AdminUiTestBase::configureJettyForUi);
      if (securityJson != null) {
        clusterBuilder.withSecurityJson(securityJson);
      }
      clusterBuilder.configure();
      baseUrl = cluster.getJettySolrRunner(0).getBaseUrl().toString();
    } catch (Exception e) {
      throw new RuntimeException("Could not start UI test cluster", e);
    }
  }

  /** Configures a Jetty node to serve the Admin UI plus the js-client bundle. */
  protected static void configureJettyForUi(JettyConfig.Builder jetty) {
    jetty
        .enableAdminUi(true)
        // exact-path mapping takes precedence over the static /libs/* servlet
        .withServlet(new ServletHolder(new JsClientServlet()), "/libs/solr/index.js");
  }

  @AfterClass
  public static void stopBrowser() {
    // reset the static per-class configuration: several test classes run in the same
    // JVM, and flags set by one class's static initializer must not leak into the next
    standaloneMode = false;
    standaloneJetty = null;
    securityJson = null;
    if (driver != null) {
      try {
        driver.quit();
      } finally {
        driver = null;
        baseUrl = null;
      }
    }
  }

  /**
   * Browser console entries drained from the driver so far in the current test. Fetching the log
   * from ChromeDriver empties its buffer, so {@link #assertNoSevereConsoleErrors} accumulates what
   * it consumed here for the failure watcher to include in the {@code console.log} artifact.
   */
  private static final List<LogEntry> consoleEntries = new ArrayList<>();

  /** Captures a screenshot, page source and console log when a test fails, for post-mortem. */
  @Rule
  public final TestRule failureArtifacts =
      new TestWatcher() {
        @Override
        protected void starting(Description description) {
          consoleEntries.clear();
        }

        @Override
        protected void failed(Throwable e, Description description) {
          if (driver == null) return;
          try {
            Path dir = createTempDir("ui-failure-" + description.getMethodName());
            byte[] png = ((TakesScreenshot) driver).getScreenshotAs(OutputType.BYTES);
            Files.write(dir.resolve("screenshot.png"), png);
            Files.writeString(dir.resolve("page.html"), driver.getPageSource());
            consoleEntries.addAll(driver.manage().logs().get(LogType.BROWSER).getAll());
            StringBuilder console = new StringBuilder();
            for (LogEntry entry : consoleEntries) {
              console.append(entry.getLevel()).append(' ').append(entry.getMessage()).append('\n');
            }
            Files.writeString(dir.resolve("console.log"), console.toString());
            log.error("UI test failure artifacts saved to {}", dir);
          } catch (Exception suppressed) {
            log.warn("Could not save UI failure artifacts", suppressed);
          }
        }
      };

  /**
   * Navigates to an Admin UI page and waits for a screen-specific anchor element to be visible.
   *
   * @param route the Angular hash route without leading {@code #/}, e.g. {@code ""} (dashboard),
   *     {@code "~cloud"} or {@code "collection1/query"}
   * @param anchor a locator for an element that indicates the screen has rendered
   * @return the anchor element
   */
  protected static WebElement openPage(String route, By anchor) {
    ensureCloudCluster();
    // Boot a fresh app instance first (a URL differing only in the #-fragment does not
    // reload the page, so the previous test's app and its in-flight callbacks would
    // otherwise survive into this test), then navigate in-app via a hash change like a
    // user would: booting the app directly on a deep link races its initial data
    // loading, intermittently leaving the target screen unresolved.
    boolean samePage = String.valueOf(driver.getCurrentUrl()).startsWith(baseUrl + "/index.html");
    driver.get(baseUrl + "/index.html#/");
    if (samePage) {
      driver.navigate().refresh();
    }
    // ng-view (#content) is empty until the app has booted and rendered its first route
    waitFor(By.cssSelector("#content > *"));
    driver.get(baseUrl + "/index.html#/" + route);
    return waitFor(anchor);
  }

  /** Waits for the given element to be visible, up to {@link #WAIT_TIMEOUT}. */
  protected static WebElement waitFor(By locator) {
    return poll(locator, el -> el.isDisplayed() ? el : null, "visible element");
  }

  /** Waits until the given element has non-blank text, and returns the text. */
  protected static String waitForText(By locator) {
    return poll(
        locator,
        el -> {
          String text = el.getText();
          return el.isDisplayed() && !text.isBlank() ? text : null;
        },
        "non-empty text");
  }

  /**
   * Polls the given element until {@code condition} returns non-null (a fresh lookup each round, so
   * elements replaced by Angular re-renders are tolerated), failing after {@link #WAIT_TIMEOUT}.
   */
  private static <T> T poll(By locator, Function<WebElement, T> condition, String description) {
    long deadlineNanos = System.nanoTime() + WAIT_TIMEOUT.toNanos();
    WebDriverException lastException = null;
    while (System.nanoTime() < deadlineNanos) {
      try {
        T result = condition.apply(driver.findElement(locator));
        if (result != null) {
          return result;
        }
        lastException = null;
      } catch (NoSuchElementException | StaleElementReferenceException e) {
        lastException = e;
      }
      try {
        Thread.sleep(200);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
    }
    throw new AssertionError(
        "Timed out waiting for " + description + " at " + locator, lastException);
  }

  /**
   * Issues a GET request to the given admin path (e.g. {@code /admin/info/system}) on the same node
   * the browser talks to, and returns the parsed response. Used to fetch the expected values that
   * the UI should display.
   */
  protected static NamedList<Object> adminApi(String path, SolrParams params)
      throws IOException, SolrServerException {
    ensureCloudCluster();
    JettySolrRunner jetty = standaloneMode ? standaloneJetty : cluster.getJettySolrRunner(0);
    try (SolrClient client = jetty.newClient()) {
      return client.request(new GenericSolrRequest(SolrRequest.METHOD.GET, path, params));
    }
  }

  /**
   * Uploads the default configset under the collection's name and creates the collection. A
   * single-replica collection is pinned to the node the browser talks to, so core-level screens
   * find its core locally.
   */
  protected static void createFixtureCollection(String name, int numShards, int numReplicas)
      throws Exception {
    ensureCloudCluster();
    cluster.uploadConfigSet(ExternalPaths.DEFAULT_CONFIGSET, name);
    CollectionAdminRequest.Create create =
        CollectionAdminRequest.createCollection(name, name, numShards, numReplicas);
    if (numShards * numReplicas == 1) {
      create.setCreateNodeSet(cluster.getJettySolrRunner(0).getNodeName());
    }
    create.process(cluster.getSolrClient());
    cluster.waitForActiveCollection(name, numShards, numShards * numReplicas);
  }

  /** Polls the condition until it holds, failing after {@link #WAIT_TIMEOUT}. */
  protected static void waitUntil(String description, BooleanSupplier condition)
      throws InterruptedException {
    long deadlineNanos = System.nanoTime() + WAIT_TIMEOUT.toNanos();
    while (System.nanoTime() < deadlineNanos) {
      if (condition.getAsBoolean()) {
        return;
      }
      Thread.sleep(250);
    }
    fail("Timed out waiting until " + description);
  }

  /** Returns the name of a core of the given collection hosted on node 0. */
  protected static String coreNameOnNode0(String collection) {
    for (String name : cluster.getJettySolrRunner(0).getCoreContainer().getAllCoreNames()) {
      if (name.startsWith(collection + "_")) {
        return name;
      }
    }
    throw new AssertionError("No core found on node 0 for collection " + collection);
  }

  /**
   * Clicks the element at the locator once visible, retrying when the page re-renders the element
   * between lookup and click (StaleElementReferenceException) — e.g. the jstree in the logging
   * screen re-renders its anchors right after a change.
   */
  protected static void click(By locator) {
    poll(
        locator,
        el -> {
          if (!el.isDisplayed()) {
            return null;
          }
          el.click();
          return Boolean.TRUE;
        },
        "clicking");
  }

  /**
   * Clears the input at the locator and types the given text, retrying when Angular re-renders the
   * element mid-interaction (StaleElementReferenceException).
   */
  protected static void setText(By locator, String text) {
    poll(
        locator,
        el -> {
          if (!el.isDisplayed()) {
            return null;
          }
          el.clear();
          el.sendKeys(text);
          return Boolean.TRUE;
        },
        "typing '" + text + "'");
  }

  /** Waits until the element's rendered text contains the given substring, and returns it. */
  protected static String waitForTextContains(By locator, String substring) {
    return poll(
        locator,
        el -> {
          String text = el.getText();
          return text.contains(substring) ? text : null;
        },
        "text containing '" + substring + "'");
  }

  /** Waits until the page source contains the given text. */
  protected static void waitForPageContains(String text) {
    long deadlineNanos = System.nanoTime() + WAIT_TIMEOUT.toNanos();
    while (System.nanoTime() < deadlineNanos) {
      if (driver.getPageSource().contains(text)) {
        return;
      }
      try {
        Thread.sleep(200);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
    }
    throw new AssertionError("Timed out waiting for page to contain: " + text);
  }

  /**
   * Selects an option in a "chosen"-decorated select element. The original select is hidden by the
   * widget, so this drives the generated container instead.
   */
  protected static void chosenSelect(String selectId, String optionText) {
    WebElement container = waitFor(By.id(selectId + "_chosen"));
    container.click();
    long deadlineNanos = System.nanoTime() + WAIT_TIMEOUT.toNanos();
    while (System.nanoTime() < deadlineNanos) {
      for (WebElement option :
          container.findElements(By.cssSelector(".chosen-results li.active-result"))) {
        if (optionText.equals(option.getText())) {
          option.click();
          return;
        }
      }
      try {
        Thread.sleep(200);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
    }
    throw new AssertionError("Option '" + optionText + "' not found in select " + selectId);
  }

  /**
   * Fails the test if the browser console contains SEVERE errors, ignoring known-benign ones and
   * any messages containing one of {@code allowedSubstrings}.
   */
  @SuppressForbidden(reason = "Selenium's log API reports java.util.logging levels")
  protected static void assertNoSevereConsoleErrors(String... allowedSubstrings) {
    List<LogEntry> entries = driver.manage().logs().get(LogType.BROWSER).getAll();
    consoleEntries.addAll(entries);
    List<LogEntry> severe =
        entries.stream()
            .filter(entry -> entry.getLevel().intValue() >= Level.SEVERE.intValue())
            .filter(
                entry ->
                    Arrays.stream(allowedSubstrings)
                        .noneMatch(allowed -> entry.getMessage().contains(allowed)))
            .filter(entry -> !entry.getMessage().contains("favicon.ico"))
            // benign race in the shared menu code: showCore() fires with a null core
            // while the per-collection menu resolves after navigation
            .filter(
                entry ->
                    !(entry.getMessage().contains("reading 'name'")
                        && entry.getMessage().contains("showCore")))
            .toList();
    assertTrue("Severe browser console errors: " + severe, severe.isEmpty());
  }

  /** Locates a Chrome/Chromium binary, or returns null if none can be found. */
  @SuppressForbidden(reason = "Reading CHROME_BIN/PATH from the environment to locate a browser")
  protected static Path findChromeBinary() {
    String sysProp = EnvUtils.getProperty("tests.selenium.chrome.binary");
    if (sysProp != null && !sysProp.isBlank()) {
      Path path = Path.of(sysProp);
      return Files.isExecutable(path) ? path : null;
    }
    String envBin = System.getenv("CHROME_BIN");
    if (envBin != null && Files.isExecutable(Path.of(envBin))) {
      return Path.of(envBin);
    }
    List<String> wellKnown =
        List.of(
            "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
            "/Applications/Chromium.app/Contents/MacOS/Chromium",
            "/usr/bin/google-chrome",
            "/usr/bin/google-chrome-stable",
            "/usr/bin/chromium",
            "/usr/bin/chromium-browser",
            "/snap/bin/chromium",
            "C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe",
            "C:\\Program Files (x86)\\Google\\Chrome\\Application\\chrome.exe");
    for (String candidate : wellKnown) {
      Path path = Path.of(candidate);
      if (Files.isExecutable(path)) {
        return path;
      }
    }
    String pathEnv = System.getenv("PATH");
    if (pathEnv != null) {
      for (String dir : pathEnv.split(File.pathSeparator)) {
        for (String name : List.of("google-chrome", "chromium", "chromium-browser")) {
          Path path = Path.of(dir, name);
          if (Files.isExecutable(path)) {
            return path;
          }
        }
      }
    }
    return null;
  }
}
