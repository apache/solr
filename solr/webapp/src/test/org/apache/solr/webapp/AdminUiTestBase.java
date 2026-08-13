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
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.logging.Level;
import org.apache.lucene.tests.util.QuickPatchThreadsFilter;
import org.apache.lucene.util.SuppressForbidden;
import org.apache.solr.SolrIgnoredThreadsFilter;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.eclipse.jetty.ee10.servlet.ServletHolder;
import org.junit.AfterClass;
import org.junit.Assume;
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
 * <p>The tests require a locally installed Chrome/Chromium browser. Discovery order: the {@code
 * tests.ui.chrome.binary} system property, the {@code CHROME_BIN} environment variable, then a list
 * of well-known install locations. When no browser is found, all tests in the class are skipped via
 * {@link Assume}. The matching chromedriver is provisioned by Selenium Manager, which may download
 * it on first use (cached under {@code ~/.cache/selenium}); if that fails (e.g. offline), tests are
 * likewise skipped.
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
public abstract class AdminUiTestBase extends SolrCloudTestCase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  protected static final Duration WAIT_TIMEOUT = Duration.ofSeconds(15);

  protected static WebDriver driver;

  /** Base url of the first node, e.g. {@code http://127.0.0.1:PORT/solr} */
  protected static String baseUrl;

  /**
   * Serves a minimal stand-in for the generated js-client bundle ({@code libs/solr/index.js}),
   * which only exists inside the built webapp, not in the source tree tests serve from. The
   * AngularJS {@code CollectionsV2} service fails to instantiate without the {@code solrApi}
   * global, taking the whole Collections screen down with it. Only the small API surface the
   * AngularJS UI actually uses is stubbed.
   */
  public static class StubJsClientServlet extends HttpServlet {
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
      resp.setContentType("text/javascript");
      resp.getWriter()
          .write(
              "var solrApi = {\n"
                  + "  ApiClient: { instance: { basePath: '/api', defaultHeaders: {} } },\n"
                  + "  CollectionsApi: function() {\n"
                  + "    this.reloadCollection = function(name, callback) {\n"
                  + "      var xhr = new XMLHttpRequest();\n"
                  + "      xhr.open('POST', '/api/collections/' + name + '/reload');\n"
                  + "      xhr.setRequestHeader('Content-Type', 'application/json');\n"
                  + "      xhr.onload = function() { callback(null, null, {status: xhr.status}); };\n"
                  + "      xhr.onerror = function() { callback(new Error('reload failed'), null, {status: xhr.status}); };\n"
                  + "      xhr.send('{}');\n"
                  + "    };\n"
                  + "  }\n"
                  + "};\n");
    }
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
          // JDK-internal scheduler backing CompletableFuture timeouts, lives forever
          || name.equals("CompletableFutureDelayScheduler");
    }
  }

  @BeforeClass
  public static void startClusterAndBrowser() throws Exception {
    Path chrome = findChromeBinary();
    Assume.assumeTrue(
        "No Chrome/Chromium binary found (set -Dtests.ui.chrome.binary=...), skipping UI tests",
        chrome != null);

    // metrics are off by default in test clusters, but UI screens (e.g. Plugins) need them;
    // restored after the class by SolrTestCase's SystemPropertiesRestoreRule
    System.setProperty("metricsEnabled", "true");
    configureCluster(2)
        .withJettyConfig(
            jetty ->
                jetty
                    .enableAdminUi(true)
                    // exact-path mapping takes precedence over the static /libs/* servlet
                    .withServlet(
                        new ServletHolder(new StubJsClientServlet()), "/libs/solr/index.js"))
        .configure();
    baseUrl = cluster.getJettySolrRunner(0).getBaseUrl().toString();

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
      Assume.assumeNoException(
          "Could not start ChromeDriver (chromedriver missing and not downloadable?)", e);
    }
    driver.manage().timeouts().pageLoadTimeout(Duration.ofSeconds(30));
  }

  @AfterClass
  public static void stopBrowser() {
    if (driver != null) {
      try {
        driver.quit();
      } finally {
        driver = null;
        baseUrl = null;
      }
    }
  }

  /** Captures a screenshot and the page source when a test fails, for post-mortem debugging. */
  @Rule
  public final TestRule screenshotOnFailure =
      new TestWatcher() {
        @Override
        protected void failed(Throwable e, Description description) {
          if (driver == null) return;
          try {
            Path dir = createTempDir("ui-failure-" + description.getMethodName());
            byte[] png = ((TakesScreenshot) driver).getScreenshotAs(OutputType.BYTES);
            Files.write(dir.resolve("screenshot.png"), png);
            Files.writeString(dir.resolve("page.html"), driver.getPageSource());
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
  private static <T> T poll(
      By locator, java.util.function.Function<WebElement, T> condition, String description) {
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
      throws IOException, org.apache.solr.client.solrj.SolrServerException {
    try (SolrClient client = cluster.getJettySolrRunner(0).newClient()) {
      return client.request(new GenericSolrRequest(SolrRequest.METHOD.GET, path, params));
    }
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
  protected static void assertNoSevereConsoleErrors(String... allowedSubstrings) {
    List<LogEntry> entries = driver.manage().logs().get(LogType.BROWSER).getAll();
    List<LogEntry> severe =
        entries.stream()
            .filter(entry -> entry.getLevel().intValue() >= Level.SEVERE.intValue())
            .filter(
                entry ->
                    java.util.Arrays.stream(allowedSubstrings)
                        .noneMatch(allowed -> entry.getMessage().contains(allowed)))
            .filter(entry -> !entry.getMessage().contains("favicon.ico"))
            .toList();
    assertTrue("Severe browser console errors: " + severe, severe.isEmpty());
  }

  /** Locates a Chrome/Chromium binary, or returns null if none can be found. */
  @SuppressForbidden(reason = "Reading CHROME_BIN/PATH from the environment to locate a browser")
  protected static Path findChromeBinary() {
    String sysProp = System.getProperty("tests.ui.chrome.binary");
    if (sysProp != null) {
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
      for (String dir : pathEnv.split(java.io.File.pathSeparator)) {
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
