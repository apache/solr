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

package org.apache.solr.shell;

import com.carrotsearch.randomizedtesting.ThreadFilter;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import java.nio.file.Path;
import java.time.Duration;
import org.apache.lucene.tests.util.QuickPatchThreadsFilter;
import org.apache.solr.SolrIgnoredThreadsFilter;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

/**
 * Port of {@code test_extraction.bats}.
 *
 * <p>Kept close to the original, including the order of the tests and the text each asserts on.
 * Where it differs:
 *
 * <ul>
 *   <li>the Tika Server the extraction handler delegates to comes from {@link
 *       TikaServerContainerRule} rather than from {@code docker run} and a {@code curl} polling
 *       loop, and its host port is the one Docker mapped instead of {@code SOLR_PORT+5} — a port
 *       this class does not own and which would collide with another class running in parallel. The
 *       rule's assumption is the original's {@code DOCKER_UNAVAILABLE} bypass;
 *   <li>Solr is started once for the class rather than once per test; the three tests already use a
 *       collection name each;
 *   <li>{@code curl} becomes the harness' HTTP helpers: {@link #http} for the queries, {@code
 *       httpPost} for the Config API call and {@code httpPostFile} for {@code curl -F
 *       "myfile=@..."};
 *   <li>the helper's {@code sleep 5} becomes a wait on the thing it was hiding — the {@code
 *       /update/extract} handler being registered on the collection, which in cloud mode happens
 *       only once the core has picked up the config overlay the POST wrote;
 *   <li>{@code solr create} and the extraction POST assert their exit status and HTTP status, which
 *       the original ignored.
 * </ul>
 */
// Testcontainers' own threads outlive the container, as the extraction module's integration tests
// also have to declare. SolrTestCase's filters are not additive, so they are repeated here.
@ThreadLeakFilters(
    defaultFilters = true,
    filters = {
      SolrIgnoredThreadsFilter.class,
      QuickPatchThreadsFilter.class,
      ExtractionTest.TestcontainersThreadsFilter.class
    })
public class ExtractionTest extends SolrShellTestCase {

  @ClassRule public static final TikaServerContainerRule tikaServer = new TikaServerContainerRule();

  @BeforeClass
  public static void startSolr() {
    // BATS' setup_file, minus the Tika container the class rule above has already started.
    // Disable security manager to allow extraction. This appears to be a bug.
    classCli.env("SOLR_SECURITY_MANAGER_ENABLED", "false");
    classCli.run("solr start -Dsolr.modules=extraction");
  }

  @Test
  public void usingCurlToExtractASinglePdfFile() {
    run("solr create -c gettingstarted -d _default");
    awaitQueryable("gettingstarted");
    applyExtractHandler("gettingstarted");

    Path pdf = harnessEnv.solrTip().resolve("example/exampledocs/solr-word.pdf");
    cli.helpers()
        .httpPostFile(
            "/solr/gettingstarted/update/extract?literal.id=doc1&commit=true", "myfile", pdf)
        .assertStatus(200);

    http("/solr/gettingstarted/select?q=id:doc1").assertContains("\"numFound\":1");
  }

  @Test
  public void usingTheBinSolrPostToolToExtractContentFromPdf() {
    run("solr create -c content_extraction -d _default");
    awaitQueryable("content_extraction");
    applyExtractHandler("content_extraction");

    // We filter to pdf to invoke the Extract handler.
    run("solr post --filetypes pdf --solr-url http://localhost:${SOLR_PORT}"
            + " --name content_extraction \"${SOLR_TIP}/example/exampledocs\"")
        .assertContains("1 files indexed.")
        .assertNotContains("ERROR");

    http("/solr/content_extraction/select?q=*:*").assertContains("\"numFound\":1");
  }

  @Test
  public void usingTheBinSolrPostToolToCrawlWebSite() {
    run("solr create -c website_extraction -d _default");
    awaitQueryable("website_extraction");
    applyExtractHandler("website_extraction");

    // The original crawled https://solr.apache.org/ live. This serves a snapshot of that page
    // locally instead: the subject is the post tool's web mode, not the website, and depending on
    // the network and on whatever the site says today buys nothing. The server is stopped for us
    // when the test ends.
    copyResource("website/index.html", "website/index.html");
    int webPort = serveHttp("website");

    // Change to --recursive 1 to crawl multiple pages, but may be too slow.
    run("solr post --mode web --solr-url http://localhost:${SOLR_PORT} -c website_extraction"
            + " --recursive 0 --delay 1 http://localhost:"
            + webPort
            + "/")
        .assertContains("POSTed web resource http://localhost:" + webPort + " (depth: 0)")
        .assertNotContains("ERROR");

    http("/solr/website_extraction/select?q=*:*").assertContains("\"numFound\":1");
  }

  /** BATS' {@code wait_for ... curl .../select}: the collection is up and answering queries. */
  private void awaitQueryable(String collection) {
    awaitOrFail(
        "collection " + collection + " to answer a query",
        Duration.ofSeconds(30),
        Duration.ofSeconds(3),
        () -> http("/solr/" + collection + "/select?q=*:*").status() == 200);
  }

  /**
   * Port of the original's {@code apply_extract_handler}: adds the {@code ExtractingRequestHandler}
   * through the Config API, pointed at the Tika Server container.
   */
  private void applyExtractHandler(String collection) {
    String json =
        "{\"add-requesthandler\":{\"name\":\"/update/extract\","
            + "\"class\":\"org.apache.solr.handler.extraction.ExtractingRequestHandler\","
            + "\"tikaserver.url\":\""
            + tikaServer.getBaseUrl()
            + "\",\"defaults\":{\"lowernames\":\"true\",\"captureAttr\":\"true\"}}}";
    cli.helpers()
        .httpPost("/solr/" + collection + "/config", "application/json", json)
        .assertStatus(200);
    awaitOrFail(
        "the /update/extract handler to be registered on " + collection,
        Duration.ofSeconds(30),
        Duration.ofSeconds(1),
        () ->
            http("/solr/" + collection + "/config/requestHandler?componentName=/update/extract")
                .body()
                .contains("ExtractingRequestHandler"));
  }

  /** Ignores the non-daemon threads Testcontainers and the Java HTTP client leave behind. */
  @SuppressWarnings("NewClassNamingConvention")
  public static class TestcontainersThreadsFilter implements ThreadFilter {
    @Override
    public boolean reject(Thread t) {
      if (t == null || t.getName() == null) {
        return false;
      }
      String name = t.getName();
      return name.startsWith("testcontainers-") || name.startsWith("HttpClient-");
    }
  }
}
