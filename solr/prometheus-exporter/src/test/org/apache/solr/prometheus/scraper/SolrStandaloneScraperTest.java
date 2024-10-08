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

package org.apache.solr.prometheus.scraper;

import io.prometheus.client.Collector;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.apache.solr.prometheus.PrometheusExporterTestBase;
import org.apache.solr.prometheus.collector.MetricSamples;
import org.apache.solr.prometheus.exporter.MetricsConfiguration;
import org.apache.solr.prometheus.exporter.PrometheusExporterSettings;
import org.apache.solr.prometheus.exporter.SolrClientFactory;
import org.apache.solr.prometheus.exporter.SolrScrapeConfiguration;
import org.apache.solr.prometheus.utils.Helpers;
import org.apache.solr.util.SolrJettyTestRule;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

public class SolrStandaloneScraperTest extends SolrTestCaseJ4 {

  @ClassRule public static final SolrJettyTestRule solrRule = new SolrJettyTestRule();

  private static MetricsConfiguration configuration;
  private static SolrStandaloneScraper solrScraper;
  private static ExecutorService executor;
  private static Http2SolrClient solrClient;

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    solrRule.startSolr(LuceneTestCase.createTempDir());

    Path configSet = LuceneTestCase.createTempDir();
    createConf(configSet);
    solrRule.newCollection().withConfigSet(configSet.toString()).create();

    PrometheusExporterSettings settings = PrometheusExporterSettings.builder().build();
    SolrScrapeConfiguration scrapeConfiguration =
        SolrScrapeConfiguration.standalone(solrRule.getBaseUrl());
    solrClient =
        new SolrClientFactory(settings, scrapeConfiguration)
            .createStandaloneSolrClient(solrRule.getBaseUrl());
    executor =
        ExecutorUtil.newMDCAwareFixedThreadPool(
            25, new SolrNamedThreadFactory("solr-cloud-scraper-tests"));
    configuration =
        Helpers.loadConfiguration("conf/prometheus-solr-exporter-scraper-test-config.xml");
    solrScraper = new SolrStandaloneScraper(solrClient, executor, "test");

    Helpers.indexAllDocs(solrClient);
  }

  public static void createConf(Path configSet) throws IOException {
    Path subHome = configSet.resolve("conf");
    Files.createDirectories(subHome);

    Path top = SolrTestCaseJ4.TEST_PATH().resolve("collection1").resolve("conf");
    Files.copy(top.resolve("managed-schema.xml"), subHome.resolve("schema.xml"));
    Files.copy(top.resolve("solrconfig.xml"), subHome.resolve("solrconfig.xml"));

    Files.copy(top.resolve("stopwords.txt"), subHome.resolve("stopwords.txt"));
    Files.copy(top.resolve("synonyms.txt"), subHome.resolve("synonyms.txt"));
  }

  @AfterClass
  public static void cleanup() throws Exception {
    // scraper also closes the client
    IOUtils.closeQuietly(solrScraper);
    ExecutorUtil.shutdownNowAndAwaitTermination(executor);
  }

  @Test
  public void pingCollections() throws IOException {
    Map<String, MetricSamples> collectionMetrics =
        solrScraper.pingAllCollections(configuration.getPingConfiguration().get(0));

    assertTrue(collectionMetrics.isEmpty());
  }

  @Test
  public void pingCores() throws Exception {
    Map<String, MetricSamples> allCoreMetrics =
        solrScraper.pingAllCores(configuration.getPingConfiguration().get(0));

    assertEquals(1, allCoreMetrics.size());

    List<Collector.MetricFamilySamples> allSamples = allCoreMetrics.get("collection1").asList();
    Collector.MetricFamilySamples samples = allSamples.get(0);

    assertEquals("solr_ping", samples.name);
    assertEquals(1, samples.samples.size());
    assertEquals(1.0, samples.samples.get(0).value, 0.001);
    assertEquals(List.of("base_url", "cluster_id"), samples.samples.get(0).labelNames);
    assertEquals(List.of(solrRule.getBaseUrl(), "test"), samples.samples.get(0).labelValues);
  }

  @Test
  public void queryCollections() throws Exception {
    List<Collector.MetricFamilySamples> collection1Metrics =
        solrScraper.collections(configuration.getCollectionsConfiguration().get(0)).asList();

    assertTrue(collection1Metrics.isEmpty());
  }

  @Test
  public void metricsForHost() throws Exception {
    Map<String, MetricSamples> metricsByHost =
        solrScraper.metricsForAllHosts(configuration.getMetricsConfiguration().get(0));

    assertEquals(1, metricsByHost.size());

    List<Collector.MetricFamilySamples> replicaSamples =
        metricsByHost.get(solrRule.getBaseUrl()).asList();

    assertEquals(1, replicaSamples.size());

    assertEquals("solr_metrics_jvm_buffers", replicaSamples.get(0).name);

    assertEquals("cluster_id", replicaSamples.get(0).samples.get(0).labelNames.get(2));
    assertEquals("test", replicaSamples.get(0).samples.get(0).labelValues.get(2));
  }

  @Test
  public void search() throws Exception {
    List<Collector.MetricFamilySamples> samples =
        solrScraper.search(configuration.getSearchConfiguration().get(0)).asList();

    assertEquals(1, samples.size());

    Collector.MetricFamilySamples sampleFamily = samples.get(0);
    assertEquals("solr_facets_category", sampleFamily.name);
    assertEquals(PrometheusExporterTestBase.FACET_VALUES.size(), sampleFamily.samples.size());

    for (Collector.MetricFamilySamples.Sample sample : sampleFamily.samples) {
      assertEquals(
          PrometheusExporterTestBase.FACET_VALUES.get(sample.labelValues.get(0)),
          sample.value,
          0.001);
    }
  }
}
