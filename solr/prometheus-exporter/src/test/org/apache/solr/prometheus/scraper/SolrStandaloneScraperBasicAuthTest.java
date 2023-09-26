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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.ExecutorService;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.apache.solr.prometheus.PrometheusExporterTestBase;
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

public class SolrStandaloneScraperBasicAuthTest extends SolrTestCaseJ4 {

  @ClassRule public static final SolrJettyTestRule solrRule = new SolrJettyTestRule();

  private static Http2SolrClient solrClient;
  private static MetricsConfiguration configuration;
  private static SolrStandaloneScraper solrScraper;
  private static ExecutorService executor;

  private static String user = "solr";
  private static String pass = "SolrRocks";
  private static String securityJson =
      "{\n"
          + "\"authentication\":{ \n"
          + "   \"blockUnknown\": true, \n"
          + "   \"class\":\"solr.BasicAuthPlugin\",\n"
          + "   \"credentials\":{\"solr\":\"IV0EHq1OnNrj6gvRCwvFwTrZ1+z1oBbnQdiVC3otuq0= Ndd7LKvVBAaZIF0QAVi1ekCfAJXr1GGfLtRUXhgrF8c=\"}, \n"
          + "   \"realm\":\"My Solr users\", \n"
          + "   \"forwardCredentials\": false \n"
          + "},\n"
          + "\"authorization\":{\n"
          + "   \"class\":\"solr.RuleBasedAuthorizationPlugin\",\n"
          + "   \"permissions\":[{\"name\":\"security-edit\",\n"
          + "      \"role\":\"admin\"}],\n"
          + "   \"user-role\":{\"solr\":\"admin\"}\n"
          + "}}";

  @BeforeClass
  public static void setupSolrHome() throws Exception {
    Path solrHome = LuceneTestCase.createTempDir();
    Files.write(solrHome.resolve("security.json"), securityJson.getBytes(StandardCharsets.UTF_8));
    solrRule.startSolr(solrHome);

    Path configSet = LuceneTestCase.createTempDir();
    SolrStandaloneScraperTest.createConf(configSet);
    solrRule
        .newCollection()
        .withConfigSet(configSet.toString())
        .withBasicAuthCredentials(user, pass)
        .create();

    configuration =
        Helpers.loadConfiguration("conf/prometheus-solr-exporter-scraper-test-config.xml");

    PrometheusExporterSettings settings = PrometheusExporterSettings.builder().build();
    SolrScrapeConfiguration scrapeConfiguration =
        SolrScrapeConfiguration.standalone(solrRule.getBaseUrl())
            .withBasicAuthCredentials(user, pass);
    solrClient =
        new SolrClientFactory(settings, scrapeConfiguration)
            .createStandaloneSolrClient(solrRule.getBaseUrl());
    executor =
        ExecutorUtil.newMDCAwareFixedThreadPool(
            25, new SolrNamedThreadFactory("solr-cloud-scraper-tests"));
    solrScraper = new SolrStandaloneScraper(solrClient, executor, "test");

    Helpers.indexAllDocs(solrClient);
  }

  @AfterClass
  public static void cleanup() throws Exception {
    // scraper also closes the client
    IOUtils.closeQuietly(solrScraper);
    ExecutorUtil.shutdownNowAndAwaitTermination(executor);
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
