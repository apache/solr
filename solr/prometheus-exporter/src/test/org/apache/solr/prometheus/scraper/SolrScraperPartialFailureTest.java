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

import java.util.List;
import java.util.concurrent.ExecutorService;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.apache.solr.prometheus.collector.MetricSamples;
import org.apache.solr.prometheus.exporter.MetricsConfiguration;
import org.apache.solr.prometheus.exporter.MetricsQuery;
import org.apache.solr.prometheus.utils.Helpers;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Verifies that {@link SolrScraper#request} discards a jsonQuery's results entirely when the query
 * throws partway through, instead of leaking whatever it already emitted before failing.
 */
public class SolrScraperPartialFailureTest extends SolrTestCaseJ4 {

  private static ExecutorService executor;

  @BeforeClass
  public static void setupExecutor() {
    executor =
        ExecutorUtil.newMDCAwareSingleThreadExecutor(
            new SolrNamedThreadFactory("solr-scraper-partial-failure-tests"));
  }

  @AfterClass
  public static void teardownExecutor() {
    ExecutorUtil.shutdownNowAndAwaitTermination(executor);
    executor = null;
  }

  /** Always answers with the same canned JSON response, regardless of what's requested. */
  private static final class FixedResponseSolrClient extends SolrClient {
    private final String json;

    private FixedResponseSolrClient(String json) {
      this.json = json;
    }

    @Override
    public NamedList<Object> request(SolrRequest<?> request, String collection) {
      NamedList<Object> response = new NamedList<>();
      response.add("response", json);
      return response;
    }

    @Override
    public void close() {}
  }

  @Test
  public void testPartialResultsAreDiscardedOnQueryFailure() throws Exception {
    MetricsConfiguration configuration =
        Helpers.loadConfiguration("conf/test-config-partial-failure.xml");
    List<MetricsQuery> queries = configuration.getMetricsConfiguration();
    assertEquals(1, queries.size());
    MetricsQuery query = queries.get(0);

    SolrClient solrClient = new FixedResponseSolrClient("{\"items\": [\"ok\", \"bad\"]}");

    SolrStandaloneScraper scraper = new SolrStandaloneScraper(null, executor, "test-cluster");
    MetricSamples samples = scraper.request(solrClient, query);

    assertTrue(
        "the sample emitted before the query threw must not appear in the scrape",
        samples.asList().isEmpty());
  }
}
