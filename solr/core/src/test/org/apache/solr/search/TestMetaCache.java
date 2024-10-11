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
package org.apache.solr.search;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Random;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.util.EmbeddedSolrServerTestRule;
import org.apache.solr.util.SolrClientTestRule;
import org.junit.ClassRule;

public class TestMetaCache extends SolrTestCaseJ4 {

  @ClassRule
  public static final SolrClientTestRule solrClientTestRule =
      new EmbeddedSolrServerTestRule() {
        @Override
        protected void before() throws Throwable {
          startSolr(LuceneTestCase.createTempDir());
        }
      };

  private static final String FILTER_CACHE_HISTOGRAM_METRIC_NAME =
      "CACHE.searcher.filterCache.histogram";

  private static Map<String, ?> getFilterCacheMetaMetrics(
      SolrClient client, String collectionName) {
    @SuppressWarnings("unchecked")
    SolrMetricManager.GaugeWrapper<Map<String, ?>> metrics =
        (SolrMetricManager.GaugeWrapper<Map<String, ?>>)
            ((EmbeddedSolrServer) client)
                .getCoreContainer()
                .getMetricManager()
                .getMetrics(
                    "solr.core.".concat(collectionName),
                    (metricName, metric) -> FILTER_CACHE_HISTOGRAM_METRIC_NAME.equals(metricName))
                .get(FILTER_CACHE_HISTOGRAM_METRIC_NAME);
    return metrics.getValue();
  }

  public void test() throws Exception {
    String name = "histogram";
    Path configSet = LuceneTestCase.createTempDir();
    SolrTestCaseJ4.copyMinConf(configSet.toFile());
    // insert a special ordMapCache configuration
    Path solrConfig = configSet.resolve("conf/solrconfig.xml");
    Files.writeString(
        solrConfig,
        Files.readString(solrConfig)
            .replace(
                "</config>",
                "<query>\n"
                    + "<filterCache\n"
                    + "      regenerator=\"solr.MetaCacheRegenerator$FilterHistogram\"\n"
                    + "      size=\"100\"\n"
                    + "      initialSize=\"1\"/>\n"
                    + "</query></config>"));

    solrClientTestRule.newCollection(name).withConfigSet(configSet.toString()).create();

    SolrClient client = solrClientTestRule.getSolrClient(name);

    final int docCount = 100;
    for (int i = 0; i < docCount; i++) {
      client.add(sdoc("id", Integer.toString(i)));
    }
    client.commit();

    Random r = random();
    for (int i = docCount << 5; i >= 0; i--) {
      int adjust = (int) (r.nextGaussian() * (docCount >> 2));
      int id = Math.abs(adjust);
      client.query(params("q", "*:*", "fq", "id:".concat(Integer.toString(id))));
    }

    Map<String, ?> metrics = getFilterCacheMetaMetrics(client, name);
    assertEquals(10, metrics.size());
    int idx = 0;
    double last = 0;
    for (Map.Entry<String, ?> e : metrics.entrySet()) {
      assertEquals(idx++, Integer.parseInt(e.getKey()));
      double val = Double.parseDouble((String) e.getValue());
      assertTrue(val >= last);
      last = val;
    }
  }
}
