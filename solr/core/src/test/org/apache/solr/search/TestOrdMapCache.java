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
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.util.EmbeddedSolrServerTestRule;
import org.apache.solr.util.SolrClientTestRule;
import org.junit.ClassRule;

public class TestOrdMapCache extends SolrTestCaseJ4 {

  @ClassRule
  public static final SolrClientTestRule solrClientTestRule =
      new EmbeddedSolrServerTestRule() {
        @Override
        protected void before() throws Throwable {
          startSolr(LuceneTestCase.createTempDir());
        }
      };

  private static final String ORD_MAP_CACHE_METRIC_NAME = "CACHE.searcher.ordMapCache";

  private static Map<String, ?> getOrdMapCacheMetrics(SolrClient client, String collectionName) {
    @SuppressWarnings("unchecked")
    SolrMetricManager.GaugeWrapper<Map<String, ?>> metrics =
        (SolrMetricManager.GaugeWrapper<Map<String, ?>>)
            ((EmbeddedSolrServer) client)
                .getCoreContainer()
                .getMetricManager()
                .getMetrics(
                    "solr.core.".concat(collectionName),
                    (metricName, metric) -> ORD_MAP_CACHE_METRIC_NAME.equals(metricName))
                .get(ORD_MAP_CACHE_METRIC_NAME);
    return metrics.getValue();
  }

  public void testDefaultBehavior() throws Exception {
    String name = "default";
    Path configSet = LuceneTestCase.createTempDir();
    SolrTestCaseJ4.copyMinConf(configSet.toFile());

    solrClientTestRule.newCollection(name).withConfigSet(configSet.toString()).create();

    SolrClient client = solrClientTestRule.getSolrClient(name);
    Map<String, ?> metrics;

    client.add(sdoc("id", "1"));
    client.commit();

    // initial sanity-check
    assertEquals(0, getOrdMapCacheMetrics(client, name).get("size"));

    client.query(
        params("q", "*:*", "facet", "true", "facet.field", "id")); // would trigger ordmap build

    // for 1-seg index, no ordmap will actually be built
    assertEquals(0, getOrdMapCacheMetrics(client, name).get("size"));

    client.add(sdoc("id", "2"));
    client.commit();

    client.query(params("q", "*:*", "facet", "true", "facet.field", "id")); // trigger ordmap build

    // for 2-seg index, ordmap will actually be built
    metrics = getOrdMapCacheMetrics(client, name);
    assertEquals(1, metrics.get("size"));
    assertEquals(1L, metrics.get("lookups"));
    assertEquals(1L, metrics.get("inserts"));
    assertEquals(0L, metrics.get("hits"));

    client.add(sdoc("id", "3"));
    client.commit();

    // expect ordmap to not be auto-warmed
    metrics = getOrdMapCacheMetrics(client, name);
    assertEquals(0, metrics.get("size"));
    assertEquals(1L, metrics.get("cumulative_lookups"));
    assertEquals(1L, metrics.get("cumulative_inserts"));
    assertEquals(0L, metrics.get("cumulative_hits"));

    client.query(
        params("q", "*:*", "facet", "true", "facet.field", "id")); // will get auto-warmed ordmap

    metrics = getOrdMapCacheMetrics(client, name);
    assertEquals(1, metrics.get("size"));
    assertEquals(2L, metrics.get("cumulative_lookups"));
    assertEquals(2L, metrics.get("cumulative_inserts"));
    assertEquals(0L, metrics.get("cumulative_hits"));
  }

  public void testLongKeepAlive() throws Exception {
    String name = "longKeepAlive";
    String keepAlive;
    switch (random().nextInt(5)) {
      case 0:
        keepAlive = "60000";
        break;
      case 1:
        keepAlive = "60s";
        break;
      case 2:
        keepAlive = "1m";
        break;
      case 3:
        keepAlive = "1h";
        break;
      case 4:
        keepAlive = "1d";
        break;
      default:
        throw new IllegalStateException();
    }
    keepAlive = "regenKeepAlive=\"" + keepAlive + "\"\n";
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
                    + "<ordMapCache\n"
                    + "      class=\"solr.CaffeineCache\"\n"
                    + "      size=\"1\"\n"
                    + "      autowarmCount=\"100%\"\n"
                    + keepAlive
                    + "      initialSize=\"1\"/>\n"
                    + "</query></config>"));

    solrClientTestRule.newCollection(name).withConfigSet(configSet.toString()).create();

    SolrClient client = solrClientTestRule.getSolrClient(name);
    Map<String, ?> metrics;

    client.add(sdoc("id", "1"));
    client.commit();

    // initial sanity-check
    assertEquals(0, getOrdMapCacheMetrics(client, name).get("size"));

    client.query(
        params("q", "*:*", "facet", "true", "facet.field", "id")); // would trigger ordmap build

    // for 1-seg index, no ordmap will actually be built
    assertEquals(0, getOrdMapCacheMetrics(client, name).get("size"));

    client.add(sdoc("id", "2"));
    client.commit();

    client.query(params("q", "*:*", "facet", "true", "facet.field", "id")); // trigger ordmap build

    // for 2-seg index, ordmap will actually be built
    metrics = getOrdMapCacheMetrics(client, name);
    assertEquals(1, metrics.get("size"));
    assertEquals(1L, metrics.get("lookups"));
    assertEquals(1L, metrics.get("inserts"));
    assertEquals(0L, metrics.get("hits"));

    client.add(sdoc("id", "3"));
    client.commit();

    // expect ordmap to be auto-warmed
    metrics = getOrdMapCacheMetrics(client, name);
    assertEquals(1, metrics.get("size"));
    assertEquals(1L, metrics.get("cumulative_lookups"));
    assertEquals(1L, metrics.get("cumulative_inserts"));
    assertEquals(0L, metrics.get("cumulative_hits"));

    client.query(
        params("q", "*:*", "facet", "true", "facet.field", "id")); // will get auto-warmed ordmap

    metrics = getOrdMapCacheMetrics(client, name);
    assertEquals(1, metrics.get("size"));
    assertEquals(2L, metrics.get("cumulative_lookups"));
    assertEquals(1L, metrics.get("cumulative_inserts"));
    assertEquals(1L, metrics.get("cumulative_hits"));
  }

  public void testShortKeepAlive() throws Exception {
    String name = "shortKeepAlive";
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
                    + "<ordMapCache\n"
                    + "      class=\"solr.CaffeineCache\"\n"
                    + "      size=\"1\"\n"
                    + "      autowarmCount=\"100%\"\n"
                    + "      regenKeepAlive=\"500\"\n"
                    + "      initialSize=\"1\"/>\n"
                    + "</query></config>"));

    solrClientTestRule.newCollection(name).withConfigSet(configSet.toString()).create();

    SolrClient client = solrClientTestRule.getSolrClient(name);
    Map<String, ?> metrics;

    client.add(sdoc("id", "1"));
    client.commit();

    // initial sanity-check
    assertEquals(0, getOrdMapCacheMetrics(client, name).get("size"));

    client.query(
        params("q", "*:*", "facet", "true", "facet.field", "id")); // would trigger ordmap build

    // for 1-seg index, no ordmap will actually be built
    assertEquals(0, getOrdMapCacheMetrics(client, name).get("size"));

    client.add(sdoc("id", "2"));
    client.commit();

    client.query(params("q", "*:*", "facet", "true", "facet.field", "id")); // trigger ordmap build

    // for 2-seg index, ordmap will actually be built
    metrics = getOrdMapCacheMetrics(client, name);
    assertEquals(1, metrics.get("size"));
    assertEquals(1L, metrics.get("lookups"));
    assertEquals(1L, metrics.get("inserts"));
    assertEquals(0L, metrics.get("hits"));

    client.add(sdoc("id", "3"));
    client.commit();

    // expect ordmap to be auto-warmed
    metrics = getOrdMapCacheMetrics(client, name);
    assertEquals(1, metrics.get("size"));
    assertEquals(1L, metrics.get("cumulative_lookups"));
    assertEquals(1L, metrics.get("cumulative_inserts"));
    assertEquals(0L, metrics.get("cumulative_hits"));

    client.query(
        params("q", "*:*", "facet", "true", "facet.field", "id")); // will get auto-warmed ordmap

    metrics = getOrdMapCacheMetrics(client, name);
    assertEquals(1, metrics.get("size"));
    assertEquals(2L, metrics.get("cumulative_lookups"));
    assertEquals(1L, metrics.get("cumulative_inserts"));
    assertEquals(1L, metrics.get("cumulative_hits"));

    Thread.sleep(300);

    client.add(sdoc("id", "4"));
    client.commit();

    // entry should still be auto-warmed at this point
    metrics = getOrdMapCacheMetrics(client, name);
    assertEquals(1, metrics.get("size"));
    assertEquals(2L, metrics.get("cumulative_lookups"));
    assertEquals(1L, metrics.get("cumulative_inserts"));
    assertEquals(1L, metrics.get("cumulative_hits"));

    // wait long enough that the entry should no longer be auto-warmed
    Thread.sleep(300);

    client.add(sdoc("id", "5"));
    client.commit();

    // we waited beyond keepAlive; expect ordmap to be empty
    metrics = getOrdMapCacheMetrics(client, name);
    assertEquals(0, metrics.get("size"));
    assertEquals(2L, metrics.get("cumulative_lookups"));
    assertEquals(1L, metrics.get("cumulative_inserts"));
    assertEquals(1L, metrics.get("cumulative_hits"));

    client.query(params("q", "*:*", "facet", "true", "facet.field", "id")); // will re-build ordmap

    metrics = getOrdMapCacheMetrics(client, name);
    assertEquals(1, metrics.get("size"));
    assertEquals(3L, metrics.get("cumulative_lookups"));
    assertEquals(2L, metrics.get("cumulative_inserts"));
    assertEquals(1L, metrics.get("cumulative_hits"));
  }

  public void testSizeLimited() throws Exception {
    String name = "sizeLimited";
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
                    + "<ordMapCache\n"
                    + "      class=\"solr.CaffeineCache\"\n"
                    + "      size=\"1\"\n"
                    + "      initialSize=\"1\"/>\n"
                    + "</query></config>"));

    solrClientTestRule.newCollection(name).withConfigSet(configSet.toString()).create();

    SolrClient client = solrClientTestRule.getSolrClient(name);
    Map<String, ?> metrics;

    client.add(sdoc("id", "1", "other", "1"));
    client.commit();

    // initial sanity-check
    assertEquals(0, getOrdMapCacheMetrics(client, name).get("size"));

    client.query(
        params("q", "*:*", "facet", "true", "facet.field", "id")); // would trigger ordmap build

    // for 1-seg index, no ordmap will actually be built
    assertEquals(0, getOrdMapCacheMetrics(client, name).get("size"));

    client.add(sdoc("id", "2", "other", "2"));
    client.commit();

    client.query(params("q", "*:*", "facet", "true", "facet.field", "id")); // trigger ordmap build

    // for 2-seg index, ordmap will actually be built
    metrics = getOrdMapCacheMetrics(client, name);
    assertEquals(1, metrics.get("size"));
    assertEquals(1L, metrics.get("lookups"));
    assertEquals(1L, metrics.get("inserts"));
    assertEquals(0L, metrics.get("hits"));

    client.query(
        params("q", "*:*", "facet", "true", "facet.field", "other")); // trigger ordmap build/evict

    // no actual hits, just evict/insert (thrashing)
    metrics = getOrdMapCacheMetrics(client, name);
    assertEquals(1, metrics.get("size"));
    assertEquals(2L, metrics.get("lookups"));
    assertEquals(2L, metrics.get("inserts"));
    assertEquals(0L, metrics.get("hits"));
  }
}
