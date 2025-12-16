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
package org.apache.solr.cloud;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest.METHOD;
import org.apache.solr.client.solrj.SolrRequest.SolrRequestType;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.InputStreamResponseParser;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.embedded.JettySolrRunner;
import org.apache.solr.search.similarities.CustomSimilarityFactory;
import org.apache.solr.search.stats.StatsCache;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

/** */
@Ignore("Abstract classes should not be executed as tests")
public abstract class TestBaseStatsCacheCloud extends SolrCloudTestCase {

  protected int numNodes = 2;
  protected String configset = "cloud-dynamic";

  protected String collectionName = "collection_" + getClass().getSimpleName();

  protected Function<Integer, SolrInputDocument> generator =
      i -> {
        SolrInputDocument doc = new SolrInputDocument("id", "id-" + i);
        if (i % 3 == 0) {
          doc.addField("foo_t", "bar baz");
        } else if (i % 3 == 1) {
          doc.addField("foo_t", "bar");
        } else {
          // skip the field
        }
        return doc;
      };

  protected CloudSolrClient solrClient;

  protected SolrClient control;

  protected int NUM_DOCS = 100;

  // implementation name
  protected abstract String getImplementationName();

  // does this implementation produce the same distrib scores as local ones?
  protected abstract boolean assertSameScores();

  @Before
  public void setupCluster() throws Exception {
    System.setProperty("metricsEnabled", "true");
    // create control core & client
    System.setProperty("solr.statsCache", getImplementationName());
    System.setProperty("solr.similarity", CustomSimilarityFactory.class.getName());
    initCore("solrconfig-minimal.xml", "schema-tiny.xml");
    control = new EmbeddedSolrServer(h.getCore());
    // create cluster
    configureCluster(numNodes) // 2 + random().nextInt(3)
        .addConfig("conf", configset(configset))
        .configure();
    solrClient = cluster.getSolrClient();
    createTestCollection();
  }

  protected void createTestCollection() throws Exception {
    CollectionAdminRequest.createCollection(collectionName, "conf", 2, numNodes)
        .process(solrClient);
    indexDocs(solrClient, collectionName, NUM_DOCS, 0, generator);
    indexDocs(control, "collection1", NUM_DOCS, 0, generator);
  }

  @After
  public void tearDownCluster() {
    System.clearProperty("solr.statsCache");
    System.clearProperty("solr.similarity");
  }

  @Test
  @SuppressWarnings({"unchecked"})
  public void testBasicStats() throws Exception {
    QueryResponse cloudRsp =
        solrClient.query(
            collectionName,
            params(
                "q", "foo_t:\"bar baz\"", "fl", "*,score", "rows", "" + NUM_DOCS, "debug", "true"));
    QueryResponse controlRsp =
        control.query(
            "collection1",
            params(
                "q", "foo_t:\"bar baz\"", "fl", "*,score", "rows", "" + NUM_DOCS, "debug", "true"));

    assertResponses(controlRsp, cloudRsp, assertSameScores());

    // test after updates
    indexDocs(solrClient, collectionName, NUM_DOCS, NUM_DOCS, generator);
    indexDocs(control, "collection1", NUM_DOCS, NUM_DOCS, generator);

    cloudRsp =
        solrClient.query(
            collectionName,
            params("q", "foo_t:\"bar baz\"", "fl", "*,score", "rows", "" + (NUM_DOCS * 2)));
    controlRsp =
        control.query(
            "collection1",
            params("q", "foo_t:\"bar baz\"", "fl", "*,score", "rows", "" + (NUM_DOCS * 2)));
    assertResponses(controlRsp, cloudRsp, assertSameScores());

    // check cache metrics
    StatsCache.StatsCacheMetrics statsCacheMetrics = new StatsCache.StatsCacheMetrics();
    for (JettySolrRunner jettySolrRunner : cluster.getJettySolrRunners()) {
      try (SolrClient client = getHttpSolrClient(jettySolrRunner.getBaseUrl().toString())) {
        var req =
            new GenericSolrRequest(
                METHOD.GET,
                "/admin/metrics",
                SolrRequestType.ADMIN,
                SolrParams.of("wt", "prometheus"));
        req.setResponseParser(new InputStreamResponseParser("prometheus"));

        NamedList<Object> resp = client.request(req);
        try (InputStream in = (InputStream) resp.get("stream")) {
          String output = new String(in.readAllBytes(), StandardCharsets.UTF_8);

          for (String line : output.lines().toList()) {
            if (line.startsWith("solr_core_indexsearcher_termstats_cache")) {
              String type = extractTypeAttribute(line);
              long value = extractMetricValue(line);
              switch (type) {
                case "lookups":
                  statsCacheMetrics.lookups.add(value);
                  break;
                case "return_local":
                  statsCacheMetrics.returnLocalStats.add(value);
                  break;
                case "merge_to_global":
                  statsCacheMetrics.mergeToGlobalStats.add(value);
                  break;
                case "missing_global_field":
                  statsCacheMetrics.missingGlobalFieldStats.add(value);
                  break;
                case "missing_global_term":
                  statsCacheMetrics.missingGlobalTermStats.add(value);
                  break;
                case "receive_global":
                  statsCacheMetrics.receiveGlobalStats.add(value);
                  break;
                case "retrieve":
                  statsCacheMetrics.retrieveStats.add(value);
                  break;
                case "send_global":
                  statsCacheMetrics.sendGlobalStats.add(value);
                  break;
                case "use_cached_global":
                  statsCacheMetrics.useCachedGlobalStats.add(value);
                  break;
              }
            }
          }
        }
      }
    }
    checkStatsCacheMetrics(statsCacheMetrics);
  }

  protected void checkStatsCacheMetrics(StatsCache.StatsCacheMetrics statsCacheMetrics) {
    assertEquals(
        statsCacheMetrics.toString(), 0, statsCacheMetrics.missingGlobalFieldStats.intValue());
    assertEquals(
        statsCacheMetrics.toString(), 0, statsCacheMetrics.missingGlobalTermStats.intValue());
  }

  protected void assertResponses(
      QueryResponse controlRsp, QueryResponse cloudRsp, boolean sameScores) {
    Map<String, SolrDocument> cloudDocs = new HashMap<>();
    Map<String, SolrDocument> controlDocs = new HashMap<>();
    cloudRsp.getResults().forEach(doc -> cloudDocs.put((String) doc.getFieldValue("id"), doc));
    controlRsp.getResults().forEach(doc -> controlDocs.put((String) doc.getFieldValue("id"), doc));
    assertEquals("number of docs", controlDocs.size(), cloudDocs.size());
    for (Map.Entry<String, SolrDocument> entry : controlDocs.entrySet()) {
      SolrDocument controlDoc = entry.getValue();
      SolrDocument cloudDoc = cloudDocs.get(entry.getKey());
      assertNotNull("missing cloud doc " + controlDoc, cloudDoc);
      Float controlScore = (Float) controlDoc.getFieldValue("score");
      Float cloudScore = (Float) cloudDoc.getFieldValue("score");
      if (sameScores) {
        assertEquals(
            "cloud score differs from control", controlScore, cloudScore, controlScore * 0.001f);
      } else {
        assertNotEquals(
            "cloud score is the same as control", controlScore, cloudScore, controlScore * 0.001f);
      }
    }
  }

  protected void indexDocs(
      SolrClient client,
      String collectionName,
      int num,
      int start,
      Function<Integer, SolrInputDocument> generator)
      throws Exception {

    UpdateRequest ureq = new UpdateRequest();
    for (int i = 0; i < num; i++) {
      SolrInputDocument doc = generator.apply(i + start);
      ureq.add(doc);
    }
    ureq.process(client, collectionName);
    client.commit(collectionName);
  }

  /**
   * Extract type label value from Prometheus format line
   * "solr_core_indexsearcher_termstats_cache{...type="lookups",...}" -> "lookups"
   */
  private String extractTypeAttribute(String line) {
    java.util.regex.Pattern pattern = java.util.regex.Pattern.compile("\\btype=\"([^\"]+)\"");
    java.util.regex.Matcher matcher = pattern.matcher(line);
    if (matcher.find()) {
      return matcher.group(1);
    }
    throw new IllegalArgumentException("No type attribute found in line: " + line);
  }

  /**
   * Extract numeric value from Prometheus format line.
   * "solr_core_indexsearcher_termstats_cache{...} 123.0" -> 123
   */
  private long extractMetricValue(String line) {
    String valueStr = line.substring(line.lastIndexOf(' ') + 1);
    return (long) Double.parseDouble(valueStr);
  }
}
