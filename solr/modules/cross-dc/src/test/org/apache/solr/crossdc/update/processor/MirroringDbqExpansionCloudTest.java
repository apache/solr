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
package org.apache.solr.crossdc.update.processor;

import static org.apache.solr.update.processor.DistributingUpdateProcessorFactory.DISTRIB_UPDATE_PARAM;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.SolrQuery;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.core.CloseHook;
import org.apache.solr.core.SolrCore;
import org.apache.solr.crossdc.common.CrossDcConf;
import org.apache.solr.embedded.JettySolrRunner;
import org.apache.solr.metrics.SolrMetricsContext;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.processor.DistributedUpdateProcessor.DistribPhase;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.apache.solr.update.processor.UpdateRequestProcessorFactory;
import org.apache.solr.util.plugin.SolrCoreAware;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Runs the expanding delete-by-query path of {@link MirroringUpdateProcessor} against a real
 * multi-node cluster, so the client it uses for the cursor query and the delete-by-id requests is
 * not mocked.
 */
public class MirroringDbqExpansionCloudTest extends SolrCloudTestCase {

  private static final String COLLECTION = "dbqExpansion";

  @BeforeClass
  public static void setupCluster() throws Exception {
    System.setProperty(
        "solr.crossdc.processorFactory", RecordingMirroringProcessorFactory.class.getName());
    configureCluster(3)
        .addConfig("conf", getFile("configs/cloud-minimal/conf").toAbsolutePath())
        .configure();
  }

  @After
  public void cleanup() throws Exception {
    cluster.deleteAllCollections();
    RecordingMirroringProcessorFactory.MIRRORED.clear();
  }

  @Test
  public void testExpand() throws Exception {
    System.setProperty(CrossDcConf.EXPAND_DBQ, CrossDcConf.ExpandDbq.EXPAND.name());
    assertDbqExpanded();
  }

  @Test
  public void testDefaultIsExpand() throws Exception {
    System.clearProperty(CrossDcConf.EXPAND_DBQ);
    assertDbqExpanded();
  }

  @Test
  public void testNone() throws Exception {
    System.setProperty(CrossDcConf.EXPAND_DBQ, CrossDcConf.ExpandDbq.NONE.name());
    indexDocs();

    deleteByQuery("text:remove");

    assertEquals(2, numDocs());
    assertTrue("no delete-by-id expected", mirroredDeleteIds().isEmpty());
    assertEquals(List.of("text:remove"), mirroredDeleteQueries());
  }

  @Test
  public void testMatchAllQueryIsNeverExpanded() throws Exception {
    System.setProperty(CrossDcConf.EXPAND_DBQ, CrossDcConf.ExpandDbq.EXPAND.name());
    indexDocs();

    deleteByQuery("*:*");

    assertEquals(0, numDocs());
    assertTrue("no delete-by-id expected", mirroredDeleteIds().isEmpty());
    assertEquals(List.of("*:*"), mirroredDeleteQueries());
  }

  @Test
  public void testExpandPagesThroughCursor() throws Exception {
    System.setProperty(CrossDcConf.EXPAND_DBQ, "expand");
    int matching = 10;
    indexDocs(matching, 2);

    // 10 matches with 3 rows per page: the delete is only complete if every cursor page is read
    System.setProperty("solr.crossdc.dbq_rows", "3");
    deleteByQuery("text:remove");

    assertEquals(2, numDocs());
    List<String> expectedIds = new ArrayList<>();
    for (int i = 0; i < matching; i++) {
      expectedIds.add(String.format(Locale.ROOT, "doc%02d", i));
    }
    assertEquals(expectedIds, mirroredDeleteIds());
    assertEquals(
        "one mirrored delete-by-id request per cursor page",
        4,
        RecordingMirroringProcessorFactory.MIRRORED.stream()
            .filter(r -> r.getDeleteById() != null)
            .count());
    assertTrue("DBQ must not be mirrored as-is", mirroredDeleteQueries().isEmpty());
  }

  private void assertDbqExpanded() throws Exception {
    indexDocs();

    // more matches than the page size, to exercise the cursor loop
    System.setProperty("solr.crossdc.dbq_rows", "2");
    deleteByQuery("text:remove");

    assertEquals(2, numDocs());
    assertEquals(List.of("doc00", "doc01", "doc02"), mirroredDeleteIds());
    assertTrue("DBQ must not be mirrored as-is", mirroredDeleteQueries().isEmpty());
  }

  /** Sends the delete to a random node, which may not host the matching documents. */
  private void deleteByQuery(String query) throws Exception {
    JettySolrRunner node =
        cluster.getJettySolrRunner(random().nextInt(cluster.getJettySolrRunners().size()));
    try (SolrClient client = node.newSolrClient(COLLECTION)) {
      new UpdateRequest().deleteByQuery(query).commit(client, COLLECTION);
    }
  }

  private void indexDocs() throws Exception {
    indexDocs(3, 2);
  }

  /** Indexes docs matching {@code text:remove}, followed by docs that do not. */
  private void indexDocs(int matching, int others) throws Exception {
    CollectionAdminRequest.createCollection(COLLECTION, "conf", 3, 2)
        .process(cluster.getSolrClient());
    cluster.waitForActiveCollection(COLLECTION, 3, 6);

    UpdateRequest add = new UpdateRequest();
    for (int i = 0; i < matching + others; i++) {
      add.add(
          "id",
          String.format(Locale.ROOT, "doc%02d", i),
          "text",
          i < matching ? "remove me" : "keep me");
    }
    add.commit(cluster.getSolrClient(), COLLECTION);
    RecordingMirroringProcessorFactory.MIRRORED.clear();
  }

  private long numDocs() throws Exception {
    return cluster.getSolrClient().query(COLLECTION, new SolrQuery("*:*")).getResults().size();
  }

  private static List<String> mirroredDeleteIds() {
    return RecordingMirroringProcessorFactory.MIRRORED.stream()
        .filter(r -> r.getDeleteById() != null)
        .flatMap(r -> r.getDeleteById().stream())
        .sorted()
        .collect(Collectors.toList());
  }

  private static List<String> mirroredDeleteQueries() {
    return RecordingMirroringProcessorFactory.MIRRORED.stream()
        .filter(r -> r.getDeleteQuery() != null)
        .flatMap(r -> r.getDeleteQuery().stream())
        .sorted()
        .collect(Collectors.toList());
  }

  /**
   * Test factory that builds a real {@link MirroringUpdateProcessor} with the DBQ expansion mode
   * taken from {@link CrossDcConf#EXPAND_DBQ}, but records mirrored requests in memory instead of
   * sending them to Kafka.
   */
  public static class RecordingMirroringProcessorFactory extends UpdateRequestProcessorFactory
      implements SolrCoreAware {

    /** Requests passed to the mirroring handler. */
    public static final List<UpdateRequest> MIRRORED = new CopyOnWriteArrayList<>();

    private volatile ProducerMetrics producerMetrics;

    @Override
    public void inform(SolrCore core) {
      SolrMetricsContext metricsContext = core.getSolrMetricsContext().getChildContext(this);
      producerMetrics = new ProducerMetrics(metricsContext, core);
      core.addCloseHook(
          new CloseHook() {
            @Override
            public void postClose(SolrCore core) {
              metricsContext.close();
            }
          });
    }

    @Override
    public UpdateRequestProcessor getInstance(
        SolrQueryRequest req, SolrQueryResponse rsp, UpdateRequestProcessor next) {
      boolean doMirroring =
          req.getParams()
              .getBool(MirroringUpdateRequestProcessorFactory.SERVER_SHOULD_MIRROR, true);
      ModifiableSolrParams mirrorParams = new ModifiableSolrParams();
      mirrorParams.set("collection", req.getCore().getCoreDescriptor().getCollectionName());
      mirrorParams.set(
          MirroringUpdateRequestProcessorFactory.SERVER_SHOULD_MIRROR, Boolean.FALSE.toString());
      return new MirroringUpdateProcessor(
          next,
          doMirroring,
          false,
          true,
          CrossDcConf.ExpandDbq.getOrDefault(
              System.getProperty(CrossDcConf.EXPAND_DBQ), CrossDcConf.ExpandDbq.EXPAND),
          Long.MAX_VALUE,
          mirrorParams,
          DistribPhase.parseParam(req.getParams().get(DISTRIB_UPDATE_PARAM)),
          doMirroring ? MIRRORED::add : null,
          producerMetrics);
    }
  }
}
