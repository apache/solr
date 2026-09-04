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
package org.apache.solr.handler.component;

import java.io.IOException;
import java.util.List;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.misc.search.DiversifiedTopDocsCollector;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocsCollector;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.SolrQuery;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.Group;
import org.apache.solr.client.solrj.response.GroupCommand;
import org.apache.solr.client.solrj.response.GroupResponse;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.ConfigRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.search.QueryCommand;
import org.junit.BeforeClass;
import org.junit.Test;

public class CustomTopDocCollectorTest extends SolrCloudTestCase {

  private static final String COLLECTION = "pollinators";
  private static final String CUSTOM_DIVERSIFIED_HANDLER = "/custom_diversified";

  private static final int numShards = 1;
  private static final int numReplicas = 1;
  private static final int nodeCount = numShards * numReplicas;

  private static final String id = "id";

  @BeforeClass
  public static void setupCluster() throws Exception {

    // create and configure cluster
    configureCluster(nodeCount).addConfig("conf", configset("cloud-dynamic")).configure();

    // create an empty collection
    CollectionAdminRequest.createCollection(COLLECTION, "conf", numShards, numReplicas)
        .process(cluster.getSolrClient());

    // register custom handler for diversified top docs collector
    cluster
        .getSolrClient()
        .request(
            new ConfigRequest(
                "{\n"
                    + "  'add-requesthandler': {\n"
                    + "    'name' : '"
                    + CUSTOM_DIVERSIFIED_HANDLER
                    + "',\n"
                    + "    'class' : '"
                    + CustomSearchHandler.class.getName()
                    + "'\n"
                    + "  }\n"
                    + "}"),
            COLLECTION);

    // add documents
    final SolrInputDocument doc1w =
        sdoc(id, "white-tailed bumble bee", "category_s1", "bumble bee", "popularity_i", "22");
    final SolrInputDocument doc1r =
        sdoc(id, "red-tailed bumble bee", "category_s1", "bumble bee", "popularity_i", "88");
    final SolrInputDocument doc1b =
        sdoc(id, "buff-tailed bumble bee", "category_s1", "bumble bee", "popularity_i", "2");
    final SolrInputDocument doc1t =
        sdoc(id, "tree bumble bee", "category_s1", "bumble bee", "popularity_i", "42");
    final SolrInputDocument doc1h =
        sdoc(id, "heath bumble bee", "category_s1", "bumble bee", "popularity_i", "43");
    final SolrInputDocument doc2 =
        sdoc(id, "honey bee", "category_s1", "bee", "popularity_i", "1000");
    final SolrInputDocument doc3 =
        sdoc(id, "solitary bee", "category_s1", "bee", "popularity_i", "1");
    final SolrInputDocument doc4 =
        sdoc(id, "monarch butterfly", "category_s1", "butterfly", "popularity_i", "33");
    final SolrInputDocument doc5 =
        sdoc(id, "lesser long-nosed bat", "category_s1", "bat", "popularity_i", "44");
    final SolrInputDocument doc6 =
        sdoc(id, "hummingbird", "category_s1", "bird", "popularity_i", "55");
    new UpdateRequest()
        .add(doc1w)
        .add(doc1r)
        .add(doc1b)
        .add(doc1t)
        .add(doc1h)
        .add(doc2)
        .add(doc3)
        .add(doc4)
        .add(doc5)
        .add(doc6)
        .commit(cluster.getSolrClient(), COLLECTION);
  }

  @Test
  public void testUngrouped() throws Exception {
    final SolrQuery solrQuery = new SolrQuery("q", "{!func} popularity_i", "fl", "*,score");
    final CloudSolrClient cloudSolrClient = cluster.getSolrClient();
    final QueryResponse rsp = cloudSolrClient.query(COLLECTION, solrQuery);
    assertEquals(10, rsp.getResults().size());

    final SolrDocument result0 = rsp.getResults().get(0);
    assertEquals("honey bee", result0.getFieldValue(id));
    assertEquals("bee", result0.getFieldValue("category_s1"));
    assertEquals(1000f, result0.getFieldValue("score"));

    final SolrDocument result1 = rsp.getResults().get(1);
    assertEquals("red-tailed bumble bee", result1.getFieldValue(id));
    assertEquals("bumble bee", result1.getFieldValue("category_s1"));
    assertEquals(88f, result1.getFieldValue("score"));

    final SolrDocument result2 = rsp.getResults().get(2);
    assertEquals("hummingbird", result2.getFieldValue(id));
    assertEquals("bird", result2.getFieldValue("category_s1"));
    assertEquals(55f, result2.getFieldValue("score"));

    final SolrDocument result3 = rsp.getResults().get(3);
    assertEquals("lesser long-nosed bat", result3.getFieldValue(id));
    assertEquals("bat", result3.getFieldValue("category_s1"));
    assertEquals(44f, result3.getFieldValue("score"));

    final SolrDocument result4 = rsp.getResults().get(4);
    assertEquals("heath bumble bee", result4.getFieldValue(id));
    assertEquals("bumble bee", result4.getFieldValue("category_s1"));
    assertEquals(43f, result4.getFieldValue("score"));

    final SolrDocument result5 = rsp.getResults().get(5);
    assertEquals("tree bumble bee", result5.getFieldValue(id));
    assertEquals("bumble bee", result5.getFieldValue("category_s1"));
    assertEquals(42f, result5.getFieldValue("score"));

    final SolrDocument result6 = rsp.getResults().get(6);
    assertEquals("monarch butterfly", result6.getFieldValue(id));
    assertEquals("butterfly", result6.getFieldValue("category_s1"));
    assertEquals(33f, result6.getFieldValue("score"));

    final SolrDocument result7 = rsp.getResults().get(7);
    assertEquals("white-tailed bumble bee", result7.getFieldValue(id));
    assertEquals("bumble bee", result7.getFieldValue("category_s1"));
    assertEquals(22f, result7.getFieldValue("score"));

    final SolrDocument result8 = rsp.getResults().get(8);
    assertEquals("buff-tailed bumble bee", result8.getFieldValue(id));
    assertEquals("bumble bee", result8.getFieldValue("category_s1"));
    assertEquals(2f, result8.getFieldValue("score"));

    final SolrDocument result9 = rsp.getResults().get(9);
    assertEquals("solitary bee", result9.getFieldValue(id));
    assertEquals("bee", result9.getFieldValue("category_s1"));
    assertEquals(1f, result9.getFieldValue("score"));
  }

  @Test
  public void testGrouped() throws Exception {
    final SolrQuery solrQuery =
        new SolrQuery(
            "q",
            "{!func} popularity_i",
            "fl",
            "*,score",
            "group",
            "true",
            "group.field",
            "category_s1",
            "group.limit",
            "2",
            "rows",
            "6");
    final CloudSolrClient cloudSolrClient = cluster.getSolrClient();
    final QueryResponse rsp = cloudSolrClient.query(COLLECTION, solrQuery);
    final GroupResponse groupResponse = rsp.getGroupResponse();
    assertNotNull(groupResponse);

    final List<GroupCommand> groupCommands = groupResponse.getValues();
    assertEquals(1, groupCommands.size());

    final GroupCommand categoryGroupCommand = groupCommands.get(0);
    assertEquals("category_s1", categoryGroupCommand.getName());
    assertEquals(10, categoryGroupCommand.getMatches());

    final List<Group> groups = categoryGroupCommand.getValues();
    assertEquals(5, groups.size());

    final Group beeGroup = groups.get(0);
    assertEquals("bee", beeGroup.getGroupValue());
    assertEquals(2, beeGroup.getResult().size());
    final SolrDocument beeGroupResult0 = beeGroup.getResult().get(0);
    assertEquals("honey bee", beeGroupResult0.getFieldValue(id));
    assertEquals("bee", beeGroupResult0.getFieldValue("category_s1"));
    assertEquals(1000f, beeGroupResult0.getFieldValue("score"));
    final SolrDocument beeGroupResult1 = beeGroup.getResult().get(1);
    assertEquals("solitary bee", beeGroupResult1.getFieldValue(id));
    assertEquals("bee", beeGroupResult1.getFieldValue("category_s1"));
    assertEquals(1f, beeGroupResult1.getFieldValue("score"));

    final Group bumbleBeeGroup = groups.get(1);
    assertEquals("bumble bee", bumbleBeeGroup.getGroupValue());
    assertEquals(2, bumbleBeeGroup.getResult().size());
    final SolrDocument bumbleBeeGroupResult0 = bumbleBeeGroup.getResult().get(0);
    assertEquals("red-tailed bumble bee", bumbleBeeGroupResult0.getFieldValue(id));
    assertEquals("bumble bee", bumbleBeeGroupResult0.getFieldValue("category_s1"));
    assertEquals(88f, bumbleBeeGroupResult0.getFieldValue("score"));
    final SolrDocument bumbleBeeGroupResult1 = bumbleBeeGroup.getResult().get(1);
    assertEquals("heath bumble bee", bumbleBeeGroupResult1.getFieldValue(id));
    assertEquals("bumble bee", bumbleBeeGroupResult1.getFieldValue("category_s1"));
    assertEquals(43f, bumbleBeeGroupResult1.getFieldValue("score"));
    // tree bumble bee (42), white-tailed bumble bee (22), and buff-tailed bumble bee (2) omitted
    // because group.limit=2

    final Group birdGroup = groups.get(2);
    assertEquals("bird", birdGroup.getGroupValue());
    assertEquals(1, birdGroup.getResult().size());
    final SolrDocument birdGroupResult0 = birdGroup.getResult().get(0);
    assertEquals("hummingbird", birdGroupResult0.getFieldValue(id));
    assertEquals("bird", birdGroupResult0.getFieldValue("category_s1"));
    assertEquals(55f, birdGroupResult0.getFieldValue("score"));

    final Group batGroup = groups.get(3);
    assertEquals("bat", batGroup.getGroupValue());
    assertEquals(1, batGroup.getResult().size());
    final SolrDocument batGroupResult0 = batGroup.getResult().get(0);
    assertEquals("lesser long-nosed bat", batGroupResult0.getFieldValue(id));
    assertEquals("bat", batGroupResult0.getFieldValue("category_s1"));
    assertEquals(44f, batGroupResult0.getFieldValue("score"));

    // here after six documents we have not yet seen a butterfly.
    // grouping pulls lower-scoring members ahead of their score-based position:
    // heath bumble bee (43) flies with red-tailed bumble bee (88), and solitary bee (1)
    // flies with honey bee (1000).
    // the butterfly (33) is simply last because it has the lowest group score.

    final Group butterflyGroup = groups.get(4);
    assertEquals("butterfly", butterflyGroup.getGroupValue());
    assertEquals(1, butterflyGroup.getResult().size());
    final SolrDocument butterflyGroupResult0 = butterflyGroup.getResult().get(0);
    assertEquals("monarch butterfly", butterflyGroupResult0.getFieldValue(id));
    assertEquals("butterfly", butterflyGroupResult0.getFieldValue("category_s1"));
    assertEquals(33f, butterflyGroupResult0.getFieldValue("score"));
  }

  @Test
  public void testUndiversified() throws Exception {
    implTestDiversified(false);
  }

  @Test
  public void testDiversified() throws Exception {
    implTestDiversified(true);
  }

  private void implTestDiversified(boolean diversified) throws Exception {
    final SolrQuery solrQuery =
        new SolrQuery(
            "q",
            "{!func} popularity_i",
            "fl",
            "*,score",
            CustomSearchHandler.DTDC_FIELD_PARAM,
            "category_s1",
            CustomSearchHandler.DTDC_LIMIT_PARAM,
            "2",
            "rows",
            "6");
    if (diversified) solrQuery.setRequestHandler(CUSTOM_DIVERSIFIED_HANDLER);
    final CloudSolrClient cloudSolrClient = cluster.getSolrClient();
    final QueryResponse rsp = cloudSolrClient.query(COLLECTION, solrQuery);
    assertEquals(6, rsp.getResults().size());

    final SolrDocument result0 = rsp.getResults().get(0);
    assertEquals("honey bee", result0.getFieldValue(id));
    assertEquals("bee", result0.getFieldValue("category_s1"));
    assertEquals(1000f, result0.getFieldValue("score"));

    final SolrDocument result1 = rsp.getResults().get(1);
    assertEquals("red-tailed bumble bee", result1.getFieldValue(id));
    assertEquals("bumble bee", result1.getFieldValue("category_s1"));
    assertEquals(88f, result1.getFieldValue("score"));

    final SolrDocument result2 = rsp.getResults().get(2);
    assertEquals("hummingbird", result2.getFieldValue(id));
    assertEquals("bird", result2.getFieldValue("category_s1"));
    assertEquals(55f, result2.getFieldValue("score"));

    final SolrDocument result3 = rsp.getResults().get(3);
    assertEquals("lesser long-nosed bat", result3.getFieldValue(id));
    assertEquals("bat", result3.getFieldValue("category_s1"));
    assertEquals(44f, result3.getFieldValue("score"));

    final SolrDocument result4 = rsp.getResults().get(4);
    assertEquals("heath bumble bee", result4.getFieldValue(id));
    assertEquals("bumble bee", result4.getFieldValue("category_s1"));
    assertEquals(43f, result4.getFieldValue("score"));

    final SolrDocument result5 = rsp.getResults().get(5);
    if (!diversified) {
      // undiversified: top-6 by score, tree bumble bee (42) is included
      assertEquals("tree bumble bee", result5.getFieldValue(id));
      assertEquals("bumble bee", result5.getFieldValue("category_s1"));
      assertEquals(42f, result5.getFieldValue("score"));
    } else { // diversified
      // diversified: tree bumble bee (42) is excluded as it would be a third bumble bee,
      // so monarch butterfly (33) appears instead
      assertEquals("monarch butterfly", result5.getFieldValue(id));
      assertEquals("butterfly", result5.getFieldValue("category_s1"));
      assertEquals(33f, result5.getFieldValue("score"));
    }
  }

  /** Custom search handler with diversified collector. */
  public static class CustomSearchHandler extends SearchHandler {

    static final String DTDC_FIELD_PARAM = "dtdc.field";
    static final String DTDC_LIMIT_PARAM = "dtdc.limit";

    @Override
    protected ResponseBuilder newResponseBuilder(
        SolrQueryRequest req, SolrQueryResponse rsp, List<SearchComponent> components) {
      final SolrParams params = req.getParams();
      final String dtdcField = params.get(DTDC_FIELD_PARAM);
      final int dtdcLimit = params.getInt(DTDC_LIMIT_PARAM, 1);
      if (dtdcField == null) {
        return super.newResponseBuilder(req, rsp, components);
      }
      return new ResponseBuilder(req, rsp, components) {
        @Override
        public QueryCommand newQueryCommand() {
          return new QueryCommand() {
            @Override
            public TopDocsCollector<? extends ScoreDoc> buildCustomTopDocsCollector(int len) {
              // Mirrors Lucene's HashedDocValuesDiversifiedCollector from
              // org.apache.lucene.misc.search.TestDiversifiedTopDocsCollector.
              return new DiversifiedTopDocsCollector(len, dtdcLimit) {
                private final String field = dtdcField;
                private SortedDocValues vals;

                @Override
                protected NumericDocValues getKeys(LeafReaderContext context) {
                  return new NumericDocValues() {
                    @Override
                    public int docID() {
                      return vals.docID();
                    }

                    @Override
                    public int nextDoc() throws IOException {
                      return vals.nextDoc();
                    }

                    @Override
                    public int advance(int target) throws IOException {
                      return vals.advance(target);
                    }

                    @Override
                    public boolean advanceExact(int target) throws IOException {
                      return vals.advanceExact(target);
                    }

                    @Override
                    public long cost() {
                      return vals.cost();
                    }

                    @Override
                    public long longValue() throws IOException {
                      return vals == null ? -1 : vals.lookupOrd(vals.ordValue()).hashCode();
                    }
                  };
                }

                @Override
                public LeafCollector getLeafCollector(LeafReaderContext context)
                    throws IOException {
                  this.vals = DocValues.getSorted(context.reader(), field);
                  return super.getLeafCollector(context);
                }
              };
            }
          };
        }
      };
    }
  }
}
