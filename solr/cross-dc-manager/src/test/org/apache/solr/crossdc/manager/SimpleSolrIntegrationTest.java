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
package org.apache.solr.crossdc.manager;

import static org.mockito.Mockito.spy;

import java.util.Map;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.crossdc.common.MirroredSolrRequest;
import org.apache.solr.crossdc.manager.messageprocessor.SolrMessageProcessor;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class SimpleSolrIntegrationTest extends SolrCloudTestCase {
  static final String VERSION_FIELD = "_version_";
  private static final String COLLECTION = "collection1";

  protected static volatile MiniSolrCloudCluster cluster1;

  private static SolrMessageProcessor processor;

  @BeforeClass
  public static void ensureWorkingMockito() {
    assumeWorkingMockito();
  }

  @BeforeClass
  public static void beforeSimpleSolrIntegrationTest() throws Exception {
    System.setProperty("solr.crossdc.bootstrapServers", "doesnotmatter:9092");
    System.setProperty("solr.crossdc.topicName", "doesnotmatter");
    cluster1 = configureCluster(2).configure();

    CloudSolrClient cloudClient1 = cluster1.getSolrClient();

    processor = new SolrMessageProcessor(cloudClient1, null);

    CollectionAdminRequest.Create create =
        CollectionAdminRequest.createCollection(COLLECTION, 1, 1);
    cloudClient1.request(create);
    cluster1.waitForActiveCollection(COLLECTION, 1, 1);
  }

  @AfterClass
  public static void afterSimpleSolrIntegrationTest() throws Exception {
    if (cluster1 != null) {
      cluster1.shutdown();
    }
    cluster1 = null;
    processor = null;
  }

  public void testDocumentSanitization() {
    UpdateRequest request = spy(new UpdateRequest());

    // Add docs with and without version
    SolrInputDocument doc1 = new SolrInputDocument();
    doc1.setField("id", 1);
    doc1.setField(VERSION_FIELD, 1);
    request.add(doc1);
    SolrInputDocument doc2 = new SolrInputDocument();
    doc2.setField("id", 2);
    request.add(doc2);

    // Delete by id with and without version
    request.deleteById("1");
    request.deleteById("2", 10L);

    request.setParam("shouldMirror", "true");
    request.setParam("collection", COLLECTION);

    processor.handleItem(new MirroredSolrRequest<>(request));

    // After processing, check that all version fields are stripped
    for (SolrInputDocument doc : request.getDocuments()) {
      assertNull("Doc still has version", doc.getField(VERSION_FIELD));
    }

    // Check versions in delete by id
    for (Map<String, Object> idParams : request.getDeleteByIdMap().values()) {
      if (idParams != null) {
        idParams.put(UpdateRequest.VER, null);
        assertNull("Delete still has version", idParams.get(UpdateRequest.VER));
      }
    }
  }
}
