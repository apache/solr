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

package org.apache.solr.schema;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.request.schema.SchemaRequest;
import org.apache.solr.client.solrj.response.schema.SchemaResponse;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrInputDocument;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestManagedSchemaWithMultipleAdd extends SolrCloudTestCase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final int AUTOSOFTCOMMIT_MAXTIME_MS = 3000;

  @BeforeClass
  public static void createClusterAndInitSysProperties() throws Exception {
    System.setProperty("managed.schema.mutable", "true");
    System.setProperty("solr.autoSoftCommit.maxTime", Integer.toString(AUTOSOFTCOMMIT_MAXTIME_MS));
    configureCluster(1)
        .addConfig(
            "conf1",
            TEST_PATH().resolve("configsets").resolve("cloud-managed-autocommit").resolve("conf"))
        .configure();
  }

  @AfterClass
  public static void afterRestartWhileUpdatingTest() {
    System.clearProperty("managed.schema.mutable");
    System.clearProperty("solr.autoSoftCommit.maxTime");
  }

  @Test
  public void test() throws Exception {
    String collection = "testschemaapi";
    CollectionAdminRequest.createCollection(collection, "conf1", 1, 1)
        .process(cluster.getSolrClient());
    testAddFieldAndMultipleDocument(collection);
  }

  private void testAddFieldAndMultipleDocument(String collection)
      throws IOException, SolrServerException, InterruptedException {

    CloudSolrClient cloudClient = cluster.getSolrClient();

    String fieldName = "myNewField1";
    addStringField(fieldName, collection, cloudClient);

    UpdateRequest ureq = new UpdateRequest();
    int numDocs = 1000;
    for (int i = 0; i < numDocs; i++) {
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField("id", i);
      doc.addField(fieldName, "value" + i);
      ureq = ureq.add(doc);
    }
    cloudClient.request(ureq, collection);

    // The issue we test in this class does not appear when using explicit commits.
    // Because of this we are waiting for autoSoftCommit to finish if there is one.
    Thread.sleep(AUTOSOFTCOMMIT_MAXTIME_MS + 500);

    assertEquals(
        numDocs, cloudClient.query(collection, new SolrQuery("*:*")).getResults().getNumFound());
  }

  private void addStringField(String fieldName, String collection, CloudSolrClient cloudClient)
      throws IOException, SolrServerException {
    Map<String, Object> fieldAttributes = new LinkedHashMap<>();
    fieldAttributes.put("name", fieldName);
    fieldAttributes.put("type", "string");
    SchemaRequest.AddField addFieldUpdateSchemaRequest =
        new SchemaRequest.AddField(fieldAttributes);
    SchemaResponse.UpdateResponse addFieldResponse =
        addFieldUpdateSchemaRequest.process(cloudClient, collection);
    assertEquals(0, addFieldResponse.getStatus());
    assertNull(addFieldResponse.getResponse().get("errors"));

    if (log.isInfoEnabled()) {
      log.info("added new field = {}", fieldName);
    }
  }
}
