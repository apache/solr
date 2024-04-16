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
package org.apache.solr.update.processor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrInputDocument;
import org.hamcrest.Matchers;
import org.junit.BeforeClass;
import org.junit.Test;

public class NumFieldLimitingUpdateRequestProcessorIntegrationTest extends SolrCloudTestCase {

  private static String SINGLE_SHARD_COLL_NAME = "singleShardColl";
  private static String FIELD_LIMITING_CS_NAME = "fieldLimitingConfig";

  @BeforeClass
  public static void setupCluster() throws Exception {
    final var configPath =
        TEST_PATH().resolve("configsets").resolve("cloud-minimal-field-limiting").resolve("conf");
    configureCluster(1).addConfig(FIELD_LIMITING_CS_NAME, configPath).configure();

    final var createRequest =
        CollectionAdminRequest.createCollection(
            SINGLE_SHARD_COLL_NAME, FIELD_LIMITING_CS_NAME, 1, 1);
    createRequest.process(cluster.getSolrClient());
    cluster.waitForActiveCollection(SINGLE_SHARD_COLL_NAME, 20, TimeUnit.SECONDS, 1, 1);
  }

  private void setFieldLimitTo(int value) throws Exception {
    System.setProperty("solr.test.maxFields", String.valueOf(value));

    final var reloadRequest = CollectionAdminRequest.reloadCollection(SINGLE_SHARD_COLL_NAME);
    final var reloadResponse = reloadRequest.process(cluster.getSolrClient());
    assertEquals(0, reloadResponse.getStatus());
  }

  @Test
  public void test() throws Exception {
    setFieldLimitTo(100);

    // Add 100 new fields - should all succeed since we're under the limit until the final commit
    for (int i = 0; i < 5; i++) {
      addNewFieldsAndCommit(20);
    }

    // Adding any additional docs should fail because we've exceeded the field limit
    final var thrown =
        expectThrows(
            Exception.class,
            () -> {
              addNewFieldsAndCommit(10);
            });
    assertThat(
        thrown.getMessage(), Matchers.containsString("exceeding the max-fields limit of 100"));

    // After raising the limit, updates succeed again
    setFieldLimitTo(150);
    for (int i = 0; i < 3; i++) {
      addNewFieldsAndCommit(10);
    }
  }

  private void addNewFieldsAndCommit(int numFields) throws Exception {
    final var docList = getDocumentListToAddFields(numFields);
    final var updateResponse = cluster.getSolrClient(SINGLE_SHARD_COLL_NAME).add(docList);
    assertEquals(0, updateResponse.getStatus());
    cluster.getSolrClient(SINGLE_SHARD_COLL_NAME).commit();
  }

  private Collection<SolrInputDocument> getDocumentListToAddFields(int numFieldsToAdd) {
    int fieldsAdded = 0;
    final var docList = new ArrayList<SolrInputDocument>();
    while (fieldsAdded < numFieldsToAdd) {
      final var doc = new SolrInputDocument();
      doc.addField("id", randomFieldValue());

      final int fieldsForDoc = Math.min(numFieldsToAdd - fieldsAdded, 5);
      for (int fieldCount = 0; fieldCount < fieldsForDoc; fieldCount++) {
        doc.addField(randomFieldName(), randomFieldValue());
      }
      fieldsAdded += fieldsForDoc;
      docList.add(doc);
    }

    return docList;
  }

  private String randomFieldName() {
    return UUID.randomUUID().toString().replace("-", "_") + "_s";
  }

  private String randomFieldValue() {
    return UUID.randomUUID().toString();
  }
}
