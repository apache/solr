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
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class NumFieldLimitingUpdateRequestProcessorIntegrationTest extends SolrCloudTestCase {

  private static String COLLECTION_NAME = "collName";
  private static String FIELD_LIMITING_CS_NAME = "fieldLimitingConfig";

  @BeforeClass
  public static void setupCluster() throws Exception {
    final var configPath =
        TEST_PATH().resolve("configsets").resolve("cloud-minimal-field-limiting").resolve("conf");
    configureCluster(1).addConfig(FIELD_LIMITING_CS_NAME, configPath).configure();

    System.setProperty("solr.test.fieldLimit.warnOnly", "false");
    System.setProperty("solr.test.maxFields", String.valueOf(100));
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();

    System.setProperty("solr.test.fieldLimit.warnOnly", "false");
    System.setProperty("solr.test.maxFields", String.valueOf(100));

    // Collection might already exist if test is being run multiple times
    final var collections = CollectionAdminRequest.listCollections(cluster.getSolrClient());
    if (collections.contains(COLLECTION_NAME)) {
      final var deleteRequest = CollectionAdminRequest.deleteCollection(COLLECTION_NAME);
      deleteRequest.processAndWait(cluster.getSolrClient(), DEFAULT_TIMEOUT);
    }

    final var createRequest =
        CollectionAdminRequest.createCollection(COLLECTION_NAME, FIELD_LIMITING_CS_NAME, 1, 1);
    createRequest.process(cluster.getSolrClient());
    cluster.waitForActiveCollection(COLLECTION_NAME, 20, TimeUnit.SECONDS, 1, 1);
  }

  @Test
  public void test() throws Exception {
    setFieldLimitTo(100);

    // Add 100 new fields - should all succeed since we're under the limit until the final commit
    for (int i = 0; i < 5; i++) {
      addNewFieldsAndCommit(20);
    }

    // Adding any additional docs should fail because we've exceeded the field limit
    ensureNewFieldCreationTriggersExpectedFailure();

    // Switch to warn only mode and ensure that updates succeed again
    setWarnOnly(true);
    ensureNewFieldsCanBeCreated();

    // Switch back to "hard" limit mode and ensure things fail again
    setWarnOnly(false);
    ensureNewFieldCreationTriggersExpectedFailure();

    // Raise the limit and ensure that updates succeed again
    setFieldLimitTo(300);
    ensureNewFieldsCanBeCreated();
  }

  private void ensureNewFieldsCanBeCreated() throws Exception {
    for (int i = 0; i < 3; i++) {
      addNewFieldsAndCommit(10);
    }
  }

  private void ensureNewFieldCreationTriggersExpectedFailure() throws Exception {
    final var thrown =
        expectThrows(
            Exception.class,
            () -> {
              addNewFieldsAndCommit(10);
            });
    assertThat(
        thrown.getMessage(), Matchers.containsString("exceeding the max-fields limit of 100"));
  }

  private void setWarnOnly(boolean warnOnly) throws Exception {
    System.setProperty("solr.test.fieldLimit.warnOnly", String.valueOf(warnOnly));

    final var reloadRequest = CollectionAdminRequest.reloadCollection(COLLECTION_NAME);
    final var reloadResponse = reloadRequest.process(cluster.getSolrClient());
    assertEquals(0, reloadResponse.getStatus());
  }

  private void setFieldLimitTo(int value) throws Exception {
    System.setProperty("solr.test.maxFields", String.valueOf(value));

    final var reloadRequest = CollectionAdminRequest.reloadCollection(COLLECTION_NAME);
    final var reloadResponse = reloadRequest.process(cluster.getSolrClient());
    assertEquals(0, reloadResponse.getStatus());
  }

  private void addNewFieldsAndCommit(int numFields) throws Exception {
    final var docList = getDocumentListToAddFields(numFields);
    final var updateResponse = cluster.getSolrClient(COLLECTION_NAME).add(docList);
    assertEquals(0, updateResponse.getStatus());
    cluster.getSolrClient(COLLECTION_NAME).commit();
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
