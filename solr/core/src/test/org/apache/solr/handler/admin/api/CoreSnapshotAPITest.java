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
package org.apache.solr.handler.admin.api;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class CoreSnapshotAPITest extends SolrTestCaseJ4 {

  private CoreSnapshotAPI coreSnapshotAPI;

  @BeforeClass
  public static void initializeCoreAndRequestFactory() throws Exception {
    initCore("solrconfig.xml", "schema.xml");

    lrf = h.getRequestFactory("/api", 0, 10);
  }

  @Before
  public void setUp() throws Exception {
    super.setUp();

    SolrQueryRequest solrQueryRequest = req();
    SolrQueryResponse solrQueryResponse = new SolrQueryResponse();
    CoreContainer coreContainer = h.getCoreContainer();

    coreSnapshotAPI = new CoreSnapshotAPI(solrQueryRequest, solrQueryResponse, coreContainer);
  }

  @Test
  public void testReportsErrorWhenCreatingSnapshotForNonexistentCore() {
    final String nonExistentCoreName = "non-existent";

    final SolrException solrException =
        expectThrows(
            SolrException.class,
            () -> {
              coreSnapshotAPI.createSnapshot(nonExistentCoreName, "my-new-snapshot", null);
            });
    assertEquals(400, solrException.code());
    assertTrue(
        "Exception message differed from expected: " + solrException.getMessage(),
        solrException.getMessage().contains("Unable to locate core " + nonExistentCoreName));
  }

  @Test
  public void testReportsErrorWhenListingSnapshotsForNonexistentCore() {
    final String nonExistentCoreName = "non-existent";

    final SolrException solrException =
        expectThrows(
            SolrException.class,
            () -> {
              coreSnapshotAPI.listSnapshots(nonExistentCoreName, null);
            });
    assertEquals(400, solrException.code());
    assertTrue(
        "Exception message differed from expected: " + solrException.getMessage(),
        solrException.getMessage().contains("Unable to locate core " + nonExistentCoreName));
  }

  @Test
  public void testReportsErrorWhenDeletingSnapshotForNonexistentCore() {
    final String nonExistentCoreName = "non-existent";

    final SolrException solrException =
        expectThrows(
            SolrException.class,
            () -> {
              coreSnapshotAPI.deleteSnapshot(nonExistentCoreName, "non-existent-snapshot", null);
            });
    assertEquals(400, solrException.code());
    assertTrue(
        "Exception message differed from expected: " + solrException.getMessage(),
        solrException.getMessage().contains("Unable to locate core " + nonExistentCoreName));
  }
}
