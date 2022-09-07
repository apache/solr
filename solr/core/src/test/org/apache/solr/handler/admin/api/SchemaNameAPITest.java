/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.solr.handler.admin.api;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.response.schema.SchemaResponse;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.SchemaHandler;
import org.apache.solr.handler.api.V2ApiUtils;
import org.apache.solr.schema.IndexSchema;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/** Unit tests for {@link SchemaNameAPI} */
public class SchemaNameAPITest extends SolrTestCaseJ4 {

  private SolrCore solrCore;
  private IndexSchema schema;

  @BeforeClass
  public static void ensureWorkingMockito() {
    assumeWorkingMockito();
  }

  @Before
  public void initMocks() {
    solrCore = mock(SolrCore.class);
    schema = mock(IndexSchema.class);
    when(solrCore.getLatestSchema()).thenReturn(schema);
  }

  @Test
  public void testLooksUpNameFromLatestCoreSchema() throws Exception {
    when(schema.getSchemaName()).thenReturn("expectedSchemaName");
    final SchemaNameAPI nameApi = new SchemaNameAPI(solrCore);

    final SchemaNameAPI.GetSchemaNameResponse response = nameApi.getSchemaName();

    assertEquals("expectedSchemaName", response.name);
    assertNull(response.error);
  }

  /**
   * Test the v2 to v1 response mapping for /schema/name
   *
   * <p>{@link SchemaHandler} uses the v2 {@link SchemaNameAPI} (and its response class {@link
   * SchemaNameAPI.GetSchemaNameResponse}) internally to serve the v1 version of this functionality.
   * So it's important to make sure that our response stays compatible with SolrJ - both because
   * that's important in its own right and because that ensures we haven't accidentally changed the
   * v1 response format.
   */
  @Test
  public void testResponseCanBeParsedBySolrJ() {
    final NamedList<Object> squashedResponse = new NamedList<>();
    final SchemaNameAPI.GetSchemaNameResponse typedResponse =
        new SchemaNameAPI.GetSchemaNameResponse();
    typedResponse.name = "someName";

    V2ApiUtils.squashIntoNamedList(squashedResponse, typedResponse);
    final SchemaResponse.SchemaNameResponse solrjResponse = new SchemaResponse.SchemaNameResponse();
    solrjResponse.setResponse(squashedResponse);

    assertEquals("someName", solrjResponse.getSchemaName());
  }
}
