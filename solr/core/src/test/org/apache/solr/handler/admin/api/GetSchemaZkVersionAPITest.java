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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.core.SolrCore;
import org.apache.solr.schema.IndexSchema;
import org.junit.Before;
import org.junit.Test;

/** Unit tests for {@link GetSchemaZkVersionAPI} */
public class GetSchemaZkVersionAPITest extends SolrTestCaseJ4 {

  private SolrCore mockCore;
  private IndexSchema mockSchema;
  private GetSchemaZkVersionAPI api;

  @Before
  public void setUpMocks() {
    assumeWorkingMockito();

    mockCore = mock(SolrCore.class);
    mockSchema = mock(IndexSchema.class);
    when(mockCore.getLatestSchema()).thenReturn(mockSchema);
    api = new GetSchemaZkVersionAPI(mockCore);
  }

  @Test
  public void testReturnsInvalidZkVersionWhenNotManagedIndexSchema() throws Exception {

    final var response = api.getSchemaZkVersion(-1);

    assertNotNull(response);
    assertEquals(-1, response.zkversion);
  }
}
