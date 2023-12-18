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

import java.util.Map;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.response.schema.SchemaResponse;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.handler.SchemaHandler;
import org.apache.solr.handler.api.V2ApiUtils;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.schema.SimilarityFactory;
import org.apache.solr.schema.StrField;
import org.junit.Before;
import org.junit.Test;

/** Unit tests for {@link GetSchemaAPI} */
public class GetSchemaAPITest extends SolrTestCaseJ4 {

  private IndexSchema mockSchema;
  private GetSchemaAPI api;

  @Before
  public void setUpMocks() {
    assumeWorkingMockito();

    mockSchema = mock(IndexSchema.class);
    api = new GetSchemaAPI(mockSchema);
  }

  @Test
  public void testReliesOnIndexSchemaWhenFetchingWholeSchema() {
    when(mockSchema.getNamedPropertyValues()).thenReturn(Map.of("flagKey", "flagValue"));

    final var response = api.getSchemaInfo();

    assertNotNull(response);
    assertNotNull(response.schema);
    assertEquals(1, response.schema.size());
    assertEquals("flagValue", response.schema.get("flagKey"));
  }

  @Test
  public void testLooksUpNameFromLatestCoreSchema() throws Exception {
    when(mockSchema.getSchemaName()).thenReturn("expectedSchemaName");

    final GetSchemaAPI.SchemaNameResponse response = api.getSchemaName();

    assertEquals("expectedSchemaName", response.name);
    assertNull(response.error);
  }

  /**
   * Test the v2 to v1 response mapping for /schema/name
   *
   * <p>{@link SchemaHandler} uses the v2 {@link GetSchemaAPI} (and its response class {@link
   * GetSchemaAPI.SchemaNameResponse}) internally to serve the v1 version of this functionality. So
   * it's important to make sure that our response stays compatible with SolrJ - both because that's
   * important in its own right and because that ensures we haven't accidentally changed the v1
   * response format.
   */
  @Test
  public void testResponseCanBeParsedBySolrJ() {
    final NamedList<Object> squashedResponse = new NamedList<>();
    final GetSchemaAPI.SchemaNameResponse typedResponse = new GetSchemaAPI.SchemaNameResponse();
    typedResponse.name = "someName";

    V2ApiUtils.squashIntoNamedList(squashedResponse, typedResponse);
    final SchemaResponse.SchemaNameResponse solrjResponse = new SchemaResponse.SchemaNameResponse();
    solrjResponse.setResponse(squashedResponse);

    assertEquals("someName", solrjResponse.getSchemaName());
  }

  @Test
  public void testReliesOnIndexSchemaWhenFetchingSimilarity() {
    final var map = new SimpleOrderedMap<Object>();
    map.add("flagKey", "flagValue");
    final SimilarityFactory mockSimFactory = mock(SimilarityFactory.class);
    when(mockSimFactory.getNamedPropertyValues()).thenReturn(map);
    when(mockSchema.getSimilarityFactory()).thenReturn(mockSimFactory);

    final var response = api.getSchemaSimilarity();

    assertNotNull(response);
    assertNotNull(response.similarity);
    assertEquals(1, response.similarity.size());
    assertEquals("flagValue", response.similarity.get("flagKey"));
  }

  @Test
  public void testReliesOnIndexSchemaWhenFetchingUniqueKey() {
    when(mockSchema.getUniqueKeyField()).thenReturn(new SchemaField("myUniqueKey", new StrField()));

    final var response = api.getSchemaUniqueKey();

    assertNotNull(response);
    assertEquals("myUniqueKey", response.uniqueKey);
  }

  @Test
  public void testReliesOnIndexSchemaWhenFetchingVersion() {
    when(mockSchema.getVersion()).thenReturn(123.456f);

    final var response = api.getSchemaVersion();

    assertNotNull(response);
    assertEquals(123.456f, response.version, 0.1f);
  }
}
