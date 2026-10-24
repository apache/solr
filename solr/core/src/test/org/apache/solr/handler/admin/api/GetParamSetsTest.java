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

import static org.apache.solr.core.CoreContainer.ALLOW_PATHS_SYSPROP;

import java.nio.file.Path;
import java.util.Map;
import org.apache.commons.io.file.PathUtils;
import org.apache.solr.SolrTestCase;
import org.apache.solr.client.api.model.IndexType;
import org.apache.solr.client.solrj.RemoteSolrException;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.request.ParamSetsApi;
import org.apache.solr.client.solrj.request.V2Request;
import org.apache.solr.util.ExternalPaths;
import org.apache.solr.util.SolrJettyTestRule;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

/** HTTP tests for fetching request param sets through the generated SolrJ request. */
public class GetParamSetsTest extends SolrTestCase {

  private static final String CORE_NAME = "paramSetsApiTestCore";

  @ClassRule public static final SolrJettyTestRule solrTestRule = new SolrJettyTestRule();

  @BeforeClass
  public static void setupSolr() throws Exception {
    // setting param sets writes params.json into the configset, so use a private copy
    Path configSet = createTempDir("configSet");
    System.setProperty(ALLOW_PATHS_SYSPROP, configSet.toAbsolutePath().toString());
    solrTestRule.startSolr(createTempDir());
    PathUtils.copyDirectory(ExternalPaths.DEFAULT_CONFIGSET, configSet);
    solrTestRule.newCollection(CORE_NAME).withConfigSet(configSet).create();
  }

  private static void setParamSet(String name, String payloadBody) throws Exception {
    new V2Request.Builder("/cores/" + CORE_NAME + "/config/params")
        .withMethod(SolrRequest.METHOD.POST)
        .withPayload("{'set': {'" + name + "': " + payloadBody + "}}")
        .build()
        .process(solrTestRule.getSolrClient(CORE_NAME));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testListParamSets() throws Exception {
    setParamSet("listA", "{'a': 'A val'}");
    setParamSet("listB", "{'b': 'B val'}");

    var response =
        new ParamSetsApi.ListParamSets(IndexType.CORE, CORE_NAME)
            .process(solrTestRule.getSolrClient(CORE_NAME));

    assertNotNull(response);
    assertNull(response.error);
    assertTrue(response.znodeVersion >= 0);
    assertNotNull(response.params);
    assertEquals("A val", ((Map<String, Object>) response.params.get("listA")).get("a"));
    assertEquals("B val", ((Map<String, Object>) response.params.get("listB")).get("b"));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testGetSingleParamSet() throws Exception {
    setParamSet("single", "{'a': 'A val', '_invariants_': {'wt': 'json'}}");

    var response =
        new ParamSetsApi.GetParamSet(IndexType.CORE, CORE_NAME, "single")
            .process(solrTestRule.getSolrClient(CORE_NAME));

    assertNull(response.error);
    assertEquals(1, response.params.size());
    Map<String, Object> paramSet = (Map<String, Object>) response.params.get("single");
    assertEquals("A val", paramSet.get("a"));
    assertEquals(Map.of("wt", "json"), paramSet.get("_invariants_"));
  }

  @Test
  public void testGetUnknownParamSetReturnsNotFound() {
    var request = new ParamSetsApi.GetParamSet(IndexType.CORE, CORE_NAME, "nonExistentParamSet");
    final RemoteSolrException ex =
        expectThrows(
            RemoteSolrException.class,
            () -> request.process(solrTestRule.getSolrClient(CORE_NAME)));
    assertEquals(404, ex.code());
    assertTrue(ex.getMessage(), ex.getMessage().contains("nonExistentParamSet"));
  }
}
