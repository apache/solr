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
package org.apache.solr.handler;

import static java.util.stream.Collectors.toList;
import static org.apache.solr.client.solrj.SolrRequest.METHOD.POST;
import static org.apache.solr.core.CoreContainer.ALLOW_PATHS_SYSPROP;
import static org.hamcrest.Matchers.containsString;

import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.solr.SolrTestCase;
import org.apache.solr.client.solrj.RemoteSolrException;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.client.solrj.request.GenericV2SolrRequest;
import org.apache.solr.client.solrj.request.SolrQuery;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.EnvUtils;
import org.apache.solr.util.ExternalPaths;
import org.apache.solr.util.SolrJettyTestRule;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

/** Tests the NDJSON (JSON Lines) support of the v1 and v2 /update endpoints. */
public class NDJsonUpdateTest extends SolrTestCase {

  private static final String COLLECTION = "ndjson";

  /** Three documents, a blank line to be skipped, and non-ASCII values to verify encoding. */
  private static final String NDJSON =
      "{\"id\":\"1\",\"title_s\":\"one\"}\n"
          + "\n"
          + "{\"id\":\"2\",\"title_s\":\"Bl\u00e5b\u00e6rsyltet\u00f8y\"}\n"
          + "{\"id\":\"3\",\"title_s\":\"\u65e5\u672c\u8a9e\"}\n";

  @ClassRule public static SolrJettyTestRule solrTestRule = new SolrJettyTestRule();

  @BeforeClass
  public static void setupSolr() throws Exception {
    EnvUtils.setProperty(
        ALLOW_PATHS_SYSPROP, ExternalPaths.SERVER_HOME.toAbsolutePath().toString());
    solrTestRule.startSolr();
    solrTestRule.newCollection(COLLECTION).withConfigSet(ExternalPaths.DEFAULT_CONFIGSET).create();
  }

  @Before
  public void clearIndex() throws Exception {
    SolrClient client = solrTestRule.getSolrClient(COLLECTION);
    client.deleteByQuery("*:*");
    client.commit();
  }

  @Test
  public void testV1UpdateByContentType() throws Exception {
    post(new GenericSolrRequest(POST, "/update", commitParams()), "application/x-ndjson");
    assertIndexed();
  }

  @Test
  public void testV1NdJsonPath() throws Exception {
    post(new GenericSolrRequest(POST, "/update/ndjson", commitParams()), "application/jsonl");
    assertIndexed();
  }

  @Test
  public void testV2UpdateByContentType() throws Exception {
    post(new GenericV2SolrRequest(POST, v2Path("/update"), commitParams()), "application/x-ndjson");
    assertIndexed();
  }

  @Test
  public void testV2NdJsonPath() throws Exception {
    post(
        new GenericV2SolrRequest(POST, v2Path("/update/ndjson"), commitParams()),
        "application/x-ndjson");
    assertIndexed();
  }

  /** The dedicated path selects NDJSON regardless of the content type, as /update/csv does. */
  @Test
  public void testNdJsonPathWinsOverContentType() throws Exception {
    post(new GenericSolrRequest(POST, "/update/ndjson", commitParams()), "application/json");
    assertIndexed();

    clearIndex();
    post(
        new GenericV2SolrRequest(POST, v2Path("/update/ndjson"), commitParams()),
        "application/json");
    assertIndexed();
  }

  @Test
  public void testContentTypeWithCharsetParameter() throws Exception {
    post(
        new GenericSolrRequest(POST, "/update", commitParams()),
        "application/x-ndjson; charset=utf-8");
    assertIndexed();

    clearIndex();
    post(
        new GenericV2SolrRequest(POST, v2Path("/update"), commitParams()),
        "application/x-ndjson; charset=utf-8");
    assertIndexed();
  }

  @Test
  public void testV2UpdateStillDefaultsToJson() throws Exception {
    GenericV2SolrRequest req = new GenericV2SolrRequest(POST, v2Path("/update"), commitParams());
    req.withContent(
        "[{\"id\":\"1\"},{\"id\":\"2\"},{\"id\":\"3\"}]".getBytes(StandardCharsets.UTF_8),
        "application/json");
    req.process(solrTestRule.getAdminClient());
    assertEquals(List.of("1", "2", "3"), query().stream().map(d -> d.get("id")).collect(toList()));
  }

  @Test
  public void testMalformedLineIsRejected() {
    RemoteSolrException e =
        expectThrows(
            RemoteSolrException.class,
            () -> {
              GenericSolrRequest req = new GenericSolrRequest(POST, "/update", commitParams());
              req.setRequiresCollection(true);
              req.withContent(
                  "{\"id\":\"1\"}\nnot json\n".getBytes(StandardCharsets.UTF_8),
                  "application/x-ndjson");
              req.process(solrTestRule.getSolrClient(COLLECTION));
            });
    assertEquals(400, e.code());
    assertThat(e.getMessage(), containsString("line 2"));
  }

  private static String v2Path(String suffix) {
    return "/cores/" + COLLECTION + suffix;
  }

  private static ModifiableSolrParams commitParams() {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("commit", "true");
    return params;
  }

  private static void post(GenericSolrRequest req, String contentType) throws Exception {
    boolean v2 = req instanceof GenericV2SolrRequest;
    req.setRequiresCollection(!v2);
    req.withContent(NDJSON.getBytes(StandardCharsets.UTF_8), contentType);
    req.process(v2 ? solrTestRule.getAdminClient() : solrTestRule.getSolrClient(COLLECTION));
  }

  /** Asserts that the three documents of {@link #NDJSON} were indexed, blank line skipped. */
  private static void assertIndexed() throws Exception {
    SolrDocumentList docs = query();
    assertEquals(3, docs.getNumFound());
    assertEquals(List.of("1", "2", "3"), docs.stream().map(d -> d.get("id")).collect(toList()));
    assertEquals(
        List.of("one", "Bl\u00e5b\u00e6rsyltet\u00f8y", "\u65e5\u672c\u8a9e"),
        docs.stream().map(d -> d.get("title_s")).collect(toList()));
  }

  private static SolrDocumentList query() throws Exception {
    SolrQuery q = new SolrQuery("*:*").setSort("id", SolrQuery.ORDER.asc);
    return solrTestRule.getSolrClient(COLLECTION).query(q).getResults();
  }
}
