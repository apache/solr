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

package org.apache.solr.jersey;

import static org.apache.solr.SolrTestCaseJ4.TEST_PATH;
import static org.apache.solr.SolrTestCaseJ4.copyMinConf;

import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.solr.SolrTestCase;
import org.apache.solr.api.JerseyResource;
import org.apache.solr.client.api.endpoint.QUERY;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.jetty.HttpJettySolrClient;
import org.apache.solr.client.solrj.request.V2Request;
import org.apache.solr.client.solrj.response.json.JsonMapResponseParser;
import org.apache.solr.common.util.Utils;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.security.AuthorizationContext;
import org.apache.solr.security.PermissionNameProvider;
import org.apache.solr.util.SolrJettyTestRule;
import org.eclipse.jetty.client.StringRequestContent;
import org.eclipse.jetty.http.HttpVersion;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

/** Exercises an extension HTTP method through Jetty, Solr dispatch, and Jersey. */
public class HttpQueryIntegrationTest extends SolrTestCase {
  @ClassRule public static final SolrJettyTestRule solrTestRule = new SolrJettyTestRule();

  @BeforeClass
  public static void setupSolr() throws Exception {
    final var solrHome = createTempDir();
    Files.copy(TEST_PATH().resolve("solr.xml"), solrHome.resolve("solr.xml"));
    final var coreDir = solrHome.resolve("collection1");
    copyMinConf(coreDir, "name=collection1\n", "solrconfig-minimal.xml");
    final var config = coreDir.resolve("conf/solrconfig.xml");
    Files.writeString(
        config,
        Files.readString(config)
            .replaceAll("<xi:include[^>]*/>", "")
            .replace(
                "</config>",
                "<requestHandler name=\"/query-spike-registration\" class=\""
                    + QueryHandler.class.getName()
                    + "\"/>\n</config>"));
    solrTestRule.startSolr(solrHome);
  }

  @Test
  public void testQueryWithJsonBody() throws Exception {
    for (var version : List.of(HttpVersion.HTTP_1_1, HttpVersion.HTTP_2)) {
      try (var solrClient =
          new HttpJettySolrClient.Builder(solrTestRule.getBaseUrl())
              .useHttp1_1(version == HttpVersion.HTTP_1_1)
              .build()) {
        final var response =
            solrClient
                .getHttpClient()
                .newRequest(queryUrl())
                .version(version)
                .method("QUERY")
                .body(
                    new StringRequestContent(
                        "application/json", "{\"query\":\"id:42\"}", StandardCharsets.UTF_8))
                .send();
        assertEquals(response.getContentAsString(), 200, response.getStatus());
        assertEquals(version, response.getVersion());
        assertEquals(
            "id:42",
            ((Map<?, ?>) Utils.fromJSONString(response.getContentAsString())).get("query"));
      }
    }
  }

  @Test
  public void testQueryThroughSolrJ() throws Exception {
    final var request =
        new V2Request.Builder("/cores/collection1/query-spike")
            .withMethod(SolrRequest.METHOD.QUERY)
            .withPayload("{\"query\":\"id:42\"}")
            .build();
    request.setResponseParser(new JsonMapResponseParser());
    assertEquals("id:42", solrTestRule.getJetty().getSolrClient().request(request).get("query"));
  }

  @Test
  public void testPostDoesNotInvokeQueryEndpoint() throws Exception {
    final var response =
        solrTestRule
            .getJetty()
            .getSolrClient()
            .getHttpClient()
            .newRequest(queryUrl())
            .method("POST")
            .body(
                new StringRequestContent(
                    "application/json", "{\"query\":\"id:42\"}", StandardCharsets.UTF_8))
            .send();
    assertEquals(response.getContentAsString(), 405, response.getStatus());
  }

  private String queryUrl() {
    return solrTestRule.getJetty().getBaseURLV2() + "/cores/collection1/query-spike";
  }

  /** Registers the test resource with the core's Jersey application. */
  public static class QueryHandler extends RequestHandlerBase {
    @Override
    public PermissionNameProvider.Name getPermissionName(AuthorizationContext request) {
      return PermissionNameProvider.Name.READ_PERM;
    }

    @Override
    public Boolean registerV2() {
      return true;
    }

    @Override
    public Collection<Class<? extends JerseyResource>> getJerseyResources() {
      return List.of(QueryResource.class);
    }

    @Override
    public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) {}

    @Override
    public String getDescription() {
      return "HTTP QUERY spike";
    }
  }

  /** Test-only V2 endpoint that echoes the JSON query to verify entity handling. */
  @Path("/cores/{coreName}/query-spike")
  public static class QueryResource extends JerseyResource {
    @QUERY
    @Consumes("application/json")
    @Produces("application/json")
    @PermissionName(PermissionNameProvider.Name.READ_PERM)
    public Map<String, Object> query(Map<String, Object> body) {
      return body;
    }
  }
}
