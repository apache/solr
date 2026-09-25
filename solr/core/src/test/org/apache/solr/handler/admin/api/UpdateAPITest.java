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

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.solr.SolrTestCase;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.client.solrj.request.GenericV2SolrRequest;
import org.apache.solr.client.solrj.request.JavaBinUpdateRequestCodec;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.RequestWriter;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.JavaBinResponseParser;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.XMLResponseParser;
import org.apache.solr.client.solrj.response.json.JsonMapResponseParser;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.EnvUtils;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.util.ExternalPaths;
import org.apache.solr.util.SolrJettyTestRule;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

/**
 * Integration tests for the v2 update API endpoints implemented via JAX-RS in {@link
 * org.apache.solr.handler.admin.api.UpdateAPI}.
 */
public class UpdateAPITest extends SolrTestCase {

  @ClassRule public static SolrJettyTestRule solrTestRule = new SolrJettyTestRule();

  private static final String CORE_NAME = "update-api-test";
  private static final String CUSTOM_JSON_CORE_NAME = "custom-json-update-api-test";

  @BeforeClass
  public static void beforeClass() throws Exception {
    EnvUtils.setProperty(
        ALLOW_PATHS_SYSPROP, ExternalPaths.SERVER_HOME.toAbsolutePath().toString());
    solrTestRule.startSolr(createTempDir());
    solrTestRule
        .newCollection(CORE_NAME)
        .withConfigSet(ExternalPaths.TECHPRODUCTS_CONFIGSET)
        .create();
    // can't use techproducts config because it enables srcField, which is incompatible with nested
    // split=/exams requests
    solrTestRule
        .newCollection(CUSTOM_JSON_CORE_NAME)
        .withConfigSet(ExternalPaths.DEFAULT_CONFIGSET)
        .create();
  }

  @Test
  public void testV1AndV2GenericUpdateParityAcrossFormats() throws Exception {
    final SolrClient client = solrTestRule.getSolrClient(CORE_NAME);

    for (UpdateFormat format : UpdateFormat.values()) {
      final String v1Id = "parity-v1-" + format.name().toLowerCase(Locale.ROOT);
      final String v2Id = "parity-v2-" + format.name().toLowerCase(Locale.ROOT);

      final NamedList<Object> v1Response = sendV1Update(client, format, v1Id);
      final NamedList<Object> v2Response = sendV2Update(client, format, v2Id);

      assertLegacySuccessfulAdd(format, v1Id, v1Response);
      assertTypedSuccessfulAdd(format, v2Id, v2Response);
      assertIndexed(client, v1Id);
      assertIndexed(client, v2Id);
    }
  }

  @Test
  public void testV1AndV2CustomJsonTransformParity() throws Exception {
    final SolrClient client = solrTestRule.getSolrClient(CUSTOM_JSON_CORE_NAME);
    final String payload = "{\"exams\":[{\"id\":\"custom-json-v1\",\"name\":\"V1 document\"}]}";

    final ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("split", "/exams");
    params.add("f", "id:/exams/id");
    params.add("f", "name_s:/exams/name");
    params.set("commit", true);

    final GenericSolrRequest v1Request =
        new GenericSolrRequest(SolrRequest.METHOD.POST, "/update/json/docs", params);
    v1Request.setRequiresCollection(true);
    v1Request.setContentWriter(
        new RequestWriter.StringPayloadContentWriter(payload, "application/json"));
    client.request(v1Request, CUSTOM_JSON_CORE_NAME);

    final String v2Payload = payload.replace("custom-json-v1", "custom-json-v2");
    final GenericV2SolrRequest v2Request =
        new GenericV2SolrRequest(
            SolrRequest.METHOD.POST, "/cores/" + CUSTOM_JSON_CORE_NAME + "/update/json", params);
    v2Request.setContentWriter(
        new RequestWriter.StringPayloadContentWriter(v2Payload, "application/json"));
    client.request(v2Request);

    assertIndexedField(client, CUSTOM_JSON_CORE_NAME, "custom-json-v1", "name_s", "V1 document");
    assertIndexedField(client, CUSTOM_JSON_CORE_NAME, "custom-json-v2", "name_s", "V1 document");
  }

  @Test
  public void testUpdateJsonViaV2Api() throws Exception {
    final SolrClient client = solrTestRule.getSolrClient(CORE_NAME);

    // POST via the V2 /update/json endpoint (also rewrites to /update/json/docs)
    final GenericV2SolrRequest addReq =
        new GenericV2SolrRequest(SolrRequest.METHOD.POST, "/cores/" + CORE_NAME + "/update/json");
    addReq.setContentWriter(
        new RequestWriter.StringPayloadContentWriter(
            "[{\"id\":\"v2updatejson1\",\"title\":\"V2 update/json test\"}]", "application/json"));
    client.request(addReq);

    // Commit via standard SolrJ commit (v2 /update is docs-only and does not support commands)
    client.commit(CORE_NAME);

    // Verify
    final ModifiableSolrParams queryParams = new ModifiableSolrParams();
    queryParams.set("q", "id:v2updatejson1");
    final QueryResponse queryRsp = new QueryRequest(queryParams).process(client, CORE_NAME);
    assertEquals(1, queryRsp.getResults().getNumFound());
  }

  @Test
  public void testGeneratedUpdateApiClientSetsAMatchingContentType() throws Exception {
    // The generated SolrJ client used to hardcode Content-Type: application/octet-stream for
    // this request body, which none of /update's @Consumes media types accept -- every call
    // failed with a 415. It must now let the caller supply a content type the server accepts.
    final SolrClient client = solrTestRule.getSolrClient(CORE_NAME);
    final String json = "[{\"id\":\"v2-generated-client-update1\"}]";
    final var request =
        new org.apache.solr.client.solrj.request.UpdateApi.Update(
            org.apache.solr.client.api.model.IndexType.CORE,
            CORE_NAME,
            new java.io.ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8)),
            "application/json");

    request.process(client);
    client.commit(CORE_NAME);

    assertIndexed(client, "v2-generated-client-update1");
  }

  @Test
  public void testUpdateXmlViaV2Api() throws Exception {
    final SolrClient client = solrTestRule.getSolrClient(CORE_NAME);

    // POST via the V2 /update/xml endpoint
    final GenericV2SolrRequest addReq =
        new GenericV2SolrRequest(SolrRequest.METHOD.POST, "/cores/" + CORE_NAME + "/update/xml");
    addReq.setContentWriter(
        new RequestWriter.StringPayloadContentWriter(
            "<add><doc><field name=\"id\">v2updatexml1</field>"
                + "<field name=\"title\">V2 update/xml test</field></doc></add>",
            "application/xml"));
    client.request(addReq);

    // Commit via standard SolrJ commit (v2 /update is docs-only and does not support commands)
    client.commit(CORE_NAME);

    // Verify
    final ModifiableSolrParams queryParams = new ModifiableSolrParams();
    queryParams.set("q", "id:v2updatexml1");
    final QueryResponse queryRsp = new QueryRequest(queryParams).process(client, CORE_NAME);
    assertEquals(1, queryRsp.getResults().getNumFound());
  }

  @Test
  public void testUpdateJavabinViaV2Api() throws Exception {
    final SolrClient client = solrTestRule.getSolrClient(CORE_NAME);
    final SolrInputDocument doc = new SolrInputDocument();
    doc.setField("id", "v2updatejavabin1");
    final UpdateRequest updateRequest = new UpdateRequest();
    updateRequest.add(doc);
    updateRequest.deleteById("v2deleteversion1");
    updateRequest.deleteByQuery("id:v2deletequery1");
    final ByteArrayOutputStream payload = new ByteArrayOutputStream();
    new JavaBinUpdateRequestCodec().marshal(updateRequest, payload);

    final ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("versions", "true");
    final GenericV2SolrRequest addReq =
        new GenericV2SolrRequest(
            SolrRequest.METHOD.POST, "/cores/" + CORE_NAME + "/update/javabin", params);
    addReq.setResponseParser(new JavaBinResponseParser());
    addReq.withContent(payload.toByteArray(), "application/javabin");
    final NamedList<Object> updateResponse = client.request(addReq);
    assertEquals(1, updateResponse.getAll("responseHeader").size());
    assertTypedVersion(updateResponse, "adds", "id", "v2updatejavabin1", true);
    assertTypedVersion(updateResponse, "deletes", "id", "v2deleteversion1", false);
    assertTypedVersion(updateResponse, "deleteByQuery", "query", "id:v2deletequery1", false);
    client.commit(CORE_NAME);

    final ModifiableSolrParams queryParams = new ModifiableSolrParams();
    queryParams.set("q", "id:v2updatejavabin1");
    final QueryResponse queryRsp = new QueryRequest(queryParams).process(client, CORE_NAME);
    assertEquals(1, queryRsp.getResults().getNumFound());
  }

  @Test
  public void testUpdateReturnsAssignedVersion() throws Exception {
    final SolrClient client = solrTestRule.getSolrClient(CORE_NAME);
    final ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("versions", "true");
    final GenericV2SolrRequest addReq =
        new GenericV2SolrRequest(
            SolrRequest.METHOD.POST, "/cores/" + CORE_NAME + "/update", params);
    addReq.setResponseParser(new JsonMapResponseParser());
    addReq.setContentWriter(
        new RequestWriter.StringPayloadContentWriter(
            "[{\"id\":\"v2version1\",\"name\":\"Generic V2 update document\"}]",
            "application/json"));

    final var response = client.request(addReq);
    assertTypedVersion(response, "adds", "id", "v2version1", true);
    client.commit(CORE_NAME);
    assertIndexedField(client, CORE_NAME, "v2version1", "name", "Generic V2 update document");
  }

  @Test
  public void testXmlUpdateResponseHasOneHeaderAndVersion() throws Exception {
    final SolrClient client = solrTestRule.getSolrClient(CORE_NAME);
    final ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("versions", "true");
    final GenericV2SolrRequest addReq =
        new GenericV2SolrRequest(
            SolrRequest.METHOD.POST, "/cores/" + CORE_NAME + "/update/xml", params);
    addReq.setResponseParser(new XMLResponseParser());
    addReq.setContentWriter(
        new RequestWriter.StringPayloadContentWriter(
            "<add><doc><field name=\"id\">v2xmlversion1</field></doc></add>", "application/xml"));

    final NamedList<Object> response = client.request(addReq);
    assertEquals(1, response.getAll("responseHeader").size());
    assertTypedVersion(response, "adds", "id", "v2xmlversion1", true);
  }

  private static NamedList<Object> sendV1Update(SolrClient client, UpdateFormat format, String id)
      throws Exception {
    final GenericSolrRequest request =
        new GenericSolrRequest(SolrRequest.METHOD.POST, "/update", updateParams());
    request.setRequiresCollection(true);
    request.setResponseParser(new JsonMapResponseParser());
    request.withContent(format.payload(id), format.contentType);
    return client.request(request, CORE_NAME);
  }

  private static NamedList<Object> sendV2Update(SolrClient client, UpdateFormat format, String id)
      throws Exception {
    final GenericV2SolrRequest request =
        new GenericV2SolrRequest(
            SolrRequest.METHOD.POST, "/cores/" + CORE_NAME + "/update", updateParams());
    request.setResponseParser(new JsonMapResponseParser());
    request.withContent(format.payload(id), format.contentType);
    return client.request(request);
  }

  private static ModifiableSolrParams updateParams() {
    final ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("versions", true);
    params.set("commit", true);
    return params;
  }

  private static void assertLegacySuccessfulAdd(
      UpdateFormat format, String expectedId, NamedList<Object> response) {
    assertEquals(format.name(), 1, response.getAll("responseHeader").size());
    final List<?> adds = (List<?>) response.get("adds");
    assertNotNull(format.name(), adds);
    assertEquals(format.name(), expectedId, adds.get(0));
    assertTrue(format.name(), ((Number) adds.get(1)).longValue() > 0);
  }

  private static void assertTypedSuccessfulAdd(
      UpdateFormat format, String expectedId, NamedList<Object> response) {
    assertEquals(format.name(), 1, response.getAll("responseHeader").size());
    assertTypedVersion(response, "adds", "id", expectedId, true);
  }

  private static void assertTypedVersion(
      NamedList<Object> response,
      String field,
      String key,
      String expectedValue,
      boolean positiveVersion) {
    final List<?> values = (List<?>) response.get(field);
    assertNotNull(field, values);
    assertEquals(1, values.size());
    final Object value = values.get(0);
    final Object actualValue;
    final Object version;
    if (value instanceof Map<?, ?> map) {
      actualValue = map.get(key);
      version = map.get("version");
    } else {
      final NamedList<?> namedValue = (NamedList<?>) value;
      actualValue = namedValue.get(key);
      version = namedValue.get("version");
    }
    assertEquals(expectedValue, actualValue);
    final long numericVersion = ((Number) version).longValue();
    assertTrue(positiveVersion ? numericVersion > 0 : numericVersion < 0);
  }

  private static void assertIndexed(SolrClient client, String id) throws Exception {
    final ModifiableSolrParams queryParams = new ModifiableSolrParams();
    queryParams.set("q", "id:" + id);
    final QueryResponse queryResponse = new QueryRequest(queryParams).process(client, CORE_NAME);
    assertEquals(id, 1, queryResponse.getResults().getNumFound());
  }

  private static void assertIndexedField(
      SolrClient client, String collection, String id, String field, String value)
      throws Exception {
    final ModifiableSolrParams queryParams = new ModifiableSolrParams();
    queryParams.set("q", "id:" + id);
    queryParams.set("fl", field);
    final QueryResponse queryResponse = new QueryRequest(queryParams).process(client, collection);
    assertEquals(1, queryResponse.getResults().getNumFound());
    assertEquals(value, queryResponse.getResults().get(0).getFieldValue(field));
  }

  private enum UpdateFormat {
    JSON("application/json") {
      @Override
      byte[] payload(String id) {
        return bytes("{\"add\":{\"doc\":{\"id\":\"" + id + "\"}}}");
      }
    },
    XML("application/xml") {
      @Override
      byte[] payload(String id) {
        return bytes("<add><doc><field name=\"id\">" + id + "</field></doc></add>");
      }
    },
    CSV("application/csv") {
      @Override
      byte[] payload(String id) {
        return bytes("id\n" + id + "\n");
      }
    },
    JAVABIN("application/javabin") {
      @Override
      byte[] payload(String id) throws Exception {
        final SolrInputDocument doc = new SolrInputDocument();
        doc.setField("id", id);
        final UpdateRequest updateRequest = new UpdateRequest();
        updateRequest.add(doc);
        final ByteArrayOutputStream payload = new ByteArrayOutputStream();
        new JavaBinUpdateRequestCodec().marshal(updateRequest, payload);
        return payload.toByteArray();
      }
    };

    private final String contentType;

    UpdateFormat(String contentType) {
      this.contentType = contentType;
    }

    abstract byte[] payload(String id) throws Exception;

    static byte[] bytes(String value) {
      return value.getBytes(StandardCharsets.UTF_8);
    }
  }
}
