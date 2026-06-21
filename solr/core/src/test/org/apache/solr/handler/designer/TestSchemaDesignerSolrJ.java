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

package org.apache.solr.handler.designer;

import static org.apache.solr.handler.admin.ConfigSetsHandler.DEFAULT_CONFIGSET_NAME;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.solr.client.api.model.SchemaDesignerInfoResponse;
import org.apache.solr.client.api.model.SchemaDesignerResponse;
import org.apache.solr.client.solrj.request.SchemaDesignerApi;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.util.ExternalPaths;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Smoke tests that exercise the Schema Designer V2 API through the generated SolrJ client. Unlike
 * {@link TestSchemaDesigner}, which calls the handler in-process and bypasses both the JAX-RS layer
 * and JSON (de)serialization, these tests round-trip every typed request body and response over
 * HTTP. They guard against regressions in the {@code @JsonProperty} / {@code @Schema} /
 * {@code @JsonAnySetter} annotations and the OpenAPI-generated SolrJ wiring. This is required as
 * the primary interaction mechanism to Schema Designer APIs is via the schema-designer.js JSON
 * calls, not SolrJ.
 */
public class TestSchemaDesignerSolrJ extends SolrCloudTestCase {

  @BeforeClass
  public static void createCluster() throws Exception {
    System.setProperty("managed.schema.mutable", "true");
    configureCluster(1)
        .addConfig(DEFAULT_CONFIGSET_NAME, ExternalPaths.DEFAULT_CONFIGSET)
        .configure();
  }

  @AfterClass
  public static void cleanup() throws Exception {
    if (cluster != null && cluster.getSolrClient() != null) {
      cluster.deleteAllCollections();
      cluster.deleteAllConfigSets();
    }
  }

  /**
   * Walks through prep → add (each of the four wrapper keys) → update → info, verifying both the
   * SolrJ request wiring and the typed-response deserialization.
   */
  @Test
  public void testTypedBodyRoundTrip() throws Exception {
    final String configSet = "solrjSmoke";

    // POST /schema-designer/{configSet}/prep — no body, baseline that SolrJ wiring works
    SchemaDesignerResponse prep =
        new SchemaDesignerApi.PrepNewSchema(configSet).process(cluster.getSolrClient());
    assertEquals(configSet, prep.configSet);
    int schemaVersion = prep.schemaVersion;

    // POST /{configSet} — addField — exercises kebab-case @JsonProperty("add-field") / @Schema(name=…)
    var addField = new SchemaDesignerApi.AddSchemaObject(configSet);
    addField.setSchemaVersion(schemaVersion);
    addField.setAddField(Map.of("name", "keywords", "type", "string", "stored", true));
    SchemaDesignerResponse addFieldResp = addField.process(cluster.getSolrClient());
    assertEquals("keywords", addFieldResp.field);
    schemaVersion = addFieldResp.schemaVersion;

    // POST /{configSet} — addFieldType — covers a different wrapper key
    var addType = new SchemaDesignerApi.AddSchemaObject(configSet);
    addType.setSchemaVersion(schemaVersion);
    addType.setAddFieldType(
        Map.of(
            "name",
            "smoke_txt",
            "class",
            "solr.TextField",
            "analyzer",
            Map.of("tokenizer", Map.of("class", "solr.StandardTokenizerFactory"))));
    SchemaDesignerResponse addTypeResp = addType.process(cluster.getSolrClient());
    assertEquals("smoke_txt", addTypeResp.fieldType);
    schemaVersion = addTypeResp.schemaVersion;

    // POST /{configSet} — addDynamicField
    var addDyn = new SchemaDesignerApi.AddSchemaObject(configSet);
    addDyn.setSchemaVersion(schemaVersion);
    addDyn.setAddDynamicField(Map.of("name", "*_smoke", "type", "string"));
    SchemaDesignerResponse addDynResp = addDyn.process(cluster.getSolrClient());
    assertEquals("*_smoke", addDynResp.dynamicField);
    schemaVersion = addDynResp.schemaVersion;

    // POST /{configSet} — addCopyField — verifies the explicit no-op response branch in
    // setSchemaObjectField (no field/type/dynamicField/fieldType is populated)
    var addCopy = new SchemaDesignerApi.AddSchemaObject(configSet);
    addCopy.setSchemaVersion(schemaVersion);
    addCopy.setAddCopyField(Map.of("source", "keywords", "dest", "_text_"));
    SchemaDesignerResponse addCopyResp = addCopy.process(cluster.getSolrClient());
    assertNull(addCopyResp.field);
    assertNull(addCopyResp.fieldType);
    assertNull(addCopyResp.dynamicField);
    schemaVersion = addCopyResp.schemaVersion;

    // PUT /{configSet} — exercises the @JsonAnyGetter/@JsonAnySetter capture for arbitrary attrs
    var update = new SchemaDesignerApi.UpdateSchemaObject(configSet);
    update.setSchemaVersion(schemaVersion);
    update.setName("keywords");
    update.setAdditionalProperties(Map.of("type", "string", "stored", true, "multiValued", true));
    SchemaDesignerResponse updateResp = update.process(cluster.getSolrClient());
    assertNotNull(updateResp.field);
    assertEquals("field", updateResp.updateType);

    // GET /{configSet} — round-trips a typed response that extends SchemaDesignerSettingsResponse
    SchemaDesignerInfoResponse info =
        new SchemaDesignerApi.GetInfo(configSet).process(cluster.getSolrClient());
    assertEquals(configSet, info.configSet);
  }

  /**
   * Exercises the {@code InputStream} body binding on {@code updateFileContents} by sending an
   * invalid {@code solrconfig.xml} and verifying the server returns the typed error fields rather
   * than throwing — this also confirms the bytes actually reached the server.
   */
  @Test
  public void testUpdateFileContentsBodyBinding() throws Exception {
    final String configSet = "solrjFileSmoke";

    new SchemaDesignerApi.PrepNewSchema(configSet).process(cluster.getSolrClient());

    byte[] invalidXml = "<config/>".getBytes(StandardCharsets.UTF_8);
    var req =
        new SchemaDesignerApi.UpdateFileContents(configSet, new ByteArrayInputStream(invalidXml));
    req.setFile("solrconfig.xml");
    SchemaDesignerResponse resp = req.process(cluster.getSolrClient());

    assertNotNull(
        "server should report a validation error for the invalid solrconfig.xml",
        resp.updateFileError);
    assertEquals("<config/>", resp.fileContent);
  }
}
