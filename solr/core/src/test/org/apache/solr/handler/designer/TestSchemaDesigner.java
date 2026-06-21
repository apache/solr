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

import static org.apache.solr.common.params.CommonParams.JSON_MIME;
import static org.apache.solr.handler.admin.ConfigSetsHandler.DEFAULT_CONFIGSET_NAME;
import static org.apache.solr.handler.designer.SchemaDesigner.getMutableId;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import org.apache.solr.client.api.model.FlexibleSolrJerseyResponse;
import org.apache.solr.client.api.model.SchemaDesignerAddRequestBody;
import org.apache.solr.client.api.model.SchemaDesignerCollectionsResponse;
import org.apache.solr.client.api.model.SchemaDesignerInfoResponse;
import org.apache.solr.client.api.model.SchemaDesignerResponse;
import org.apache.solr.client.api.model.SchemaDesignerSchemaDiffResponse;
import org.apache.solr.client.api.model.SchemaDesignerUpdateRequestBody;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.SolrQuery;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.ContentStreamBase;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.TestSampleDocumentsLoader;
import org.apache.solr.jersey.SolrJacksonMapper;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.ManagedIndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.util.ExternalPaths;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestSchemaDesigner extends SolrCloudTestCase implements SchemaDesignerConstants {

  private CoreContainer cc;
  private SchemaDesigner schemaDesigner;
  private SolrQueryRequest mockReq;

  @BeforeClass
  public static void createCluster() throws Exception {
    System.setProperty("managed.schema.mutable", "true");
    configureCluster(1)
        .addConfig(DEFAULT_CONFIGSET_NAME, ExternalPaths.DEFAULT_CONFIGSET)
        .configure();
  }

  @AfterClass
  public static void tearDownCluster() throws Exception {
    if (cluster != null && cluster.getSolrClient() != null) {
      cluster.deleteAllCollections();
      cluster.deleteAllConfigSets();
    }
  }

  @Before
  public void setupTest() {
    assumeWorkingMockito();
    assertNotNull(cluster);
    cc = cluster.getJettySolrRunner(0).getCoreContainer();
    assertNotNull(cc);
    mockReq = mock(SolrQueryRequest.class);
    schemaDesigner =
        new SchemaDesigner(
            cc,
            SchemaDesigner.newSchemaSuggester(),
            SchemaDesigner.newSampleDocumentsLoader(),
            mockReq);
  }

  public void testTSV() throws Exception {
    String configSet = "testTSV";

    ModifiableSolrParams reqParams = new ModifiableSolrParams();
    reqParams.set(CONFIG_SET_PARAM, configSet);
    reqParams.set(LANGUAGES_PARAM, "en");
    reqParams.set(ENABLE_DYNAMIC_FIELDS_PARAM, false);
    when(mockReq.getParams()).thenReturn(reqParams);

    String tsv = "id\tcol1\tcol2\n1\tfoo\tbar\n2\tbaz\tbah\n";

    // POST some sample TSV docs
    ContentStream stream = new ContentStreamBase.StringStream(tsv, "text/csv");
    when(mockReq.getContentStreams()).thenReturn(List.of(stream));

    // POST /schema-designer/analyze
    SchemaDesignerResponse response =
        schemaDesigner.analyze(configSet, null, null, null, List.of("en"), false, null, null);
    assertNotNull(response.configSet);
    assertNotNull(response.schemaVersion);
    assertEquals(Integer.valueOf(2), response.numDocs);

    reqParams.clear();
    reqParams.set(CONFIG_SET_PARAM, configSet);
    when(mockReq.getContentStreams()).thenReturn(null);
    schemaDesigner.cleanupTempSchema(configSet);

    String mutableId = getMutableId(configSet);
    assertFalse(cc.getZkController().getClusterState().hasCollection(mutableId));
    SolrZkClient zkClient = cc.getZkController().getZkClient();
    assertFalse(zkClient.exists("/configs/" + mutableId));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testAddTechproductsProgressively() throws Exception {
    Path docsDir = ExternalPaths.SOURCE_HOME.resolve("example/exampledocs");
    assertTrue(docsDir + " not found!", Files.isDirectory(docsDir));
    List<Path> toAdd;
    try (Stream<Path> files = Files.list(docsDir)) {
      toAdd =
          files
              .filter(
                  (dir) -> {
                    String name = dir.getFileName().toString();
                    return name.endsWith(".xml")
                        || name.endsWith(".json")
                        || name.endsWith(".csv")
                        || name.endsWith(".jsonl");
                  })
              .toList();
      assertNotNull("No test data files found in " + docsDir, toAdd);
    }

    String configSet = "techproducts";

    // GET /schema-designer/info
    SchemaDesignerInfoResponse infoResponse = schemaDesigner.getInfo(configSet);
    // response should just be the default values
    Map<String, Object> expSettings =
        Map.of(
            ENABLE_DYNAMIC_FIELDS_PARAM, true,
            ENABLE_FIELD_GUESSING_PARAM, true,
            ENABLE_NESTED_DOCS_PARAM, false,
            LANGUAGES_PARAM, List.of());
    assertDesignerSettings(expSettings, infoResponse);
    int schemaVersion = infoResponse.schemaVersion;
    assertEquals(schemaVersion, -1); // shouldn't exist yet

    // Use the prep endpoint to prepare the new schema
    SchemaDesignerResponse response = schemaDesigner.prepNewSchema(configSet, null);
    assertNotNull(response.configSet);
    assertNotNull(response.schemaVersion);
    schemaVersion = response.schemaVersion;

    for (Path next : toAdd) {
      // Analyze some sample documents to refine the schema
      ModifiableSolrParams reqParams = new ModifiableSolrParams();
      reqParams.set(CONFIG_SET_PARAM, configSet);
      when(mockReq.getParams()).thenReturn(reqParams);

      // POST some sample JSON docs
      ContentStreamBase.FileStream stream = new ContentStreamBase.FileStream(next);
      stream.setContentType(
          TestSampleDocumentsLoader.guessContentTypeFromFilename(next.getFileName().toString()));
      when(mockReq.getContentStreams()).thenReturn(List.of(stream));

      // POST /schema-designer/analyze
      response =
          schemaDesigner.analyze(
              configSet, schemaVersion, null, null, List.of("en"), false, null, null);

      assertNotNull(response.configSet);
      assertNotNull(response.schemaVersion);
      assertNotNull(response.fields);
      assertNotNull(response.fieldTypes);
      assertNotNull(response.docIds);

      // capture the schema version for MVCC
      schemaVersion = response.schemaVersion;
    }

    // get info (from the temp)
    // GET /schema-designer/info
    infoResponse = schemaDesigner.getInfo(configSet);
    expSettings =
        Map.of(
            ENABLE_DYNAMIC_FIELDS_PARAM, false,
            ENABLE_FIELD_GUESSING_PARAM, true,
            ENABLE_NESTED_DOCS_PARAM, false,
            LANGUAGES_PARAM, List.of("en"),
            COPY_FROM_PARAM, "_default");
    assertDesignerSettings(expSettings, infoResponse);

    // query to see how the schema decisions impact retrieval / ranking
    ModifiableSolrParams queryParams = new ModifiableSolrParams();
    queryParams.set(SCHEMA_VERSION_PARAM, String.valueOf(schemaVersion));
    queryParams.set(CONFIG_SET_PARAM, configSet);
    queryParams.set(CommonParams.Q, "*:*");
    when(mockReq.getParams()).thenReturn(queryParams);
    when(mockReq.getContentStreams()).thenReturn(null);

    // GET /schema-designer/query
    FlexibleSolrJerseyResponse queryResp = schemaDesigner.query(configSet);
    assertNotNull(queryResp.unknownProperties().get("responseHeader"));
    @SuppressWarnings("unchecked")
    Map<String, Object> queryResponse =
        (Map<String, Object>) queryResp.unknownProperties().get("response");
    assertNotNull("response object must be a map with numFound/docs", queryResponse);
    assertEquals(47L, queryResponse.get("numFound"));
    @SuppressWarnings("unchecked")
    List<Object> queryDocs = (List<Object>) queryResponse.get("docs");
    assertNotNull("response.docs must be a list", queryDocs);
    assertFalse("response.docs must be non-empty", queryDocs.isEmpty());

    // publish schema to a config set that can be used by real collections
    String collection = "techproducts";
    schemaDesigner.publish(configSet, schemaVersion, collection, true, 1, 1, true, true, true);
    assertNotNull(cc.getZkController().zkStateReader.getCollection(collection));

    // listCollectionsForConfig
    SchemaDesignerCollectionsResponse collectionsResp =
        schemaDesigner.listCollectionsForConfig(configSet);
    List<String> collections = collectionsResp.collections;
    assertNotNull(collections);
    assertTrue(collections.contains(collection));

    // now try to create another temp, which should fail since designer is disabled for this
    // configSet now
    try {
      schemaDesigner.prepNewSchema(configSet, null);
      fail("Prep should fail for locked schema " + configSet);
    } catch (SolrException solrExc) {
      assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, solrExc.code());
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testSuggestFilmsXml() throws Exception {
    String configSet = "films";

    Path filmsDir = ExternalPaths.SOURCE_HOME.resolve("example/films");
    assertTrue(filmsDir + " not found!", Files.isDirectory(filmsDir));
    Path filmsXml = filmsDir.resolve("films.xml");
    assertTrue("example/films/films.xml not found", Files.isRegularFile(filmsXml));

    // POST some sample XML docs
    ContentStreamBase.FileStream stream = new ContentStreamBase.FileStream(filmsXml);
    stream.setContentType("application/xml");
    ModifiableSolrParams reqParams = new ModifiableSolrParams();
    reqParams.set(CONFIG_SET_PARAM, configSet);
    when(mockReq.getParams()).thenReturn(reqParams);
    when(mockReq.getContentStreams()).thenReturn(List.of(stream));

    // POST /schema-designer/analyze
    SchemaDesignerResponse response =
        schemaDesigner.analyze(configSet, null, null, null, null, true, null, null);

    assertNotNull(response.configSet);
    assertNotNull(response.schemaVersion);
    assertNotNull(response.fields);
    assertNotNull(response.fieldTypes);
    List<String> docIds = response.docIds;
    assertNotNull(docIds);
    assertEquals(100, docIds.size()); // designer limits the doc ids to top 100

    String idField = response.uniqueKeyField;
    assertNotNull(idField);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testBasicUserWorkflow() throws Exception {
    String configSet = "testJson";

    // Use the prep endpoint to prepare the new schema
    SchemaDesignerResponse response = schemaDesigner.prepNewSchema(configSet, null);
    assertNotNull(response.configSet);
    assertNotNull(response.schemaVersion);

    Map<String, Object> expSettings =
        Map.of(
            ENABLE_DYNAMIC_FIELDS_PARAM, true,
            ENABLE_FIELD_GUESSING_PARAM, true,
            ENABLE_NESTED_DOCS_PARAM, false,
            LANGUAGES_PARAM, List.of(),
            COPY_FROM_PARAM, "_default");
    assertDesignerSettings(expSettings, response);

    // Analyze some sample documents to refine the schema
    Path booksJson = ExternalPaths.SOURCE_HOME.resolve("example/exampledocs/books.json");
    ContentStreamBase.FileStream stream = new ContentStreamBase.FileStream(booksJson);
    stream.setContentType(JSON_MIME);
    ModifiableSolrParams reqParams = new ModifiableSolrParams();
    reqParams.set(CONFIG_SET_PARAM, configSet);
    when(mockReq.getParams()).thenReturn(reqParams);
    when(mockReq.getContentStreams()).thenReturn(List.of(stream));

    // POST /schema-designer/analyze
    response = schemaDesigner.analyze(configSet, null, null, null, null, null, null, null);

    assertNotNull(response.configSet);
    assertNotNull(response.schemaVersion);
    assertNotNull(response.fields);
    assertNotNull(response.fieldTypes);
    assertNotNull(response.docIds);
    String idField = response.uniqueKeyField;
    assertNotNull(idField);
    assertDesignerSettings(expSettings, response);

    // capture the schema version for MVCC
    int schemaVersion = response.schemaVersion;

    // load the contents of a file
    Collection<String> files = response.files;
    assertTrue(files != null && !files.isEmpty());

    String file = null;
    for (String f : files) {
      if ("solrconfig.xml".equals(f)) {
        file = f;
        break;
      }
    }
    assertNotNull("solrconfig.xml not found in files!", file);
    byte[] solrconfigBytes =
        cc.getConfigSetService().downloadFileFromConfig(getMutableId(configSet), file);
    assertNotNull(solrconfigBytes);
    String solrconfigXml = new String(solrconfigBytes, StandardCharsets.UTF_8);

    // Update solrconfig.xml
    response =
        schemaDesigner.updateFileContents(
            configSet,
            file,
            new ByteArrayInputStream(solrconfigXml.getBytes(StandardCharsets.UTF_8)));
    schemaVersion = response.schemaVersion;

    // this should fail b/c the updated solrconfig.xml is invalid
    response =
        schemaDesigner.updateFileContents(
            configSet,
            file,
            new ByteArrayInputStream("<config/>".getBytes(StandardCharsets.UTF_8)));
    assertNotNull(response.updateFileError);

    // remove dynamic fields and change the language to "en" only
    when(mockReq.getContentStreams()).thenReturn(null);
    // POST /schema-designer/analyze
    response =
        schemaDesigner.analyze(
            configSet, schemaVersion, null, null, List.of("en"), false, false, null);

    expSettings =
        Map.of(
            ENABLE_DYNAMIC_FIELDS_PARAM, false,
            ENABLE_FIELD_GUESSING_PARAM, false,
            ENABLE_NESTED_DOCS_PARAM, false,
            LANGUAGES_PARAM, List.of("en"),
            COPY_FROM_PARAM, "_default");
    assertDesignerSettings(expSettings, response);

    List<String> filesInResp = response.files;
    assertEquals(5, filesInResp.size());
    assertTrue(filesInResp.contains("lang/stopwords_en.txt"));

    schemaVersion = response.schemaVersion;

    // add the dynamic fields back and change the languages too
    response =
        schemaDesigner.analyze(
            configSet, schemaVersion, null, null, Arrays.asList("en", "fr"), true, false, null);

    expSettings =
        Map.of(
            ENABLE_DYNAMIC_FIELDS_PARAM, true,
            ENABLE_FIELD_GUESSING_PARAM, false,
            ENABLE_NESTED_DOCS_PARAM, false,
            LANGUAGES_PARAM, Arrays.asList("en", "fr"),
            COPY_FROM_PARAM, "_default");
    assertDesignerSettings(expSettings, response);

    filesInResp = response.files;
    assertEquals(7, filesInResp.size());
    assertTrue(filesInResp.contains("lang/stopwords_fr.txt"));

    schemaVersion = response.schemaVersion;

    // add back all the default languages (using "*" wildcard -> empty list)
    response =
        schemaDesigner.analyze(
            configSet, schemaVersion, null, null, List.of("*"), false, null, null);

    expSettings =
        Map.of(
            ENABLE_DYNAMIC_FIELDS_PARAM, false,
            ENABLE_FIELD_GUESSING_PARAM, false,
            ENABLE_NESTED_DOCS_PARAM, false,
            LANGUAGES_PARAM, List.of(),
            COPY_FROM_PARAM, "_default");
    assertDesignerSettings(expSettings, response);

    filesInResp = response.files;
    assertEquals(43, filesInResp.size());
    assertTrue(filesInResp.contains("lang/stopwords_fr.txt"));
    assertTrue(filesInResp.contains("lang/stopwords_en.txt"));
    assertTrue(filesInResp.contains("lang/stopwords_it.txt"));

    schemaVersion = response.schemaVersion;

    // Get the value of a sample document
    String docId = "978-0641723445";
    String fieldName = "series_t";

    // GET /schema-designer/sample
    FlexibleSolrJerseyResponse sampleResp =
        schemaDesigner.getSampleValue(configSet, fieldName, idField, docId);
    assertNotNull(sampleResp.unknownProperties().get(idField));
    assertNotNull(sampleResp.unknownProperties().get(fieldName));
    assertNotNull(sampleResp.unknownProperties().get("analysis"));

    // at this point the user would refine the schema by
    // editing suggestions for fields and adding/removing fields / field types as needed

    // add a new field
    // POST /schema-designer/add
    response =
        schemaDesigner.addSchemaObject(
            configSet, schemaVersion, loadAddBody("schema-designer/add-new-field.json"));
    assertNotNull(response.field);
    schemaVersion = response.schemaVersion;
    assertNotNull(response.fields);

    // update an existing field
    // switch a single-valued field to a multivalued field, which triggers a full rebuild of the
    // "temp" collection
    // PUT /schema-designer/update
    response =
        schemaDesigner.updateSchemaObject(
            configSet, schemaVersion, loadUpdateBody("schema-designer/update-author-field.json"));
    assertNotNull(response.field);
    schemaVersion = response.schemaVersion;

    // add a new type
    // POST /schema-designer/add
    response =
        schemaDesigner.addSchemaObject(
            configSet, schemaVersion, loadAddBody("schema-designer/add-new-type.json"));
    final String expectedTypeName = "test_txt";
    assertEquals(expectedTypeName, response.fieldType);
    schemaVersion = response.schemaVersion;
    assertNotNull(response.fieldTypes);
    @SuppressWarnings("unchecked")
    List<Map<String, Object>> fieldTypes = response.fieldTypes;
    Optional<Map<String, Object>> expected =
        fieldTypes.stream().filter(m -> expectedTypeName.equals(m.get("name"))).findFirst();
    assertTrue(
        "New field type '" + expectedTypeName + "' not found in add type response!",
        expected.isPresent());

    // POST /schema-designer/update
    response =
        schemaDesigner.updateSchemaObject(
            configSet, schemaVersion, loadUpdateBody("schema-designer/update-type.json"));
    schemaVersion = response.schemaVersion;

    // query to see how the schema decisions impact retrieval / ranking
    ModifiableSolrParams queryParams = new ModifiableSolrParams();
    queryParams.set(SCHEMA_VERSION_PARAM, String.valueOf(schemaVersion));
    queryParams.set(CONFIG_SET_PARAM, configSet);
    queryParams.set(CommonParams.Q, "*:*");
    when(mockReq.getParams()).thenReturn(queryParams);
    when(mockReq.getContentStreams()).thenReturn(null);

    // GET /schema-designer/query
    FlexibleSolrJerseyResponse queryResp2 = schemaDesigner.query(configSet);
    assertNotNull(queryResp2.unknownProperties().get("responseHeader"));
    @SuppressWarnings("unchecked")
    Map<String, Object> queryResponse2 =
        (Map<String, Object>) queryResp2.unknownProperties().get("response");
    assertNotNull("response object must be a map with numFound/docs", queryResponse2);
    @SuppressWarnings("unchecked")
    List<Object> queryDocs2 = (List<Object>) queryResponse2.get("docs");
    assertEquals(4, queryDocs2.size());

    // publish schema to a config set that can be used by real collections
    String collection = "test123";
    schemaDesigner.publish(configSet, schemaVersion, collection, true, 1, 1, true, true, false);

    assertNotNull(cc.getZkController().zkStateReader.getCollection(collection));

    // listCollectionsForConfig
    SchemaDesignerCollectionsResponse collectionsResp2 =
        schemaDesigner.listCollectionsForConfig(configSet);
    List<String> collections = collectionsResp2.collections;
    assertNotNull(collections);
    assertTrue(collections.contains(collection));

    // verify temp designer objects were cleaned up during the publish operation ...
    String mutableId = getMutableId(configSet);
    assertFalse(cc.getZkController().getClusterState().hasCollection(mutableId));
    SolrZkClient zkClient = cc.getZkController().getZkClient();
    assertFalse(zkClient.exists("/configs/" + mutableId));

    SolrQuery query = new SolrQuery("*:*");
    query.setRows(0);
    QueryResponse qr = cluster.getSolrClient().query(collection, query);
    // this proves the docs were stored in the filestore too
    assertEquals(4, qr.getResults().getNumFound());
  }

  @SuppressWarnings("unchecked")
  public void testFieldUpdates() throws Exception {
    String configSet = "fieldUpdates";

    // Use the prep endpoint to prepare the new schema
    SchemaDesignerResponse response = schemaDesigner.prepNewSchema(configSet, null);
    assertNotNull(response.configSet);
    assertNotNull(response.schemaVersion);
    int schemaVersion = response.schemaVersion;

    // add our test field that we'll test various updates to
    // POST /schema-designer/add
    response =
        schemaDesigner.addSchemaObject(
            configSet, schemaVersion, loadAddBody("schema-designer/add-new-field.json"));
    assertNotNull(response.field);

    final String fieldName = "keywords";

    Optional<Map<String, Object>> maybeField =
        response.fields.stream().filter(m -> fieldName.equals(m.get("name"))).findFirst();
    assertTrue(maybeField.isPresent());
    Map<String, Object> field = maybeField.get();
    assertEquals(Boolean.FALSE, field.get("indexed"));
    assertEquals(Boolean.FALSE, field.get("required"));
    assertEquals(Boolean.TRUE, field.get("stored"));
    assertEquals(Boolean.TRUE, field.get("docValues"));
    assertEquals(Boolean.TRUE, field.get("useDocValuesAsStored"));
    assertEquals(Boolean.FALSE, field.get("multiValued"));
    assertEquals("string", field.get("type"));

    String mutableId = getMutableId(configSet);
    SchemaDesignerConfigSetHelper configSetHelper =
        new SchemaDesignerConfigSetHelper(cc, SchemaDesigner.newSchemaSuggester());
    ManagedIndexSchema schema = schemaDesigner.loadLatestSchema(mutableId);

    // make it required
    Map<String, Object> updateField =
        Map.of("name", fieldName, "type", field.get("type"), "required", true);
    configSetHelper.updateField(configSet, updateField, schema);

    schema = schemaDesigner.loadLatestSchema(mutableId);
    SchemaField schemaField = schema.getField(fieldName);
    assertTrue(schemaField.isRequired());

    updateField =
        Map.of("name", fieldName, "type", field.get("type"), "required", false, "stored", false);
    configSetHelper.updateField(configSet, updateField, schema);
    schema = schemaDesigner.loadLatestSchema(mutableId);
    schemaField = schema.getField(fieldName);
    assertFalse(schemaField.isRequired());
    assertFalse(schemaField.stored());

    updateField =
        Map.of(
            "name",
            fieldName,
            "type",
            field.get("type"),
            "required",
            false,
            "stored",
            false,
            "multiValued",
            true);
    configSetHelper.updateField(configSet, updateField, schema);
    schema = schemaDesigner.loadLatestSchema(mutableId);
    schemaField = schema.getField(fieldName);
    assertFalse(schemaField.isRequired());
    assertFalse(schemaField.stored());
    assertTrue(schemaField.multiValued());

    updateField = Map.of("name", fieldName, "type", "strings", "copyDest", "_text_");
    configSetHelper.updateField(configSet, updateField, schema);
    schema = schemaDesigner.loadLatestSchema(mutableId);
    schemaField = schema.getField(fieldName);
    assertTrue(schemaField.multiValued());
    assertEquals("strings", schemaField.getType().getTypeName());
    assertFalse(schemaField.isRequired());
    assertTrue(schemaField.stored());
    List<String> srcFields = schema.getCopySources("_text_");
    assertEquals(List.of(fieldName), srcFields);
  }

  @SuppressWarnings({"unchecked"})
  public void testSchemaDiffEndpoint() throws Exception {
    String configSet = "testDiff";

    // Use the prep endpoint to prepare the new schema
    SchemaDesignerResponse response = schemaDesigner.prepNewSchema(configSet, null);
    assertNotNull(response.configSet);
    assertNotNull(response.schemaVersion);
    int schemaVersion = response.schemaVersion;

    // publish schema to a config set that can be used by real collections
    String collection = "diff456";
    schemaDesigner.publish(configSet, schemaVersion, collection, true, 1, 1, true, true, false);

    assertNotNull(cc.getZkController().zkStateReader.getCollection(collection));

    // Load the schema designer for the existing config set and make some changes to it
    ModifiableSolrParams reqParams = new ModifiableSolrParams();
    reqParams.set(CONFIG_SET_PARAM, configSet);
    when(mockReq.getParams()).thenReturn(reqParams);
    when(mockReq.getContentStreams()).thenReturn(null);
    response = schemaDesigner.analyze(configSet, null, null, null, null, true, false, null);

    // Update id field to not use docValues
    @SuppressWarnings("unchecked")
    List<SimpleOrderedMap<Object>> fields =
        (List<SimpleOrderedMap<Object>>) (List<?>) response.fields;
    SimpleOrderedMap<Object> idFieldMap =
        fields.stream().filter(field -> field.get("name").equals("id")).findFirst().get();
    idFieldMap.remove("copyDest"); // Don't include copyDest as it is not a property of SchemaField
    SimpleOrderedMap<Object> idFieldMapUpdated = idFieldMap.clone();
    idFieldMapUpdated.setVal(idFieldMapUpdated.indexOf("docValues", 0), Boolean.FALSE);
    idFieldMapUpdated.setVal(idFieldMapUpdated.indexOf("useDocValuesAsStored", 0), Boolean.FALSE);
    idFieldMapUpdated.setVal(idFieldMapUpdated.indexOf("uninvertible", 0), Boolean.TRUE);
    idFieldMapUpdated.setVal(
        idFieldMapUpdated.indexOf("omitTermFreqAndPositions", 0), Boolean.FALSE);

    Map<String, Object> mapParams = new SimpleOrderedMap<>(idFieldMapUpdated.toSolrParams());
    mapParams.put("termVectors", Boolean.FALSE);
    schemaVersion = response.schemaVersion;

    SchemaDesignerUpdateRequestBody idFieldUpdate =
        SolrJacksonMapper.getObjectMapper()
            .convertValue(mapParams, SchemaDesignerUpdateRequestBody.class);
    response = schemaDesigner.updateSchemaObject(configSet, schemaVersion, idFieldUpdate);

    // Add a new field
    schemaVersion = response.schemaVersion;
    // POST /schema-designer/add
    response =
        schemaDesigner.addSchemaObject(
            configSet, schemaVersion, loadAddBody("schema-designer/add-new-field.json"));
    assertNotNull(response.field);

    // Add a new field type
    schemaVersion = response.schemaVersion;
    // POST /schema-designer/add
    response =
        schemaDesigner.addSchemaObject(
            configSet, schemaVersion, loadAddBody("schema-designer/add-new-type.json"));
    assertNotNull(response.fieldType);

    // Let's do a diff now
    SchemaDesignerSchemaDiffResponse diffResp = schemaDesigner.getSchemaDiff(configSet);

    Map<String, Object> diff = diffResp.diff;

    // field asserts
    assertNotNull(diff.get("fields"));
    Map<String, Object> fieldsDiff = (Map<String, Object>) diff.get("fields");
    assertNotNull(fieldsDiff.get("updated"));
    Map<String, Object> mapDiff = (Map<String, Object>) fieldsDiff.get("updated");
    assertEquals(
        Arrays.asList(
            Map.of(
                "omitTermFreqAndPositions",
                true,
                "useDocValuesAsStored",
                true,
                "docValues",
                true,
                "uninvertible",
                false),
            Map.of(
                "omitTermFreqAndPositions",
                false,
                "useDocValuesAsStored",
                false,
                "docValues",
                false,
                "uninvertible",
                true)),
        mapDiff.get("id"));
    assertNotNull(fieldsDiff.get("added"));
    Map<String, Object> fieldsAdded = (Map<String, Object>) fieldsDiff.get("added");
    assertNotNull(fieldsAdded.get("keywords"));

    // field type asserts
    assertNotNull(diff.get("fieldTypes"));
    Map<String, Object> fieldTypesDiff = (Map<String, Object>) diff.get("fieldTypes");
    assertNotNull(fieldTypesDiff.get("added"));
    Map<String, Object> fieldTypesAdded = (Map<String, Object>) fieldTypesDiff.get("added");
    assertNotNull(fieldTypesAdded.get("test_txt"));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testQueryReturnsErrorDetailsOnIndexingFailure() throws Exception {
    String configSet = "queryIndexErrTest";

    // Prep the schema and analyze sample docs so the temp collection and stored docs exist
    schemaDesigner.prepNewSchema(configSet, null);
    ContentStreamBase.StringStream stream =
        new ContentStreamBase.StringStream("[{\"id\":\"doc1\",\"title\":\"test doc\"}]", JSON_MIME);
    ModifiableSolrParams reqParams = new ModifiableSolrParams();
    reqParams.set(CONFIG_SET_PARAM, configSet);
    when(mockReq.getParams()).thenReturn(reqParams);
    when(mockReq.getContentStreams()).thenReturn(List.of(stream));
    schemaDesigner.analyze(configSet, null, null, null, null, null, null, null);

    // Build a fresh API instance whose indexedVersion cache is empty (so it always
    // attempts to re-index before running the query), and which simulates indexing errors.
    Map<Object, Throwable> fakeErrors = new HashMap<>();
    fakeErrors.put("doc1", new RuntimeException("simulated indexing failure"));
    SchemaDesigner apiWithErrors =
        new SchemaDesigner(
            cc,
            SchemaDesigner.newSchemaSuggester(),
            SchemaDesigner.newSampleDocumentsLoader(),
            mockReq) {
          @Override
          protected Map<Object, Throwable> indexSampleDocsWithRebuildOnAnalysisError(
              String idField,
              List<SolrInputDocument> docs,
              String collectionName,
              boolean asBatch,
              String[] analysisErrorHolder)
              throws IOException, SolrServerException {
            return fakeErrors;
          }
        };

    when(mockReq.getContentStreams()).thenReturn(null);
    FlexibleSolrJerseyResponse response = apiWithErrors.query(configSet);

    Map<String, Object> props = response.unknownProperties();
    assertNotNull("updateError must be present in error response", props.get(UPDATE_ERROR));
    assertEquals(400, props.get("updateErrorCode"));
    Map<Object, Throwable> details = (Map<Object, Throwable>) props.get(ERROR_DETAILS);
    assertNotNull("errorDetails must be present in error response", details);
    assertTrue("errorDetails must contain the failing doc id", details.containsKey("doc1"));
  }

  @Test
  public void testRequireSchemaVersionRejectsNegativeValues() throws Exception {
    String configSet = "schemaVersionValidation";
    schemaDesigner.prepNewSchema(configSet, null);

    // null schemaVersion must be rejected
    SolrException nullEx =
        expectThrows(
            SolrException.class, () -> schemaDesigner.addSchemaObject(configSet, null, null));
    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, nullEx.code());

    // negative schemaVersion must be rejected (was previously bypassing validation)
    SolrException negEx =
        expectThrows(
            SolrException.class, () -> schemaDesigner.addSchemaObject(configSet, -1, null));
    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, negEx.code());

    // same contract must hold for updateSchemaObject
    SolrException updateNegEx =
        expectThrows(
            SolrException.class, () -> schemaDesigner.updateSchemaObject(configSet, -1, null));
    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, updateNegEx.code());
  }

  protected void assertDesignerSettings(Map<String, Object> expected, Map<String, Object> actual) {
    for (String expKey : expected.keySet()) {
      Object expValue = expected.get(expKey);
      assertEquals(
          "Value for designer setting '" + expKey + "' not match expected!",
          expValue,
          actual.get(expKey));
    }
  }

  protected void assertDesignerSettings(
      Map<String, Object> expected, SchemaDesignerResponse response) {
    Map<String, Object> actual = new HashMap<>();
    actual.put(LANGUAGES_PARAM, response.languages);
    actual.put(ENABLE_FIELD_GUESSING_PARAM, response.enableFieldGuessing);
    actual.put(ENABLE_DYNAMIC_FIELDS_PARAM, response.enableDynamicFields);
    actual.put(ENABLE_NESTED_DOCS_PARAM, response.enableNestedDocs);
    actual.put(COPY_FROM_PARAM, response.copyFrom);
    assertDesignerSettings(expected, actual);
  }

  protected void assertDesignerSettings(
      Map<String, Object> expected, SchemaDesignerInfoResponse response) {
    Map<String, Object> actual = new HashMap<>();
    actual.put(LANGUAGES_PARAM, response.languages);
    actual.put(ENABLE_FIELD_GUESSING_PARAM, response.enableFieldGuessing);
    actual.put(ENABLE_DYNAMIC_FIELDS_PARAM, response.enableDynamicFields);
    actual.put(ENABLE_NESTED_DOCS_PARAM, response.enableNestedDocs);
    actual.put(COPY_FROM_PARAM, response.copyFrom);
    assertDesignerSettings(expected, actual);
  }

  private SchemaDesignerAddRequestBody loadAddBody(String fixturePath) throws IOException {
    return SolrJacksonMapper.getObjectMapper()
        .readValue(getFile(fixturePath).toFile(), SchemaDesignerAddRequestBody.class);
  }

  private SchemaDesignerUpdateRequestBody loadUpdateBody(String fixturePath) throws IOException {
    return SolrJacksonMapper.getObjectMapper()
        .readValue(getFile(fixturePath).toFile(), SchemaDesignerUpdateRequestBody.class);
  }
}
