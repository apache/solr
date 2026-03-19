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
import static org.apache.solr.handler.designer.SchemaDesignerAPI.getMutableId;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import jakarta.ws.rs.core.Response;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import org.apache.solr.client.api.model.FlexibleSolrJerseyResponse;
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
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.ManagedIndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.util.ExternalPaths;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.noggit.JSONUtil;

public class TestSchemaDesignerAPI extends SolrCloudTestCase implements SchemaDesignerConstants {

  private CoreContainer cc;
  private SchemaDesignerAPI schemaDesignerAPI;
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
    schemaDesignerAPI =
        new SchemaDesignerAPI(
            cc,
            SchemaDesignerAPI.newSchemaSuggester(),
            SchemaDesignerAPI.newSampleDocumentsLoader(),
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
    when(mockReq.getContentStreams()).thenReturn(Collections.singletonList(stream));

    // POST /schema-designer/analyze
    FlexibleSolrJerseyResponse response =
        schemaDesignerAPI.analyze(configSet, null, null, null, List.of("en"), false, null, null);
    assertNotNull(response.unknownProperties().get(CONFIG_SET_PARAM));
    assertNotNull(response.unknownProperties().get(SCHEMA_VERSION_PARAM));
    assertEquals(2, response.unknownProperties().get("numDocs"));

    reqParams.clear();
    reqParams.set(CONFIG_SET_PARAM, configSet);
    when(mockReq.getContentStreams()).thenReturn(null);
    schemaDesignerAPI.cleanupTempSchema(configSet);

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
    FlexibleSolrJerseyResponse response = schemaDesignerAPI.getInfo(configSet);
    // response should just be the default values
    Map<String, Object> expSettings =
        Map.of(
            ENABLE_DYNAMIC_FIELDS_PARAM, true,
            ENABLE_FIELD_GUESSING_PARAM, true,
            ENABLE_NESTED_DOCS_PARAM, false,
            LANGUAGES_PARAM, List.of());
    assertDesignerSettings(expSettings, response.unknownProperties());
    int schemaVersion = (Integer) response.unknownProperties().get(SCHEMA_VERSION_PARAM);
    assertEquals(schemaVersion, -1); // shouldn't exist yet

    // Use the prep endpoint to prepare the new schema
    response = schemaDesignerAPI.prepNewSchema(configSet, null);
    assertNotNull(response.unknownProperties().get(CONFIG_SET_PARAM));
    assertNotNull(response.unknownProperties().get(SCHEMA_VERSION_PARAM));
    schemaVersion = (Integer) response.unknownProperties().get(SCHEMA_VERSION_PARAM);

    for (Path next : toAdd) {
      // Analyze some sample documents to refine the schema
      ModifiableSolrParams reqParams = new ModifiableSolrParams();
      reqParams.set(CONFIG_SET_PARAM, configSet);
      when(mockReq.getParams()).thenReturn(reqParams);

      // POST some sample JSON docs
      ContentStreamBase.FileStream stream = new ContentStreamBase.FileStream(next);
      stream.setContentType(
          TestSampleDocumentsLoader.guessContentTypeFromFilename(next.getFileName().toString()));
      when(mockReq.getContentStreams()).thenReturn(Collections.singletonList(stream));

      // POST /schema-designer/analyze
      response =
          schemaDesignerAPI.analyze(
              configSet, schemaVersion, null, null, List.of("en"), false, null, null);

      assertNotNull(response.unknownProperties().get(CONFIG_SET_PARAM));
      assertNotNull(response.unknownProperties().get(SCHEMA_VERSION_PARAM));
      assertNotNull(response.unknownProperties().get("fields"));
      assertNotNull(response.unknownProperties().get("fieldTypes"));
      assertNotNull(response.unknownProperties().get("docIds"));

      // capture the schema version for MVCC
      schemaVersion = (Integer) response.unknownProperties().get(SCHEMA_VERSION_PARAM);
    }

    // get info (from the temp)
    // GET /schema-designer/info
    response = schemaDesignerAPI.getInfo(configSet);
    expSettings =
        Map.of(
            ENABLE_DYNAMIC_FIELDS_PARAM, false,
            ENABLE_FIELD_GUESSING_PARAM, true,
            ENABLE_NESTED_DOCS_PARAM, false,
            LANGUAGES_PARAM, Collections.singletonList("en"),
            COPY_FROM_PARAM, "_default");
    assertDesignerSettings(expSettings, response.unknownProperties());

    // query to see how the schema decisions impact retrieval / ranking
    ModifiableSolrParams queryParams = new ModifiableSolrParams();
    queryParams.set(SCHEMA_VERSION_PARAM, String.valueOf(schemaVersion));
    queryParams.set(CONFIG_SET_PARAM, configSet);
    queryParams.set(CommonParams.Q, "*:*");
    when(mockReq.getParams()).thenReturn(queryParams);
    when(mockReq.getContentStreams()).thenReturn(null);

    // GET /schema-designer/query
    response = schemaDesignerAPI.query(configSet);
    assertNotNull(response.unknownProperties().get("responseHeader"));
    @SuppressWarnings("unchecked")
    Map<String, Object> queryResponse =
        (Map<String, Object>) response.unknownProperties().get("response");
    assertNotNull("response object must be a map with numFound/docs", queryResponse);
    assertEquals(47L, queryResponse.get("numFound"));
    @SuppressWarnings("unchecked")
    List<Object> queryDocs = (List<Object>) queryResponse.get("docs");
    assertNotNull("response.docs must be a list", queryDocs);
    assertFalse("response.docs must be non-empty", queryDocs.isEmpty());

    // publish schema to a config set that can be used by real collections
    String collection = "techproducts";
    schemaDesignerAPI.publish(configSet, schemaVersion, collection, true, 1, 1, true, true, true);
    assertNotNull(cc.getZkController().zkStateReader.getCollection(collection));

    // listCollectionsForConfig
    response = schemaDesignerAPI.listCollectionsForConfig(configSet);
    List<String> collections = (List<String>) response.unknownProperties().get("collections");
    assertNotNull(collections);
    assertTrue(collections.contains(collection));

    // now try to create another temp, which should fail since designer is disabled for this
    // configSet now
    try {
      schemaDesignerAPI.prepNewSchema(configSet, null);
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
    when(mockReq.getContentStreams()).thenReturn(Collections.singletonList(stream));

    // POST /schema-designer/analyze
    FlexibleSolrJerseyResponse response =
        schemaDesignerAPI.analyze(configSet, null, null, null, null, true, null, null);

    assertNotNull(response.unknownProperties().get(CONFIG_SET_PARAM));
    assertNotNull(response.unknownProperties().get(SCHEMA_VERSION_PARAM));
    assertNotNull(response.unknownProperties().get("fields"));
    assertNotNull(response.unknownProperties().get("fieldTypes"));
    List<String> docIds = (List<String>) response.unknownProperties().get("docIds");
    assertNotNull(docIds);
    assertEquals(100, docIds.size()); // designer limits the doc ids to top 100

    String idField = (String) response.unknownProperties().get(UNIQUE_KEY_FIELD_PARAM);
    assertNotNull(idField);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testBasicUserWorkflow() throws Exception {
    String configSet = "testJson";

    // Use the prep endpoint to prepare the new schema
    FlexibleSolrJerseyResponse response = schemaDesignerAPI.prepNewSchema(configSet, null);
    assertNotNull(response.unknownProperties().get(CONFIG_SET_PARAM));
    assertNotNull(response.unknownProperties().get(SCHEMA_VERSION_PARAM));

    Map<String, Object> expSettings =
        Map.of(
            ENABLE_DYNAMIC_FIELDS_PARAM, true,
            ENABLE_FIELD_GUESSING_PARAM, true,
            ENABLE_NESTED_DOCS_PARAM, false,
            LANGUAGES_PARAM, List.of(),
            COPY_FROM_PARAM, "_default");
    assertDesignerSettings(expSettings, response.unknownProperties());

    // Analyze some sample documents to refine the schema
    Path booksJson = ExternalPaths.SOURCE_HOME.resolve("example/exampledocs/books.json");
    ContentStreamBase.FileStream stream = new ContentStreamBase.FileStream(booksJson);
    stream.setContentType(JSON_MIME);
    ModifiableSolrParams reqParams = new ModifiableSolrParams();
    reqParams.set(CONFIG_SET_PARAM, configSet);
    when(mockReq.getParams()).thenReturn(reqParams);
    when(mockReq.getContentStreams()).thenReturn(Collections.singletonList(stream));

    // POST /schema-designer/analyze
    response = schemaDesignerAPI.analyze(configSet, null, null, null, null, null, null, null);

    assertNotNull(response.unknownProperties().get(CONFIG_SET_PARAM));
    assertNotNull(response.unknownProperties().get(SCHEMA_VERSION_PARAM));
    assertNotNull(response.unknownProperties().get("fields"));
    assertNotNull(response.unknownProperties().get("fieldTypes"));
    assertNotNull(response.unknownProperties().get("docIds"));
    String idField = (String) response.unknownProperties().get(UNIQUE_KEY_FIELD_PARAM);
    assertNotNull(idField);
    assertDesignerSettings(expSettings, response.unknownProperties());

    // capture the schema version for MVCC
    int schemaVersion = (Integer) response.unknownProperties().get(SCHEMA_VERSION_PARAM);

    // load the contents of a file
    Collection<String> files = (Collection<String>) response.unknownProperties().get("files");
    assertTrue(files != null && !files.isEmpty());

    String file = null;
    for (String f : files) {
      if ("solrconfig.xml".equals(f)) {
        file = f;
        break;
      }
    }
    assertNotNull("solrconfig.xml not found in files!", file);
    response = schemaDesignerAPI.getFileContents(configSet, file);
    String solrconfigXml = (String) response.unknownProperties().get(file);
    assertNotNull(solrconfigXml);

    // Update solrconfig.xml
    when(mockReq.getContentStreams())
        .thenReturn(
            Collections.singletonList(
                new ContentStreamBase.StringStream(solrconfigXml, "application/xml")));
    response = schemaDesignerAPI.updateFileContents(configSet, file);
    schemaVersion = (Integer) response.unknownProperties().get(SCHEMA_VERSION_PARAM);

    // update solrconfig.xml with some invalid XML mess
    when(mockReq.getContentStreams())
        .thenReturn(
            Collections.singletonList(
                new ContentStreamBase.StringStream("<config/>", "application/xml")));

    // this should fail b/c the updated solrconfig.xml is invalid
    response = schemaDesignerAPI.updateFileContents(configSet, file);
    assertNotNull(response.unknownProperties().get("updateFileError"));

    // remove dynamic fields and change the language to "en" only
    when(mockReq.getContentStreams()).thenReturn(null);
    // POST /schema-designer/analyze
    response =
        schemaDesignerAPI.analyze(
            configSet, schemaVersion, null, null, List.of("en"), false, false, null);

    expSettings =
        Map.of(
            ENABLE_DYNAMIC_FIELDS_PARAM, false,
            ENABLE_FIELD_GUESSING_PARAM, false,
            ENABLE_NESTED_DOCS_PARAM, false,
            LANGUAGES_PARAM, Collections.singletonList("en"),
            COPY_FROM_PARAM, "_default");
    assertDesignerSettings(expSettings, response.unknownProperties());

    List<String> filesInResp = (List<String>) response.unknownProperties().get("files");
    assertEquals(5, filesInResp.size());
    assertTrue(filesInResp.contains("lang/stopwords_en.txt"));

    schemaVersion = (Integer) response.unknownProperties().get(SCHEMA_VERSION_PARAM);

    // add the dynamic fields back and change the languages too
    response =
        schemaDesignerAPI.analyze(
            configSet, schemaVersion, null, null, Arrays.asList("en", "fr"), true, false, null);

    expSettings =
        Map.of(
            ENABLE_DYNAMIC_FIELDS_PARAM, true,
            ENABLE_FIELD_GUESSING_PARAM, false,
            ENABLE_NESTED_DOCS_PARAM, false,
            LANGUAGES_PARAM, Arrays.asList("en", "fr"),
            COPY_FROM_PARAM, "_default");
    assertDesignerSettings(expSettings, response.unknownProperties());

    filesInResp = (List<String>) response.unknownProperties().get("files");
    assertEquals(7, filesInResp.size());
    assertTrue(filesInResp.contains("lang/stopwords_fr.txt"));

    schemaVersion = (Integer) response.unknownProperties().get(SCHEMA_VERSION_PARAM);

    // add back all the default languages (using "*" wildcard -> empty list)
    response =
        schemaDesignerAPI.analyze(
            configSet, schemaVersion, null, null, List.of("*"), false, null, null);

    expSettings =
        Map.of(
            ENABLE_DYNAMIC_FIELDS_PARAM, false,
            ENABLE_FIELD_GUESSING_PARAM, false,
            ENABLE_NESTED_DOCS_PARAM, false,
            LANGUAGES_PARAM, List.of(),
            COPY_FROM_PARAM, "_default");
    assertDesignerSettings(expSettings, response.unknownProperties());

    filesInResp = (List<String>) response.unknownProperties().get("files");
    assertEquals(43, filesInResp.size());
    assertTrue(filesInResp.contains("lang/stopwords_fr.txt"));
    assertTrue(filesInResp.contains("lang/stopwords_en.txt"));
    assertTrue(filesInResp.contains("lang/stopwords_it.txt"));

    schemaVersion = (Integer) response.unknownProperties().get(SCHEMA_VERSION_PARAM);

    // Get the value of a sample document
    String docId = "978-0641723445";
    String fieldName = "series_t";

    // GET /schema-designer/sample
    response = schemaDesignerAPI.getSampleValue(configSet, fieldName, idField, docId);
    assertNotNull(response.unknownProperties().get(idField));
    assertNotNull(response.unknownProperties().get(fieldName));
    assertNotNull(response.unknownProperties().get("analysis"));

    // at this point the user would refine the schema by
    // editing suggestions for fields and adding/removing fields / field types as needed

    // add a new field
    stream = new ContentStreamBase.FileStream(getFile("schema-designer/add-new-field.json"));
    stream.setContentType(JSON_MIME);
    when(mockReq.getContentStreams()).thenReturn(Collections.singletonList(stream));

    // POST /schema-designer/add
    response = schemaDesignerAPI.addSchemaObject(configSet, schemaVersion);
    assertNotNull(response.unknownProperties().get("add-field"));
    schemaVersion = (Integer) response.unknownProperties().get(SCHEMA_VERSION_PARAM);
    assertNotNull(response.unknownProperties().get("fields"));

    // update an existing field
    // switch a single-valued field to a multivalued field, which triggers a full rebuild of the
    // "temp" collection
    stream = new ContentStreamBase.FileStream(getFile("schema-designer/update-author-field.json"));
    stream.setContentType(JSON_MIME);
    when(mockReq.getContentStreams()).thenReturn(Collections.singletonList(stream));

    // PUT /schema-designer/update
    response = schemaDesignerAPI.updateSchemaObject(configSet, schemaVersion);
    assertNotNull(response.unknownProperties().get("field"));
    schemaVersion = (Integer) response.unknownProperties().get(SCHEMA_VERSION_PARAM);

    // add a new type
    stream = new ContentStreamBase.FileStream(getFile("schema-designer/add-new-type.json"));
    stream.setContentType(JSON_MIME);
    when(mockReq.getContentStreams()).thenReturn(Collections.singletonList(stream));

    // POST /schema-designer/add
    response = schemaDesignerAPI.addSchemaObject(configSet, schemaVersion);
    final String expectedTypeName = "test_txt";
    assertEquals(expectedTypeName, response.unknownProperties().get("add-field-type"));
    schemaVersion = (Integer) response.unknownProperties().get(SCHEMA_VERSION_PARAM);
    assertNotNull(response.unknownProperties().get("fieldTypes"));
    List<SimpleOrderedMap<Object>> fieldTypes =
        (List<SimpleOrderedMap<Object>>) response.unknownProperties().get("fieldTypes");
    Optional<SimpleOrderedMap<Object>> expected =
        fieldTypes.stream().filter(m -> expectedTypeName.equals(m.get("name"))).findFirst();
    assertTrue(
        "New field type '" + expectedTypeName + "' not found in add type response!",
        expected.isPresent());

    stream = new ContentStreamBase.FileStream(getFile("schema-designer/update-type.json"));
    stream.setContentType(JSON_MIME);
    when(mockReq.getContentStreams()).thenReturn(Collections.singletonList(stream));

    // POST /schema-designer/update
    response = schemaDesignerAPI.updateSchemaObject(configSet, schemaVersion);
    schemaVersion = (Integer) response.unknownProperties().get(SCHEMA_VERSION_PARAM);

    // query to see how the schema decisions impact retrieval / ranking
    ModifiableSolrParams queryParams = new ModifiableSolrParams();
    queryParams.set(SCHEMA_VERSION_PARAM, String.valueOf(schemaVersion));
    queryParams.set(CONFIG_SET_PARAM, configSet);
    queryParams.set(CommonParams.Q, "*:*");
    when(mockReq.getParams()).thenReturn(queryParams);
    when(mockReq.getContentStreams()).thenReturn(null);

    // GET /schema-designer/query
    response = schemaDesignerAPI.query(configSet);
    assertNotNull(response.unknownProperties().get("responseHeader"));
    @SuppressWarnings("unchecked")
    Map<String, Object> queryResponse2 =
        (Map<String, Object>) response.unknownProperties().get("response");
    assertNotNull("response object must be a map with numFound/docs", queryResponse2);
    @SuppressWarnings("unchecked")
    List<Object> queryDocs2 = (List<Object>) queryResponse2.get("docs");
    assertEquals(4, queryDocs2.size());

    // Download ZIP
    when(mockReq.getContentStreams()).thenReturn(null);
    Response downloadResponse = schemaDesignerAPI.downloadConfig(configSet);
    assertNotNull(downloadResponse);
    assertEquals(200, downloadResponse.getStatus());
    assertTrue(
        String.valueOf(downloadResponse.getHeaderString("Content-Disposition"))
            .contains("_configset.zip"));

    // publish schema to a config set that can be used by real collections
    String collection = "test123";
    schemaDesignerAPI.publish(configSet, schemaVersion, collection, true, 1, 1, true, true, false);

    assertNotNull(cc.getZkController().zkStateReader.getCollection(collection));

    // listCollectionsForConfig
    response = schemaDesignerAPI.listCollectionsForConfig(configSet);
    List<String> collections = (List<String>) response.unknownProperties().get("collections");
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
    FlexibleSolrJerseyResponse response = schemaDesignerAPI.prepNewSchema(configSet, null);
    assertNotNull(response.unknownProperties().get(CONFIG_SET_PARAM));
    assertNotNull(response.unknownProperties().get(SCHEMA_VERSION_PARAM));
    int schemaVersion = (Integer) response.unknownProperties().get(SCHEMA_VERSION_PARAM);

    // add our test field that we'll test various updates to
    ContentStreamBase.FileStream stream =
        new ContentStreamBase.FileStream(getFile("schema-designer/add-new-field.json"));
    stream.setContentType(JSON_MIME);
    when(mockReq.getContentStreams()).thenReturn(Collections.singletonList(stream));

    // POST /schema-designer/add
    response = schemaDesignerAPI.addSchemaObject(configSet, schemaVersion);
    assertNotNull(response.unknownProperties().get("add-field"));

    final String fieldName = "keywords";

    Optional<SimpleOrderedMap<Object>> maybeField =
        ((List<SimpleOrderedMap<Object>>) response.unknownProperties().get("fields"))
            .stream().filter(m -> fieldName.equals(m.get("name"))).findFirst();
    assertTrue(maybeField.isPresent());
    SimpleOrderedMap<Object> field = maybeField.get();
    assertEquals(Boolean.FALSE, field.get("indexed"));
    assertEquals(Boolean.FALSE, field.get("required"));
    assertEquals(Boolean.TRUE, field.get("stored"));
    assertEquals(Boolean.TRUE, field.get("docValues"));
    assertEquals(Boolean.TRUE, field.get("useDocValuesAsStored"));
    assertEquals(Boolean.FALSE, field.get("multiValued"));
    assertEquals("string", field.get("type"));

    String mutableId = getMutableId(configSet);
    SchemaDesignerConfigSetHelper configSetHelper =
        new SchemaDesignerConfigSetHelper(cc, SchemaDesignerAPI.newSchemaSuggester());
    ManagedIndexSchema schema = schemaDesignerAPI.loadLatestSchema(mutableId);

    // make it required
    Map<String, Object> updateField =
        Map.of("name", fieldName, "type", field.get("type"), "required", true);
    configSetHelper.updateField(configSet, updateField, schema);

    schema = schemaDesignerAPI.loadLatestSchema(mutableId);
    SchemaField schemaField = schema.getField(fieldName);
    assertTrue(schemaField.isRequired());

    updateField =
        Map.of("name", fieldName, "type", field.get("type"), "required", false, "stored", false);
    configSetHelper.updateField(configSet, updateField, schema);
    schema = schemaDesignerAPI.loadLatestSchema(mutableId);
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
    schema = schemaDesignerAPI.loadLatestSchema(mutableId);
    schemaField = schema.getField(fieldName);
    assertFalse(schemaField.isRequired());
    assertFalse(schemaField.stored());
    assertTrue(schemaField.multiValued());

    updateField = Map.of("name", fieldName, "type", "strings", "copyDest", "_text_");
    configSetHelper.updateField(configSet, updateField, schema);
    schema = schemaDesignerAPI.loadLatestSchema(mutableId);
    schemaField = schema.getField(fieldName);
    assertTrue(schemaField.multiValued());
    assertEquals("strings", schemaField.getType().getTypeName());
    assertFalse(schemaField.isRequired());
    assertTrue(schemaField.stored());
    List<String> srcFields = schema.getCopySources("_text_");
    assertEquals(Collections.singletonList(fieldName), srcFields);
  }

  @SuppressWarnings({"unchecked"})
  public void testSchemaDiffEndpoint() throws Exception {
    String configSet = "testDiff";

    // Use the prep endpoint to prepare the new schema
    FlexibleSolrJerseyResponse response = schemaDesignerAPI.prepNewSchema(configSet, null);
    assertNotNull(response.unknownProperties().get(CONFIG_SET_PARAM));
    assertNotNull(response.unknownProperties().get(SCHEMA_VERSION_PARAM));
    int schemaVersion = (Integer) response.unknownProperties().get(SCHEMA_VERSION_PARAM);

    // publish schema to a config set that can be used by real collections
    String collection = "diff456";
    schemaDesignerAPI.publish(configSet, schemaVersion, collection, true, 1, 1, true, true, false);

    assertNotNull(cc.getZkController().zkStateReader.getCollection(collection));

    // Load the schema designer for the existing config set and make some changes to it
    ModifiableSolrParams reqParams = new ModifiableSolrParams();
    reqParams.set(CONFIG_SET_PARAM, configSet);
    when(mockReq.getParams()).thenReturn(reqParams);
    when(mockReq.getContentStreams()).thenReturn(null);
    response = schemaDesignerAPI.analyze(configSet, null, null, null, null, true, false, null);

    // Update id field to not use docValues
    List<SimpleOrderedMap<Object>> fields =
        (List<SimpleOrderedMap<Object>>) response.unknownProperties().get("fields");
    SimpleOrderedMap<Object> idFieldMap =
        fields.stream().filter(field -> field.get("name").equals("id")).findFirst().get();
    idFieldMap.remove("copyDest"); // Don't include copyDest as it is not a property of SchemaField
    SimpleOrderedMap<Object> idFieldMapUpdated = idFieldMap.clone();
    idFieldMapUpdated.setVal(idFieldMapUpdated.indexOf("docValues", 0), Boolean.FALSE);
    idFieldMapUpdated.setVal(idFieldMapUpdated.indexOf("useDocValuesAsStored", 0), Boolean.FALSE);
    idFieldMapUpdated.setVal(idFieldMapUpdated.indexOf("uninvertible", 0), Boolean.TRUE);
    idFieldMapUpdated.setVal(
        idFieldMapUpdated.indexOf("omitTermFreqAndPositions", 0), Boolean.FALSE);

    Map<String, Object> mapParams = idFieldMapUpdated.toSolrParams().toMap(new HashMap<>());
    mapParams.put("termVectors", Boolean.FALSE);
    schemaVersion = (Integer) response.unknownProperties().get(SCHEMA_VERSION_PARAM);

    ContentStreamBase.StringStream stringStream =
        new ContentStreamBase.StringStream(JSONUtil.toJSON(mapParams), JSON_MIME);
    when(mockReq.getContentStreams()).thenReturn(Collections.singletonList(stringStream));

    response = schemaDesignerAPI.updateSchemaObject(configSet, schemaVersion);

    // Add a new field
    schemaVersion = (Integer) response.unknownProperties().get(SCHEMA_VERSION_PARAM);
    ContentStreamBase.FileStream fileStream =
        new ContentStreamBase.FileStream(getFile("schema-designer/add-new-field.json"));
    fileStream.setContentType(JSON_MIME);
    when(mockReq.getContentStreams()).thenReturn(Collections.singletonList(fileStream));
    // POST /schema-designer/add
    response = schemaDesignerAPI.addSchemaObject(configSet, schemaVersion);
    assertNotNull(response.unknownProperties().get("add-field"));

    // Add a new field type
    schemaVersion = (Integer) response.unknownProperties().get(SCHEMA_VERSION_PARAM);
    fileStream = new ContentStreamBase.FileStream(getFile("schema-designer/add-new-type.json"));
    fileStream.setContentType(JSON_MIME);
    when(mockReq.getContentStreams()).thenReturn(Collections.singletonList(fileStream));
    // POST /schema-designer/add
    response = schemaDesignerAPI.addSchemaObject(configSet, schemaVersion);
    assertNotNull(response.unknownProperties().get("add-field-type"));

    // Let's do a diff now
    response = schemaDesignerAPI.getSchemaDiff(configSet);

    Map<String, Object> diff = (Map<String, Object>) response.unknownProperties().get("diff");

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
    schemaDesignerAPI.prepNewSchema(configSet, null);
    ContentStreamBase.StringStream stream =
        new ContentStreamBase.StringStream("[{\"id\":\"doc1\",\"title\":\"test doc\"}]", JSON_MIME);
    ModifiableSolrParams reqParams = new ModifiableSolrParams();
    reqParams.set(CONFIG_SET_PARAM, configSet);
    when(mockReq.getParams()).thenReturn(reqParams);
    when(mockReq.getContentStreams()).thenReturn(Collections.singletonList(stream));
    schemaDesignerAPI.analyze(configSet, null, null, null, null, null, null, null);

    // Build a fresh API instance whose indexedVersion cache is empty (so it always
    // attempts to re-index before running the query), and which simulates indexing errors.
    Map<Object, Throwable> fakeErrors = new HashMap<>();
    fakeErrors.put("doc1", new RuntimeException("simulated indexing failure"));
    SchemaDesignerAPI apiWithErrors =
        new SchemaDesignerAPI(
            cc,
            SchemaDesignerAPI.newSchemaSuggester(),
            SchemaDesignerAPI.newSampleDocumentsLoader(),
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
    schemaDesignerAPI.prepNewSchema(configSet, null);

    // null schemaVersion must be rejected
    SolrException nullEx =
        expectThrows(SolrException.class, () -> schemaDesignerAPI.addSchemaObject(configSet, null));
    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, nullEx.code());

    // negative schemaVersion must be rejected (was previously bypassing validation)
    SolrException negEx =
        expectThrows(SolrException.class, () -> schemaDesignerAPI.addSchemaObject(configSet, -1));
    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, negEx.code());

    // same contract must hold for updateSchemaObject
    SolrException updateNegEx =
        expectThrows(
            SolrException.class, () -> schemaDesignerAPI.updateSchemaObject(configSet, -1));
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
}
