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

import static org.apache.solr.common.util.Utils.toJavabin;
import static org.apache.solr.handler.admin.ConfigSetsHandler.DEFAULT_CONFIGSET_NAME;
import static org.apache.solr.handler.designer.SchemaDesigner.getMutableId;
import static org.apache.solr.schema.IndexSchema.NEST_PATH_FIELD_NAME;
import static org.apache.solr.schema.IndexSchema.ROOT_FIELD_NAME;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.ConfigSetService;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.filestore.FileStore;
import org.apache.solr.handler.configsets.DownloadConfigSet;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.ManagedIndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.util.ExternalPaths;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestSchemaDesignerConfigSetHelper extends SolrCloudTestCase
    implements SchemaDesignerConstants {

  private CoreContainer cc;
  private SchemaDesignerConfigSetHelper helper;

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
    helper = new SchemaDesignerConfigSetHelper(cc, SchemaDesigner.newSchemaSuggester());
  }

  @Test
  public void testDefaultConfigSetContainsGenericTypesOnly() throws Exception {
    ManagedIndexSchema schema = helper.loadLatestSchema(DEFAULT_CONFIGSET_NAME);
    assertNotNull(schema.getFieldTypeByName("text_general"));
    assertNull(schema.getFieldTypeByName("text_en"));
    assertNull(schema.getFieldTypeByName("text_fr"));
    assertNull(schema.getFieldOrNull("*_txt_en"));
    assertFalse(
        cc.getConfigSetService()
            .getAllConfigFiles(DEFAULT_CONFIGSET_NAME)
            .contains("lang/stopwords_en.txt"));
  }

  @Test
  public void testMultilingualConfigSetIsBootstrapped() throws Exception {
    assertTrue(cc.getConfigSetService().checkConfigExists("_multilingual"));
    ManagedIndexSchema schema = helper.loadLatestSchema("_multilingual");
    assertNotNull(schema.getFieldTypeByName("text_general"));
    assertNotNull(schema.getFieldTypeByName("text_en"));
    assertNotNull(schema.getFieldTypeByName("text_fr"));
    assertNotNull(schema.getFieldOrNull("*_txt_en"));
    assertNotNull(schema.getFieldOrNull("*_txt_fr"));
    assertTrue(
        cc.getConfigSetService()
            .getAllConfigFiles("_multilingual")
            .contains("lang/protwords_en.txt"));
  }

  @Test
  public void testBootstrapPreservesExistingMultilingualConfigSet() throws Exception {
    var service = cc.getConfigSetService();
    String file = "lang/stopwords_fr.txt";
    byte[] original = service.downloadFileFromConfig("_multilingual", file);
    byte[] customized = "custom-stopword\n".getBytes(StandardCharsets.UTF_8);
    try {
      service.uploadFileToConfig("_multilingual", file, customized, true);
      ConfigSetService.createConfigSetService(cc);
      assertArrayEquals(customized, service.downloadFileFromConfig("_multilingual", file));
    } finally {
      service.uploadFileToConfig("_multilingual", file, original, true);
    }
  }

  @Test
  public void testLanguageRestorePreservesCustomSource() throws Exception {
    String source = "customLanguageSource";
    var service = cc.getConfigSetService();
    service.copyConfig("_multilingual", source);
    ManagedIndexSchema custom = helper.loadLatestSchema(source);
    custom =
        custom.replaceFieldType(
            "text_fr",
            "solr.TextField",
            Map.of(
                "name",
                "text_fr",
                "class",
                "solr.TextField",
                "positionIncrementGap",
                "222",
                "analyzer",
                Map.of("tokenizer", Map.of("name", "keyword"))));
    custom = custom.deleteDynamicFields(List.of("*_txt_fr"));
    custom = helper.removeLanguageSpecificObjectsAndFiles(source, custom, List.of("fr"));
    custom.persistManagedSchema(false);
    byte[] stopwords = "custom-stopword\n".getBytes(StandardCharsets.UTF_8);
    service.uploadFileToConfig(source, "lang/stopwords_fr.txt", stopwords, true);

    String target = getMutableId("languageRestorePreservesCustomSource");
    service.copyConfig(DEFAULT_CONFIGSET_NAME, target);
    ManagedIndexSchema schema =
        helper.syncLanguageSpecificObjectsAndFiles(
            target, helper.loadLatestSchema(target), List.of("fr", "es"), true, source);
    assertEquals(
        222,
        schema.getFieldTypeByName("text_fr").getIndexAnalyzer().getPositionIncrementGap("title"));
    assertEquals(
        222,
        schema
            .getFieldOrNull("*_txt_fr")
            .getType()
            .getIndexAnalyzer()
            .getPositionIncrementGap("title"));
    assertNotNull(schema.getFieldTypeByName("text_es"));
    assertNotNull(schema.getFieldOrNull("*_txt_es"));
    assertArrayEquals(stopwords, service.downloadFileFromConfig(target, "lang/stopwords_fr.txt"));
    assertNotNull(helper.loadLatestSchema(target).getFieldTypeByName("text_es"));

    String originalDefault = "originalDefaultForLanguageRestore";
    service.copyConfig(DEFAULT_CONFIGSET_NAME, originalDefault);
    try {
      service.deleteConfig(DEFAULT_CONFIGSET_NAME);
      service.copyConfig(source, DEFAULT_CONFIGSET_NAME);
      String upgradeTarget = getMutableId("restoreFromExistingDefault");
      service.copyConfig(originalDefault, upgradeTarget);
      ManagedIndexSchema restored =
          helper.syncLanguageSpecificObjectsAndFiles(
              upgradeTarget,
              helper.loadLatestSchema(upgradeTarget),
              List.of("fr"),
              true,
              DEFAULT_CONFIGSET_NAME);
      assertEquals(
          222,
          restored
              .getFieldTypeByName("text_fr")
              .getIndexAnalyzer()
              .getPositionIncrementGap("title"));
      assertArrayEquals(
          stopwords, service.downloadFileFromConfig(upgradeTarget, "lang/stopwords_fr.txt"));
    } finally {
      service.deleteConfig(DEFAULT_CONFIGSET_NAME);
      service.copyConfig(originalDefault, DEFAULT_CONFIGSET_NAME);
      service.deleteConfig(originalDefault);
    }
  }

  @Test
  public void testRestoreLanguageDynamicFieldsFromSlimDefault() throws Exception {
    String target = getMutableId("restoreLanguageDynamicFields");
    cc.getConfigSetService().copyConfig(DEFAULT_CONFIGSET_NAME, target);
    ManagedIndexSchema schema =
        helper.syncLanguageSpecificObjectsAndFiles(
            target, helper.loadLatestSchema(target), List.of("fr"), false, DEFAULT_CONFIGSET_NAME);
    assertNull(schema.getFieldOrNull("*_txt_fr"));
    schema = helper.restoreDynamicFields(target, schema, List.of("fr"), DEFAULT_CONFIGSET_NAME);
    assertNotNull(schema.getFieldOrNull("*_txt_fr"));
    assertNull(schema.getFieldOrNull("*_txt_en"));
    schema.persistManagedSchema(false);
    assertNotNull(helper.loadLatestSchema(target).getFieldOrNull("*_txt_fr"));
  }

  @Test
  public void testSetupMutable() throws Exception {
    String configSet = "testSetupMutable";
    String mutableId = getMutableId(configSet);
    // create our test config by copying from _default
    cc.getConfigSetService().copyConfig(DEFAULT_CONFIGSET_NAME, mutableId);

    SolrConfig solrConfig = helper.loadSolrConfig(mutableId);
    assertNotNull(solrConfig);
    ManagedIndexSchema schema = helper.loadLatestSchema(solrConfig);
    assertNotNull(schema);
    int currentVersion = helper.getCurrentSchemaVersion(mutableId);
    assertEquals(schema.getSchemaZkVersion(), currentVersion);

    assertTrue(schema.persistManagedSchema(false));

    // create the temp collection
    helper.createCollection(mutableId, mutableId);

    List<String> collsForConfig = helper.listCollectionsForConfig(configSet);
    assertTrue(collsForConfig.isEmpty());

    helper.reloadTempCollection(mutableId, true);

    // version is incremented b/c we called persist on the schema above
    int version = helper.getCurrentSchemaVersion(mutableId);
    assertEquals(1, version);

    helper.checkSchemaVersion(mutableId, version, -1);

    schema =
        helper.syncLanguageSpecificObjectsAndFiles(
            mutableId, schema, List.of(), true, DEFAULT_CONFIGSET_NAME);
    assertEquals(2, schema.getSchemaZkVersion());

    byte[] zipped = DownloadConfigSet.zipConfigSet(cc.getConfigSetService(), mutableId);
    assertTrue(zipped != null && zipped.length > 0);
  }

  @Test
  public void testDownloadAndZip() throws IOException {
    byte[] zipped = DownloadConfigSet.zipConfigSet(cc.getConfigSetService(), "_multilingual");
    ZipInputStream stream = new ZipInputStream(new ByteArrayInputStream(zipped));

    boolean foundSolrConfig = false;
    boolean foundStopWords = false;

    ZipEntry entry;
    while ((entry = stream.getNextEntry()) != null) {
      // ZipEntry names have file separators that are OS specific. This normalizes to forward slash.
      String entryName = entry.getName().replace('\\', '/');
      if ("solrconfig.xml".equals(entryName)) {
        foundSolrConfig = true;
      } else if ("lang/stopwords_en.txt".equals(entryName)) {
        foundStopWords = true;
      }
    }
    assertTrue("Did not find solrconfig.xml in downloaded configset", foundSolrConfig);
    assertTrue("Did not find stopwords_en.txt in downloaded configset", foundStopWords);
  }

  @Test
  public void testEnableDisableOptions() throws Exception {
    String configSet = "testEnableDisableOptions";
    String mutableId = getMutableId(configSet);
    cc.getConfigSetService().copyConfig(DEFAULT_CONFIGSET_NAME, mutableId);

    ManagedIndexSchema schema = helper.loadLatestSchema(helper.loadSolrConfig(mutableId));
    assertEquals(schema.getSchemaZkVersion(), helper.getCurrentSchemaVersion(mutableId));

    schema =
        helper.syncLanguageSpecificObjectsAndFiles(
            mutableId, schema, List.of("en"), true, DEFAULT_CONFIGSET_NAME);
    assertNotNull(schema.getFieldTypeByName("text_en"));
    assertNotNull(schema.getFieldOrNull("*_txt_en"));
    assertNull(schema.getFieldTypeByName("text_fr"));

    schema =
        helper.syncLanguageSpecificObjectsAndFiles(
            mutableId, schema, List.of("en"), false, DEFAULT_CONFIGSET_NAME);
    assertNotNull(schema.getFieldTypeByName("text_en"));
    assertNull(schema.getFieldOrNull("*_txt_en"));
    assertNull(schema.getFieldTypeByName("text_fr"));

    schema =
        helper.syncLanguageSpecificObjectsAndFiles(
            mutableId, schema, Arrays.asList("en", "fr"), false, DEFAULT_CONFIGSET_NAME);
    assertNotNull(schema.getFieldTypeByName("text_en"));
    assertNull(schema.getFieldOrNull("*_txt_en"));
    assertNotNull(schema.getFieldTypeByName("text_fr"));

    schema =
        helper.syncLanguageSpecificObjectsAndFiles(
            mutableId, schema, Arrays.asList("en", "fr"), true, DEFAULT_CONFIGSET_NAME);
    assertNotNull(schema.getFieldTypeByName("text_en"));
    assertNotNull(schema.getFieldOrNull("*_txt_en"));
    assertTrue(
        cluster
            .getZkClient()
            .exists(SchemaDesigner.getConfigSetZkPath(mutableId, "lang/stopwords_en.txt")));
    assertNotNull(schema.getFieldTypeByName("text_fr"));
    assertNotNull(schema.getFieldOrNull("*_txt_fr"));
    assertNull(schema.getFieldOrNull("*_txt_ga"));

    // add a field that uses text_en and then try removing "en" from the lang set
    helper.createCollection(mutableId, mutableId); // need to create field
    Map<String, Object> addField = Map.of("name", "title", "type", "text_en");
    String addedFieldName = helper.addSchemaObject(configSet, Map.of("add-field", addField));
    assertEquals("title", addedFieldName);

    schema = helper.loadLatestSchema(helper.loadSolrConfig(mutableId));
    assertNotNull(schema.getField("title"));

    schema =
        helper.syncLanguageSpecificObjectsAndFiles(
            mutableId, schema, List.of("fr"), true, DEFAULT_CONFIGSET_NAME);
    assertNotNull(schema.getFieldTypeByName("text_en")); // being used, so not removed
    assertNotNull(schema.getFieldOrNull("*_txt_en"));
    assertTrue(
        cluster
            .getZkClient()
            .exists(SchemaDesigner.getConfigSetZkPath(mutableId, "lang/stopwords_en.txt")));
    assertNotNull(schema.getFieldTypeByName("text_fr"));
    assertNotNull(schema.getFieldOrNull("*_txt_fr"));
    assertNull(schema.getFieldOrNull("*_txt_ga"));

    schema =
        helper.syncLanguageSpecificObjectsAndFiles(
            mutableId, schema, List.of(), true, DEFAULT_CONFIGSET_NAME);
    assertNotNull(schema.getFieldTypeByName("text_en"));
    assertNotNull(schema.getFieldOrNull("*_txt_en"));
    assertNotNull(schema.getFieldTypeByName("text_fr"));
    assertNotNull(schema.getFieldOrNull("*_txt_fr"));
    assertNotNull(schema.getFieldTypeByName("text_ga"));
    assertNotNull(schema.getFieldOrNull("*_txt_ga"));

    schema =
        helper.syncLanguageSpecificObjectsAndFiles(
            mutableId, schema, List.of(), false, DEFAULT_CONFIGSET_NAME);
    assertNotNull(schema.getFieldTypeByName("text_en"));
    assertNull(schema.getFieldOrNull("*_txt_en"));
    assertNotNull(schema.getFieldTypeByName("text_fr"));
    assertNull(schema.getFieldOrNull("*_txt_fr"));
    assertNotNull(schema.getFieldTypeByName("text_ga"));
    assertNull(schema.getFieldOrNull("*_txt_ga"));

    schema = helper.toggleNestedDocsFields(schema, true);
    assertTrue(schema.hasExplicitField(ROOT_FIELD_NAME));
    assertTrue(schema.hasExplicitField(NEST_PATH_FIELD_NAME));
    schema = helper.toggleNestedDocsFields(schema, false);
    assertFalse(schema.hasExplicitField(ROOT_FIELD_NAME));
    assertFalse(schema.hasExplicitField(NEST_PATH_FIELD_NAME));
    schema = helper.toggleNestedDocsFields(schema, true);
    assertTrue(schema.hasExplicitField(ROOT_FIELD_NAME));
    assertTrue(schema.hasExplicitField(NEST_PATH_FIELD_NAME));
  }

  @Test
  public void testPersistSampleDocs() throws Exception {
    String configSet = "testPersistSampleDocs";

    SolrInputDocument doc = new SolrInputDocument();
    doc.setField("id", "1");
    doc.setField("author", "Ken Follet");
    doc.setField("title", "The Pillars of the Earth");
    doc.setField("series", "Kingsbridge Series");
    doc.setField("pages", 809);
    doc.setField("published_year", 1989);

    helper.storeSampleDocs(
        configSet, SchemaDesignerConfigSetHelper.readAllBytes(() -> toJavabin(List.of(doc))));

    List<SolrInputDocument> docs = helper.retrieveSampleDocs(configSet);
    assertTrue(docs != null && docs.size() == 1);
    assertEquals("1", docs.get(0).getFieldValue("id"));

    helper.deleteStoredSampleDocs(configSet);

    String path = helper.getSampleDocsPathFromConfigSet(configSet);
    FileStore.FileType type = cc.getFileStore().getType(path, true);
    assertEquals(FileStore.FileType.NOFILE, type);
  }

  @Test
  public void testRetrieveNonExistentDocsReturnsEmptyDocList() throws Exception {
    String configSet = "testRetrieveNonExistentDocsReturnsEmptyDocList";
    List<SolrInputDocument> docs = helper.retrieveSampleDocs(configSet);
    assertNotNull(docs);
    assertTrue(docs.isEmpty());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testAnalyzeField() throws Exception {
    String configSet = "testAnalyzeField";

    String mutableId = getMutableId(configSet);
    cc.getConfigSetService().copyConfig(DEFAULT_CONFIGSET_NAME, mutableId);

    ManagedIndexSchema schema = helper.loadLatestSchema(helper.loadSolrConfig(mutableId));
    assertEquals(schema.getSchemaZkVersion(), helper.getCurrentSchemaVersion(mutableId));
    helper.createCollection(mutableId, mutableId);

    Map<String, Object> addField = Map.of("name", "title", "type", "text_general");
    String addedFieldName = helper.addSchemaObject(configSet, Map.of("add-field", addField));
    assertEquals("title", addedFieldName);

    Map<String, Object> analysis =
        helper.analyzeField(configSet, "title", "The Pillars of the Earth");

    var index =
        (List<Object>)
            Utils.getObjectByPath(analysis, false, List.of("field_names", "title", "index"));
    assertNotNull(index);
    assertFalse(index.isEmpty());
  }

  @Test
  public void testCopyFieldUpdates() throws Exception {
    String configSet = "testCopyFieldUpdates";
    String mutableId = getMutableId(configSet);
    cc.getConfigSetService().copyConfig(DEFAULT_CONFIGSET_NAME, mutableId);

    ManagedIndexSchema schema = helper.loadLatestSchema(helper.loadSolrConfig(mutableId));
    assertEquals(schema.getSchemaZkVersion(), helper.getCurrentSchemaVersion(mutableId));
    helper.createCollection(mutableId, mutableId);

    // add / update field
    Map<String, Object> addField = Map.of("name", "author", "type", "string");
    String addedFieldName = helper.addSchemaObject(configSet, Map.of("add-field", addField));
    assertEquals("author", addedFieldName);

    helper.addSchemaObject(
        configSet, Map.of("add-field", Map.of("name", "_catch_all_", "type", "text_general")));

    Map<String, Object> updateField =
        Map.of("name", "author", "type", "string", "copyDest", "_text_");
    ManagedIndexSchema latest = helper.loadLatestSchema(helper.loadSolrConfig(mutableId));
    latest.getField("_catch_all_");

    Map<String, Object> resp = helper.updateSchemaObject(configSet, updateField, latest);
    assertNotNull(resp);
    assertEquals("field", resp.get("updateType"));
    assertEquals(false, resp.get("rebuild"));

    updateField = Map.of("name", "author", "type", "string", "copyDest", "_text_,_catch_all_");
    latest = helper.loadLatestSchema(helper.loadSolrConfig(mutableId));
    resp = helper.updateSchemaObject(configSet, updateField, latest);
    assertNotNull(resp);
    assertEquals("field", resp.get("updateType"));
    assertEquals(false, resp.get("rebuild"));

    latest = helper.loadLatestSchema(helper.loadSolrConfig(mutableId));
    assertEquals(List.of("author"), latest.getCopySources("_text_"));
    assertEquals(List.of("author"), latest.getCopySources("_catch_all_"));

    updateField = Map.of("name", "author", "type", "string", "copyDest", "");
    latest = helper.loadLatestSchema(helper.loadSolrConfig(mutableId));
    resp = helper.updateSchemaObject(configSet, updateField, latest);
    assertNotNull(resp);
    assertEquals("field", resp.get("updateType"));
    assertEquals(false, resp.get("rebuild"));
    latest = helper.loadLatestSchema(helper.loadSolrConfig(mutableId));
    assertTrue(latest.getCopySources("_text_").isEmpty());
    assertTrue(latest.getCopySources("_catch_all_").isEmpty());
  }

  @Test
  public void testAddUpdateObjects() throws Exception {
    String configSet = "testAddUpdateObjects";
    String mutableId = getMutableId(configSet);
    cc.getConfigSetService().copyConfig(DEFAULT_CONFIGSET_NAME, mutableId);

    ManagedIndexSchema schema = helper.loadLatestSchema(helper.loadSolrConfig(mutableId));
    assertEquals(schema.getSchemaZkVersion(), helper.getCurrentSchemaVersion(mutableId));
    helper.createCollection(mutableId, mutableId);

    // add / update field
    Map<String, Object> addField = Map.of("name", "author", "type", "string");
    String addedFieldName = helper.addSchemaObject(configSet, Map.of("add-field", addField));
    assertEquals("author", addedFieldName);

    Map<String, Object> updateField = Map.of("name", "author", "type", "string", "required", true);
    ManagedIndexSchema latest = helper.loadLatestSchema(helper.loadSolrConfig(mutableId));
    Map<String, Object> resp = helper.updateSchemaObject(configSet, updateField, latest);
    assertNotNull(resp);
    assertEquals("field", resp.get("updateType"));
    assertEquals(false, resp.get("rebuild"));

    SchemaField addedField = latest.getField("author");
    assertFalse(addedField.multiValued());
    assertTrue(addedField.hasDocValues());

    // an update that requires a full-rebuild
    updateField =
        Map.of(
            "name",
            "author",
            "type",
            "string",
            "required",
            true,
            "docValues",
            true,
            "multiValued",
            true,
            "copyDest",
            "_text_");
    resp =
        helper.updateSchemaObject(
            configSet, updateField, helper.loadLatestSchema(helper.loadSolrConfig(mutableId)));
    assertNotNull(resp);
    assertEquals("field", resp.get("updateType"));
    assertEquals(true, resp.get("rebuild"));

    // did the copy field update get applied?
    latest = helper.loadLatestSchema(helper.loadSolrConfig(mutableId));
    assertEquals(List.of("author"), latest.getCopySources("_text_"));

    // switch the author field type to strings
    updateField =
        Map.of("name", "author", "type", "strings", "docValues", true, "copyDest", "_text_");
    resp =
        helper.updateSchemaObject(
            configSet, updateField, helper.loadLatestSchema(helper.loadSolrConfig(mutableId)));
    assertNotNull(resp);
    assertEquals("field", resp.get("updateType"));
    // tricky, we didn't actually change the field to multiValue (it already was)
    assertEquals(false, resp.get("rebuild"));

    // add / update field type
    Map<String, Object> addType =
        Map.of("name", "testType", "class", "solr.StrField", "docValues", true);
    String addTypeName = helper.addSchemaObject(configSet, Map.of("add-field-type", addType));
    assertEquals("testType", addTypeName);

    latest = helper.loadLatestSchema(helper.loadSolrConfig(mutableId));
    FieldType addedType = latest.getFieldTypeByName(addTypeName);
    assertNotNull(addedType);
    SimpleOrderedMap<Object> props = addedType.getNamedPropertyValues(false);
    assertTrue(props.getBooleanArg("docValues"));
    assertFalse(addedType.isMultiValued());

    Map<String, Object> updateType =
        Map.of(
            "name", "testType", "class", "solr.StrField", "docValues", true, "multiValued", true);
    resp =
        helper.updateSchemaObject(
            configSet, updateType, helper.loadLatestSchema(helper.loadSolrConfig(mutableId)));
    assertNotNull(resp);
    assertEquals("type", resp.get("updateType"));
    assertEquals(true, resp.get("rebuild"));

    // add / update dynamic field
    Map<String, Object> addDynField = Map.of("name", "*_test", "type", "string");
    String addedDynFieldName =
        helper.addSchemaObject(configSet, Map.of("add-dynamic-field", addDynField));
    assertEquals("*_test", addedDynFieldName);

    // update the dynamic field
    Map<String, Object> updateDynField =
        Map.of("name", "*_test", "type", "string", "docValues", false);
    resp =
        helper.updateSchemaObject(
            configSet, updateDynField, helper.loadLatestSchema(helper.loadSolrConfig(mutableId)));
    assertEquals("*_test", addedDynFieldName);
    assertNotNull(resp);
    assertEquals("dynamicField", resp.get("updateType"));
  }
}
