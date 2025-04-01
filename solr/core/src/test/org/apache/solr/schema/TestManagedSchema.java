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
package org.apache.solr.schema;

import com.fasterxml.jackson.core.type.TypeReference;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.commons.io.file.PathUtils;
import org.apache.lucene.tests.mockfile.FilterPath;
import org.apache.solr.client.api.model.CoreStatusResponse;
import org.apache.solr.client.solrj.JacksonContentWriter;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.AbstractBadConfigTestBase;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.admin.CoreAdminHandler;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestManagedSchema extends AbstractBadConfigTestBase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static Path tmpSolrHome;
  private static Path tmpConfDir;

  private static final String collection = "collection1";
  private static final String confDir = collection + "/conf";

  @Before
  public void initManagedSchemaCore() throws Exception {
    tmpSolrHome = createTempDir();
    tmpConfDir = FilterPath.unwrap(tmpSolrHome.resolve(confDir));
    Path testHomeConfDir = TEST_HOME().resolve(confDir);
    Files.createDirectories(tmpConfDir);
    PathUtils.copyFileToDirectory(
        testHomeConfDir.resolve("solrconfig-managed-schema.xml"),
        tmpConfDir,
        StandardCopyOption.REPLACE_EXISTING);
    PathUtils.copyFileToDirectory(testHomeConfDir.resolve("solrconfig-basic.xml"), tmpConfDir);
    PathUtils.copyFileToDirectory(
        testHomeConfDir.resolve("solrconfig-managed-schema-test.xml"), tmpConfDir);
    PathUtils.copyFileToDirectory(
        testHomeConfDir.resolve("solrconfig.snippet.randomindexconfig.xml"), tmpConfDir);
    PathUtils.copyFileToDirectory(
        testHomeConfDir.resolve("schema-one-field-no-dynamic-field.xml"), tmpConfDir);
    PathUtils.copyFileToDirectory(
        testHomeConfDir.resolve("schema-one-field-no-dynamic-field-unique-key.xml"), tmpConfDir);
    PathUtils.copyFileToDirectory(testHomeConfDir.resolve("schema-minimal.xml"), tmpConfDir);
    PathUtils.copyFileToDirectory(testHomeConfDir.resolve("schema_codec.xml"), tmpConfDir);
    PathUtils.copyFileToDirectory(testHomeConfDir.resolve("schema-bm25.xml"), tmpConfDir);

    // initCore will trigger an upgrade to managed schema, since the solrconfig has
    // <schemaFactory class="ManagedIndexSchemaFactory" ... />
    System.setProperty("managed.schema.mutable", "false");
    System.setProperty("enable.update.log", "false");
    initCore("solrconfig-managed-schema.xml", "schema-minimal.xml", tmpSolrHome);
  }

  @After
  public void afterClass() {
    deleteCore();
    System.clearProperty("managed.schema.mutable");
    System.clearProperty("enable.update.log");
  }

  public void testUpgrade() throws Exception {
    Path managedSchemaFile = tmpConfDir.resolve("managed-schema.xml");
    assertTrue(Files.exists(managedSchemaFile));
    String managedSchema = Files.readString(managedSchemaFile, StandardCharsets.UTF_8);
    assertTrue(managedSchema.contains("DO NOT EDIT"));
    Path upgradedOriginalSchemaFile = tmpConfDir.resolve("schema-minimal.xml.bak");
    assertTrue(Files.exists(upgradedOriginalSchemaFile));
    assertSchemaResource(collection, "managed-schema.xml");
  }

  public void testUpgradeThenRestart() throws Exception {
    assertSchemaResource(collection, "managed-schema.xml");
    deleteCore();
    Path nonManagedSchemaFile = tmpConfDir.resolve("schema-minimal.xml");
    assertFalse(Files.exists(nonManagedSchemaFile));
    initCore("solrconfig-managed-schema.xml", "schema-minimal.xml", tmpSolrHome);
    Path managedSchemaFile = tmpConfDir.resolve("managed-schema.xml");
    assertTrue(Files.exists(managedSchemaFile));
    String managedSchema = Files.readString(managedSchemaFile, StandardCharsets.UTF_8);
    assertTrue(managedSchema.contains("DO NOT EDIT"));
    Path upgradedOriginalSchemaFile = tmpConfDir.resolve("schema-minimal.xml.bak");
    assertTrue(Files.exists(upgradedOriginalSchemaFile));
    assertSchemaResource(collection, "managed-schema.xml");
  }

  public void testUpgradeThenRestartNonManaged() throws Exception {
    deleteCore();
    // After upgrade to managed schema, fail to restart when solrconfig doesn't contain
    // <schemaFactory class="ManagedIndexSchemaFactory">...</schemaFactory>
    assertConfigs(
        "solrconfig-basic.xml",
        "schema-minimal.xml",
        tmpSolrHome.toString(),
        "Can't find resource 'schema-minimal.xml'");
  }

  public void testUpgradeThenRestartNonManagedAfterPuttingBackNonManagedSchema() throws Exception {
    assertSchemaResource(collection, "managed-schema.xml");
    deleteCore();
    Path nonManagedSchemaFile = tmpConfDir.resolve("schema-minimal.xml");
    assertFalse(Files.exists(nonManagedSchemaFile));
    Path upgradedOriginalSchemaFile = tmpConfDir.resolve("schema-minimal.xml.bak");
    assertTrue(Files.exists(upgradedOriginalSchemaFile));

    // After upgrade to managed schema, downgrading to non-managed should work after putting back
    // the non-managed schema.
    Files.move(upgradedOriginalSchemaFile, nonManagedSchemaFile);
    initCore("solrconfig-basic.xml", "schema-minimal.xml", tmpSolrHome);
    assertSchemaResource(collection, "schema-minimal.xml");
  }

  public void testDefaultSchemaFactory() throws Exception {
    deleteCore();
    initCore("solrconfig-managed-schema-test.xml", "schema-minimal.xml", tmpSolrHome);

    final CoreContainer cores = h.getCoreContainer();
    final CoreAdminHandler admin = new CoreAdminHandler(cores);
    SolrQueryRequest request =
        req(CoreAdminParams.ACTION, CoreAdminParams.CoreAdminAction.STATUS.toString());
    SolrQueryResponse response = new SolrQueryResponse();
    admin.handleRequestBody(request, response);
    assertNull("Exception on create", response.getException());
    assertSchemaResource(collection, "managed-schema.xml");
  }

  private void assertSchemaResource(String collection, String expectedSchemaResource)
      throws Exception {
    final CoreContainer cores = h.getCoreContainer();
    final CoreAdminHandler admin = new CoreAdminHandler(cores);
    SolrQueryRequest request =
        req(CoreAdminParams.ACTION, CoreAdminParams.CoreAdminAction.STATUS.toString());
    SolrQueryResponse response = new SolrQueryResponse();
    admin.handleRequestBody(request, response);
    assertNull("Exception on create", response.getException());
    NamedList<?> responseValues = response.getValues();
    final var statusByCore =
        JacksonContentWriter.DEFAULT_MAPPER.convertValue(
            responseValues.get("status"),
            new TypeReference<Map<String, CoreStatusResponse.SingleCoreData>>() {});
    assertEquals(
        "Schema resource name differs from expected name",
        expectedSchemaResource,
        statusByCore.get(collection).schema);
  }

  public void testAddFieldWhenNotMutable() throws Exception {
    assertSchemaResource(collection, "managed-schema.xml");
    String errString = "This ManagedIndexSchema is not mutable.";
    ignoreException(Pattern.quote(errString));
    try {
      IndexSchema oldSchema = h.getCore().getLatestSchema();
      String fieldName = "new_field";
      String fieldType = "string";
      Map<String, ?> options = Collections.emptyMap();
      SchemaField newField = oldSchema.newField(fieldName, fieldType, options);
      IndexSchema newSchema = oldSchema.addField(newField);
      h.getCore().setLatestSchema(newSchema);
      fail();
    } catch (Exception e) {
      for (Throwable t = e; t != null; t = t.getCause()) {
        // short circuit out if we found what we expected
        if (t.getMessage() != null && t.getMessage().contains(errString)) return;
      }
      // otherwise, rethrow it, possibly completely unrelated
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR,
          "Unexpected error, expected error matching: " + errString,
          e);
    } finally {
      resetExceptionIgnores();
    }
  }

  public void testAddFieldPersistence() throws Exception {
    assertSchemaResource(collection, "managed-schema.xml");
    deleteCore();
    Path managedSchemaFile = tmpConfDir.resolve("managed-schema.xml");
    Files.delete(
        managedSchemaFile); // Delete managed-schema.xml, so it won't block parsing a new schema

    System.setProperty("managed.schema.mutable", "true");
    initCore("solrconfig-managed-schema.xml", "schema-one-field-no-dynamic-field.xml", tmpSolrHome);

    assertTrue(Files.exists(managedSchemaFile));
    String managedSchemaContents = Files.readString(managedSchemaFile, StandardCharsets.UTF_8);
    assertFalse(managedSchemaContents.contains("\"new_field\""));

    Map<String, Object> options = new HashMap<>();
    options.put("stored", "false");
    IndexSchema oldSchema = h.getCore().getLatestSchema();
    String fieldName = "new_field";
    String fieldType = "string";
    SchemaField newField = oldSchema.newField(fieldName, fieldType, options);
    IndexSchema newSchema = oldSchema.addField(newField);
    h.getCore().setLatestSchema(newSchema);

    assertTrue(Files.exists(managedSchemaFile));
    try (InputStream stream = Files.newInputStream(managedSchemaFile)) {
      managedSchemaContents = new String(stream.readAllBytes(), StandardCharsets.UTF_8);
    }
    assertTrue(
        managedSchemaContents.contains(
            "<field name=\"new_field\" type=\"string\" stored=\"false\"/>"));
  }

  public void testAddedFieldIndexableAndQueryable() throws Exception {
    assertSchemaResource(collection, "managed-schema.xml");
    deleteCore();
    Path managedSchemaFile = tmpConfDir.resolve("managed-schema.xml");
    // Delete managed-schema.xml, so it won't block parsing a new schema
    Files.delete(managedSchemaFile);
    System.setProperty("managed.schema.mutable", "true");
    initCore("solrconfig-managed-schema.xml", "schema-one-field-no-dynamic-field.xml", tmpSolrHome);

    assertTrue(Files.exists(managedSchemaFile));
    String managedSchemaContents = Files.readString(managedSchemaFile, StandardCharsets.UTF_8);
    assertFalse(managedSchemaContents.contains("\"new_field\""));

    clearIndex();

    String errString = "unknown field 'new_field'";
    ignoreException(Pattern.quote(errString));
    try {
      assertU(adoc("new_field", "thing1 thing2", "str", "X"));
      fail();
    } catch (Exception e) {
      for (Throwable t = e; t != null; t = t.getCause()) {
        // short circuit out if we found what we expected
        if (t.getMessage() != null && t.getMessage().contains(errString)) return;
      }
      // otherwise, rethrow it, possibly completely unrelated
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR,
          "Unexpected error, expected error matching: " + errString,
          e);
    } finally {
      resetExceptionIgnores();
    }
    assertU(commit());
    assertQ(req("new_field:thing1"), "//*[@numFound='0']");

    Map<String, Object> options = new HashMap<>();
    options.put("stored", "false");
    IndexSchema oldSchema = h.getCore().getLatestSchema();
    String fieldName = "new_field";
    String fieldType = "text";
    SchemaField newField = oldSchema.newField(fieldName, fieldType, options);
    IndexSchema newSchema = oldSchema.addField(newField);
    h.getCore().setLatestSchema(newSchema);

    assertU(adoc("new_field", "thing1 thing2", "str", "X"));
    assertU(commit());

    assertQ(req("new_field:thing1"), "//*[@numFound='1']");
  }

  public void testAddFieldWhenItAlreadyExists() throws Exception {
    deleteCore();
    Path managedSchemaFile = tmpConfDir.resolve("managed-schema.xml");
    // Delete managed-schema.xml, so it won't block parsing a new schema
    Files.delete(managedSchemaFile);
    System.setProperty("managed.schema.mutable", "true");
    initCore("solrconfig-managed-schema.xml", "schema-one-field-no-dynamic-field.xml", tmpSolrHome);

    assertNotNull(
        "Field 'str' is not present in the schema",
        h.getCore().getLatestSchema().getFieldOrNull("str"));

    String errString = "Field 'str' already exists.";
    ignoreException(Pattern.quote(errString));
    try {
      Map<String, Object> options = new HashMap<>();
      IndexSchema oldSchema = h.getCore().getLatestSchema();
      String fieldName = "str";
      String fieldType = "string";
      SchemaField newField = oldSchema.newField(fieldName, fieldType, options);
      IndexSchema newSchema = oldSchema.addField(newField);
      h.getCore().setLatestSchema(newSchema);
      fail("Should fail when adding a field that already exists");
    } catch (Exception e) {
      for (Throwable t = e; t != null; t = t.getCause()) {
        // short circuit out if we found what we expected
        if (t.getMessage() != null && t.getMessage().contains(errString)) return;
      }
      // otherwise, rethrow it, possibly completely unrelated
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR,
          "Unexpected error, expected error matching: " + errString,
          e);
    } finally {
      resetExceptionIgnores();
    }
  }

  public void testAddSameFieldTwice() throws Exception {
    deleteCore();
    Path managedSchemaFile = tmpConfDir.resolve("managed-schema.xml");
    // Delete managed-schema.xml, so it won't block parsing a new schema
    Files.delete(managedSchemaFile);
    System.setProperty("managed.schema.mutable", "true");
    initCore("solrconfig-managed-schema.xml", "schema-one-field-no-dynamic-field.xml", tmpSolrHome);

    Map<String, Object> options = new HashMap<>();
    options.put("stored", "false");
    IndexSchema oldSchema = h.getCore().getLatestSchema();
    String fieldName = "new_field";
    String fieldType = "text";
    SchemaField newField = oldSchema.newField(fieldName, fieldType, options);
    IndexSchema newSchema = oldSchema.addField(newField);
    h.getCore().setLatestSchema(newSchema);

    String errString = "Field 'new_field' already exists.";
    ignoreException(Pattern.quote(errString));
    try {
      newSchema = newSchema.addField(newField);
      h.getCore().setLatestSchema(newSchema);
      fail("Should fail when adding the same field twice");
    } catch (Exception e) {
      for (Throwable t = e; t != null; t = t.getCause()) {
        // short circuit out if we found what we expected
        if (t.getMessage() != null && t.getMessage().contains(errString)) return;
      }
      // otherwise, rethrow it, possibly completely unrelated
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR,
          "Unexpected error, expected error matching: " + errString,
          e);
    } finally {
      resetExceptionIgnores();
    }
  }

  public void testAddDynamicField() throws Exception {
    deleteCore();
    Path managedSchemaFile = tmpConfDir.resolve("managed-schema.xml");
    // Delete managed-schema.xml, so it won't block parsing a new schema
    Files.delete(managedSchemaFile);
    System.setProperty("managed.schema.mutable", "true");
    initCore("solrconfig-managed-schema.xml", "schema-one-field-no-dynamic-field.xml", tmpSolrHome);

    assertNull(
        "Field '*_s' is present in the schema",
        h.getCore().getLatestSchema().getFieldOrNull("*_s"));

    String errString = "Can't add dynamic field '*_s'.";
    ignoreException(Pattern.quote(errString));
    try {
      Map<String, Object> options = new HashMap<>();
      IndexSchema oldSchema = h.getCore().getLatestSchema();
      String fieldName = "*_s";
      String fieldType = "string";
      SchemaField newField = oldSchema.newField(fieldName, fieldType, options);
      IndexSchema newSchema = oldSchema.addField(newField);
      h.getCore().setLatestSchema(newSchema);
      fail("Should fail when adding a dynamic field");
    } catch (Exception e) {
      for (Throwable t = e; t != null; t = t.getCause()) {
        // short circuit out if we found what we expected
        if (t.getMessage() != null && t.getMessage().contains(errString)) return;
      }
      // otherwise, rethrow it, possibly completely unrelated
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR,
          "Unexpected error, expected error matching: " + errString,
          e);
    } finally {
      resetExceptionIgnores();
    }
  }

  public void testAddWithSchemaCodecFactory() throws Exception {
    deleteCore();
    Path managedSchemaFile = tmpConfDir.resolve("managed-schema.xml");
    // Delete managed-schema.xml, so it won't block parsing a new schema
    Files.delete(managedSchemaFile);
    System.setProperty("managed.schema.mutable", "true");
    initCore("solrconfig-managed-schema.xml", "schema_codec.xml", tmpSolrHome);

    String uniqueKey = "string_f";
    assertNotNull(
        "Unique key field '" + uniqueKey + "' is not present in the schema",
        h.getCore().getLatestSchema().getFieldOrNull(uniqueKey));

    String fieldName = "string_disk_new_field";
    assertNull(
        "Field '" + fieldName + "' is present in the schema",
        h.getCore().getLatestSchema().getFieldOrNull(fieldName));

    Map<String, Object> options = new HashMap<>();
    IndexSchema oldSchema = h.getCore().getLatestSchema();
    String fieldType = "string_disk";
    SchemaField newField = oldSchema.newField(fieldName, fieldType, options);
    IndexSchema newSchema = oldSchema.addField(newField);
    h.getCore().setLatestSchema(newSchema);

    assertU(adoc(fieldName, "thing", uniqueKey, "aBc"));
    assertU(commit());

    assertQ(req(fieldName + ":thing"), "//*[@numFound='1']");
  }

  public void testAddWithSchemaSimilarityFactory() throws Exception {
    deleteCore();
    Path managedSchemaFile = tmpConfDir.resolve("managed-schema.xml");
    // Delete managed-schema.xml, so it won't block parsing a new schema
    Files.delete(managedSchemaFile);
    System.setProperty("managed.schema.mutable", "true");
    initCore("solrconfig-managed-schema.xml", "schema-bm25.xml", tmpSolrHome);

    String uniqueKey = "id";
    assertNotNull(
        "Unique key field '" + uniqueKey + "' is not present in the schema",
        h.getCore().getLatestSchema().getFieldOrNull(uniqueKey));

    String fieldName = "new_text_field";
    assertNull(
        "Field '" + fieldName + "' is present in the schema",
        h.getCore().getLatestSchema().getFieldOrNull(fieldName));

    Map<String, Object> options = new HashMap<>();
    IndexSchema oldSchema = h.getCore().getLatestSchema();
    String fieldType = "text";
    SchemaField newField = oldSchema.newField(fieldName, fieldType, options);
    IndexSchema newSchema = oldSchema.addField(newField);
    h.getCore().setLatestSchema(newSchema);

    assertU(adoc(fieldName, "thing", uniqueKey, "123"));
    assertU(commit());

    assertQ(req(fieldName + ":thing"), "//*[@numFound='1']");
  }

  public void testPersistUniqueKey() throws Exception {
    assertSchemaResource(collection, "managed-schema.xml");
    deleteCore();
    Path managedSchemaFile = tmpConfDir.resolve("managed-schema.xml");
    // Delete managed-schema.xml, so it won't block parsing a new schema
    Files.delete(managedSchemaFile);
    System.setProperty("managed.schema.mutable", "true");
    initCore(
        "solrconfig-managed-schema.xml",
        "schema-one-field-no-dynamic-field-unique-key.xml",
        tmpSolrHome);

    assertTrue(Files.exists(managedSchemaFile));
    String managedSchemaContents = Files.readString(managedSchemaFile, StandardCharsets.UTF_8);
    assertFalse(managedSchemaContents.contains("\"new_field\""));

    Map<String, Object> options = new HashMap<>();
    options.put("stored", "false");
    IndexSchema oldSchema = h.getCore().getLatestSchema();
    assertEquals("str", oldSchema.getUniqueKeyField().getName());
    String fieldName = "new_field";
    String fieldType = "string";
    SchemaField newField = oldSchema.newField(fieldName, fieldType, options);
    IndexSchema newSchema = oldSchema.addField(newField);
    assertEquals("str", newSchema.getUniqueKeyField().getName());
    h.getCore().setLatestSchema(newSchema);
    log.info("####close harness");
    h.close();
    log.info("####close harness end");
    initCore();

    assertTrue(Files.exists(managedSchemaFile));
    try (InputStream stream = Files.newInputStream(managedSchemaFile)) {
      managedSchemaContents = new String(stream.readAllBytes(), StandardCharsets.UTF_8);
    }
    assertTrue(
        managedSchemaContents.contains(
            "<field name=\"new_field\" type=\"string\" stored=\"false\"/>"));
    IndexSchema newNewSchema = h.getCore().getLatestSchema();
    assertNotNull(newNewSchema.getUniqueKeyField());
    assertEquals("str", newNewSchema.getUniqueKeyField().getName());
  }

  public void testAddFieldThenReload() throws Exception {
    deleteCore();
    Path managedSchemaFile = tmpConfDir.resolve("managed-schema.xml");
    // Delete managed-schema.xml, so it won't block parsing a new schema
    Files.delete(managedSchemaFile);
    System.setProperty("managed.schema.mutable", "true");
    initCore("solrconfig-managed-schema.xml", "schema-one-field-no-dynamic-field.xml", tmpSolrHome);

    String fieldName = "new_text_field";
    assertNull(
        "Field '" + fieldName + "' is present in the schema",
        h.getCore().getLatestSchema().getFieldOrNull(fieldName));

    Map<String, Object> options = new HashMap<>();
    IndexSchema oldSchema = h.getCore().getLatestSchema();
    String fieldType = "text";
    SchemaField newField = oldSchema.newField(fieldName, fieldType, options);
    IndexSchema newSchema = oldSchema.addField(newField);
    h.getCore().setLatestSchema(newSchema);

    h.reload();
  }
}
