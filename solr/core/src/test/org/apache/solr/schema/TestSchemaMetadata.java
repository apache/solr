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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.xml.parsers.DocumentBuilderFactory;
import org.apache.lucene.util.Version;
import org.apache.solr.SolrTestCase;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.request.SolrQueryRequestBase;
import org.apache.solr.response.JSONResponseWriter;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.rest.schema.FieldTypeXmlAdapter;
import org.apache.solr.util.DOMConfigNode;
import org.junit.Before;
import org.junit.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

/** Field-level schema metadata: loading, validation and the managed schema round-trip. */
public class TestSchemaMetadata extends SolrTestCase {

  private static final String SCHEMA =
      """
      <schema name="example" version="1.7">
        <meta>
          <str name="dc:description">Product catalog</str>
        </meta>
        <fieldType name="string" class="solr.StrField"/>
        <fieldType name="price" class="solr.FloatPointField">
          <meta>
            <str name="ext:unit">EUR</str>
          </meta>
        </fieldType>
        <field name="id" type="string" indexed="true" stored="true" required="true"
               postingsFormat="Direct" docValuesFormat="Direct">
          <meta>
            <str name="dc:description">unique document identifier</str>
            <int name="ext:max_length">32</int>
            <long name="ext:big">5000000000</long>
            <float name="ext:ratio">1.5</float>
            <double name="ext:weight">2.25</double>
            <bool name="ext:internal">false</bool>
            <null name="ext:nothing"/>
            <str name="class">not a similarity class</str>
            <arr name="dc:subject"><str>catalog</str><str>pk</str></arr>
            <lst name="ext:owner"><str name="team">catalog</str><int name="size">4</int></lst>
          </meta>
        </field>
        <field name="name" type="string" indexed="true" stored="true"/>
        <dynamicField name="*_price" type="price" indexed="true" stored="true">
          <meta>
            <str name="dc:description">Price in a regional currency; prefix is ISO region</str>
          </meta>
        </dynamicField>
        <uniqueKey>id</uniqueKey>
      </schema>
      """;

  private SolrResourceLoader loader;

  @Before
  public void setUpLoader() {
    loader = new SolrResourceLoader(createTempDir());
  }

  private IndexSchema load(String xml) {
    return new IndexSchema(
        "schema.xml",
        () ->
            IndexSchemaFactory.getParsedSchema(
                new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)),
                loader,
                "schema.xml"),
        Version.LATEST,
        loader,
        null);
  }

  private static String persist(IndexSchema schema) throws IOException {
    StringWriter writer = new StringWriter();
    schema.persist(writer);
    return writer.toString();
  }

  private static Map<String, Object> expectedIdMeta() {
    Map<String, Object> owner = new LinkedHashMap<>();
    owner.put("team", "catalog");
    owner.put("size", 4);
    Map<String, Object> expected = new HashMap<>();
    expected.put("dc:description", "unique document identifier");
    expected.put("ext:max_length", 32);
    expected.put("ext:big", 5000000000L);
    expected.put("ext:ratio", 1.5f);
    expected.put("ext:weight", 2.25d);
    expected.put("ext:internal", false);
    expected.put("ext:nothing", null);
    expected.put("class", "not a similarity class");
    expected.put("dc:subject", List.of("catalog", "pk"));
    expected.put("ext:owner", owner);
    return expected;
  }

  private static void assertExampleMetadata(IndexSchema schema) {
    assertEquals("Product catalog", schema.getMetadata().get("dc:description"));
    assertEquals("EUR", schema.getFieldTypeByName("price").getMetadata().get("ext:unit"));
    assertEquals(expectedIdMeta(), schema.getField("id").getMetadata().getEntries());
    assertTrue(schema.getField("name").getMetadata().isEmpty());
    assertTrue(schema.getFieldTypeByName("string").getMetadata().isEmpty());

    SchemaField dynamicInstance = schema.getField("eu_price");
    assertEquals(
        "Price in a regional currency; prefix is ISO region",
        dynamicInstance.getMetadata().get("dc:description"));
  }

  @Test
  public void testLoadAllLevels() {
    IndexSchema schema = load(SCHEMA);
    assertExampleMetadata(schema);

    SimpleOrderedMap<Object> props = schema.getField("id").getNamedPropertyValues(false);
    assertTrue(props.get(SchemaMetadata.META) instanceof SimpleOrderedMap);
    assertTrue(
        ((SimpleOrderedMap<?>) props.get(SchemaMetadata.META)).get("ext:owner")
            instanceof SimpleOrderedMap);
    assertTrue(
        schema.getField("id").getNamedPropertyValues(true).get(SchemaMetadata.META)
            instanceof SimpleOrderedMap);
    assertNull(schema.getField("name").getNamedPropertyValues(false).get(SchemaMetadata.META));
  }

  @Test
  public void testRoundTrip() throws IOException {
    IndexSchema schema = load(SCHEMA);
    String persisted = persist(schema);

    IndexSchema reloaded = load(persisted);
    assertExampleMetadata(reloaded);
    assertEquals(schema.getMetadata(), reloaded.getMetadata());
    assertEquals(persisted, persist(reloaded));
  }

  @Test
  public void testFieldFormatsSurvivePersist() throws IOException {
    IndexSchema reloaded = load(persist(load(SCHEMA)));
    Map<String, ?> args = reloaded.getField("id").getArgs();
    assertEquals("Direct", args.get(FieldProperties.POSTINGS_FORMAT));
    assertEquals("Direct", args.get(FieldProperties.DOC_VALUES_FORMAT));
  }

  /**
   * Older nodes reject unknown field attributes but ignore child elements, so the persisted schema
   * must only carry metadata as child elements.
   */
  @Test
  public void testPersistedFieldsUseOnlyKnownAttributes() throws Exception {
    String persisted = persist(load(SCHEMA));
    Document doc =
        DocumentBuilderFactory.newInstance()
            .newDocumentBuilder()
            .parse(new InputSource(new StringReader(persisted)));

    Set<String> allowed =
        Set.of(
            "name",
            "type",
            "default",
            FieldProperties.POSTINGS_FORMAT,
            FieldProperties.DOC_VALUES_FORMAT);
    int fieldsWithMeta = 0;
    for (String tag : List.of(IndexSchema.FIELD, IndexSchema.DYNAMIC_FIELD)) {
      NodeList fields = doc.getElementsByTagName(tag);
      for (int i = 0; i < fields.getLength(); i++) {
        Element field = (Element) fields.item(i);
        NamedNodeMap attrs = field.getAttributes();
        for (int j = 0; j < attrs.getLength(); j++) {
          String attr = attrs.item(j).getNodeName();
          assertTrue(
              "unexpected attribute " + attr,
              allowed.contains(attr) || FieldProperties.propertyNameToInt(attr, false) != 0);
        }
        if (field.getElementsByTagName(SchemaMetadata.META).getLength() > 0) {
          fieldsWithMeta++;
        }
      }
    }
    assertEquals(2, fieldsWithMeta);
    Node first = doc.getDocumentElement().getFirstChild();
    while (first != null && first.getNodeType() != Node.ELEMENT_NODE) {
      first = first.getNextSibling();
    }
    assertNotNull(first);
    assertEquals(SchemaMetadata.META, first.getNodeName());
  }

  private void assertLoadFails(String xml, String expectedMessagePart) {
    Exception e = expectThrows(Exception.class, () -> load(xml));
    Throwable t = e;
    while (t != null && (t.getMessage() == null || !t.getMessage().contains(expectedMessagePart))) {
      t = t.getCause();
    }
    assertNotNull("expected a message containing '" + expectedMessagePart + "', got: " + e, t);
  }

  private static String withFieldMeta(String meta) {
    return "<schema name=\"bad\" version=\"1.7\">"
        + "<fieldType name=\"string\" class=\"solr.StrField\"/>"
        + "<field name=\"id\" type=\"string\"><meta>"
        + meta
        + "</meta></field></schema>";
  }

  @Test
  public void testInvalidMetaFailsLoad() {
    assertLoadFails(withFieldMeta("<string name=\"x\">a</string>"), "unknown tag <string>");
    assertLoadFails(withFieldMeta("<str>a</str>"), "has no 'name'");
    assertLoadFails(
        withFieldMeta("<str name=\"x\">a</str><int name=\"x\">1</int>"), "duplicate name 'meta/x'");
    assertLoadFails(
        withFieldMeta("<lst name=\"o\"><str name=\"y\">a</str><str name=\"y\">b</str></lst>"),
        "duplicate name 'meta/o/y'");
    assertLoadFails(withFieldMeta("<str name=\"1bad\">a</str>"), "invalid key 'meta/1bad'");
    assertLoadFails(
        withFieldMeta(
            "<lst name=\"a\"><lst name=\"b\"><lst name=\"c\"><lst name=\"d\"/></lst></lst></lst>"),
        "nested deeper than");
  }

  private ManagedIndexSchema loadManaged(String xml) throws IOException {
    Path instanceDir = createTempDir();
    Files.createDirectories(instanceDir.resolve("conf"));
    Files.writeString(
        instanceDir.resolve("conf").resolve("solrconfig.xml"),
        "<config><luceneMatchVersion>" + Version.LATEST + "</luceneMatchVersion></config>");
    SolrConfig solrConfig = new SolrConfig(instanceDir, "solrconfig.xml");
    return new ManagedIndexSchema(
        solrConfig,
        "managed-schema.xml",
        () ->
            IndexSchemaFactory.getParsedSchema(
                new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)),
                solrConfig.getResourceLoader(),
                "managed-schema.xml"),
        true,
        "managed-schema.xml",
        0,
        new Object());
  }

  @Test
  public void testManagedSchemaChangesKeepMetadata() throws IOException {
    ManagedIndexSchema schema = loadManaged(SCHEMA);
    SchemaField added =
        schema.newField(
            "added_price",
            "price",
            Map.of(SchemaMetadata.META, Map.of("dc:description", "added field")));
    schema = schema.addFields(List.of(added), Map.of(), false);
    SchemaField addedDynamic =
        schema.newDynamicField(
            "*_cost", "price", Map.of(SchemaMetadata.META, Map.of("dc:description", "cost")));
    schema = schema.addDynamicFields(List.of(addedDynamic), Map.of(), false);

    Map<String, Object> replacement = new HashMap<>();
    replacement.put("name", "price");
    replacement.put("class", "solr.FloatPointField");
    replacement.put(SchemaMetadata.META, Map.of("ext:unit", "USD"));
    schema = schema.replaceFieldType("price", "solr.FloatPointField", replacement);

    for (IndexSchema s : List.of(schema, load(persist(schema)))) {
      assertEquals("Product catalog", s.getMetadata().get("dc:description"));
      assertEquals("USD", s.getFieldTypeByName("price").getMetadata().get("ext:unit"));
      assertEquals("added field", s.getField("added_price").getMetadata().get("dc:description"));
      assertEquals("cost", s.getField("x_cost").getMetadata().get("dc:description"));
      assertEquals(
          "Price in a regional currency; prefix is ISO region",
          s.getField("eu_price").getMetadata().get("dc:description"));
      assertEquals(expectedIdMeta(), s.getField("id").getMetadata().getEntries());
    }
  }

  @Test
  public void testMalformedTypedValue() {
    for (String meta : List.of("<int name=\"n\">abc</int>", "<bool name=\"b\">maybe</bool>")) {
      Exception e = expectThrows(Exception.class, () -> load(withFieldMeta(meta)));
      SolrException solrException = null;
      for (Throwable t = e; t != null; t = t.getCause()) {
        if (t instanceof SolrException se && se.getMessage().contains("field 'id'")) {
          solrException = se;
          break;
        }
      }
      assertNotNull("expected a SolrException naming the field, got: " + e, solrException);
      assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, solrException.code());
      assertTrue(solrException.getMessage(), solrException.getMessage().contains("invalid <"));
    }
  }

  @Test
  public void testParseDepthLimit() {
    StringBuilder xml = new StringBuilder();
    for (int i = 0; i <= SchemaMetadata.MAX_PARSE_DEPTH; i++) {
      xml.append("<lst name=\"l").append(i).append("\">");
    }
    for (int i = 0; i <= SchemaMetadata.MAX_PARSE_DEPTH; i++) {
      xml.append("</lst>");
    }
    assertLoadFails(
        withFieldMeta(xml.toString()),
        "nested deeper than " + SchemaMetadata.MAX_PARSE_DEPTH + " levels");

    Object nested = "leaf";
    for (int i = 0; i <= SchemaMetadata.MAX_PARSE_DEPTH + 1; i++) {
      nested = Map.of("k", nested);
    }
    Object json = nested;
    SolrException e =
        expectThrows(SolrException.class, () -> SchemaMetadata.fromObject(json, "test"));
    assertTrue(
        e.getMessage(),
        e.getMessage().contains("nested deeper than " + SchemaMetadata.MAX_PARSE_DEPTH));
  }

  @Test
  public void testDollarSignsRoundTrip() throws IOException {
    Map<String, Object> meta = new LinkedHashMap<>();
    meta.put("ext:template", "template ${name} and ${unclosed");
    meta.put("ext:price", "costs $$5 or $");
    meta.put("ext:nested", Map.of("inner", "${x}"));
    meta.put("ext:list", List.of("${y}", "$"));
    ManagedIndexSchema schema = loadManaged(SCHEMA);
    SchemaField field = schema.newField("dollars", "string", Map.of(SchemaMetadata.META, meta));
    schema = schema.addFields(List.of(field), Map.of(), false);

    IndexSchema reloaded = load(persist(schema));
    assertEquals(
        SchemaMetadata.fromObject(meta, "test"), reloaded.getField("dollars").getMetadata());
  }

  @Test
  public void testUnsupportedCharacters() {
    for (String bad : List.of("a\uFFFFb", "a\u0001b", "a\rb", "a\uD800b")) {
      SolrException e =
          expectThrows(
              SolrException.class, () -> SchemaMetadata.fromObject(Map.of("k", bad), "test"));
      assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, e.code());
      assertTrue(e.getMessage(), e.getMessage().contains("unsupported character U+"));
    }
    SchemaMetadata ok =
        SchemaMetadata.fromObject(Map.of("k", "tab\tnewline\n\uD83D\uDE00 \u00E9"), "test");
    assertEquals("tab\tnewline\n\uD83D\uDE00 \u00E9", ok.get("k"));
  }

  @Test
  public void testMoreThanOneMetaElementRejected() {
    String xml =
        "<schema name=\"bad\" version=\"1.7\">"
            + "<fieldType name=\"string\" class=\"solr.StrField\"/>"
            + "<field name=\"id\" type=\"string\">"
            + "<meta><str name=\"a\">1</str></meta><meta><str name=\"b\">2</str></meta>"
            + "</field></schema>";
    assertLoadFails(xml, "more than one <meta> element");
  }

  @Test
  public void testFieldTypeJsonRejectsMisspelledMeta() {
    Map<String, Object> json = new HashMap<>();
    json.put("name", "t");
    json.put("class", "solr.StrField");
    json.put("metadata", Map.of("dc:description", "x"));
    SolrException e = expectThrows(SolrException.class, () -> FieldTypeXmlAdapter.toNode(json));
    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, e.code());
    assertTrue(e.getMessage(), e.getMessage().contains("put descriptive metadata in a <meta>"));
  }

  @Test
  public void testDocumentOrderKept() throws IOException {
    String xml =
        withFieldMeta(
            "<str name=\"a\">1</str><int name=\"b\">2</int><str name=\"c\">3</str>"
                + "<arr name=\"d\"><str>x</str><int>1</int><str>y</str></arr>");
    IndexSchema schema = load(xml);
    Map<String, Object> entries = schema.getField("id").getMetadata().getEntries();
    assertEquals(List.of("a", "b", "c", "d"), new ArrayList<>(entries.keySet()));
    assertEquals(List.of("x", 1, "y"), entries.get("d"));

    IndexSchema reloaded = load(persist(schema));
    Map<String, Object> reloadedEntries = reloaded.getField("id").getMetadata().getEntries();
    assertEquals(List.of("a", "b", "c", "d"), new ArrayList<>(reloadedEntries.keySet()));
    assertEquals(List.of("x", 1, "y"), reloadedEntries.get("d"));
  }

  @Test
  public void testLimits() {
    String okKey = "k" + "x".repeat(SchemaMetadataValidator.MAX_KEY_LENGTH - 1);
    SchemaMetadata.fromObject(Map.of(okKey, "v"), "test");
    assertLimit(Map.of(okKey + "x", "v"), "key 'meta/" + okKey + "x' is longer than");

    String okValue = "v".repeat(SchemaMetadataValidator.MAX_STRING_LENGTH);
    SchemaMetadata.fromObject(Map.of("k", okValue), "test");
    assertLimit(Map.of("k", okValue + "v"), "is longer than");

    Map<String, Object> entries = new LinkedHashMap<>();
    for (int i = 0; i < SchemaMetadataValidator.MAX_ENTRIES; i++) {
      entries.put("k" + i, i);
    }
    SchemaMetadata.fromObject(entries, "test");
    entries.put("one_more", 1);
    assertLimit(entries, "more than " + SchemaMetadataValidator.MAX_ENTRIES + " entries");

    List<Object> items = new ArrayList<>();
    for (int i = 0; i < SchemaMetadataValidator.MAX_ENTRIES; i++) {
      items.add(i);
    }
    assertLimit(Map.of("list", items), "more than " + SchemaMetadataValidator.MAX_ENTRIES);
  }

  private static void assertLimit(Map<String, Object> meta, String expectedMessagePart) {
    SolrException e =
        expectThrows(SolrException.class, () -> SchemaMetadata.fromObject(meta, "test"));
    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, e.code());
    assertTrue(e.getMessage(), e.getMessage().contains(expectedMessagePart));
  }

  @Test
  public void testReservedAttributesRejected() {
    String fieldDescription =
        """
        <schema name="bad" version="1.7">
          <fieldType name="string" class="solr.StrField"/>
          <field name="id" type="string" description="unique id"/>
        </schema>
        """;
    assertLoadFails(fieldDescription, "put descriptive metadata in a <meta> child element");

    String fieldMetaAttr =
        """
        <schema name="bad" version="1.7">
          <fieldType name="string" class="solr.StrField"/>
          <field name="id" type="string" meta="x"/>
        </schema>
        """;
    assertLoadFails(fieldMetaAttr, "'meta' is not an attribute");

    String typeDescription =
        """
        <schema name="bad" version="1.7">
          <fieldType name="string" class="solr.StrField" description="strings"/>
        </schema>
        """;
    assertLoadFails(typeDescription, "put descriptive metadata in a <meta> child element");
  }

  @Test
  public void testCreateFromJsonMap() {
    IndexSchema schema = load(SCHEMA);
    Map<String, Object> meta = new LinkedHashMap<>();
    meta.put("dc:description", "from JSON");
    meta.put("ext:small", 5L);
    meta.put("ext:large", 5000000000L);
    meta.put("ext:decimal", new BigDecimal("1.25"));
    meta.put("ext:list", List.of(1L, "two"));
    Map<String, Object> props = new HashMap<>();
    props.put("stored", "true");
    props.put(SchemaMetadata.META, meta);

    SchemaField field = SchemaField.create("json", schema.getFieldTypeByName("string"), props);
    Map<String, Object> entries = field.getMetadata().getEntries();
    assertEquals(5, entries.get("ext:small"));
    assertEquals(5000000000L, entries.get("ext:large"));
    assertEquals(1.25d, entries.get("ext:decimal"));
    assertEquals(List.of(1, "two"), entries.get("ext:list"));
    assertFalse(field.getNamedPropertyValues(false).get("stored") instanceof String);

    assertFalse(field.getArgs().containsKey(SchemaMetadata.META));
    SchemaField copy =
        SchemaField.create(
            "json", schema.getFieldTypeByName("string"), field.getArgs(), field.getMetadata());
    assertEquals(field.getMetadata(), copy.getMetadata());

    Map<String, Object> badProps = new HashMap<>(Map.of(SchemaMetadata.META, "just a string"));
    SolrException e =
        expectThrows(
            SolrException.class,
            () -> SchemaField.create("bad", schema.getFieldTypeByName("string"), badProps));
    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, e.code());
  }

  @Test
  public void testFieldTypeJsonToXml() {
    Map<String, Object> owner = new LinkedHashMap<>();
    owner.put("team", "catalog");
    Map<String, Object> meta = new LinkedHashMap<>();
    meta.put("dc:description", "JSON field type");
    meta.put("ext:n", 7L);
    meta.put("ext:flag", true);
    meta.put("ext:tags", new ArrayList<>(List.of("a", "b")));
    meta.put("ext:owner", owner);
    Map<String, Object> json = new HashMap<>();
    json.put("name", "jsonType");
    json.put("class", "solr.StrField");
    json.put(SchemaMetadata.META, meta);

    Node node = FieldTypeXmlAdapter.toNode(json);
    assertNull(((Element) node).getAttributes().getNamedItem(SchemaMetadata.META));
    SchemaMetadata parsed =
        SchemaMetadata.fromConfigNode(new DOMConfigNode(node), "fieldType 'jsonType'");
    assertEquals(SchemaMetadata.fromObject(meta, "fieldType 'jsonType'"), parsed);
    assertEquals(7, parsed.get("ext:n"));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testMetaRendersAsJsonObject() throws IOException {
    IndexSchema schema = load(SCHEMA);
    for (String jsonNl : List.of("flat", "map")) {
      SolrQueryResponse rsp = new SolrQueryResponse();
      rsp.add("fields", List.of(schema.getField("id").getNamedPropertyValues(false)));
      SolrQueryRequestBase req =
          new SolrQueryRequestBase(null, new ModifiableSolrParams().set("json.nl", jsonNl)) {};
      StringWriter writer = new StringWriter();
      new JSONResponseWriter().write(writer, req, rsp);

      Map<String, Object> parsed = (Map<String, Object>) Utils.fromJSONString(writer.toString());
      Map<String, Object> field = ((List<Map<String, Object>>) parsed.get("fields")).get(0);
      assertTrue(jsonNl, field.get(SchemaMetadata.META) instanceof Map);
      Map<String, Object> meta = (Map<String, Object>) field.get(SchemaMetadata.META);
      assertEquals("unique document identifier", meta.get("dc:description"));
      assertTrue(jsonNl, meta.get("ext:owner") instanceof Map);
    }
  }
}
