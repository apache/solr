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

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.mappings.MappingsTestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Test {@link MappingType} */
public class TestMappingType extends SolrTestCaseJ4 {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-mappings.xml", "schema-mappings.xml");
  }

  @After
  public void tearDown() throws Exception {
    clearIndex();
    assertU(commit());
    super.tearDown();
  }

  @Test
  public void testMappingsSchema() {
    IndexSchema schema = h.getCore().getLatestSchema();

    Map<String, FieldType> fieldTypes = schema.getFieldTypes();
    Assert.assertEquals("Wrong number of fieldType", 11, fieldTypes.size());
    Map<String, FieldType> mappingTypes =
        fieldTypes.entrySet().stream()
            .filter(e -> e.getKey().equals("mapping"))
            .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()));
    Assert.assertTrue(
        "Mappings should all be MappingType",
        mappingTypes.entrySet().stream().allMatch(e -> (e.getValue() instanceof MappingType)));

    Map<String, SchemaField> mappingFields =
        schema.getFields().entrySet().stream()
            .filter(e -> e.getKey().endsWith("mapping"))
            .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()));
    Assert.assertEquals("Wrong number of mapping fields", 4, mappingFields.size());
    Assert.assertTrue(
        "Mapping fields should all be MappingType",
        mappingFields.entrySet().stream()
            .allMatch(e -> (e.getValue().getType() instanceof MappingType)));
    Assert.assertTrue(
        "Mapping types should all be PolyFields",
        mappingFields.entrySet().stream().allMatch(e -> e.getValue().getType().isPolyField()));
    Assert.assertTrue(
        "Mapping fields should all be PolyFields",
        mappingFields.entrySet().stream().allMatch(e -> e.getValue().isPolyField()));
  }

  @Test
  public void testSingleValuedXml() {
    /*
     * <mapping name="single_mapping"><str name="key">key_2</str><str
     * name="value">vLvdE MmVaK</str></mapping>
     */
    int requiredDocs = 5;
    Map<String, String> mappings = doAddDocs("single_mapping", requiredDocs, false, false);

    String findKeyFormat =
        "//doc/mapping[@name=\"single_mapping\"]/str[@name=\"key\"][text()='%s']";
    String findValueFormat = "/parent::mapping/str[@name=\"value\"][text()='%s']";

    String[] tests =
        mappings.entrySet().stream()
            .map(
                e ->
                    String.format(Locale.ENGLISH, findKeyFormat, e.getKey())
                        + String.format(Locale.ENGLISH, findValueFormat, e.getValue()))
            .toList()
            .toArray(new String[0]);

    String response = assertXmlQ(req("q", "*:*", "indent", "true"), tests);
    log.info(response);
  }

  @Test
  public void testSingleValuedJson() throws Exception {
    /*
     * "single_mapping":{ "key":"key_0", "value":"Bqxsd" }
     */
    int requiredDocs = 5;
    Map<String, String> mappings = doAddDocs("single_mapping", requiredDocs, false, true);

    String findKeyFormat = "/response/docs/[%d]/single_mapping/key==\"key_%d\"";
    String findValueFormat = "/response/docs/[%d]/single_mapping/value==\"%s\"";

    String[] tests = new String[mappings.size() * 2];

    for (int i = 0, j = 0; i < mappings.size(); i++, j++) {
      tests[i] = String.format(Locale.ENGLISH, findKeyFormat, j, j);
      tests[i++] = String.format(Locale.ENGLISH, findValueFormat, j, mappings.get("key_" + j));
    }

    String response = assertJQ(req("q", "*:*", "indent", "true", "wt", "json"), tests);
    log.info(response);
  }

  @Test
  public void testMultiValuedXml() {
    /*
     * <arr name="multi_mapping"> <mapping name="key_0"><str
     * name="key">key_0</str><str name="value">vLvdE MmVaK_value_0</str>
     * </mapping></arr>
     */
    int requiredDocs = 5;
    Map<String, String> mappings = doAddDocs("multi_mapping", requiredDocs, true, false);

    String findKeyFormat =
        "//doc/arr[@name=\"multi_mapping\"]/mapping[@name=\"%s\"]/str[@name=\"key\"][text()='%s']";
    String findValueFormat = "/parent::mapping/str[@name=\"value\"][text()='%s']";

    String[] tests =
        mappings.entrySet().stream()
            .map(
                e ->
                    String.format(Locale.ENGLISH, findKeyFormat, e.getKey(), e.getKey())
                        + String.format(Locale.ENGLISH, findValueFormat, e.getValue()))
            .toList()
            .toArray(new String[0]);

    String response = assertXmlQ(req("q", "*:*", "indent", "true"), tests);
    log.info(response);
  }

  @Test
  public void testMultiValuedJson() throws Exception {
    /*
     * "multi_mapping":[{ "key":"key_0", "value":"mPfsP_value_0" },{ "key":"key_1",
     * "value":"mPfsP_value_1" }]
     */
    int required = 5;
    Map<String, String> mappings = doAddDocs("multi_mapping", required, true, true);

    String findKeyFormat = "/response/docs/[%d]/multi_mapping/[%d]/key==\"key_%d_%d\"";
    String findValueFormat = "/response/docs/[%d]/multi_mapping/[%d]/value==\"%s\"";

    List<String> list = new ArrayList<>();
    for (int i = 0; i < required; i++) {
      for (int j = 0; j < required; j++) {
        list.add(String.format(Locale.ENGLISH, findKeyFormat, i, j, i, j));
        list.add(
            String.format(
                Locale.ENGLISH, findValueFormat, i, j, mappings.get("key_" + i + "_" + j)));
      }
    }
    String[] tests = list.toArray(new String[0]);

    String response = assertJQ(req("q", "*:*", "indent", "true"), tests);
    log.info(response);
  }

  @Test
  public void testFloatValueXml() {
    /*
     * <mapping name="float_mapping"><str name="key">key_2</str><float
     * name="value">1.23</float></mapping>
     */
    int requiredDocs = 5;
    Map<String, String> mappings =
        doAddDocs("float_mapping", requiredDocs, false, NumberType.FLOAT, false);

    String findKeyFormat = "//doc/mapping[@name=\"float_mapping\"]/str[@name=\"key\"][text()='%s']";
    String findValueFormat = "/parent::mapping/float[@name=\"value\"][text()='%s']";

    String[] tests =
        mappings.entrySet().stream()
            .map(
                e ->
                    String.format(Locale.ENGLISH, findKeyFormat, e.getKey())
                        + String.format(Locale.ENGLISH, findValueFormat, e.getValue()))
            .toList()
            .toArray(new String[0]);

    String response = assertXmlQ(req("q", "*:*", "indent", "true"), tests);
    log.info(response);
  }

  @Test
  public void testFloatValueJson() throws Exception {
    /*
     * "float_mapping":{ "key":"key_0", "value":"12.34" }
     */
    int requiredDocs = 5;
    Map<String, String> mappings =
        doAddDocs("float_mapping", requiredDocs, false, NumberType.FLOAT, true);

    // json output writes everything as str:
    String findKeyFormat = "/response/docs/[%d]/float_mapping/key==\"key_%d\"";
    String findValueFormat = "/response/docs/[%d]/float_mapping/value==\"%s\"";

    String[] tests = new String[mappings.size() * 2];

    for (int i = 0, j = 0; i < mappings.size(); i++, j++) {
      tests[i] = String.format(Locale.ENGLISH, findKeyFormat, j, j);
      tests[i++] = String.format(Locale.ENGLISH, findValueFormat, j, mappings.get("key_" + j));
    }

    String response = assertJQ(req("q", "*:*", "indent", "true", "wt", "json"), tests);
    log.info(response);
  }

  @Test
  public void testDateStrMapping() throws Exception {
    /*
     * <mapping name="date_str_mapping"><date
     * name="key">2025-11-21T16:09:15Z</date><str name="value">mPfsP</str></mapping>
     */
    int requiredDocs = 5;
    Map<String, String> mappings =
        doAddDocs("date_str_mapping", requiredDocs, false, null, NumberType.DATE, false);

    String findKeyFormat =
        "//doc/mapping[@name=\"date_str_mapping\"]/date[@name=\"key\"][text()='%s']";
    String findValueFormat = "/parent::mapping/str[@name=\"value\"][text()='%s']";

    String[] tests =
        mappings.entrySet().stream()
            .map(
                e ->
                    String.format(Locale.ENGLISH, findKeyFormat, e.getKey())
                        + String.format(Locale.ENGLISH, findValueFormat, e.getValue()))
            .toList()
            .toArray(new String[0]);

    String response = assertXmlQ(req("q", "*:*", "indent", "true"), tests);
    log.info(response);
  }

  @Test
  public void testSearchField() {
    int requiredDocs = 5;
    Map<String, String> mappings = doAddDocs("single_mapping", requiredDocs, false, false);

    String findKeyFormat =
        "//doc/mapping[@name=\"single_mapping\"]/str[@name=\"key\"][text()='%s']";
    String findValueFormat = "/parent::mapping/str[@name=\"value\"][text()='%s']";

    String[] tests =
        mappings.entrySet().stream()
            .map(
                e ->
                    String.format(Locale.ENGLISH, findKeyFormat, e.getKey())
                        + String.format(Locale.ENGLISH, findValueFormat, e.getValue()))
            .toList()
            .toArray(new String[0]);

    // q=single_mapping:* requires docValues on 'single_mapping'
    String response = assertXmlQ(req("q", "single_mapping:*", "indent", "true"), tests);
    log.info(response);
  }

  // generate string-string mappings
  private Map<String, String> doAddDocs(
      String field, int nb, boolean multiVal, boolean predictableStrKey) {
    return doAddDocs(field, nb, multiVal, null, null, predictableStrKey);
  }

  // generate string-NumberType mappings
  private Map<String, String> doAddDocs(
      String field, int nb, boolean multiVal, NumberType subType, boolean predictableStrKey) {
    return doAddDocs(field, nb, multiVal, subType, null, predictableStrKey);
  }

  // generate solr input documents with the given keyType and subType (value type)
  private Map<String, String> doAddDocs(
      String field,
      int nb,
      boolean multiVal,
      NumberType subType,
      NumberType keyType,
      boolean predictableStrKey) {
    List<SolrInputDocument> docs =
        MappingsTestUtils.generateDocs(
            random(), field, nb, multiVal, subType, keyType, predictableStrKey);
    Map<String, String> mappings = new HashMap<>();
    for (SolrInputDocument doc : docs) {
      SolrInputField inField = doc.getField(field);
      if (inField.getName().endsWith("mapping")) {
        Collection<Object> values = inField.getValues();
        for (Object value : values) {
          String[] mapping = MappingType.parseCommaSeparatedList(value.toString());
          mappings.put(mapping[MappingType.KEY], mapping[MappingType.VALUE]);
        }
      }
      assertU(adoc(doc));
    }
    assertU(commit());
    return mappings;
  }
}
