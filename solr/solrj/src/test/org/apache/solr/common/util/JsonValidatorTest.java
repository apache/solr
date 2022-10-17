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
package org.apache.solr.common.util;

import java.util.List;
import org.apache.solr.SolrTestCaseJ4;

public class JsonValidatorTest extends SolrTestCaseJ4 {

  public void testSchemaValidation() {
    final JsonSchemaValidator personSchemaValidator =
        new JsonSchemaValidator(
            "{"
                + "  type:object,"
                + "  properties: {"
                + "   age : {type: number},"
                + "   adult : {type: boolean},"
                + "   name: {type: string}}}");
    List<String> errs =
        personSchemaValidator.validateJson(Utils.fromJSONString("{name:x, age:21, adult:true}"));
    assertNull(errs);
    errs =
        personSchemaValidator.validateJson(
            Utils.fromJSONString("{name:x, age:'21', adult:'true'}"));
    assertNotNull(errs);
    errs =
        personSchemaValidator.validateJson(
            Utils.fromJSONString("{name:x, age:'x21', adult:'true'}"));
    assertEquals(1, errs.size());

    Exception e =
        expectThrows(
            Exception.class,
            () -> {
              new JsonSchemaValidator(
                  "{"
                      + "  type:object,"
                      + "  properties: {"
                      + "   age : {type: int},"
                      + "   adult : {type: Boolean},"
                      + "   name: {type: string}}}");
            });
    assertTrue(e.getMessage().contains("Unknown type int"));

    e =
        expectThrows(
            Exception.class,
            () -> {
              new JsonSchemaValidator(
                  "{"
                      + "  type:object,"
                      + "   x : y,"
                      + "  properties: {"
                      + "   age : {type: number},"
                      + "   adult : {type: boolean},"
                      + "   name: {type: string}}}");
            });
    assertTrue(e.getMessage().contains("Unknown key"));

    e =
        expectThrows(
            Exception.class,
            () -> {
              new JsonSchemaValidator(
                  "{"
                      + "  type:object,"
                      + "  propertes: {"
                      + "   age : {type: number},"
                      + "   adult : {type: boolean},"
                      + "   name: {type: string}}}");
            });
    assertTrue(e.getMessage().contains("Unknown key : propertes"));

    final JsonSchemaValidator personWithEnumValidator =
        new JsonSchemaValidator(
            "{"
                + "  type:object,"
                + "  properties: {"
                + "   age : {type: number},"
                + "   sex: {type: string, enum:[M, F]},"
                + "   adult : {type: boolean},"
                + "   name: {type: string}}}");
    errs =
        personWithEnumValidator.validateJson(Utils.fromJSONString("{name: 'Joe Average' , sex:M}"));
    assertNull("errs are " + errs, errs);
    errs =
        personWithEnumValidator.validateJson(Utils.fromJSONString("{name: 'Joe Average' , sex:m}"));
    assertEquals(1, errs.size());
    assertTrue(errs.get(0).contains("Value of enum"));

    {
      final JsonSchemaValidator durationValidator =
          new JsonSchemaValidator(
              "{"
                  + "  type:object,"
                  + "  properties: {"
                  + "   i : {type: integer},"
                  + "   l : {type: long},"
                  + "   name: {type: string}}}");
      for (Long val :
          new Long[] {
            30L, 30L * 24, 30L * 24 * 60, 30L * 24 * 60 * 60, 30L * 24 * 60 * 60 * 1000
          }) { // month: days, hours, minutes, seconds, milliseconds
        if (val <= Integer.MAX_VALUE) {
          errs =
              durationValidator.validateJson(
                  Utils.fromJSONString(
                      "{name: 'val', i:" + Integer.toString(val.intValue()) + "}"));
          assertNull("errs are " + errs, errs);
        }
        errs =
            durationValidator.validateJson(
                Utils.fromJSONString("{name: 'val', l:" + val.toString() + "}"));
        assertNull("errs are " + errs, errs);
      }
    }

    String schema =
        "{\n"
            + "  'type': 'object',\n"
            + "  'properties': {\n"
            + "    'links': {\n"
            + "      'type': 'array',\n"
            + "      'items':{"
            + "          'type': 'object',\n"
            + "          'properties': {\n"
            + "            'rel': {\n"
            + "              'type': 'string'\n"
            + "            },\n"
            + "            'href': {\n"
            + "              'type': 'string'\n"
            + "            }\n"
            + "          }\n"
            + "        }\n"
            + "    }\n"
            + "\n"
            + "  }\n"
            + "}";
    final JsonSchemaValidator nestedObjectValidator = new JsonSchemaValidator(schema);
    nestedObjectValidator.validateJson(
        Utils.fromJSONString(
            "{\n"
                + "  'links': [\n"
                + "    {\n"
                + "        'rel': 'x',\n"
                + "        'href': 'x'\n"
                + "    },\n"
                + "    {\n"
                + "        'rel': 'x',\n"
                + "        'href': 'x'\n"
                + "    },\n"
                + "    {\n"
                + "        'rel': 'x',\n"
                + "        'href': 'x'\n"
                + "    }\n"
                + "  ]\n"
                + "}"));

    schema = "{\n" + "'type' : 'object',\n" + "'oneOf' : ['a', 'b']\n" + "}";

    final JsonSchemaValidator mutuallyExclusivePropertiesValidator =
        new JsonSchemaValidator(schema);
    errs =
        mutuallyExclusivePropertiesValidator.validateJson(Utils.fromJSONString("" + "{'c':'val'}"));
    assertNotNull(errs);
    errs =
        mutuallyExclusivePropertiesValidator.validateJson(Utils.fromJSONString("" + "{'a':'val'}"));
    assertNull(errs);
  }
}
