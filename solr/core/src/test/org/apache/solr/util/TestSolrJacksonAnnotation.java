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

package org.apache.solr.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.Map;
import org.apache.solr.SolrTestCase;
import org.apache.solr.common.annotation.JsonProperty;
import org.apache.solr.common.util.JsonSchemaCreator;
import org.apache.solr.common.util.JsonSchemaValidator;
import org.apache.solr.common.util.Utils;

public class TestSolrJacksonAnnotation extends SolrTestCase {

  public void testSerDe() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    mapper.setAnnotationIntrospector(new SolrJacksonAnnotationInspector());

    TestObj o = new TestObj();
    o.field = "v1";
    o.f2 = "v2";
    o.ifld = 1234;
    o.lfld = 5678L;
    String json = mapper.writeValueAsString(o);

    @SuppressWarnings("unchecked")
    Map<Object, Object> m = (Map<Object, Object>) Utils.fromJSONString(json);
    assertEquals("v1", m.get("field"));
    assertEquals("v2", m.get("friendlyName"));
    assertEquals("1234", String.valueOf(m.get("friendlyIntFld")));
    assertEquals("5678", String.valueOf(m.get("friendlyLongFld")));
    TestObj o1 = mapper.readValue(json, TestObj.class);

    assertEquals("v1", o1.field);
    assertEquals("v2", o1.f2);
    assertEquals(1234, o1.ifld);
    assertEquals(5678L, o1.lfld);

    Map<String, Object> schema = JsonSchemaCreator.getSchema(TestObj.class);
    assertEquals("string", Utils.getObjectByPath(schema, true, "/properties/friendlyName/type"));
    assertEquals("integer", Utils.getObjectByPath(schema, true, "/properties/friendlyIntFld/type"));
    assertEquals("long", Utils.getObjectByPath(schema, true, "/properties/friendlyLongFld/type"));
    assertEquals("friendlyName", Utils.getObjectByPath(schema, true, "/required[0]"));

    JsonSchemaValidator validator = new JsonSchemaValidator(schema);
    List<String> errs = validator.validateJson(m);
    assertNull(errs);
    m.remove("friendlyName");
    errs = validator.validateJson(m);
    assertFalse(errs.isEmpty());
    assertTrue(errs.get(0).contains("Missing required attribute"));
    m.put("friendlyIntFld", Boolean.TRUE);
    errs = validator.validateJson(m);
    assertTrue(errs.get(0).contains("Value is not valid"));
    m.put("friendlyIntFld", "another String");
    errs = validator.validateJson(m);
    assertTrue(errs.get(0).contains("Value is not valid"));
  }

  public static class TestObj {
    @JsonProperty() public String field;

    @JsonProperty(value = "friendlyName", required = true)
    public String f2;

    @JsonProperty("friendlyIntFld")
    public int ifld;

    @JsonProperty("friendlyLongFld")
    public long lfld;
  }
}
