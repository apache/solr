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
package org.apache.solr.handler;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.apache.solr.SolrTestCase;
import org.apache.solr.common.util.Utils;
import org.apache.solr.util.ExternalPaths;

/** The techproducts example must index all fields sent to {@code /update/json/docs}. */
public class TechproductsJsonDocsParamsTest extends SolrTestCase {

  public void testUpdateJsonDocsParamSetDoesNotDropFields() throws Exception {
    Path paramsJson = ExternalPaths.TECHPRODUCTS_CONFIGSET.resolve("conf").resolve("params.json");
    Object parsed = Utils.fromJSON(Files.readAllBytes(paramsJson));
    @SuppressWarnings("unchecked")
    Map<String, Object> paramSet =
        (Map<String, Object>)
            Utils.getObjectByPath(parsed, false, List.of("params", "_UPDATE_JSON_DOCS"));
    assertNotNull(paramSet);
    assertEquals("_src_", paramSet.get("srcField"));
    assertFalse(
        "mapUniqueKeyOnly would index only the id and the source (SOLR-10424)",
        paramSet.containsKey("mapUniqueKeyOnly"));
  }
}
