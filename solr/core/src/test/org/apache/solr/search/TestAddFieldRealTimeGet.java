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
package org.apache.solr.search;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import org.apache.commons.io.file.PathUtils;
import org.apache.lucene.tests.mockfile.FilterPath;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.junit.Before;

public class TestAddFieldRealTimeGet extends TestRTGBase {

  private static final String collection = "collection1";
  private static final String confDir = collection + "/conf";

  @Before
  public void initManagedSchemaCore() throws Exception {
    final Path tmpSolrHome = createTempDir();
    Path tmpConfDir = FilterPath.unwrap(tmpSolrHome.resolve(confDir));
    Path testHomeConfDir = TEST_HOME().resolve(confDir);
    Files.createDirectories(tmpConfDir);
    final String configFileName = "solrconfig-managed-schema.xml";
    final String schemaFileName = "schema-id-and-version-fields-only.xml";
    PathUtils.copyFileToDirectory(testHomeConfDir.resolve(configFileName), tmpConfDir);
    PathUtils.copyFileToDirectory(testHomeConfDir.resolve(schemaFileName), tmpConfDir);
    PathUtils.copyFileToDirectory(
        testHomeConfDir.resolve("solrconfig.snippet.randomindexconfig.xml"), tmpConfDir);

    // initCore will trigger an upgrade to managed schema, since the solrconfig has
    // <schemaFactory class="ManagedIndexSchemaFactory" ... />
    System.setProperty("managed.schema.mutable", "true");
    System.setProperty("enable.update.log", "true");
    initCore(configFileName, schemaFileName, tmpSolrHome);
  }

  public void test() throws Exception {
    clearIndex();
    assertU(commit());

    String newFieldName = "newfield";
    String newFieldType = "string";
    String newFieldValue = "xyz";

    ignoreException("unknown field");
    assertFailedU(
        "Should fail due to unknown field '" + newFieldName + "'",
        adoc("id", "1", newFieldName, newFieldValue));
    unIgnoreException("unknown field");

    IndexSchema schema = h.getCore().getLatestSchema();
    SchemaField newField = schema.newField(newFieldName, newFieldType, Collections.emptyMap());
    IndexSchema newSchema = schema.addField(newField);
    h.getCore().setLatestSchema(newSchema);

    String newFieldKeyValue = "'" + newFieldName + "':'" + newFieldValue + "'";
    assertU(adoc("id", "1", newFieldName, newFieldValue));
    assertJQ(req("q", "id:1"), "/response/numFound==0");
    assertJQ(
        req("qt", "/get", "id", "1", "fl", "id," + newFieldName),
        "=={'doc':{'id':'1'," + newFieldKeyValue + "}}");
    assertJQ(
        req("qt", "/get", "ids", "1", "fl", "id," + newFieldName),
        "=={'response':{'numFound':1,'start':0,'numFoundExact':true,'docs':[{'id':'1',"
            + newFieldKeyValue
            + "}]}}");

    assertU(commit());

    assertJQ(req("q", "id:1"), "/response/numFound==1");
    assertJQ(
        req("qt", "/get", "id", "1", "fl", "id," + newFieldName),
        "=={'doc':{'id':'1'," + newFieldKeyValue + "}}");
    assertJQ(
        req("qt", "/get", "ids", "1", "fl", "id," + newFieldName),
        "=={'response':{'numFound':1,'start':0,'numFoundExact':true,'docs':[{'id':'1',"
            + newFieldKeyValue
            + "}]}}");
  }
}
