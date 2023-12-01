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
package org.apache.solr.response;

import java.util.Arrays;
import java.util.List;
import org.apache.lucene.index.IndexableField;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.schema.SchemaField;
import org.junit.BeforeClass;

public class DocsStreamerTest extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeClass() throws Exception {
    System.setProperty("enable.update.log", "false"); // schema12 doesn't support _version_
    initCore("solrconfig.xml", "schema12.xml");
  }

  public void testDenseVectorField() throws Exception {
    List<Float> values = Arrays.asList(1.1f, 2.2f, 3.3f, 4.4f);
    List<String> dsValues =
        Arrays.asList("1.1", "2.2", "3.3", "4.4"); // should be Float too, see SOLR-16952

    SchemaField sf = h.getCore().getLatestSchema().getField("vector");
    List<IndexableField> fields = sf.createFields(values);
    for (int idx = 1; idx < fields.size(); ++idx) {
      Object value = DocsStreamer.getValue(sf, fields.get(idx));
      assertEquals(dsValues.get(idx - 1), value);
    }
  }
}
