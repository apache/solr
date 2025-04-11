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
package org.apache.solr.response.transform;

import java.io.IOException;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.schema.IndexSchema;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/** Test cases for {@link EqualTermsDocTransformer} */
public class TestEqualTermsTransformer extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml", "schema.xml");
  }

  @Before
  public void before() {
    clearIndex();
    // Standard test documents
    assertU(adoc("id", "1", "subject", "John Smith"));
    assertU(adoc("id", "2", "subject", "Alice Jones"));
    assertU(adoc("id", "3", "subject", "Bob Miller"));
    assertU(commit());
  }

  @Test
  public void testBasicTransformation() throws IOException {
    // Test with matching value
    SolrDocument doc = new SolrDocument();
    doc.addField("subject", "John Smith");

    IndexSchema schema = h.getCore().getLatestSchema();
    EqualTermsDocTransformer transformer =
        new EqualTermsDocTransformer("isJohn", schema.getField("subject"), "John Smith");
    transformer.transform(doc, 0);

    assertEquals(true, doc.getFieldValue("isJohn"));

    // Test with non-matching value
    SolrDocument doc2 = new SolrDocument();
    doc2.addField("subject", "Alice Jones");

    transformer.transform(doc2, 0);

    assertEquals(false, doc2.getFieldValue("isJohn"));

    // Test with null field value
    SolrDocument doc3 = new SolrDocument();

    transformer.transform(doc3, 0);

    assertEquals(false, doc3.getFieldValue("isJohn"));
  }

  @Test
  public void testQueryTimeTransformation() throws Exception {
    // Test with matching value
    assertJQ(
        req(
            "q",
            "*:*",
            "sort",
            "id asc",
            "fl",
            "id,isJohn:[equalterms field=subject value='John Smith']"),
        "/response/docs/[0]/id=='1'",
        "/response/docs/[0]/isJohn==true",
        "/response/docs/[1]/isJohn==false",
        "/response/docs/[2]/isJohn==false");

    // Test with value not present in any documents
    assertJQ(
        req(
            "q",
            "*:*",
            "sort",
            "id asc",
            "fl",
            "id,isUnknown:[equalterms field=subject value='Unknown Person']"),
        "/response/docs/[0]/isUnknown==false",
        "/response/docs/[1]/isUnknown==false",
        "/response/docs/[2]/isUnknown==false");

    // Test with case insensitivity (due to analyzer)
    assertJQ(
        req(
            "q",
            "*:*",
            "sort",
            "id asc",
            "fl",
            "id,isMatch:[equalterms field=subject value='john   smith']"),
        "/response/docs/[0]/id=='1'", // John Smith
        "/response/docs/[0]/isMatch==true");
  }
}
