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

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.SolrTestCaseJ4.SuppressSSL;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.beans.Field;
import org.apache.solr.client.solrj.request.SolrQuery;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.util.SolrJettyTestRule;
import org.junit.BeforeClass;
import org.junit.ClassRule;

/** Test "binary" based fields using SolrJ and javabn codec (with and w/o bean mapping) */
@SuppressSSL(bugUrl = "https://issues.apache.org/jira/browse/SOLR-5776")
public class TestBinaryField extends SolrTestCaseJ4 {

  @ClassRule public static SolrJettyTestRule solrTestRule = new SolrJettyTestRule();

  @BeforeClass
  public static void beforeTest() throws Exception {
    Path homeDir = createTempDir();
    Path collDir = homeDir.resolve("collection1");

    copyMinConf(collDir, "name=collection1\n", "solrconfig-basic.xml");

    // Copy the custom schema for binary field tests
    Path sourceConfDir = TEST_HOME().resolve("collection1").resolve("conf");
    Files.copy(
        sourceConfDir.resolve("schema-binaryfield.xml"),
        collDir.resolve("conf/schema.xml"),
        StandardCopyOption.REPLACE_EXISTING);

    solrTestRule.startSolr(homeDir);
  }

  /**
   * @see TestUseDocValuesAsStored#testBinary
   */
  public void testSimple() throws Exception {
    try (SolrClient client = solrTestRule.getSolrClient()) {
      byte[] buf = new byte[10];
      for (int i = 0; i < 10; i++) {
        buf[i] = (byte) i;
      }

      final Map<String, byte[]> expected = new HashMap<>();

      SolrInputDocument doc = null;
      doc = new SolrInputDocument();
      doc.addField("id", "1");
      expected.put("1", Arrays.copyOfRange(buf, 2, (2 + 5)));
      doc.addField("data", ByteBuffer.wrap(buf, 2, 5));
      doc.addField("data_dv", ByteBuffer.wrap(buf, 2, 5));
      doc.addField("rev_data", ByteBuffer.wrap(buf, 2, 5));
      doc.addField("rev_data_dv", ByteBuffer.wrap(buf, 2, 5));
      doc.addField("str_data", prefix_base64(Arrays.copyOfRange(buf, 2, (2 + 5))));
      doc.addField("str_data_dv", prefix_base64(Arrays.copyOfRange(buf, 2, (2 + 5))));
      client.add(doc);

      doc = new SolrInputDocument();
      doc.addField("id", "2");
      expected.put("2", Arrays.copyOfRange(buf, 4, (4 + 3)));
      doc.addField("data", ByteBuffer.wrap(buf, 4, 3));
      doc.addField("data_dv", ByteBuffer.wrap(buf, 4, 3));
      doc.addField("rev_data", ByteBuffer.wrap(buf, 4, 3));
      doc.addField("rev_data_dv", ByteBuffer.wrap(buf, 4, 3));
      doc.addField("str_data", prefix_base64(Arrays.copyOfRange(buf, 4, (4 + 3))));
      doc.addField("str_data_dv", prefix_base64(Arrays.copyOfRange(buf, 4, (4 + 3))));
      client.add(doc);

      doc = new SolrInputDocument();
      doc.addField("id", "3");
      expected.put("3", Arrays.copyOf(buf, buf.length));
      doc.addField("data", buf);
      doc.addField("data_dv", buf);
      doc.addField("rev_data", buf);
      doc.addField("rev_data_dv", buf);
      doc.addField("str_data", prefix_base64(buf));
      doc.addField("str_data_dv", prefix_base64(buf));
      client.add(doc);

      client.commit();

      QueryResponse resp =
          client.query(
              new SolrQuery("*:*")
                  .setFields(
                      "id",
                      "data",
                      "data_dv",
                      "rev_data",
                      "rev_data_dv",
                      "str_data",
                      "str_data_dv"));
      SolrDocumentList res = resp.getResults();
      List<Bean> beans = resp.getBeans(Bean.class);
      assertEquals(3, res.size());
      assertEquals(3, beans.size());

      for (SolrDocument d : res) {
        final String id = d.getFieldValue("id").toString();
        assertTrue("Unexpected id: " + id, expected.containsKey(id));
        final byte[] expected_bytes = expected.get(id);
        final String expected_string = prefix_base64(expected_bytes);

        assertArrayEquals(expected_bytes, (byte[]) d.getFieldValue("data"));
        assertArrayEquals(expected_bytes, (byte[]) d.getFieldValue("data_dv"));

        assertArrayEquals(expected_bytes, (byte[]) d.getFieldValue("rev_data"));
        assertArrayEquals(expected_bytes, (byte[]) d.getFieldValue("rev_data_dv"));

        assertEquals(expected_string, d.getFieldValue("str_data"));
        assertEquals(expected_string, d.getFieldValue("str_data_dv"));
      }
      for (Bean d : beans) {
        assertTrue("Unexpected id: " + d.id, expected.containsKey(d.id));
        final byte[] expected_bytes = expected.get(d.id);
        final String expected_string = prefix_base64(expected_bytes);

        assertArrayEquals(expected_bytes, d.data);
        assertArrayEquals(expected_bytes, d.data_dv);

        assertArrayEquals(expected_bytes, d.rev_data);
        assertArrayEquals(expected_bytes, d.rev_data_dv);

        assertEquals(expected_string, d.str_data);
        assertEquals(expected_string, d.str_data_dv);
      }
    }
  }

  /**
   * @see StrBinaryField
   */
  public static String prefix_base64(final byte[] val) {
    return StrBinaryField.PREFIX + Base64.getEncoder().encodeToString(val);
  }

  public static class Bean {
    @Field String id;
    @Field byte[] data;
    @Field byte[] data_dv;
    @Field byte[] rev_data;
    @Field byte[] rev_data_dv;
    @Field String str_data;
    @Field String str_data_dv;
  }
}
