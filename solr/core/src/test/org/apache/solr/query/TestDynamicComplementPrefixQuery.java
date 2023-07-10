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
package org.apache.solr.query;

import java.io.IOException;
import java.util.Random;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.UnicodeUtil;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.index.NoMergePolicyFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestDynamicComplementPrefixQuery extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeClass() throws Exception {
    systemSetPropertySolrTestsMergePolicyFactory(NoMergePolicyFactory.class.getName());
    System.setProperty("enable.update.log", "false"); // schema12 doesn't support _version_
    initCore("solrconfig.xml", "schema12.xml");
  }

  @AfterClass
  public static void afterClass() throws Exception {
    systemClearPropertySolrTestsMergePolicyFactory();
    System.clearProperty("enable.update.log");
  }

  private static final String[] FIELD_TYPES = new String[] {"v_s", "v_s_dv", "v_ss", "v_ss_dv"};
  private static final String[] MULTIVALUED_FIELD_TYPES = new String[] {"v_ss", "v_ss_dv"};
  private static final String IGNORE_PREFIX =
      new String(Character.toChars(UnicodeUtil.UNI_REPLACEMENT_CHAR));

  private static String getUnignoredString(Random r, int minLength, int maxLength) {
    String ret;
    while ((ret = TestUtil.randomRealisticUnicodeString(r, minLength, maxLength))
        .startsWith(IGNORE_PREFIX)) {
      // loop until we get an unignored prefix
    }
    return ret;
  }

  @Test
  public void testEquality1() {
    boolean noInvert = random().nextBoolean();
    Term t1 = new Term("field1", "text");
    MultiTermQuery q1 = new DynamicComplementPrefixQuery(t1, noInvert, random().nextBoolean());
    MultiTermQuery q2 = new DynamicComplementPrefixQuery(t1, noInvert, random().nextBoolean());
    assertEquals(q1, q2);
    q1.setRewriteMethod(MultiTermQuery.CONSTANT_SCORE_REWRITE);
    q2.setRewriteMethod(MultiTermQuery.CONSTANT_SCORE_REWRITE);
    assertEquals(q1, q2);
    q2.setRewriteMethod(
        MultiTermQuery.SCORING_BOOLEAN_REWRITE); // this should never actually happen IRL
    assertNotEquals(q1, q2);
  }

  @Test
  public void testEquality2() {
    boolean noInvert = random().nextBoolean();
    Term t1 = new Term("field1", "text");
    MultiTermQuery q1 = new DynamicComplementPrefixQuery(t1, noInvert, random().nextBoolean());
    MultiTermQuery q2 = new DynamicComplementPrefixQuery(t1, !noInvert, random().nextBoolean());
    assertNotEquals("different noInvert should not be equal", q1, q2);
  }

  @Test
  public void testEquality3() {
    boolean noInvert = random().nextBoolean();
    Term t1 = new Term("field1", "text");
    Term t2 = new Term("field2", "text");
    MultiTermQuery q1 = new DynamicComplementPrefixQuery(t1, noInvert, random().nextBoolean());
    MultiTermQuery q2 = new DynamicComplementPrefixQuery(t2, noInvert, random().nextBoolean());
    assertNotEquals("different fields should not be equal", q1, q2);
  }

  @Test
  public void testEquality4() {
    boolean noInvert = random().nextBoolean();
    Term t1 = new Term("field1", "text1");
    Term t2 = new Term("field1", "text2");
    MultiTermQuery q1 = new DynamicComplementPrefixQuery(t1, noInvert, random().nextBoolean());
    MultiTermQuery q2 = new DynamicComplementPrefixQuery(t2, noInvert, random().nextBoolean());
    assertNotEquals("different term text should not be equal", q1, q2);
  }

  @Test
  public void test() throws SolrServerException, IOException {
    final Random r = random();
    final int maxTermLen = 12;
    final int mainPrefixLen = 3;
    final int maxTermLenComplement = maxTermLen - mainPrefixLen;
    final String mainPrefix = getUnignoredString(r, mainPrefixLen, mainPrefixLen);
    final int docCount = 10000;
    final int segmentCountTarget = r.nextInt(10) + 1;
    final int avgSegSize = docCount / segmentCountTarget;
    final SolrClient client = new EmbeddedSolrServer(h.getCore());
    boolean reallyMultiValuedForSegment = r.nextBoolean();
    for (int i = 0; i < 10000; i++) {
      String v;
      if (r.nextBoolean()) {
        v = getUnignoredString(r, 0, maxTermLen);
      } else {
        v = mainPrefix.concat(TestUtil.randomRealisticUnicodeString(r, 0, maxTermLenComplement));
      }
      int paramsSize = 2 + (FIELD_TYPES.length * 2);
      if (reallyMultiValuedForSegment) {
        paramsSize += MULTIVALUED_FIELD_TYPES.length * 2;
      }
      String[] params = new String[paramsSize];
      int j = 0;
      params[j++] = "id";
      params[j++] = Integer.toString(i);
      for (String ft : FIELD_TYPES) {
        params[j++] = ft;
        params[j++] = v;
      }
      if (reallyMultiValuedForSegment) {
        // multivalued fields may be optimized to singleValued, so we sometimes add
        // extra (ignored by queries) values to force on-disk multivalued segment
        for (String ft : MULTIVALUED_FIELD_TYPES) {
          params[j++] = ft;
          params[j++] = IGNORE_PREFIX;
        }
      }
      assertEquals(0, client.add(sdoc((Object[]) params)).getStatus());
      if (r.nextInt(avgSegSize) == 0) {
        assertEquals(0, client.commit().getStatus());
        reallyMultiValuedForSegment = r.nextBoolean();
      }
    }

    assertNumFoundConsistent(client, mainPrefix, FIELD_TYPES);
    assertNumFoundConsistent(client, IGNORE_PREFIX, MULTIVALUED_FIELD_TYPES);

    for (int i = 0; i < 100; i++) {
      String prefix = getUnignoredString(r, 1, mainPrefixLen + 1);
      assertNumFoundConsistent(client, prefix, FIELD_TYPES);
    }
  }

  private static void assertNumFoundConsistent(
      SolrClient client, String prefix, String[] fieldTypes)
      throws SolrServerException, IOException {
    final long compareNumFound =
        client
            .query(
                params(
                    "q",
                    "{!prefix forceAutomaton=true f=$f v=$v}",
                    "f",
                    fieldTypes[0],
                    "v",
                    prefix,
                    "rows",
                    "0"))
            .getResults()
            .getNumFound();
    for (String ft : fieldTypes) {
      for (String noInvert : new String[] {"true", "false"}) {
        final long numFound =
            client
                .query(
                    params(
                        "q",
                        "{!prefix noInvert=$noInvert f=$f v=$v}",
                        "noInvert",
                        noInvert,
                        "f",
                        ft,
                        "v",
                        prefix,
                        "rows",
                        "0"))
                .getResults()
                .getNumFound();
        assertEquals(compareNumFound, numFound);
      }
    }
  }
}
