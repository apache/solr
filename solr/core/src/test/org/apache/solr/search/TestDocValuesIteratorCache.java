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

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.core.SolrCore;
import org.apache.solr.index.NoMergePolicyFactory;
import org.apache.solr.util.EmbeddedSolrServerTestRule;
import org.apache.solr.util.RefCounted;
import org.apache.solr.util.SolrClientTestRule;
import org.junit.ClassRule;

public class TestDocValuesIteratorCache extends SolrTestCaseJ4 {

  private static final int DOC_COUNT = 1000;

  @ClassRule
  public static final SolrClientTestRule solrClientTestRule =
      new EmbeddedSolrServerTestRule() {
        @Override
        protected void before() throws Throwable {
          // must set NoMergePolicyFactory, because OrdinalMap building depends on the predictable
          // existence of multiple segments; if the merge policy happens to combine into a single
          // segment, no OrdinalMap will be built, throwing off our tests
          systemSetPropertySolrTestsMergePolicyFactory(NoMergePolicyFactory.class.getName());
          startSolr(LuceneTestCase.createTempDir());
        }

        @Override
        protected void after() {
          systemClearPropertySolrTestsMergePolicyFactory();
          super.after();
        }
      };

  @SuppressWarnings("try")
  public void test() throws Exception {
    Path configSet = LuceneTestCase.createTempDir();
    SolrTestCaseJ4.copyMinConf(configSet.toFile());
    Files.copy(
        TEST_PATH().resolve("collection1/conf/schema-docValuesMulti.xml"),
        configSet.resolve("conf/schema.xml"),
        StandardCopyOption.REPLACE_EXISTING);

    solrClientTestRule.newCollection().withConfigSet(configSet.toString()).create();

    SolrClient client = solrClientTestRule.getSolrClient();

    Random r = random();
    String[][] expectVals = indexDocs(client, r);

    try (SolrCore core =
        ((EmbeddedSolrServer) client).getCoreContainer().getCore(DEFAULT_TEST_CORENAME)) {
      RefCounted<SolrIndexSearcher> sref = core.getSearcher();
      try (Closeable c = sref::decref) {
        SolrIndexSearcher s = sref.get();
        assertEquals(DOC_COUNT, s.maxDoc());
        SolrDocumentFetcher docFetcher = s.getDocFetcher();
        Map<String, SolrDocumentFetcher.DVIterEntry> dvIterCache = new HashMap<>();
        final Set<String> getFields = Set.of("stringdv");
        final SolrDocument doc = new SolrDocument();
        for (int i = DOC_COUNT * 10; i >= 0; i--) {
          int checkId = r.nextInt(DOC_COUNT);
          doc.clear();
          docFetcher.decorateDocValueFields(doc, checkId, getFields, dvIterCache);
          String[] expected = expectVals[checkId];
          if (expected == null) {
            assertTrue(doc.isEmpty());
          } else {
            assertEquals(1, doc.size());
            Collection<Object> actualVals = doc.getFieldValues("stringdv");
            assertEquals(expected.length, actualVals.size());
            int j = 0;
            for (Object o : actualVals) {
              assertEquals(expected[j++], o);
            }
          }
        }
      }
    }
  }

  private String[][] indexDocs(SolrClient client, Random r)
      throws SolrServerException, IOException {
    String[][] ret = new String[DOC_COUNT][];
    int pct = r.nextInt(100);
    for (int i = 0; i < DOC_COUNT; i++) {
      if (r.nextInt(100) > pct) {
        client.add(sdoc("id", Integer.toString(i)));
      } else {
        String str1 = TestUtil.randomSimpleString(r);
        String str2 = TestUtil.randomSimpleString(r);
        client.add(sdoc("id", Integer.toString(i), "stringdv", str1, "stringdv", str2));
        int cmp = str1.compareTo(str2);
        if (cmp == 0) {
          ret[i] = new String[] {str1};
        } else if (cmp < 0) {
          ret[i] = new String[] {str1, str2};
        } else {
          ret[i] = new String[] {str2, str1};
        }
      }
      if (r.nextInt(DOC_COUNT / 5) == 0) {
        // aim for 5 segments
        client.commit();
      }
    }
    client.commit();
    return ret;
  }
}
