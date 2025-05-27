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
package org.apache.solr.update;

import java.io.IOException;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.LeafReader;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.core.SolrCore;
import org.junit.BeforeClass;
import org.junit.Test;

public class SolrIndexFingerprintTest extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeTests() throws Exception {
    initCore("solrconfig.xml", "schema.xml");
  }

  @Test
  public void testSequentialVsParallelFingerprint() throws Exception {
    long maxVersion = Long.MAX_VALUE;
    SolrCore core = h.getCore();

    // Create a set of 3 segments
    assertU(adoc("id", "101"));
    assertU(adoc("id", "102"));
    assertU(adoc("id", "103"));
    assertU(commit());

    assertU(adoc("id", "104"));
    assertU(adoc("id", "105"));
    assertU(adoc("id", "106"));
    assertU(commit());

    assertU(adoc("id", "107"));
    assertU(adoc("id", "108"));
    assertU(adoc("id", "109"));
    assertU(commit());

    try (var searcher = core.getSearcher().get()) {
      // Compute fingerprint sequentially to compare with parallel computation
      IndexFingerprint expectedFingerprint =
          searcher.getTopReaderContext().leaves().stream()
              .map(
                  ctx -> {
                    try {
                      LeafReader noCacheLeafReader =
                          new FilterLeafReader(ctx.reader()) {
                            @Override
                            public CacheHelper getReaderCacheHelper() {
                              return null;
                            }

                            @Override
                            public CacheHelper getCoreCacheHelper() {
                              return null;
                            }
                          };
                      return core.getIndexFingerprint(
                          searcher, noCacheLeafReader.getContext(), maxVersion);
                    } catch (IOException e) {
                      throw new RuntimeException(e);
                    }
                  })
              .reduce(new IndexFingerprint(maxVersion), IndexFingerprint::reduce);

      IndexFingerprint actualFingerprint = searcher.getIndexFingerprint(maxVersion);
      assertEquals(expectedFingerprint, actualFingerprint);
    }
  }
}
