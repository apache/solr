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
package org.apache.solr.cuvs;

import com.nvidia.cuvs.lucene.Lucene99AcceleratedHNSWVectorsFormat;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import org.apache.commons.io.file.PathUtils;
import org.apache.lucene.document.Document;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.tests.mockfile.FilterPath;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.core.SolrCore;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.util.RefCounted;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Integration test to verify the indexing and searching using {@link
 * org.apache.solr.cuvs.CuVSCodec}
 *
 * @since 10.0.0
 */
public class TestCuVSCodecSupportIT extends SolrTestCaseJ4 {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static Random random;
  private static List<List<Float>> dataset1;
  private static List<List<Float>> dataset2;
  private static final int DATASET_SIZE = 1000;
  private static final int DATASET_DIMENSION = 8;
  private static final int TOPK = 5;
  private static final String ID_FIELD = "id";
  private static final String VECTOR_FIELD1 = "vector_field1";
  private static final String VECTOR_FIELD2 = "vector_field2";
  private static final String SOLRCONFIG_XML = "solrconfig.xml";
  private static final String SCHEMA_XML = "schema.xml";
  private static final String COLLECTION = "collection1";
  private static final String CONF_DIR = COLLECTION + "/conf";

  @BeforeClass
  public static void beforeClass() throws Exception {
    assumeTrue("Skipping cuvs tests", Lucene99AcceleratedHNSWVectorsFormat.supported());
    Path tmpSolrHome = createTempDir();
    Path tmpConfDir = FilterPath.unwrap(tmpSolrHome.resolve(CONF_DIR));
    Path testHomeConfDir = TEST_HOME().resolve(CONF_DIR);
    Files.createDirectories(tmpConfDir);
    PathUtils.copyFileToDirectory(testHomeConfDir.resolve(SOLRCONFIG_XML), tmpConfDir);
    PathUtils.copyFileToDirectory(testHomeConfDir.resolve(SCHEMA_XML), tmpConfDir);

    initCore(SOLRCONFIG_XML, SCHEMA_XML, tmpSolrHome);
    random = new Random(222);
    dataset1 = generateRandomVectors(random, DATASET_SIZE, DATASET_DIMENSION);
    dataset2 = generateRandomVectors(random, DATASET_SIZE, DATASET_DIMENSION);
  }

  @Test
  public void testIndexAndSearch() throws IOException {
    SolrCore solrCore = h.getCore();
    SolrConfig config = solrCore.getSolrConfig();
    String codecFactory = config.get("codecFactory").attr("class");
    assertEquals(
        "Unexpected solrconfig codec factory",
        "org.apache.solr.cuvs.CuVSCodecFactory",
        codecFactory);
    assertEquals("Unexpected core codec", "Lucene103", solrCore.getCodec().getName());

    // Index documents
    for (int i = 0; i < DATASET_SIZE; i++) {
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField(ID_FIELD, String.valueOf(i));
      doc.addField(VECTOR_FIELD1, dataset1.get(i));
      doc.addField(VECTOR_FIELD2, dataset2.get(i));
      assertU(adoc(doc));
    }
    assertU(commit());

    // Search documents
    final RefCounted<SolrIndexSearcher> refCountedSearcher = solrCore.getSearcher();
    IndexSearcher searcher = refCountedSearcher.get();

    KnnFloatVectorQuery q1 =
        new KnnFloatVectorQuery(VECTOR_FIELD1, getQuery(random, DATASET_DIMENSION), TOPK);
    TopDocs results1 = searcher.search(q1, TOPK);
    List<Integer> expected1 = Arrays.asList(390, 643, 127, 593, 627);
    assertSearchResults(searcher, results1, expected1);

    KnnFloatVectorQuery q2 =
        new KnnFloatVectorQuery(VECTOR_FIELD2, getQuery(random, DATASET_DIMENSION), TOPK);
    TopDocs results2 = searcher.search(q2, TOPK);
    List<Integer> expected2 = Arrays.asList(29, 88, 103, 915, 261);
    assertSearchResults(searcher, results2, expected2);

    refCountedSearcher.decref();
  }

  private static void assertSearchResults(
      IndexSearcher searcher, TopDocs results, List<Integer> expected) throws IOException {
    if (log.isInfoEnabled()) {
      log.info("Search results has ({} total hits)", results.totalHits);
    }
    int numResults = results.scoreDocs.length;
    for (int i = 0; i < numResults; i++) {
      ScoreDoc sd = results.scoreDocs[i];
      Document doc = searcher.storedFields().document(sd.doc);
      int r = i + 1;
      if (log.isInfoEnabled()) {
        log.info("Rank {}: doc {} (id={}), score: {}", r, sd.doc, doc.get("id"), sd.score);
      }
      int idx = Integer.valueOf(doc.get("id"));
      assertTrue("Expected doc id is missing:" + idx, expected.contains(idx));
    }
    assertTrue(numResults + " TopK results were returned instead of " + TOPK, numResults == TOPK);
  }

  private static List<List<Float>> generateRandomVectors(Random random, int size, int dimensions) {
    List<List<Float>> dataset = new ArrayList<List<Float>>();
    for (int i = 0; i < size; i++) {
      List<Float> row = new ArrayList<Float>();
      for (int j = 0; j < dimensions; j++) {
        row.add(random.nextFloat() * 100);
      }
      dataset.add(row);
    }
    return dataset;
  }

  private static float[] getQuery(Random random, int dimension) {
    List<Float> ql = generateRandomVectors(random, 1, dimension).get(0);
    float[] query = new float[dimension];
    for (int i = 0; i < dimension; i++) {
      query[i] = ql.get(i);
    }
    return query;
  }
}
