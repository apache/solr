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
package org.apache.solr.search.vector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.CommonParams;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests the {@code rerankOversample} local param of the {@code knn} query parser against a
 * quantized dense vector field, where the HNSW search ranks candidates using lossy quantized
 * vectors and the re-ranking phase re-scores them against the raw full precision vectors.
 */
public class KnnQParserOversampleRerankTest extends SolrTestCaseJ4 {

  private static final String IDField = "id";

  /** Not quantized: the knn search already scores against the raw vectors. */
  private static final String exactField = "vector";

  /** 4 bit scalar quantized: the knn search scores against lossy quantized vectors. */
  private static final String quantizedField = "v_scalar_half_byte";

  /**
   * The 5 nearest neighbours of {@code [1.0, 2.0, 3.0, 4.0]} by exact cosine similarity. The top
   * four are separated by less than 0.003, so they are easily reordered by quantization.
   */
  private static final String[] EXPECTED_EXACT_TOP_5 =
      new String[] {
        "//result[@numFound='5']",
        "//result/doc[1]/str[@name='id'][.='1']",
        "//result/doc[2]/str[@name='id'][.='4']",
        "//result/doc[3]/str[@name='id'][.='2']",
        "//result/doc[4]/str[@name='id'][.='10']",
        "//result/doc[5]/str[@name='id'][.='3']"
      };

  @Before
  public void prepareIndex() throws Exception {
    initCore("solrconfig_codec.xml", "schema-densevector-quantized.xml");

    for (SolrInputDocument doc : prepareDocs()) {
      assertU(adoc(doc));
    }
    assertU(commit());
  }

  /** Indexes the same 10 vectors into both an exact and a quantized field. */
  private List<SolrInputDocument> prepareDocs() {
    List<List<Float>> vectors =
        List.of(
            Arrays.asList(1f, 2f, 3f, 4f), //        id 1,  cosine = 1.0
            Arrays.asList(1.5f, 2.5f, 3.5f, 4.5f), // id 2,  cosine = 0.998
            Arrays.asList(7.5f, 15.5f, 17.5f, 22.5f), // id 3,  cosine = 0.992
            Arrays.asList(1.4f, 2.4f, 3.4f, 4.4f), // id 4,  cosine = 0.999
            Arrays.asList(30f, 22f, 35f, 20f), //     id 5,  cosine = 0.862
            Arrays.asList(40f, 1f, 1f, 200f), //      id 6,  cosine = 0.756
            Arrays.asList(5f, 10f, 20f, 40f), //      id 7,  cosine = 0.970
            Arrays.asList(120f, 60f, 30f, 15f), //    id 8,  cosine = 0.515
            Arrays.asList(200f, 50f, 100f, 25f), //   id 9,  cosine = 0.554
            Arrays.asList(1.8f, 2.5f, 3.7f, 4.9f)); //id 10, cosine = 0.997

    List<SolrInputDocument> docs = new ArrayList<>(vectors.size());
    for (int i = 0; i < vectors.size(); i++) {
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField(IDField, i + 1);
      doc.addField(exactField, vectors.get(i));
      doc.addField(quantizedField, vectors.get(i));
      docs.add(doc);
    }
    return docs;
  }

  @After
  public void cleanUp() {
    clearIndex();
    deleteCore();
  }

  @Test
  public void exactField_isTheReferenceRanking() {
    // sanity check: the un-quantized field produces the exact cosine ranking
    assertQ(
        req(CommonParams.Q, "{!knn f=" + exactField + " topK=5}[1.0, 2.0, 3.0, 4.0]", "fl", "id"),
        EXPECTED_EXACT_TOP_5);
  }

  @Test
  public void rerankOversampledQuantizedSearch_shouldRestoreExactRanking() {
    // topK * rerankOversample = 25 candidates covers all 10 documents, so every document is
    // re-scored against its raw vector: the ranking must match the exact one, whatever the
    // quantized vectors ranked them as
    assertQ(
        req(
            CommonParams.Q,
            "{!knn f=" + quantizedField + " topK=5 rerankOversample=5}[1.0, 2.0, 3.0, 4.0]",
            "fl",
            "id"),
        EXPECTED_EXACT_TOP_5);
  }

  @Test
  public void rerankOversampledQuantizedSearch_shouldScoreWithRawVectors() {
    // the re-ranked score is the exact cosine similarity of the raw vector, so the best hit
    // scores identically to the same document on the un-quantized field
    assertQ(
        req(
            CommonParams.Q,
            "{!knn f=" + quantizedField + " topK=1 rerankOversample=10}[1.0, 2.0, 3.0, 4.0]",
            "fl",
            "id,score"),
        "//result[@numFound='1']",
        "//result/doc[1]/str[@name='id'][.='1']",
        // cosine of a vector with itself, as computed by Lucene's COSINE similarity function
        "//result/doc[1]/float[@name='score'][.='1.0']");
  }

  @Test
  public void rerankOversampleOnQuantizedField_shouldReturnExactlyTopK() {
    assertQ(
        req(
            CommonParams.Q,
            "{!knn f=" + quantizedField + " topK=3 rerankOversample=4}[1.0, 2.0, 3.0, 4.0]",
            "fl",
            "id"),
        "//result[@numFound='3']");
  }

  @Test
  public void rerankOversampleOnQuantizedField_withPreFilter_shouldReturnTopKFilteredResults() {
    assertQ(
        req(
            CommonParams.Q,
            "{!knn f="
                + quantizedField
                + " topK=3 rerankOversample=5 preFilter='id:(1 4 7 8 9 10)'}[1.0, 2.0, 3.0, 4.0]",
            "fl",
            "id"),
        "//result[@numFound='3']",
        "//result/doc[1]/str[@name='id'][.='1']",
        "//result/doc[2]/str[@name='id'][.='4']",
        "//result/doc[3]/str[@name='id'][.='10']");
  }
}
