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
package org.apache.solr.search.neural;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.solr.search.neural.KnnQParser.DEFAULT_TOP_K;

public class KnnBaseTest extends SolrTestCaseJ4 {
  String IDField = "id";
  String vectorField = "vector";
  String vectorField2 = "vector2";
  String vectorFieldByteEncoding = "vector_byte_encoding";

  @Before
  public void prepareIndex() throws Exception {
    /* vectorDimension="4" similarityFunction="cosine" */
    initCore("solrconfig_codec.xml", "schema-densevector.xml");

    List<SolrInputDocument> docsToIndex = this.prepareDocs();
    for (SolrInputDocument doc : docsToIndex) {
      assertU(adoc(doc));
    }

    assertU(commit());
  }

  private List<SolrInputDocument> prepareDocs() {
    int docsCount = 13;
    List<SolrInputDocument> docs = new ArrayList<>(docsCount);
    for (int i = 1; i < docsCount + 1; i++) {
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField(IDField, i);
      docs.add(doc);
    }

    docs.get(0)
        .addField(vectorField, Arrays.asList(1f, 2f, 3f, 4f)); // cosine distance vector1= 1.0
    docs.get(1)
        .addField(
            vectorField, Arrays.asList(1.5f, 2.5f, 3.5f, 4.5f)); // cosine distance vector1= 0.998
    docs.get(2)
        .addField(
            vectorField,
            Arrays.asList(7.5f, 15.5f, 17.5f, 22.5f)); // cosine distance vector1= 0.992
    docs.get(3)
        .addField(
            vectorField, Arrays.asList(1.4f, 2.4f, 3.4f, 4.4f)); // cosine distance vector1= 0.999
    docs.get(4)
        .addField(vectorField, Arrays.asList(30f, 22f, 35f, 20f)); // cosine distance vector1= 0.862
    docs.get(5)
        .addField(vectorField, Arrays.asList(40f, 1f, 1f, 200f)); // cosine distance vector1= 0.756
    docs.get(6)
        .addField(vectorField, Arrays.asList(5f, 10f, 20f, 40f)); // cosine distance vector1= 0.970
    docs.get(7)
        .addField(
            vectorField, Arrays.asList(120f, 60f, 30f, 15f)); // cosine distance vector1= 0.515
    docs.get(8)
        .addField(
            vectorField, Arrays.asList(200f, 50f, 100f, 25f)); // cosine distance vector1= 0.554
    docs.get(9)
        .addField(
            vectorField, Arrays.asList(1.8f, 2.5f, 3.7f, 4.9f)); // cosine distance vector1= 0.997
    docs.get(10)
        .addField(vectorField2, Arrays.asList(1f, 2f, 3f, 4f)); // cosine distance vector2= 1
    docs.get(11)
        .addField(
            vectorField2,
            Arrays.asList(7.5f, 15.5f, 17.5f, 22.5f)); // cosine distance vector2= 0.992
    docs.get(12)
        .addField(
            vectorField2, Arrays.asList(1.5f, 2.5f, 3.5f, 4.5f)); // cosine distance vector2= 0.998

    docs.get(0).addField(vectorFieldByteEncoding, Arrays.asList(1, 2, 3, 4));
    docs.get(1).addField(vectorFieldByteEncoding, Arrays.asList(2, 2, 1, 4));
    docs.get(2).addField(vectorFieldByteEncoding, Arrays.asList(1, 2, 1, 2));
    docs.get(3).addField(vectorFieldByteEncoding, Arrays.asList(7, 2, 1, 3));
    docs.get(4).addField(vectorFieldByteEncoding, Arrays.asList(19, 2, 4, 4));
    docs.get(5).addField(vectorFieldByteEncoding, Arrays.asList(19, 2, 4, 4));
    docs.get(6).addField(vectorFieldByteEncoding, Arrays.asList(18, 2, 4, 4));
    docs.get(7).addField(vectorFieldByteEncoding, Arrays.asList(8, 3, 2, 4));

    return docs;
  }

  @After
  public void cleanUp() {
    clearIndex();
    deleteCore();
  }

  protected List<SolrInputDocument> prepareHighDimensionFloatVectorsDocs(int highDimension) {
    int docsCount = 13;
    String field = "2048_float_vector";
    List<SolrInputDocument> docs = new ArrayList<>(docsCount);

    for (int i = 1; i < docsCount + 1; i++) {
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField(IDField, i);
      docs.add(doc);
    }

    for (int i = 0; i < docsCount; i++) {
      List<Integer> highDimensionalityVector = new ArrayList<>();
      for (int j = i * highDimension; j < highDimension; j++) {
        highDimensionalityVector.add(j);
      }
      docs.get(i).addField(field, highDimensionalityVector);
    }
    Collections.reverse(docs);
    return docs;
  }

  protected List<SolrInputDocument> prepareHighDimensionByteVectorsDocs(int highDimension) {
    int docsCount = 13;
    String field = "2048_byte_vector";
    List<SolrInputDocument> docs = new ArrayList<>(docsCount);

    for (int i = 1; i < docsCount + 1; i++) {
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField(IDField, i);
      docs.add(doc);
    }

    for (int i = 0; i < docsCount; i++) {
      List<Integer> highDimensionalityVector = new ArrayList<>();
      for (int j = i * highDimension; j < highDimension; j++) {
        highDimensionalityVector.add(j % 127);
      }
      docs.get(i).addField(field, highDimensionalityVector);
    }
    Collections.reverse(docs);
    return docs;
  }
}
