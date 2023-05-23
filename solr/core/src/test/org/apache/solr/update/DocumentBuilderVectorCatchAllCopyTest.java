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

import static org.hamcrest.core.Is.is;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.KnnVectorField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.core.SolrCore;
import org.hamcrest.MatcherAssert;
import org.junit.BeforeClass;
import org.junit.Test;

/** */
public class DocumentBuilderVectorCatchAllCopyTest extends SolrTestCaseJ4 {
  static final int save_min_len = DocumentBuilder.MIN_LENGTH_TO_MOVE_LAST;

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml", "schema-vector-catchall.xml");
  }

  @Test
  public void denseVector_shouldWorkWithCatchAllCopyFields() {
    SolrCore core = h.getCore();

    SolrInputDocument doc = new SolrInputDocument();
    doc.addField("id", "0");
    doc.addField("cat", "hello world");
    doc.addField("vector", Arrays.asList(1.1f, 2.1f, 3.1f, 4.1f));

    Document out = DocumentBuilder.toDocument(doc, core.getLatestSchema());

    // from /solr/core/src/test-files/solr/collection1/conf/schema.xml
    KnnVectorField expectedDestination =
        new KnnVectorField(
            "vector2", new float[] {1.1f, 2.1f, 3.1f, 4.1f}, VectorSimilarityFunction.DOT_PRODUCT);

    MatcherAssert.assertThat(
        ((KnnFloatVectorField) out.getField("vector2")).vectorValue(),
        is(expectedDestination.vectorValue()));

    List<IndexableField> storedFields =
        StreamSupport.stream(out.spliterator(), false)
            .filter(f -> (f.fieldType().stored() && f.name().equals("vector2")))
            .collect(Collectors.toList());
    MatcherAssert.assertThat(storedFields.size(), is(4));

    MatcherAssert.assertThat(storedFields.get(0).numericValue(), is(1.1f));
    MatcherAssert.assertThat(storedFields.get(1).numericValue(), is(2.1f));
    MatcherAssert.assertThat(storedFields.get(2).numericValue(), is(3.1f));
    MatcherAssert.assertThat(storedFields.get(3).numericValue(), is(4.1f));
  }
}
