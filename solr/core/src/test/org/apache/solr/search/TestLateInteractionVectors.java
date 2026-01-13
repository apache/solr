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

import static org.apache.lucene.search.LateInteractionFloatValuesSource.ScoreFunction.SUM_MAX_SIM;
import static org.apache.solr.schema.LateInteractionVectorField.multiFloatVectorToString;
import static org.apache.solr.schema.LateInteractionVectorField.stringToMultiFloatVector;
import static org.hamcrest.Matchers.startsWith;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.lucene.document.LateInteractionField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.LateInteractionVectorField;
import org.apache.solr.schema.SchemaField;
import org.junit.After;
import org.junit.Before;

// nocommit: jdocs
public class TestLateInteractionVectors extends SolrTestCaseJ4 {

  @Before
  public void init() throws Exception {
    initCore("solrconfig-basic.xml", "schema-late-vec.xml");
  }

  @After
  public void cleanUp() {
    clearIndex();
    deleteCore();
  }

  public void testStringEncodingAndDecoding() throws Exception {
    final int DIMENSIONS = 4;

    // some basic whitespace and int/float equivilences...
    final float[][] basic = new float[][] {{1, 2, 3, 4}, {-5, 6, 7, 8}};
    final List<String> basicWs =
        Arrays.asList(
            "[[1.0,2.0,3.0,4.0],[-5.0,6.0,7.0,8.0]]",
            "[[1,2,3,4],[-5,6,7,8.0]]",
            " [ [ 1,+2,  3,4 ]   ,   [-05, 6,7, 8.000] ]   ");

    for (String in : basicWs) {
      assertEquals(in, basic, stringToMultiFloatVector(DIMENSIONS, in));
    }

    // round trips of some "simple" fixed data with known string values
    final Map<String, float[][]> simple =
        Map.of(
            "[[1.0,2.0,3.0,4.0]]",
            new float[][] {{1, 2, 3, 4}},
            basicWs.get(0),
            basic,
            "[[1.1754944E-38,1.4E-45,3.4028235E38,-0.0]]",
            new float[][] {{Float.MIN_NORMAL, Float.MIN_VALUE, Float.MAX_VALUE, -0.0F}});
    for (Map.Entry<String, float[][]> e : simple.entrySet()) {
      // one way each way
      assertEquals(e.getValue(), stringToMultiFloatVector(DIMENSIONS, e.getKey()));
      assertEquals(e.getKey(), multiFloatVectorToString(e.getValue()));
      // round trip each way
      assertEquals(
          e.getValue(),
          stringToMultiFloatVector(DIMENSIONS, multiFloatVectorToString(e.getValue())));
      assertEquals(
          e.getKey(), multiFloatVectorToString(stringToMultiFloatVector(DIMENSIONS, e.getKey())));
    }

    // round trips of randomized vectors
    final int randomIters = atLeast(50);
    for (int iter = 0; iter < randomIters; iter++) {
      final float[][] data = new float[atLeast(5)][];
      for (int d = 0; d < data.length; d++) {
        final float[] vec = data[d] = new float[DIMENSIONS];
        for (int v = 0; v < DIMENSIONS; v++) {
          vec[v] = random().nextFloat();
        }
      }
      assertEquals(data, stringToMultiFloatVector(DIMENSIONS, multiFloatVectorToString(data)));
    }
  }

  public void testStringDecodingValidation() {
    final int DIMENSIONS = 2;

    // these should all be SyntaxErrors starting with "Expected..."
    for (String bad :
        Arrays.asList(
            "",
            "garbage",
            "[]",
            "[",
            "]",
            "[[1,2],",
            "[[1,2],[]]",
            "[[1,2]garbage]",
            "[[1,2],[3]]",
            "[[1,2],[,3]]",
            "[[1,2],[3,,]]",
            "[[1,2],[3,asdf]]")) {
      final SyntaxError e =
          expectThrows(
              SyntaxError.class,
              () -> {
                stringToMultiFloatVector(DIMENSIONS, bad);
              });
      assertThat(e.getMessage(), startsWith("Expected "));
    }

    // Extra stuff at the end of input is "Unexpected..."
    for (String bad : Arrays.asList("[[1,2]]garbage", "[[1,2]]      garbage")) {
      final SyntaxError e =
          expectThrows(
              SyntaxError.class,
              () -> {
                stringToMultiFloatVector(DIMENSIONS, bad);
              });
      assertThat(e.getMessage(), startsWith("Unexpected "));
    }

    // nocommit: other kinds of decoding errors to check for?
  }

  /** Low level test of createFields */
  public void createFields() throws Exception {
    final Map<String,float[][]> data = Map.of("[[1,2,3,4]]",
                                              new float[][] { { 1F, 2F, 3F, 4F }},
                                              "[[1,2,3,4],[5,6,7,8]]",
                                              new float[][] { { 1F, 2F, 3F, 4F }, { 5F, 6F, 7F, 8F }});

    try (SolrQueryRequest r = req()) {
      // defaults with stored + doc values
      for (String input : data.keySet()) {
        final SchemaField f = r.getSchema().getField("lv_4_def");
        final float[][] expected = data.get(input);
        final List<IndexableField> actual = f.getType().createFields(f, input);
        assertEquals(2, actual.size());
        
        if (actual.get(0) instanceof LateInteractionField lif) {
          assertEquals(expected, lif.getValue());
        } else {
          fail("first Field isn't a LIF: " + actual.get(0).getClass());
        }
        if (actual.get(1) instanceof StoredField stored) {
          assertEquals(input, stored.stringValue());
        } else {
          fail("second Field isn't stored: " + actual.get(1).getClass());
        }
      }
      
      // stored=false, only doc values
      for (String input : data.keySet()) {
        final SchemaField f = r.getSchema().getField("lv_4_nostored");
        final float[][] expected = data.get(input);
        final List<IndexableField> actual = f.getType().createFields(f, input);
        assertEquals(1, actual.size());
        
        if (actual.get(0) instanceof LateInteractionField lif) {
          assertEquals(expected, lif.getValue());
        } else {
          fail("first Field isn't a LIF: " + actual.get(0).getClass());
        }
      }
    }
  }
  
  public void testSimpleIndexAndRetrieval() throws Exception {
    // for simplicity, use a single doc, with identical values in several fields

    final float[][] d3 = new float[][] {{0.1F, 0.2F, 0.3F}, {0.5F, -0.6F, 0.7F}, {0.1F, 0F, 0F}};
    final String d3s = multiFloatVectorToString(d3);
    final float[][] d4 =
        new float[][] {{0.1F, 0.2F, 0.3F, 0.4F}, {0.5F, -0.6F, 0.7F, 0.8F}, {0.1F, 0F, 0F, 0F}};
    final String d4s = multiFloatVectorToString(d4);
    // quick round trip sanity checks
    assertEquals(d3, stringToMultiFloatVector(3, d3s));
    assertEquals(d4, stringToMultiFloatVector(4, d4s));

    // now index the strings
    assertU(
        add(
            doc(
                "id", "xxx",
                "lv_3_def", d3s,
                "lv_3_nostored", d3s,
                "lv_4_def", d4s,
                "lv_4_cosine", d4s,
                "lv_4_nostored", d4s)));

    assertU(commit());

    final float[][] q3 = new float[][] {{0.1F, 0.3F, 0.4F}, {0F, 0F, 0.1F}};
    final String q3s = multiFloatVectorToString(q3);
    final float[][] q4 = new float[][] {{0.9F, 0.9F, 0.9F, 0.9F}, {0.1F, 0.1F, 0.1F, 0.1F}};
    final String q4s = multiFloatVectorToString(q4);
    // quick round trip sanity checks
    assertEquals(q3, stringToMultiFloatVector(3, q3s));
    assertEquals(q4, stringToMultiFloatVector(4, q4s));

    // expected values based on Lucene's underlying raw computation
    // (this also ensures that our configured simFunc is being used correctly)
    final float euclid3 = SUM_MAX_SIM.compare(q3, d3, VectorSimilarityFunction.EUCLIDEAN);
    final float euclid4 = SUM_MAX_SIM.compare(q4, d4, VectorSimilarityFunction.EUCLIDEAN);
    final float cosine4 = SUM_MAX_SIM.compare(q4, d4, VectorSimilarityFunction.COSINE);

    // quick sanity check that our data is useful for differentiation...
    assertNotEquals(euclid4, cosine4);

    // retrieve our doc, and check it's returned field values as well as our sim function results
    assertQ(
        req(
            "q", "id:xxx",
            "fl", "*",
            "fl", "euclid_3_def:lateVector(lv_3_def,'" + q3s + "')",
            "fl", "euclid_3_nostored:lateVector(lv_3_nostored,'" + q3s + "')",
            "fl", "euclid_4_def:lateVector(lv_4_def,'" + q4s + "')",
            "fl", "euclid_4_nostored:lateVector(lv_4_nostored,'" + q4s + "')",
            "fl", "cosine_4:lateVector(lv_4_cosine,'" + q4s + "')"),
        "//*[@numFound='1']",

        // stored fields
        "//str[@name='lv_3_def'][.='" + d3s + "']",
        "//str[@name='lv_4_def'][.='" + d4s + "']",
        "//str[@name='lv_4_cosine'][.='" + d4s + "']",

        // dv only non-stored fields
        "//str[@name='lv_3_nostored'][.='"+d3s+"']",
        "//str[@name='lv_4_nostored'][.='"+d4s+"']",

        // function computations
        "//float[@name='euclid_3_def'][.=" + euclid3 + "]",
        "//float[@name='euclid_3_nostored'][.=" + euclid3 + "]",
        "//float[@name='euclid_4_def'][.=" + euclid4 + "]",
        "//float[@name='euclid_4_nostored'][.=" + euclid4 + "]",
        "//float[@name='cosine_4'][.=" + cosine4 + "]",

        // nocommit: other checks?

        "//*[@numFound='1']");
  }

  // nocommit: add test using late interaction value source in rescorer

}
