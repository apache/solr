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

import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.junit.BeforeClass;

public class KnnQParserChildTest extends SolrTestCaseJ4 {

  private static final int MAX_TOP_K = 100;
  private static final int MIN_NUM_PARENTS = MAX_TOP_K * 10;
  private static final int MIN_NUM_KIDS_PER_PARENT = 5;

  @BeforeClass
  public static void prepareIndex() throws Exception {
    /* vectorDimension="4" similarityFunction="cosine" */
    initCore("solrconfig_codec.xml", "schema-densevector.xml");

    final int numParents = atLeast(MIN_NUM_PARENTS);
    for (int p = 0; p < numParents; p++) {
      final String parentId = "parent-" + p;
      final SolrInputDocument parent = doc(f("id", parentId), f("type_s", "PARENT"));
      final int numKids = atLeast(MIN_NUM_KIDS_PER_PARENT);
      for (int k = 0; k < numKids; k++) {
        final String kidId = parentId + "-kid-" + k;
        final SolrInputDocument kid =
            doc(f("id", kidId), f("parent_s", parentId), f("type_s", "KID"));

        kid.addField("vector", randomFloatVector(random()));
        kid.addField("vector_byte_encoding", randomByteVector(random()));

        parent.addChildDocument(kid);
      }
      assertU(adoc(parent));
      if (rarely(random())) {
        assertU(commit());
      }
    }
    assertU(commit());
  }

  /** Direct usage knn w/childOf to confim that a diverse set of child docs are returned */
  public void testDiverseKids() {
    final int numIters = atLeast(100);
    for (int iter = 0; iter < numIters; iter++) {
      final String topK = "" + TestUtil.nextInt(random(), 2, MAX_TOP_K);

      // check floats...
      assertQ(
          req(
              "q",
              "{!knn f=vector topK=$k allParents='type_s:PARENT'}"
                  + vecStr(randomFloatVector(random())),
              "indent",
              "true",
              "fl",
              "id,parent_s",
              "_iter",
              "" + iter,
              "k",
              topK,
              "rows",
              topK),
          "*[count(//doc/str[@name='parent_s' and not(following::str[@name='parent_s']/text() = text())])="
              + topK
              + "]",
          "*[count(//doc/str[@name='id' and not(following::str[@name='id']/text() = text())])="
              + topK
              + "]");

      // check bytes...
      assertQ(
          req(
              "q",
              "{!knn f=vector_byte_encoding topK=$k allParents='type_s:PARENT'}"
                  + vecStr(randomByteVector(random())),
              "indent",
              "true",
              "fl",
              "id,parent_s",
              "_iter",
              "" + iter,
              "k",
              topK,
              "rows",
              topK),
          "*[count(//doc/str[@name='parent_s' and not(following::str[@name='parent_s']/text() = text())])="
              + topK
              + "]",
          "*[count(//doc/str[@name='id' and not(following::str[@name='id']/text() = text())])="
              + topK
              + "]");
    }
  }

  /** Sanity check that knn w/diversification works as expected when wrapped in parent query */
  public void testParentsOfDiverseKids() {

    final int numIters = atLeast(100);
    for (int iter = 0; iter < numIters; iter++) {
      final String topK = "" + TestUtil.nextInt(random(), 2, MAX_TOP_K);

      // check floats...
      assertQ(
          req(
              "q",
              "{!parent which='type_s:PARENT' score=max v=$knn}",
              "knn",
              "{!knn f=vector topK=$k allParents='type_s:PARENT'}"
                  + vecStr(randomFloatVector(random())),
              "indent",
              "true",
              "fl",
              "id",
              "_iter",
              "" + iter,
              "k",
              topK,
              "rows",
              topK),
          "*[count(//doc/str[@name='id' and not(following::str[@name='id']/text() = text())])="
              + topK
              + "]");

      // check bytes...
      assertQ(
          req(
              "q",
              "{!parent which='type_s:PARENT' score=max v=$knn}",
              "knn",
              "{!knn f=vector_byte_encoding topK=$k allParents='type_s:PARENT'}"
                  + vecStr(randomByteVector(random())),
              "indent",
              "true",
              "fl",
              "id",
              "_iter",
              "" + iter,
              "k",
              topK,
              "rows",
              topK),
          "*[count(//doc/str[@name='id' and not(following::str[@name='id']/text() = text())])="
              + topK
              + "]");
    }
  }

  /** Format a vector as a string for use in queries */
  protected static String vecStr(final List<? extends Number> vector) {
    return "[" + vector.stream().map(Object::toString).collect(Collectors.joining(",")) + "]";
  }

  /** Random vector of size 4 */
  protected static List<Float> randomFloatVector(Random r) {
    // we don't want nextFloat() because it's bound by -1:1
    // but we also don't want NaN, or +/- Infinity (so we don't mess with intBitsToFloat)
    // we could be fancier to get *all* the possible "real" floats, but this is good enough...

    // Note: bias first vec entry to ensure we never have an all zero vector (invalid w/cosine sim
    // used in configs)
    return List.of(
        1F + (r.nextFloat() * 10000F),
        r.nextFloat() * 10000F,
        r.nextFloat() * 10000F,
        r.nextFloat() * 10000F);
  }

  /** Random vector of size 4 */
  protected static List<Byte> randomByteVector(Random r) {
    final byte[] byteBuff = new byte[4];
    r.nextBytes(byteBuff);
    // Note: bias first vec entry to ensure we never have an all zero vector (invalid w/cosine sim
    // used in configs)
    return List.of(
        (Byte) (byte) (byteBuff[0] + 1),
        (Byte) byteBuff[1],
        (Byte) byteBuff[1],
        (Byte) byteBuff[1]);
  }

  /** Convenience method for building a SolrInputDocument */
  protected static SolrInputDocument doc(SolrInputField... fields) {
    SolrInputDocument d = new SolrInputDocument();
    for (SolrInputField f : fields) {
      d.put(f.getName(), f);
    }
    return d;
  }

  /** Convenience method for building a SolrInputField */
  protected static SolrInputField f(String name, Object value) {
    final SolrInputField f = new SolrInputField(name);
    f.setValue(value);
    return f;
  }
}
