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
package org.apache.solr.client.solrj.response;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.solr.SolrTestCase;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.junit.Test;

/** Intensive tests for {@link ResponseNormalizer}. */
public class ResponseNormalizerTest extends SolrTestCase {

  @Test
  public void testNullAndEmpty() {
    assertNull(ResponseNormalizer.normalize(null));
    assertEquals(0, ResponseNormalizer.normalize(new NamedList<>()).size());
  }

  @Test
  public void testAlreadyCanonicalPassesThrough() {
    NamedList<Object> header = new SimpleOrderedMap<>();
    header.add("status", 0);
    NamedList<Object> in = new SimpleOrderedMap<>();
    in.add("responseHeader", header);

    NamedList<Object> out = ResponseNormalizer.normalize(in);
    assertTrue(out.get("responseHeader") instanceof NamedList);
    assertEquals(0, ((NamedList<?>) out.get("responseHeader")).get("status"));
  }

  @Test
  public void testMapBecomesNamedListRecursively() {
    Map<String, Object> inner = new LinkedHashMap<>();
    inner.put("a", 1);
    Map<String, Object> mid = new LinkedHashMap<>();
    mid.put("inner", inner);
    NamedList<Object> in = new NamedList<>();
    in.add("mid", mid);

    NamedList<Object> out = ResponseNormalizer.normalize(in);
    Object midOut = out.get("mid");
    assertTrue("mid should be NamedList", midOut instanceof NamedList);
    Object innerOut = ((NamedList<?>) midOut).get("inner");
    assertTrue("inner should be NamedList", innerOut instanceof NamedList);
    assertEquals(1, ((NamedList<?>) innerOut).get("a"));
  }

  @Test
  public void testDocListReconstruction() {
    Map<String, Object> doc1 = new LinkedHashMap<>();
    doc1.put("id", "1");
    Map<String, Object> response = new LinkedHashMap<>();
    response.put("numFound", 5L);
    response.put("start", 0L);
    response.put("maxScore", 1.5);
    response.put("numFoundExact", false);
    response.put("docs", new ArrayList<>(List.of(doc1)));
    NamedList<Object> in = new NamedList<>();
    in.add("response", response);

    NamedList<Object> out = ResponseNormalizer.normalize(in);
    Object r = out.get("response");
    assertTrue("response should be SolrDocumentList", r instanceof SolrDocumentList);
    SolrDocumentList docs = (SolrDocumentList) r;
    assertEquals(5L, docs.getNumFound());
    assertEquals(0L, docs.getStart());
    assertEquals(Float.valueOf(1.5f), docs.getMaxScore());
    assertFalse("numFoundExact must survive the conversion", docs.getNumFoundExact());
    assertEquals(1, docs.size());
    assertEquals("1", docs.get(0).getFirstValue("id"));
  }

  @Test
  public void testEmptyDocList() {
    Map<String, Object> response = new LinkedHashMap<>();
    response.put("numFound", 0L);
    response.put("docs", new ArrayList<>());
    NamedList<Object> in = new NamedList<>();
    in.add("response", response);

    SolrDocumentList docs = (SolrDocumentList) ResponseNormalizer.normalize(in).get("response");
    assertEquals(0L, docs.getNumFound());
    assertTrue(docs.isEmpty());
  }

  @Test
  public void testDocListValuedFieldIsReconstructed() {
    // a doc field whose value is itself a {numFound,docs} object becomes a nested SolrDocumentList
    Map<String, Object> child = new LinkedHashMap<>();
    child.put("id", "child-1");
    Map<String, Object> childList = new LinkedHashMap<>();
    childList.put("numFound", 1L);
    childList.put("docs", new ArrayList<>(List.of(child)));

    Map<String, Object> parent = new LinkedHashMap<>();
    parent.put("id", "parent-1");
    parent.put("nested", childList);

    Map<String, Object> response = new LinkedHashMap<>();
    response.put("numFound", 1L);
    response.put("docs", new ArrayList<>(List.of(parent)));
    NamedList<Object> in = new NamedList<>();
    in.add("response", response);

    SolrDocumentList docs = (SolrDocumentList) ResponseNormalizer.normalize(in).get("response");
    SolrDocument parentDoc = docs.get(0);
    Object nested = parentDoc.getFieldValue("nested");
    assertTrue("nested docList field reconstructed", nested instanceof SolrDocumentList);
    assertEquals("child-1", ((SolrDocumentList) nested).get(0).getFirstValue("id"));
  }

  /**
   * A nested-document schema stamps every child with {@code _nest_path_}, and {@code [child]}
   * returns it under {@code fl=*}, so a named child says what it is. The shapes here are the ones a
   * live response carries: a single child under its own field name, an array of children under
   * theirs, and a grandchild inside the single child. The binary and XML parsers hand all three
   * back as documents ({@code <doc name="lonely">} in XML), so this one must too.
   */
  @Test
  public void testNamedNestedDocumentsAreReconstructed() {
    Map<String, Object> grandChild = new LinkedHashMap<>();
    grandChild.put("id", "3");
    grandChild.put("test2_s", "secondTest");
    grandChild.put("_nest_path_", "/lonely#/lonelyGrandChild#");

    Map<String, Object> lonely = new LinkedHashMap<>();
    lonely.put("id", "2");
    lonely.put("test_s", "testing");
    lonely.put("_nest_path_", "/lonely#");
    lonely.put("lonelyGrandChild", grandChild);

    Map<String, Object> topping = new LinkedHashMap<>();
    topping.put("id", "4");
    topping.put("type_s", "Regular");
    topping.put("_nest_path_", "/toppings#0");

    Map<String, Object> parent = new LinkedHashMap<>();
    parent.put("id", "1");
    parent.put("lonely", lonely);
    parent.put("toppings", new ArrayList<>(List.of(topping)));

    Map<String, Object> response = new LinkedHashMap<>();
    response.put("numFound", 1L);
    response.put("docs", new ArrayList<>(List.of(parent)));
    NamedList<Object> in = new NamedList<>();
    in.add("response", response);

    SolrDocument parentDoc =
        ((SolrDocumentList) ResponseNormalizer.normalize(in).get("response")).get(0);

    Object single = parentDoc.getFieldValue("lonely");
    assertTrue("a named child must be a SolrDocument, not a map", single instanceof SolrDocument);
    assertEquals("testing", ((SolrDocument) single).getFirstValue("test_s"));

    Object nestedGrandChild = ((SolrDocument) single).getFieldValue("lonelyGrandChild");
    assertTrue("a grandchild must be reconstructed too", nestedGrandChild instanceof SolrDocument);

    Object array = parentDoc.getFieldValue("toppings");
    assertTrue("a named child array stays a List", array instanceof List);
    assertTrue(
        "its elements must be SolrDocuments", ((List<?>) array).get(0) instanceof SolrDocument);

    // Named children are field values, not child documents -- the same as binary and XML, where
    // ChildDocTransformer calls setField for a named path and addChildDocuments only for anonymous.
    assertFalse(
        "a named child is a field value, so the parent has no child documents",
        parentDoc.hasChildDocuments());
  }

  /** An unmarked object stays a map: most map-valued fields in a response are not documents. */
  @Test
  public void testUnmarkedObjectIsNotPromotedToDocument() {
    Map<String, Object> notADoc = new LinkedHashMap<>();
    notADoc.put("id", "2");
    notADoc.put("test_s", "testing");

    Map<String, Object> parent = new LinkedHashMap<>();
    parent.put("id", "1");
    parent.put("someStruct", notADoc);

    Map<String, Object> response = new LinkedHashMap<>();
    response.put("numFound", 1L);
    response.put("docs", new ArrayList<>(List.of(parent)));
    NamedList<Object> in = new NamedList<>();
    in.add("response", response);

    SolrDocument parentDoc =
        ((SolrDocumentList) ResponseNormalizer.normalize(in).get("response")).get(0);
    assertTrue(
        "an object with no nest marker must stay a NamedList",
        parentDoc.getFieldValue("someStruct") instanceof NamedList);
  }

  @Test
  public void testListOfMapsNormalized() {
    Map<String, Object> a = new LinkedHashMap<>();
    a.put("x", 1);
    Map<String, Object> b = new LinkedHashMap<>();
    b.put("y", 2);
    NamedList<Object> in = new NamedList<>();
    in.add("things", new ArrayList<>(Arrays.asList(a, b)));

    NamedList<Object> out = ResponseNormalizer.normalize(in);
    List<?> things = (List<?>) out.get("things");
    assertTrue(things.get(0) instanceof NamedList);
    assertEquals(1, ((NamedList<?>) things.get(0)).get("x"));
  }

  @Test
  public void testMixedNumberTypesPreserved() {
    // normalizer preserves numeric values as-is (widening happens at the getter layer)
    Map<String, Object> header = new LinkedHashMap<>();
    header.put("status", 0L); // JSON Long
    header.put("QTime", 7L);
    NamedList<Object> in = new NamedList<>();
    in.add("responseHeader", header);

    NamedList<Object> out = ResponseNormalizer.normalize(in);
    NamedList<?> h = (NamedList<?>) out.get("responseHeader");
    assertEquals(0L, h.get("status"));
    assertEquals(7L, h.get("QTime"));
  }

  @Test
  public void testNotADocListWhenNumFoundMissing() {
    // a map with "docs" but no numeric numFound is NOT a doc list -> stays a NamedList
    Map<String, Object> notDocs = new LinkedHashMap<>();
    notDocs.put("docs", new ArrayList<>());
    NamedList<Object> in = new NamedList<>();
    in.add("x", notDocs);

    assertTrue(ResponseNormalizer.normalize(in).get("x") instanceof NamedList);
  }

  /**
   * A plain {@link NamedList} must not be promoted to a {@link SimpleOrderedMap}. The two are
   * written differently — a JSON writer renders a SimpleOrderedMap as {@code {"foo":10}} and a
   * NamedList as {@code ["foo",10]} — and SimpleOrderedMap also implements {@link java.util.Map},
   * whose contract assumes unique keys that a general NamedList does not guarantee. Normalizing
   * must preserve the concrete type rather than widen it.
   */
  public void testPlainNamedListIsNotPromotedToMap() {
    NamedList<Object> plain = new NamedList<>();
    plain.add("dup", 1);
    plain.add("dup", 2);

    NamedList<Object> in = new SimpleOrderedMap<>();
    in.add("section", plain);

    Object out = ResponseNormalizer.normalize(in).get("section");
    assertTrue("must stay a NamedList", out instanceof NamedList);
    assertFalse(
        "a plain NamedList must not become a SimpleOrderedMap", out instanceof SimpleOrderedMap);

    // and the repeated key survives, which is the reason the distinction matters
    NamedList<?> outList = (NamedList<?>) out;
    assertEquals(2, outList.size());
    assertEquals("dup", outList.getName(0));
    assertEquals("dup", outList.getName(1));
    assertEquals(1, outList.getVal(0));
    assertEquals(2, outList.getVal(1));
  }

  /** A SimpleOrderedMap stays one: it is what the binary parser produces and extractors cast to. */
  public void testSimpleOrderedMapStaysOne() {
    NamedList<Object> inner = new SimpleOrderedMap<>();
    inner.add("a", 1);

    NamedList<Object> in = new SimpleOrderedMap<>();
    in.add("section", inner);

    Object out = ResponseNormalizer.normalize(in).get("section");
    assertTrue("must stay a SimpleOrderedMap", out instanceof SimpleOrderedMap);
  }

  /**
   * JSON conveys nested documents as a {@code _childDocuments_} field holding a list of maps; the
   * binary and XML parsers hand them back as child documents, so this one must too.
   */
  @Test
  public void testChildDocumentsAreReconstructed() {
    Map<String, Object> kid = new LinkedHashMap<>();
    kid.put("id", "kid1");
    Map<String, Object> parent = new LinkedHashMap<>();
    parent.put("id", "parent1");
    parent.put(CommonParams.CHILDDOC, List.of(kid));
    Map<String, Object> docList = new LinkedHashMap<>();
    docList.put("numFound", 1);
    docList.put("docs", List.of(parent));
    NamedList<Object> in = new SimpleOrderedMap<>();
    in.add("response", docList);

    SolrDocumentList out = (SolrDocumentList) ResponseNormalizer.normalize(in).get("response");
    SolrDocument outParent = out.get(0);
    assertTrue("child documents must be reconstructed", outParent.hasChildDocuments());
    assertEquals(1, outParent.getChildDocumentCount());
    assertEquals("kid1", outParent.getChildDocuments().get(0).getFieldValue("id"));
    assertNull(
        "the raw field must not remain alongside the children",
        outParent.getFieldValue(CommonParams.CHILDDOC));
  }

  /** Children nest, so a grandchild must be reconstructed too. */
  @Test
  public void testChildDocumentsNest() {
    Map<String, Object> grandkid = new LinkedHashMap<>();
    grandkid.put("id", "grandkid1");
    Map<String, Object> kid = new LinkedHashMap<>();
    kid.put("id", "kid1");
    kid.put(CommonParams.CHILDDOC, List.of(grandkid));
    Map<String, Object> parent = new LinkedHashMap<>();
    parent.put("id", "parent1");
    parent.put(CommonParams.CHILDDOC, List.of(kid));
    Map<String, Object> docList = new LinkedHashMap<>();
    docList.put("numFound", 1);
    docList.put("docs", List.of(parent));
    NamedList<Object> in = new SimpleOrderedMap<>();
    in.add("response", docList);

    SolrDocumentList out = (SolrDocumentList) ResponseNormalizer.normalize(in).get("response");
    SolrDocument outKid = out.get(0).getChildDocuments().get(0);
    assertTrue("grandchildren must be reconstructed", outKid.hasChildDocuments());
    assertEquals("grandkid1", outKid.getChildDocuments().get(0).getFieldValue("id"));
  }
}
