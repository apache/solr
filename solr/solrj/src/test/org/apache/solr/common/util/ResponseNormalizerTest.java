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
package org.apache.solr.common.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.solr.SolrTestCase;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
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
}
