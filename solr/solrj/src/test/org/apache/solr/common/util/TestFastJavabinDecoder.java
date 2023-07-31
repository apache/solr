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

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.FastStreamingDocsCallback;
import org.apache.solr.client.solrj.impl.StreamingBinaryResponseParser;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.util.FastJavaBinDecoder.Tag;

public class TestFastJavabinDecoder extends SolrTestCaseJ4 {

  public void testTagRead() throws Exception {
    Utils.BAOS baos = new Utils.BAOS();
    FastOutputStream faos = FastOutputStream.wrap(baos);

    try (JavaBinCodec codec = new JavaBinCodec(faos, null)) {
      codec.writeVal(10);
      codec.writeVal(100);
      codec.writeVal("Hello!");
    }

    faos.flushBuffer();
    faos.close();

    FastInputStream fis = new FastInputStream(null, baos.getbuf(), 0, baos.size());
    try (FastJavaBinDecoder.StreamCodec scodec = new FastJavaBinDecoder.StreamCodec(fis)) {
      scodec.start();
      Tag tag = scodec.getTag();
      assertEquals(Tag._SINT, tag);
      assertEquals(10, scodec.readSmallInt(scodec.dis));
      tag = scodec.getTag();
      assertEquals(Tag._SINT, tag);
      assertEquals(100, scodec.readSmallInt(scodec.dis));
      tag = scodec.getTag();
      assertEquals(Tag._STR, tag);
      assertEquals("Hello!", scodec.readStr(fis).toString());
    }
  }

  public void testSimple() throws IOException {
    String sampleObj =
        "{k : v , "
            + "mapk : {k1: v1, k2 : [v2_1 , v2_2 ]},"
            + "listk : [ 1, 2, 3 ],"
            + "maps : [ {id: kov1}, {id : kov2} ,{id:kov3 , longv : 234} ],"
            + "}";

    @SuppressWarnings({"rawtypes"})
    Map m = (Map) Utils.fromJSONString(sampleObj);
    Utils.BAOS baos = new Utils.BAOS();
    try (JavaBinCodec jbc = new JavaBinCodec()) {
      jbc.marshal(m, baos);
    }

    @SuppressWarnings({"rawtypes"})
    Map m2;
    try (JavaBinCodec jbc = new JavaBinCodec()) {
      m2 = (Map) jbc.unmarshal(new FastInputStream(null, baos.getbuf(), 0, baos.size()));
    }
    @SuppressWarnings({"rawtypes"})
    LinkedHashMap fastMap =
        (LinkedHashMap)
            new FastJavaBinDecoder()
                .withInputStream(new FastInputStream(null, baos.getbuf(), 0, baos.size()))
                .decode(FastJavaBinDecoder.getEntryListener());
    assertEquals(
        Utils.writeJson(m2, new StringWriter(), true).toString(),
        Utils.writeJson(fastMap, new StringWriter(), true).toString());

    @SuppressWarnings({"unchecked", "rawtypes"})
    Object newMap =
        new FastJavaBinDecoder()
            .withInputStream(new FastInputStream(null, baos.getbuf(), 0, baos.size()))
            .decode(
                e -> {
                  e.listenContainer(
                      new LinkedHashMap<>(),
                      e_ -> {
                        Map rootMap = (Map) e_.ctx();
                        if (e_.type() == DataEntry.Type.ENTRY_ITER) {
                          e_.listenContainer(
                              rootMap.computeIfAbsent(e_.name(), o -> new ArrayList<>()),
                              FastJavaBinDecoder.getEntryListener());
                        } else if (e_.type() == DataEntry.Type.KEYVAL_ITER) {
                          e_.listenContainer(
                              rootMap.computeIfAbsent(e_.name(), o -> new LinkedHashMap<>()),
                              e1 -> {
                                Map<CharSequence, String> m1 = (Map<CharSequence, String>) e1.ctx();
                                if ("k1".contentEquals(e1.name())) {
                                  m1.put(e1.name(), e1.val().toString());
                                }
                                // eat up k2
                              });
                        } else if (e_.type() == DataEntry.Type.STR) {
                          rootMap.put(e_.name(), e_.val().toString());
                        }
                      });
                });
    ((Map) m2.get("mapk")).remove("k2");
    assertEquals(
        Utils.writeJson(m2, new StringWriter(), true).toString(),
        Utils.writeJson(newMap, new StringWriter(), true).toString());
  }

  public void testFastJavabinStreamingDecoder() throws IOException {
    Utils.BAOS baos = new Utils.BAOS();
    try (InputStream is = getClass().getResourceAsStream("/solrj/javabin_sample.bin")) {
      is.transferTo(baos);
    }

    SolrDocumentList list;
    try (JavaBinCodec jbc = new JavaBinCodec()) {
      @SuppressWarnings({"rawtypes"})
      SimpleOrderedMap o = (SimpleOrderedMap) jbc.unmarshal(baos.toByteArray());
      list = (SolrDocumentList) o.get("response");
    }

    System.out.println(
        " " + list.getNumFound() + " , " + list.getStart() + " , " + list.getMaxScore());
    class Pojo {
      long _idx;
      CharSequence id;
      boolean inStock;
      float price;

      @SuppressWarnings({"rawtypes"})
      List<NamedList> children;
    }
    StreamingBinaryResponseParser parser =
        new StreamingBinaryResponseParser(
            new FastStreamingDocsCallback() {

              @Override
              public Object initDocList(Long numFound, Long start, Float maxScore) {
                assertEquals((Long) list.getNumFound(), numFound);
                assertEquals((Long) list.getStart(), start);
                assertEquals(list.getMaxScore(), maxScore);
                return new int[1];
              }

              @Override
              public Object startDoc(Object docListObj) {
                Pojo pojo = new Pojo();
                pojo._idx = ((int[]) docListObj)[0]++;
                return pojo;
              }

              @Override
              public void field(DataEntry field, Object docObj) {
                Pojo pojo = (Pojo) docObj;
                if ("id".contentEquals(field.name())) {
                  pojo.id = ((Utf8CharSequence) field.val()).clone();
                } else if (field.type() == DataEntry.Type.BOOL
                    && "inStock".contentEquals(field.name())) {
                  pojo.inStock = field.boolVal();
                } else if (field.type() == DataEntry.Type.FLOAT
                    && "price".contentEquals(field.name())) {
                  pojo.price = field.floatVal();
                }
              }

              @Override
              public void endDoc(Object docObj) {
                Pojo pojo = (Pojo) docObj;
                SolrDocument doc = list.get((int) pojo._idx);
                assertEquals(doc.get("id"), pojo.id.toString());
                if (doc.get("inStock") != null) assertEquals(doc.get("inStock"), pojo.inStock);
                if (doc.get("price") != null)
                  assertEquals((Float) doc.get("price"), pojo.price, 0.001);
              }
            });
    parser.processResponse(new FastInputStream(null, baos.getbuf(), 0, baos.size()), null);
  }

  public void testParsingWithChildDocs() throws IOException {
    SolrDocument d1 = TestJavaBinCodec.generateSolrDocumentWithChildDocs();
    d1.setField("id", "101");
    SolrDocument d2 = TestJavaBinCodec.generateSolrDocumentWithChildDocs();
    d2.setField("id", "102");
    d2.setField("longs", Arrays.asList(100l, 200l));

    SolrDocumentList sdocs = new SolrDocumentList();
    sdocs.setStart(0);
    sdocs.setNumFound(2);
    sdocs.add(d1);
    sdocs.add(d2);

    SimpleOrderedMap<SolrDocumentList> orderedMap = new SimpleOrderedMap<>();
    orderedMap.add("response", sdocs);

    Utils.BAOS baos = new Utils.BAOS();
    try (JavaBinCodec jbc = new JavaBinCodec()) {
      jbc.marshal(orderedMap, baos);
    }
    boolean[] useListener = new boolean[1];
    useListener[0] = true;

    class Pojo {
      CharSequence id;
      CharSequence subject;
      CharSequence cat;
      long[] longs;
      final List<Pojo> children = new ArrayList<>();

      public void compare(SolrDocument d) {
        assertEquals(String.valueOf(id), String.valueOf(d.getFieldValue("id")));
        assertEquals(String.valueOf(subject), String.valueOf(d.getFieldValue("subject")));
        assertEquals(String.valueOf(cat), String.valueOf(d.getFieldValue("cat")));
        assertEquals(
            Objects.requireNonNullElse(d.getChildDocuments(), Collections.emptyList()).size(),
            children.size());
        @SuppressWarnings({"unchecked"})
        List<Long> l = (List<Long>) d.getFieldValue("longs");
        if (l != null) {
          assertNotNull(longs);
          for (int i = 0; i < l.size(); i++) {
            Long v = l.get(i);
            assertEquals(v.longValue(), longs[i]);
          }
        }
        List<SolrDocument> childDocuments = d.getChildDocuments();
        if (childDocuments == null) return;
        for (int i = 0; i < childDocuments.size(); i++) {
          children.get(i).compare(childDocuments.get(i));
        }
      }
    }
    List<Pojo> l = new ArrayList<>();
    StreamingBinaryResponseParser binaryResponseParser =
        new StreamingBinaryResponseParser(
            new FastStreamingDocsCallback() {

              @Override
              public Object initDocList(Long numFound, Long start, Float maxScore) {
                return l;
              }

              @Override
              @SuppressWarnings({"unchecked"})
              public Object startDoc(Object docListObj) {
                Pojo pojo = new Pojo();
                ((List) docListObj).add(pojo);
                return pojo;
              }

              @Override
              public void field(DataEntry field, Object docObj) {
                Pojo pojo = (Pojo) docObj;
                if ("id".contentEquals(field.name())) {
                  pojo.id = field.strValue();
                } else if ("subject".contentEquals(field.name())) {
                  pojo.subject = field.strValue();
                } else if ("cat".contentEquals(field.name())) {
                  pojo.cat = field.strValue();
                } else if (field.type() == DataEntry.Type.ENTRY_ITER
                    && "longs".contentEquals(field.name())) {
                  if (useListener[0]) {
                    field.listenContainer(pojo.longs = new long[field.length()], READLONGS);
                  } else {
                    @SuppressWarnings({"unchecked"})
                    List<Long> longList = (List<Long>) field.val();
                    pojo.longs = new long[longList.size()];
                    for (int i = 0; i < longList.size(); i++) {
                      pojo.longs[i] = longList.get(i);
                    }
                  }
                }
              }

              @Override
              public Object startChildDoc(Object parentDocObj) {
                Pojo parent = (Pojo) parentDocObj;
                Pojo child = new Pojo();
                parent.children.add(child);
                return child;
              }
            });
    binaryResponseParser.processResponse(
        new FastInputStream(null, baos.getbuf(), 0, baos.size()), null);
    for (int i = 0; i < sdocs.size(); i++) {
      l.get(i).compare(sdocs.get(i));
    }

    l.clear();

    useListener[0] = false;
    binaryResponseParser.processResponse(
        new FastInputStream(null, baos.getbuf(), 0, baos.size()), null);
    for (int i = 0; i < sdocs.size(); i++) {
      l.get(i).compare(sdocs.get(i));
    }
  }

  static final DataEntry.EntryListener READLONGS =
      e -> {
        if (e.type() != DataEntry.Type.LONG) return;
        long[] array = (long[]) e.ctx();
        array[(int) e.index()] = e.longVal();
      };
}
