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
package org.apache.solr.response;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.util.ByteUtils;
import org.apache.solr.common.util.JavaBinCodec;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrQueryRequestBase;
import org.apache.solr.response.JavaBinResponseWriter.Resolver;
import org.apache.solr.search.DocList;
import org.apache.solr.search.ReturnFields;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.SolrReturnFields;
import org.junit.BeforeClass;

/**
 * Test for JavaBinResponseWriter
 *
 * @since solr 1.4
 */
public class TestJavaBinResponseWriter extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeClass() throws Exception {
    System.setProperty(
        "solr.index.updatelog.enabled", "false"); // schema12 doesn't support _version_
    initCore("solrconfig.xml", "schema12.xml");
  }

  public void testBytesRefWriting() {
    compareStringFormat("ThisIsUTF8String");
    compareStringFormat("Thailand (ประเทศไทย)");
    compareStringFormat(
        "LIVE: सबरीमाला मंदिर के पास पहुंची दो महिलाएं, जमकर हो रहा विरोध-प्रदर्शन");
  }

  private void compareStringFormat(String input) {
    byte[] bytes1 = new byte[1024];
    int len1 = ByteUtils.UTF16toUTF8(input, 0, input.length(), bytes1, 0);
    BytesRef bytesref = new BytesRef(input);
    System.out.println();
    assertEquals(len1, bytesref.length);
    for (int i = 0; i < len1; i++) {
      assertEquals(input + " not matching char at :" + i, bytesref.bytes[i], bytes1[i]);
    }
  }

  /** Tests known types implementation by asserting correct encoding/decoding of UUIDField */
  public void testUUID() throws Exception {
    String s = UUID.randomUUID().toString().toLowerCase(Locale.ROOT);
    assertU(adoc("id", "101", "uuid", s));
    assertU(commit());
    SolrQueryRequest req = withPath("/select", lrf.makeRequest("q", "*:*"));
    SolrQueryResponse rsp = h.queryAndResponse(req);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    h.getCore().getQueryResponseWriter("javabin").write(baos, req, rsp);
    NamedList<?> res;
    try (JavaBinCodec jbc = new JavaBinCodec()) {
      res = (NamedList<?>) jbc.unmarshal(new ByteArrayInputStream(baos.toByteArray()));
    }
    SolrDocumentList docs = (SolrDocumentList) res.get("response");
    for (Object doc : docs) {
      SolrDocument document = (SolrDocument) doc;
      assertEquals(
          "Returned object must be a string",
          "java.lang.String",
          document.getFieldValue("uuid").getClass().getName());
      assertEquals("Wrong UUID string returned", s, document.getFieldValue("uuid"));
    }

    req.close();
  }

  public void testStoredFieldTypesInResponse() throws Exception {
    assertU(adoc("id", "javabintypes1", "foo_i", "42", "foo_is", "7", "foo_is", "11"));
    assertU(commit());

    SolrQueryRequest req = req("q", "id:javabintypes1", "fl", "foo_i,foo_is");
    try {
      SolrQueryResponse rsp = h.queryAndResponse(null, req);
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      h.getCore().getQueryResponseWriter("javabin").write(baos, req, rsp);

      NamedList<?> response;
      try (JavaBinCodec codec = new JavaBinCodec()) {
        response = (NamedList<?>) codec.unmarshal(new ByteArrayInputStream(baos.toByteArray()));
      }
      SolrDocumentList docs = (SolrDocumentList) response.get("response");
      assertEquals(1, docs.size());

      SolrDocument doc = docs.get(0);
      Object singleValued = doc.getFieldValue("foo_i");
      assertEquals(Integer.valueOf(42), singleValued);
      assertEquals(Integer.class, singleValued.getClass());

      Object multiValued = doc.getFieldValue("foo_is");
      assertTrue(multiValued instanceof List<?>);
      assertEquals(List.of(7, 11), multiValued);
    } finally {
      req.close();
      clearIndex();
      assertU(commit());
    }
  }

  public void testInvalidStoredValueDoesNotAbortJavaBinResponse() throws Exception {
    assertU(adoc("id", "javabin-invalid-stored-value"));
    assertU(commit());

    SolrQueryRequest req = req("q", "id:javabin-invalid-stored-value", "fl", "bad_s,good_i,good_s");
    try {
      SolrQueryResponse rsp = h.queryAndResponse(null, req);
      ResultContext original = (ResultContext) rsp.getResponse();
      SolrDocument doc = new SolrDocument();
      doc.setField(
          "bad_s",
          new Field("bad_s", "unused", StringField.TYPE_STORED) {
            @Override
            public String stringValue() {
              throw new IllegalStateException("synthetic stored-value conversion failure");
            }

            @Override
            public String toString() {
              return "bad_s";
            }
          });
      doc.setField("good_i", req.getSchema().getField("good_i").createField(7));
      doc.setField("good_s", new StringField("good_s", "still-returned", Field.Store.YES));

      ResultContext withInvalidField =
          new ResultContext() {
            @Override
            public DocList getDocList() {
              return original.getDocList();
            }

            @Override
            public ReturnFields getReturnFields() {
              return original.getReturnFields();
            }

            @Override
            public SolrIndexSearcher getSearcher() {
              return original.getSearcher();
            }

            @Override
            public Query getQuery() {
              return original.getQuery();
            }

            @Override
            public SolrQueryRequest getRequest() {
              return original.getRequest();
            }

            @Override
            public Iterator<SolrDocument> getProcessedDocuments() {
              return List.of(doc).iterator();
            }
          };
      rsp.getValues().remove("response");
      rsp.add("response", withInvalidField);

      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      h.getCore().getQueryResponseWriter("javabin").write(baos, req, rsp);

      NamedList<?> response;
      try (JavaBinCodec codec = new JavaBinCodec()) {
        response = (NamedList<?>) codec.unmarshal(new ByteArrayInputStream(baos.toByteArray()));
      }
      SolrDocumentList docs = (SolrDocumentList) response.get("response");
      assertEquals(1, docs.size());
      SolrDocument returned = docs.get(0);
      assertFalse(returned.containsKey("bad_s"));
      assertEquals(Integer.valueOf(7), returned.getFieldValue("good_i"));
      assertEquals("still-returned", returned.getFieldValue("good_s"));
    } finally {
      req.close();
      clearIndex();
      assertU(commit());
    }
  }

  public void testOmitHeader() throws Exception {
    SolrQueryRequest req = req("q", "*:*", "omitHeader", "true");
    SolrQueryResponse rsp = h.queryAndResponse(null, req);

    NamedList<Object> res = JavaBinResponseWriter.getParsedResponse(req, rsp);
    assertNull(res.get("responseHeader"));
    req.close();

    req = req("q", "*:*");
    rsp = h.queryAndResponse(null, req);
    res = JavaBinResponseWriter.getParsedResponse(req, rsp);
    assertNotNull(res.get("responseHeader"));
    req.close();
  }

  public void testResolverSolrDocumentPartialFields() throws Exception {
    SolrQueryRequestBase req =
        lrf.makeRequest(
            "q", "*:*",
            "fl", "id,xxx,ddd_s");
    SolrDocument in = new SolrDocument();
    in.addField("id", 345);
    in.addField("aaa_s", "aaa");
    in.addField("bbb_s", "bbb");
    in.addField("ccc_s", "ccc");
    in.addField("ddd_s", "ddd");
    in.addField("eee_s", "eee");

    Resolver r = new Resolver(req, new SolrReturnFields(req));
    Object o = r.resolve(in, new JavaBinCodec());

    assertNotNull("obj is null", o);
    assertTrue("obj is not doc", o instanceof SolrDocument);

    SolrDocument out = new SolrDocument();
    for (Map.Entry<String, Object> e : in) {
      if (r.isWritable(e.getKey())) out.put(e.getKey(), e.getValue());
    }
    assertTrue("id not found", out.getFieldNames().contains("id"));
    assertTrue("ddd_s not found", out.getFieldNames().contains("ddd_s"));
    assertEquals("Wrong number of fields found", 2, out.getFieldNames().size());
    req.close();
  }
}
