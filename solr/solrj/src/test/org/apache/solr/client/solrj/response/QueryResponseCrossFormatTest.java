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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import org.apache.solr.SolrTestCase;
import org.apache.solr.client.solrj.response.json.JsonMapResponseParser;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.util.JavaBinCodec;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.ResponseNormalizer;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.junit.Test;

/**
 * Proves QueryResponse reaches identical typed results whether the wire format was binary or JSON
 * (json.nl=map), once the response is passed through {@link ResponseNormalizer}. Binary is the
 * canonical baseline; JSON must match it.
 */
public class QueryResponseCrossFormatTest extends SolrTestCase {

  private static NamedList<Object> canonical() {
    NamedList<Object> header = new SimpleOrderedMap<>();
    header.add("status", 0);
    header.add("QTime", 7);

    SolrDocumentList docs = new SolrDocumentList();
    docs.setNumFound(2);
    docs.setStart(0);
    SolrDocument d1 = new SolrDocument();
    d1.addField("id", "1");
    SolrDocument d2 = new SolrDocument();
    d2.addField("id", "2");
    docs.add(d1);
    docs.add(d2);

    NamedList<Object> catCounts = new SimpleOrderedMap<>();
    catCounts.add("electronics", 3);
    catCounts.add("books", 1);
    NamedList<Object> facetFields = new SimpleOrderedMap<>();
    facetFields.add("cat", catCounts);
    NamedList<Object> facetCounts = new SimpleOrderedMap<>();
    facetCounts.add("facet_queries", new SimpleOrderedMap<>());
    facetCounts.add("facet_fields", facetFields);

    NamedList<Object> body = new SimpleOrderedMap<>();
    body.add("responseHeader", header);
    body.add("response", docs);
    body.add("facet_counts", facetCounts);
    return body;
  }

  private static void assertResponse(String fmt, QueryResponse r) {
    assertEquals(fmt + " status", 0, r.getStatus());
    assertEquals(fmt + " qtime", 7, r.getQTime());
    assertNotNull(fmt + " results", r.getResults());
    assertEquals(fmt + " numFound", 2, r.getResults().getNumFound());
    assertEquals(fmt + " doc0", "1", r.getResults().get(0).getFirstValue("id"));

    // facet section parity
    assertNotNull(fmt + " facetFields", r.getFacetFields());
    assertEquals(fmt + " facet name", "cat", r.getFacetFields().get(0).getName());
    assertEquals(fmt + " facet valueCount", 2, r.getFacetFields().get(0).getValueCount());
    assertEquals(fmt + " facet count", 3L, r.getFacetFields().get(0).getValues().get(0).getCount());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testBinaryBaseline() throws Exception {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try (JavaBinCodec codec = new JavaBinCodec()) {
      codec.marshal(canonical(), out);
    }
    NamedList<Object> parsed;
    try (JavaBinCodec codec = new JavaBinCodec()) {
      parsed = (NamedList<Object>) codec.unmarshal(new ByteArrayInputStream(out.toByteArray()));
    }
    QueryResponse r = new QueryResponse();
    r.setResponse(ResponseNormalizer.normalize(parsed));
    assertResponse("binary", r);
  }

  @Test
  public void testJsonMapMatchesBinary() throws Exception {
    String json =
        "{\"responseHeader\":{\"status\":0,\"QTime\":7},"
            + "\"response\":{\"numFound\":2,\"start\":0,"
            + "\"docs\":[{\"id\":\"1\"},{\"id\":\"2\"}]},"
            + "\"facet_counts\":{\"facet_queries\":{},"
            + "\"facet_fields\":{\"cat\":{\"electronics\":3,\"books\":1}}}}";
    NamedList<Object> parsed =
        new JsonMapResponseParser()
            .processResponse(
                new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8)), "UTF-8");
    QueryResponse r = new QueryResponse();
    r.setResponse(ResponseNormalizer.normalize(parsed));
    assertResponse("json-map", r);
  }

  @Test
  public void testXmlMatchesBinary() throws Exception {
    String xml =
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
            + "<response>\n"
            + "  <lst name=\"responseHeader\"><int name=\"status\">0</int>"
            + "<int name=\"QTime\">7</int></lst>\n"
            + "  <result name=\"response\" numFound=\"2\" start=\"0\">\n"
            + "    <doc><str name=\"id\">1</str></doc>\n"
            + "    <doc><str name=\"id\">2</str></doc>\n"
            + "  </result>\n"
            + "  <lst name=\"facet_counts\">\n"
            + "    <lst name=\"facet_queries\"/>\n"
            + "    <lst name=\"facet_fields\">\n"
            + "      <lst name=\"cat\"><int name=\"electronics\">3</int>"
            + "<int name=\"books\">1</int></lst>\n"
            + "    </lst>\n"
            + "  </lst>\n"
            + "</response>";
    NamedList<Object> parsed =
        new XMLResponseParser()
            .processResponse(
                new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)), "UTF-8");
    QueryResponse r = new QueryResponse();
    r.setResponse(ResponseNormalizer.normalize(parsed));
    assertResponse("xml", r);
  }
}
