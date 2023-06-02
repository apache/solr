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
package org.apache.solr.client.solrj;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.cbor.CBORFactory;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.solr.client.solrj.impl.BinaryRequestWriter;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.RequestWriter;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.NamedList;
import org.junit.BeforeClass;

/**
 * This test only tests a subset of tests specified in the parent class. It's because we do not
 * provide a full impl of all types of responses in SolrJ. It just handles very common types and
 * converts them from Map to NamedList
 */
public class SolrExampleCborTest extends SolrExampleTests {
  @BeforeClass
  public static void beforeTest() throws Exception {
    createAndStartJetty(legacyExampleCollection1SolrHome());
  }

  @Override
  public SolrClient createNewSolrClient() {
    return new HttpSolrClient.Builder(getServerUrl())
        .allowMultiPartPost(random().nextBoolean())
        .withRequestWriter(cborRequestWriter())
        .withResponseParser(cborResponseparser())
        .build();
  }

  @Override
  public void testQueryPerf() {
    /*Ignore*/
  }

  @Override
  public void testExampleConfig() {
    /*Ignore*/
  }

  @Override
  public void testAugmentFields() {
    /*Ignore*/
  }

  @Override
  public void testRawFields() {
    /*Ignore*/
  }

  @Override
  public void testUpdateRequestWithParameters() {
    /*Ignore*/
  }

  @Override
  public void testContentStreamRequest() {
    /*Ignore*/
  }

  @Override
  public void testStreamingRequest() {
    /*Ignore*/
  }

  @Override
  public void testMultiContentWriterRequest() {
    /*Ignore*/
  }

  @Override
  public void testMultiContentStreamRequest() {
    /*Ignore*/
  }

  @Override
  public void testLukeHandler() {
    /*Ignore*/
  }

  @Override
  public void testStatistics() {
    /*Ignore*/
  }

  @Override
  public void testFaceting() {
    /*Ignore*/
  }

  @Override
  public void testPivotFacets() {
    /*Ignore*/
  }

  @Override
  public void testPivotFacetsStats() {
    /*Ignore*/
  }

  @Override
  public void testPivotFacetsStatsNotSupported() {
    /*Ignore*/
  }

  @Override
  public void testPivotFacetsQueries() {
    /*Ignore*/
  }

  @Override
  public void testPivotFacetsRanges() {
    /*Ignore*/
  }

  @Override
  public void testPivotFacetsMissing() {
    /*Ignore*/
  }

  @Override
  public void testUpdateField() {
    /*Ignore*/
  }

  @Override
  public void testUpdateMultiValuedField() {
    /*Ignore*/
  }

  @Override
  public void testSetNullUpdates() {
    /*Ignore*/
  }

  @Override
  public void testSetNullUpdateOrder() {
    /*Ignore*/
  }

  @Override
  public void testQueryWithParams() {
    /*Ignore*/
  }

  @Override
  public void testChildDoctransformer() {
    /*Ignore*/
  }

  @Override
  public void testExpandComponent() {
    /*Ignore*/
  }

  @Override
  public void testMoreLikeThis() {
    /*Ignore*/
  }

  @Override
  public void testAddChildToChildFreeDoc() {
    /*Ignore*/
  }

  @Override
  public void testDeleteParentDoc() {
    /*Ignore*/
  }

  @Override
  public void testCommitWithinOnAdd() {
    /*Ignore*/
  }

  @Override
  public void testAddDelete() {
    /*Ignore*/
  }

  @SuppressWarnings("rawtypes")
  private static RequestWriter cborRequestWriter() {
    return new BinaryRequestWriter() {

      @Override
      public ContentWriter getContentWriter(SolrRequest<?> request) {
        if (request instanceof UpdateRequest) {
          UpdateRequest updateRequest = (UpdateRequest) request;
          List<SolrInputDocument> docs = updateRequest.getDocuments();
          if (docs == null || docs.isEmpty()) return super.getContentWriter(request);
          return new ContentWriter() {
            @Override
            public void write(OutputStream os) throws IOException {

              List<Map> mapDocs = new ArrayList<>();
              for (SolrInputDocument doc : docs) {
                mapDocs.add(doc.toMap(new LinkedHashMap<>()));
              }

              CBORFactory jf = new CBORFactory();
              ObjectMapper cborMapper = new ObjectMapper(jf);
              cborMapper.writeValue(os, mapDocs);
            }

            @Override
            public String getContentType() {
              return "application/cbor";
            }
          };
        } else {
          return super.getContentWriter(request);
        }
      }

      @Override
      public String getUpdateContentType() {
        return "application/cbor";
      }

      @Override
      public String getPath(SolrRequest<?> req) {
        if (req instanceof UpdateRequest) {
          UpdateRequest updateRequest = (UpdateRequest) req;
          List<SolrInputDocument> docs = updateRequest.getDocuments();
          if (docs == null || docs.isEmpty()) return super.getPath(req);
          return "/update/cbor";
        } else {
          return super.getPath(req);
        }
      }
    };
  }

  private static ResponseParser cborResponseparser() {
    return new ResponseParser() {

      @Override
      public String getWriterType() {
        return "cbor";
      }

      @Override
      @SuppressWarnings({"rawtypes", "unchecked"})
      public NamedList<Object> processResponse(InputStream b, String encoding) {
        ObjectMapper objectMapper = new ObjectMapper(new CBORFactory());
        try {
          Map m = (Map) objectMapper.readValue(b, Object.class);
          NamedList nl = new NamedList();
          m.forEach(
              (k, v) -> {
                if (v instanceof Map) {
                  Map map = (Map) v;
                  if ("response".equals(k)) {
                    v = convertResponse((Map) v);
                  } else {
                    v = new NamedList(map);
                  }
                }
                nl.add(k.toString(), v);
              });
          return nl;
        } catch (IOException e) {
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "parsing error", e);
        }
      }

      @SuppressWarnings({"rawtypes", "unchecked"})
      private SolrDocumentList convertResponse(Map m) {
        List<Map> docs = (List<Map>) m.get("docs");
        SolrDocumentList sdl = new SolrDocumentList();
        Object o = m.get("numFound");
        if (o != null) sdl.setNumFound(((Number) o).longValue());
        o = m.get("start");
        if (o != null) sdl.setStart(((Number) o).longValue());
        o = m.get("numFoundExact");
        if (o != null) sdl.setNumFoundExact((Boolean) o);

        if (docs != null) {
          for (Map doc : docs) {
            SolrDocument sd = new SolrDocument();
            doc.forEach((k, v) -> sd.addField(k.toString(), v));
            sdl.add(sd);
          }
        }
        return sdl;
      }

      @Override
      public NamedList<Object> processResponse(Reader reader) {
        return null;
      }
    };
  }
}
