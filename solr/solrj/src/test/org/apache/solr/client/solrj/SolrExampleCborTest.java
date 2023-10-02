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
import com.fasterxml.jackson.dataformat.cbor.CBORGenerator;
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
import org.junit.Ignore;

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
    return new HttpSolrClient.Builder(getCoreUrl())
        .allowMultiPartPost(random().nextBoolean())
        .withRequestWriter(cborRequestWriter())
        .withResponseParser(cborResponseparser())
        .build();
  }

  @Override
  @Ignore
  public void testQueryPerf() {
    /*Ignore*/
  }

  @Override
  @Ignore
  public void testExampleConfig() {
    /*Ignore*/
  }

  @Override
  @Ignore
  public void testAugmentFields() {
    /*Ignore*/
  }

  @Override
  @Ignore
  public void testRawFields() {
    /*Ignore*/
  }

  @Override
  @Ignore
  public void testUpdateRequestWithParameters() {
    /*Ignore*/
  }

  @Override
  @Ignore
  public void testContentStreamRequest() {
    /*Ignore*/
  }

  @Override
  @Ignore
  public void testStreamingRequest() {
    /*Ignore*/
  }

  @Override
  @Ignore
  public void testMultiContentWriterRequest() {
    /*Ignore*/
  }

  @Override
  @Ignore
  public void testMultiContentStreamRequest() {
    /*Ignore*/
  }

  @Override
  @Ignore
  public void testLukeHandler() {
    /*Ignore*/
  }

  @Override
  @Ignore
  public void testStatistics() {
    /*Ignore*/
  }

  @Override
  @Ignore
  public void testFaceting() {
    /*Ignore*/
  }

  @Override
  @Ignore
  public void testPivotFacets() {
    /*Ignore*/
  }

  @Override
  public void testPivotFacetsStats() {
    /*Ignore*/
  }

  @Override
  @Ignore
  public void testPivotFacetsStatsNotSupported() {
    /*Ignore*/
  }

  @Override
  @Ignore
  public void testPivotFacetsQueries() {
    /*Ignore*/
  }

  @Override
  @Ignore
  public void testPivotFacetsRanges() {
    /*Ignore*/
  }

  @Override
  @Ignore
  public void testPivotFacetsMissing() {
    /*Ignore*/
  }

  @Override
  @Ignore
  public void testUpdateField() {
    /*Ignore*/
  }

  @Override
  @Ignore
  public void testUpdateMultiValuedField() {
    /*Ignore*/
  }

  @Override
  @Ignore
  public void testSetNullUpdates() {
    /*Ignore*/
  }

  @Override
  @Ignore
  public void testSetNullUpdateOrder() {
    /*Ignore*/
  }

  @Override
  @Ignore
  public void testQueryWithParams() {
    /*Ignore*/
  }

  @Override
  @Ignore
  public void testChildDoctransformer() {
    /*Ignore*/
  }

  @Override
  @Ignore
  public void testExpandComponent() {
    /*Ignore*/
  }

  @Override
  @Ignore
  public void testMoreLikeThis() {
    /*Ignore*/
  }

  @Override
  @Ignore
  public void testAddChildToChildFreeDoc() {
    /*Ignore*/
  }

  @Override
  @Ignore
  public void testDeleteParentDoc() {
    /*Ignore*/
  }

  @Override
  @Ignore
  public void testCommitWithinOnAdd() {
    /*Ignore*/
  }

  @Override
  @Ignore
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

              ObjectMapper cborMapper =
                  new ObjectMapper(
                      CBORFactory.builder().enable(CBORGenerator.Feature.STRINGREF).build());
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
