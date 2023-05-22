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
package org.apache.solr.util;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.cbor.CBORFactory;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.impl.InputStreamResponseParser;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.RequestWriter;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.util.JavaBinCodec;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.Utils;
import org.apache.solr.handler.loader.CborStream;
import org.apache.solr.response.XMLResponseWriter;

public class TestCborDataFormat extends SolrCloudTestCase {

  @SuppressWarnings("unchecked")
  public void testRoundTrip() throws Exception {
    String testCollection = "testRoundTrip";

    MiniSolrCloudCluster cluster =
        configureCluster(1)
            .addConfig(
                "conf", TEST_PATH().resolve("configsets").resolve("cloud-managed").resolve("conf"))
            .configure();
    try {
      System.setProperty("managed.schema.mutable", "true");
      CollectionAdminRequest.createCollection(testCollection, "conf", 1, 1)
          .process(cluster.getSolrClient());
      GenericSolrRequest req =
          new GenericSolrRequest(SolrRequest.METHOD.POST, "/schema")
              .setContentWriter(
                  new RequestWriter.StringPayloadContentWriter(
                      "{\n"
                          + "\"add-field-type\" : {"
                          + "\"name\":\"knn_vector_10\",\"class\":\"solr.DenseVectorField\",\"vectorDimension\":10,\"similarityFunction\":\"cosine\",\"knnAlgorithm\":\"hnsw\"  },\n"
                          + "\"add-field\" : ["
                          + "{\"name\":\"name\",\"type\":\"sortabletext\",\"multiValued\":false,\"stored\":true},\n"
                          + "{\"name\":\"initial_release_date\",\"type\":\"string\",\"stored\":true},\n"
                          + "{\"name\":\"directed_by\",\"type\":\"string\",\"multiValued\":true,\"stored\":true    },\n"
                          + "{\"name\":\"genre\",\"type\":\"string\",\"multiValued\":true,\"stored\":true},\n"
                          + "{\"name\":\"film_vector\",\"type\":\"knn_vector_10\",\"indexed\":true,\"stored\":true}]}",
                      XMLResponseWriter.CONTENT_TYPE_XML_UTF8));
      cluster.getSolrClient().request(req, testCollection);
      Path filmsJson = new File(ExternalPaths.SOURCE_HOME, "example/films/films.json").toPath();
      byte[] b = Files.readAllBytes(filmsJson);

      cluster
          .getSolrClient()
          .request(
              new GenericSolrRequest(
                      SolrRequest.METHOD.POST,
                      "/update/cbor",
                      new MapSolrParams(Map.of("commit", "true")))
                  .withContent(readAsCbor(new ByteArrayInputStream(b)), "application/cbor"),
              testCollection);

      NamedList<Object> result =
          cluster
              .getSolrClient()
              .request(new QueryRequest(new SolrQuery("*:*").setRows(1111)), testCollection);
      SolrDocumentList sdl = (SolrDocumentList) result.get("response");
      assertEquals(1100, sdl.size());

      QueryRequest qr = new QueryRequest(new SolrQuery("*:*").setRows(1111));
      qr.setResponseParser(new InputStreamResponseParser("cbor"));
      result = cluster.getSolrClient().request(qr, testCollection);
      InputStream is = (InputStream) result.get("stream");
      ObjectMapper objectMapper = new ObjectMapper(new CBORFactory());
      Object o = objectMapper.readValue(is, Object.class);
      List<Object> l = (List<Object>) Utils.getObjectByPath(o, false, "response/docs");
      assertEquals(1100, l.size());
    } finally {
      cluster.shutdown();
      System.clearProperty("managed.schema.mutable");
    }
  }

  @SuppressWarnings("unchecked")
  public void test() throws Exception {
    Path filmsJson = new File(ExternalPaths.SOURCE_HOME, "example/films/films.json").toPath();

    long sz = Files.size(filmsJson);
    assertEquals(633600, sz);

    List<Object> films = null;
    try (InputStream is = Files.newInputStream(filmsJson)) {
      films = (List<Object>) Utils.fromJSON(is);
    }
    ByteArrayOutputStream baos = new ByteArrayOutputStream();

    new JavaBinCodec().marshal(Map.of("films", films), baos);
    assertEquals(234520, baos.toByteArray().length);

    try (InputStream is = Files.newInputStream(filmsJson)) {
      byte[] bytes = readAsCbor(is);
      assertEquals(290672, bytes.length);
      LongAdder docsSz = new LongAdder();
      new CborStream(null, (document) -> docsSz.increment())
          .stream(new ByteArrayInputStream(bytes));
      assertEquals(films.size(), docsSz.intValue());
    }
  }

  private byte[] readAsCbor(InputStream is) throws IOException {
    ByteArrayOutputStream baos;
    ObjectMapper jsonMapper = new ObjectMapper(new JsonFactory());

    // Read JSON file as a JsonNode
    JsonNode jsonNode = jsonMapper.readTree(is);
    // Create a CBOR ObjectMapper
    CBORFactory jf = new CBORFactory();
    ObjectMapper cborMapper = new ObjectMapper(jf);
    baos = new ByteArrayOutputStream();
    JsonGenerator jsonGenerator = cborMapper.createGenerator(baos);

    jsonGenerator.writeTree(jsonNode);
    jsonGenerator.close();
    byte[] bytes = baos.toByteArray();
    return bytes;
  }
}
