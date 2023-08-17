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
import com.fasterxml.jackson.dataformat.cbor.CBORGenerator;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.InputStreamResponseParser;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.RequestWriter;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.util.JavaBinCodec;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.Utils;
import org.apache.solr.handler.loader.CborLoader;
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
      CloudSolrClient client = cluster.getSolrClient();
      CollectionAdminRequest.createCollection(testCollection, "conf", 1, 1).process(client);
      modifySchema(testCollection, client);

      byte[] b =
          Files.readAllBytes(
              new File(ExternalPaths.SOURCE_HOME, "example/films/films.json").toPath());
      // every operation is performed twice. We should only take the second number
      // so that we give JVM a chance to optimize that code
      index(testCollection, client, createJsonReq(b), true);
      index(testCollection, client, createJsonReq(b), true);

      index(testCollection, client, createJavabinReq(b), true);
      index(testCollection, client, createJavabinReq(b), true);

      index(testCollection, client, createCborReq(b), true);
      index(testCollection, client, createCborReq(b), false);

      runQuery(testCollection, client, "javabin");
      runQuery(testCollection, client, "javabin");
      runQuery(testCollection, client, "json");
      runQuery(testCollection, client, "json");
      b = runQuery(testCollection, client, "cbor");
      int compactSz = b.length;
      b = runQuery(testCollection, client, "cbor-noncompact");
      assertTrue(compactSz < b.length);
      b = runQuery(testCollection, client, "cbor");
      ObjectMapper objectMapper = new ObjectMapper(new CBORFactory());
      Object o = objectMapper.readValue(b, Object.class);
      List<Object> l = (List<Object>) Utils.getObjectByPath(o, false, "response/docs");
      assertEquals(1100, l.size());
      client.deleteByQuery(testCollection, "*:*").getStatus();
      index(
          testCollection,
          client,
          createCborReq(getNestedDocs().getBytes(StandardCharsets.UTF_8)),
          false);
      SolrQuery q = new SolrQuery("*:*").setRows(100);
      QueryResponse result = new QueryRequest(q).process(client, testCollection);
      assertEquals(6, result.getResults().size());
    } finally {
      System.clearProperty("managed.schema.mutable");
      cluster.shutdown();
    }
  }

  private void index(
      String testCollection, CloudSolrClient client, GenericSolrRequest r, boolean del)
      throws Exception {
    RTimer timer = new RTimer();
    client.request(r, testCollection);
    System.out.println("INDEX_TIME: " + r.contentWriter.getContentType() + " : " + timer.getTime());
    if (del) {
      UpdateRequest req = new UpdateRequest().deleteByQuery("*:*");
      req.setParam("commit", "true");
      client.request(req, testCollection);
    }
  }

  private byte[] runQuery(String testCollection, CloudSolrClient client, String wt)
      throws SolrServerException, IOException {
    NamedList<Object> result;
    QueryRequest request;
    RTimer timer = new RTimer();
    SolrQuery q = new SolrQuery("*:*").setRows(1111);
    request = new QueryRequest(q);
    if (wt.equals("cbor-noncompact")) {
      q.set("string_ref", false);
      request.setResponseParser(new InputStreamResponseParser("cbor"));
    } else {
      request.setResponseParser(new InputStreamResponseParser(wt));
    }
    result = client.request(request, testCollection);
    byte[] b = copyStream((InputStream) result.get("stream"));
    System.out.println(wt + "_time : " + timer.getTime());
    System.out.println(wt + "_size : " + b.length);
    return b;
  }

  private static byte[] copyStream(InputStream inputStream) throws IOException {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    byte[] buffer = new byte[4096];
    int bytesRead;
    while ((bytesRead = inputStream.read(buffer)) != -1) {
      outputStream.write(buffer, 0, bytesRead);
    }
    return outputStream.toByteArray();
  }

  private void modifySchema(String testCollection, CloudSolrClient client)
      throws SolrServerException, IOException {
    GenericSolrRequest req =
        new GenericSolrRequest(SolrRequest.METHOD.POST, "/schema")
            .setContentWriter(
                new RequestWriter.StringPayloadContentWriter(
                    "{\n"
                        + "\"add-field-type\" : {"
                        + "\"name\":\"knn_vector_10\",\"class\":\"solr.DenseVectorField\",\"vectorDimension\":10,\"similarityFunction\":\"cosine\",\"knnAlgorithm\":\"hnsw\"},\n"
                        + "\"add-field\" : ["
                        + "{\"name\":\"name\",\"type\":\"sortabletext\",\"multiValued\":false,\"stored\":true},\n"
                        + "{\"name\":\"initial_release_date\",\"type\":\"string\",\"stored\":true},\n"
                        + "{\"name\":\"directed_by\",\"type\":\"string\",\"multiValued\":true,\"stored\":true},\n"
                        + "{\"name\":\"genre\",\"type\":\"string\",\"multiValued\":true,\"stored\":true},\n"
                        + "{\"name\":\"film_vector\",\"type\":\"knn_vector_10\",\"indexed\":true,\"stored\":true}]}",
                    XMLResponseWriter.CONTENT_TYPE_XML_UTF8));

    client.request(req, testCollection);
  }

  private GenericSolrRequest createJsonReq(byte[] b) {
    return new GenericSolrRequest(
            SolrRequest.METHOD.POST,
            "/update/json/docs",
            new MapSolrParams(Map.of("commit", "true")))
        .withContent(b, "application/json");
  }

  @SuppressWarnings("rawtypes")
  private GenericSolrRequest createJavabinReq(byte[] b) throws IOException {
    List l = (List) Utils.fromJSON(b);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    new JavaBinCodec().marshal(l.iterator(), baos);

    return new GenericSolrRequest(
            SolrRequest.METHOD.POST, "/update", new MapSolrParams(Map.of("commit", "true")))
        .withContent(baos.toByteArray(), "application/javabin");
  }

  private GenericSolrRequest createCborReq(byte[] b) throws IOException {
    return new GenericSolrRequest(
            SolrRequest.METHOD.POST, "/update/cbor", new MapSolrParams(Map.of("commit", "true")))
        .withContent(serializeToCbor(b), "application/cbor");
  }

  @SuppressWarnings("unchecked")
  public void test() throws Exception {
    Path filmsJson = new File(ExternalPaths.SOURCE_HOME, "example/films/films.json").toPath();

    List<Object> films = null;
    try (InputStream is = Files.newInputStream(filmsJson)) {
      films = (List<Object>) Utils.fromJSON(is);
    }
    ByteArrayOutputStream baos = new ByteArrayOutputStream();

    new JavaBinCodec().marshal(Map.of("films", films), baos);
    assertEquals(234520, baos.toByteArray().length);

    byte[] b = Files.readAllBytes(filmsJson);
    byte[] bytes = serializeToCbor(b);
    assertEquals(210439, bytes.length);
    LongAdder docsSz = new LongAdder();
    new CborLoader(null, (document) -> docsSz.increment()).stream(new ByteArrayInputStream(bytes));
    assertEquals(films.size(), docsSz.intValue());
  }

  private byte[] serializeToCbor(byte[] is) throws IOException {
    ByteArrayOutputStream baos;
    ObjectMapper jsonMapper = new ObjectMapper(new JsonFactory());

    // Read JSON file as a JsonNode
    JsonNode jsonNode = jsonMapper.readTree(is);
    // Create a CBOR ObjectMapper
    baos = new ByteArrayOutputStream();

    ObjectMapper cborMapper =
        new ObjectMapper(CBORFactory.builder().enable(CBORGenerator.Feature.STRINGREF).build());
    JsonGenerator jsonGenerator = cborMapper.createGenerator(baos);

    jsonGenerator.writeTree(jsonNode);
    jsonGenerator.close();
    return baos.toByteArray();
  }

  private String getNestedDocs() throws IOException {
    return "[{ \"id\": \"P11!prod\",\n"
        + "   \"name_s\": \"Swingline Stapler\",\n"
        + "   \"type_s\": \"PRODUCT\",\n"
        + "   \"description_s\": \"The Cadillac of office staplers ...\",\n"
        + "   \"skus\": [\n"
        + "       { \"id\": \"P11!S21\",\n"
        + "         \"type_s\": \"SKU\",\n"
        + "         \"color_s\": \"RED\",\n"
        + "         \"price_i\": 42,\n"
        + "         \"manuals\": [\n"
        + "             { \"id\": \"P11!D41\",\n"
        + "               \"type_s\": \"MANUAL\",\n"
        + "               \"name_s\": \"Red Swingline Brochure\",\n"
        + "               \"pages_i\":1,\n"
        + "               \"content_s\": \"...\"\n"
        + "             } ]\n"
        + "       },\n"
        + "       { \"id\": \"P11!S31\",\n"
        + "         \"type_s\": \"SKU\",\n"
        + "         \"color_s\": \"BLACK\",\n"
        + "         \"price_i\": 3\n"
        + "       },\n"
        + "       { \"id\": \"P11!D51\",\n"
        + "         \"type_s\": \"MANUAL\",\n"
        + "         \"name_s\": \"Quick Reference Guide\",\n"
        + "         \"pages_i\":1,\n"
        + "         \"content_s\": \"How to use your stapler ...\"\n"
        + "       },\n"
        + "       { \"id\": \"P11!D61\",\n"
        + "         \"type_s\": \"MANUAL\",\n"
        + "         \"name_s\": \"Warranty Details\",\n"
        + "         \"pages_i\":42,\n"
        + "         \"content_s\": \"... lifetime guarantee ...\"\n"
        + "       }\n"
        + "    ]\n"
        + "} ]";
  }
}
