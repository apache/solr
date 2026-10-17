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

import java.lang.invoke.MethodHandles;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.response.InputStreamResponseParser;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.Utils;
import org.apache.solr.embedded.JettyConfig;
import org.apache.solr.embedded.JettySolrRunner;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests Raw JSON output for fields when used with and without the unique key field.
 *
 * <p>See SOLR-7993
 */
public class TestRawTransformer extends SolrCloudTestCase {

  private static final String DEBUG_LABEL = MethodHandles.lookup().lookupClass().getName();

  /** A basic client for operations at the cloud level, default collection will be set */
  private static JettySolrRunner JSR;

  private static SolrClient CLIENT;

  @BeforeClass
  public static void beforeClass() throws Exception {
    if (random().nextBoolean()) {
      initStandalone();
      JSR.start();
      if (random().nextBoolean()) {
        CLIENT = JSR.getSolrClient();
      } else {
        CLIENT = new EmbeddedSolrServer(JSR.getCoreContainer(), null);
      }
    } else {
      initCloud();
      CLIENT = JSR.getSolrClient();
      JSR = null;
    }
    initIndex();
  }

  private static void initStandalone() throws Exception {
    initCore("solrconfig-minimal.xml", "schema_latest.xml");
    Path homeDir = createTempDir();
    final Path collDir = homeDir.resolve("collection1");
    final Path confDir = collDir.resolve("conf");
    Files.createDirectories(confDir);
    Path srcDir = TEST_HOME().resolve("collection1").resolve("conf");
    Files.writeString(
        confDir.resolve("schema.xml"), schemaWithJsonField(srcDir.resolve("schema_latest.xml")));
    Files.copy(srcDir.resolve("solrconfig-minimal.xml"), confDir.resolve("solrconfig.xml"));
    for (String file :
        new String[] {
          "solrconfig.snippet.randomindexconfig.xml",
          "stopwords.txt",
          "synonyms.txt",
          "protwords.txt",
          "currency.xml",
          "enumsConfig.xml"
        }) {
      Files.copy(srcDir.resolve(file), confDir.resolve(file));
    }
    Files.createFile(collDir.resolve("core.properties"));
    JSR =
        new JettySolrRunner(
            homeDir.toAbsolutePath().toString(), new Properties(), JettyConfig.builder().build());
  }

  private static void initCloud() throws Exception {
    final String configName = DEBUG_LABEL + "_config-set";
    final Path configDir = copyConfWithJsonField();

    final int numNodes = 3;
    MiniSolrCloudCluster cloud =
        configureCluster(numNodes).addConfig(configName, configDir).configure();

    Map<String, String> collectionProperties = new LinkedHashMap<>();
    collectionProperties.put("config", "solrconfig-minimal.xml");
    collectionProperties.put("schema", "schema_latest.xml");
    CloudSolrClient cloudSolrClient = cloud.getSolrClient();
    CollectionAdminRequest.createCollection("collection1", configName, numNodes, 1)
        .setProperties(collectionProperties)
        .process(cloudSolrClient);

    JSR = cloud.getRandomJetty(random());
  }

  /**
   * The fixture schema defines no field whose name ends in {@code _json}, so a document field such
   * as {@code payload_json} cannot be indexed against it. Returns the text of the given schema with
   * one such dynamic field added. Only the copies of the schema this test runs against are
   * extended; the checked-in fixture is left as it is.
   */
  private static String schemaWithJsonField(Path schema) throws Exception {
    String text = Files.readString(schema);
    String patched =
        text.replace(
            "</schema>",
            "  <dynamicField name=\"*_json\" type=\"string\" indexed=\"true\" stored=\"true\""
                + " multiValued=\"true\"/>\n</schema>");
    assertFalse("the fixture schema has no </schema> to extend", patched.equals(text));
    return patched;
  }

  /** Copies the collection1 conf dir, extending its schema as {@link #schemaWithJsonField}. */
  private static Path copyConfWithJsonField() throws Exception {
    Path srcDir = TEST_HOME().resolve("collection1").resolve("conf");
    Path destDir = createTempDir().resolve("conf");
    try (Stream<Path> paths = Files.walk(srcDir)) {
      for (Path src : paths.toList()) {
        // resolve through a string: destDir is on the test filesystem, srcDir is not
        Path dest = destDir.resolve(srcDir.relativize(src).toString());
        if (Files.isDirectory(src)) {
          Files.createDirectories(dest);
        } else if (src.getFileName().toString().equals("schema_latest.xml")) {
          Files.writeString(dest, schemaWithJsonField(src));
        } else {
          Files.copy(src, dest);
        }
      }
    }
    return destDir;
  }

  @AfterClass
  public static void afterClass() throws Exception {
    if (CLIENT != null) {
      IOUtils.closeQuietly(CLIENT);
      CLIENT = null;
    }
    if (JSR != null) {
      JSR.stop();
      JSR = null;
    }
  }

  @After
  public void cleanup() {
    if (JSR != null) {
      assertU(delQ("*:*"));
      assertU(commit());
    }
  }

  private static final int MAX = 10;

  private static void initIndex() throws Exception {
    // Build a simple index
    // TODO: why are we indexing 10 docs here? Wouldn't one suffice?
    for (int i = 0; i < MAX; i++) {
      SolrInputDocument sdoc = new SolrInputDocument();
      sdoc.addField("id", i);
      // below are single-valued fields
      sdoc.addField(
          "subject",
          "{poffL:[{offL:[{oGUID:\"79D5A31D-B3E4-4667-B812-09DF4336B900\",oID:\"OO73XRX\",prmryO:1,oRank:1,addTp:\"Office\",addCd:\"AA4GJ5T\",ad1:\"102 S 3rd St Ste 100\",city:\"Carson City\",st:\"MI\",zip:\"48811\",lat:43.176885,lng:-84.842919,phL:[\"(989) 584-1308\"],faxL:[\"(989) 584-6453\"]}]}]}");
      sdoc.addField(
          "author",
          "<root><child1>some</child1><child2>trivial</child2><child3>xml</child3></root>");
      // below are multiValued fields
      sdoc.addField("links", "{an_array:[1,2,3]}");
      sdoc.addField("links", "{an_array:[4,5,6]}");
      sdoc.addField("content_type", "<root>one</root>");
      sdoc.addField("content_type", "<root>two</root>");
      // a field whose name ends in _json, matched by the *_json glob in
      // testSuffixGlobJsonTransformer; its dynamic field is added by schemaWithJsonField
      sdoc.addField("payload_json", "{\"seq\":[1,2]}");
      sdoc.addField("payload_json", "{\"seq\":[3,4]}");
      CLIENT.add("collection1", sdoc);
    }
    CLIENT.commit("collection1");
    assertEquals(
        MAX,
        CLIENT
            .query("collection1", new ModifiableSolrParams(Map.of("q", new String[] {"*:*"})))
            .getResults()
            .getNumFound());
  }

  @Test
  public void testXmlTransformer() throws Exception {
    QueryRequest req =
        new QueryRequest(
            new ModifiableSolrParams(
                Map.of(
                    "q",
                    new String[] {"*:*"},
                    "fl",
                    new String[] {"author:[xml],content_type:[xml]"},
                    "wt",
                    new String[] {"xml"},
                    "indent",
                    new String[] {"false"})));
    req.setResponseParser(XML_STREAM_RESPONSE_PARSER);
    NamedList<Object> rsp = CLIENT.request(req, "collection1");
    String strResponse = InputStreamResponseParser.consumeResponseToString(rsp);

    assertTrue(
        "response does not contain raw XML encoding: " + strResponse,
        strResponse.contains(
            "<raw name=\"author\"><root><child1>some</child1><child2>trivial</child2><child3>xml</child3></root></raw>"));
    assertTrue(
        "response (multiValued) does not contain raw XML encoding: " + strResponse,
        Pattern.compile(
                "<arr name=\"content_type\">\\s*<raw><root>one</root></raw>\\s*<raw><root>two</root></raw>\\s*</arr>")
            .matcher(strResponse)
            .find());

    req =
        new QueryRequest(
            new ModifiableSolrParams(
                Map.of(
                    "q",
                    new String[] {"*:*"},
                    "fl",
                    new String[] {"author,content_type"},
                    "wt",
                    new String[] {"xml"},
                    "indent",
                    new String[] {"false"})));
    req.setResponseParser(XML_STREAM_RESPONSE_PARSER);
    rsp = CLIENT.request(req, "collection1");
    strResponse = InputStreamResponseParser.consumeResponseToString(rsp);

    assertTrue(
        "response does not contain escaped XML encoding: " + strResponse,
        strResponse.contains("<str name=\"author\">&lt;root&gt;&lt;child1"));
    assertTrue(
        "response (multiValued) does not contain escaped XML encoding: " + strResponse,
        Pattern.compile("<arr name=\"content_type\">\\s*<str>&lt;root&gt;")
            .matcher(strResponse)
            .find());

    req =
        new QueryRequest(
            new ModifiableSolrParams(
                Map.of(
                    "q",
                    new String[] {"*:*"},
                    "fl",
                    new String[] {"author:[xml],content_type:[xml]"},
                    "wt",
                    new String[] {"json"},
                    "indent",
                    new String[] {"false"})));
    req.setResponseParser(JSON_STREAM_RESPONSE_PARSER);
    rsp = CLIENT.request(req, "collection1");
    strResponse = InputStreamResponseParser.consumeResponseToString(rsp);

    assertTrue(
        "unexpected serialization of XML field value in JSON response: " + strResponse,
        strResponse.contains("\"author\":\"<root><child1>some</child1>"));
    assertTrue(
        "unexpected (multiValued) serialization of XML field value in JSON response: "
            + strResponse,
        strResponse.contains("\"content_type\":[\"<root>one</root>"));
  }

  @Test
  public void testJsonTransformer() throws Exception {
    QueryRequest req =
        new QueryRequest(
            new ModifiableSolrParams(
                Map.of(
                    "q",
                    new String[] {"*:*"},
                    "fl",
                    new String[] {"subject:[json],links:[json]"},
                    "wt",
                    new String[] {"json"},
                    "indent",
                    new String[] {"false"})));
    req.setResponseParser(JSON_STREAM_RESPONSE_PARSER);
    NamedList<Object> rsp = CLIENT.request(req, "collection1");
    String strResponse = InputStreamResponseParser.consumeResponseToString(rsp);

    assertTrue(
        "response does not contain right JSON encoding: " + strResponse,
        strResponse.contains("\"subject\":{poffL:[{offL:[{oGUID:\"7"));
    assertTrue(
        "response (multiValued) does not contain right JSON encoding: " + strResponse,
        Pattern.compile("\"links\":\\[\\{an_array:\\[1,2,3]},\\s*\\{an_array:\\[4,5,6]}]")
            .matcher(strResponse)
            .find());

    req =
        new QueryRequest(
            new ModifiableSolrParams(
                Map.of(
                    "q",
                    new String[] {"*:*"},
                    "fl",
                    new String[] {"id", "subject,links"},
                    "wt",
                    new String[] {"json"},
                    "indent",
                    new String[] {"false"})));
    req.setResponseParser(JSON_STREAM_RESPONSE_PARSER);
    rsp = CLIENT.request(req, "collection1");
    strResponse = InputStreamResponseParser.consumeResponseToString(rsp);

    assertTrue(
        "response does not contain right JSON encoding: " + strResponse,
        strResponse.contains("subject\":\""));
    assertTrue(
        "response (multiValued) does not contain right JSON encoding: " + strResponse,
        strResponse.contains("\"links\":[\""));
  }

  @Test
  public void testGlobJsonTransformer() throws Exception {
    QueryRequest req =
        new QueryRequest(
            new ModifiableSolrParams(
                Map.of(
                    "q",
                    new String[] {"*:*"},
                    "fl",
                    new String[] {"id,su*:[json],link?:[json]"},
                    "wt",
                    new String[] {"json"},
                    "indent",
                    new String[] {"false"})));
    req.setResponseParser(JSON_STREAM_RESPONSE_PARSER);
    NamedList<Object> rsp = CLIENT.request(req, "collection1");
    String strResponse = InputStreamResponseParser.consumeResponseToString(rsp);

    assertTrue(
        "glob did not write the single valued field raw: " + strResponse,
        strResponse.contains("\"subject\":{poffL:[{offL:[{oGUID:\"7"));
    assertTrue(
        "glob did not write the multiValued field raw: " + strResponse,
        Pattern.compile("\"links\":\\[\\{an_array:\\[1,2,3]},\\s*\\{an_array:\\[4,5,6]}]")
            .matcher(strResponse)
            .find());
    assertFalse(
        "field not matching the globs was returned: " + strResponse,
        strResponse.contains("\"author\""));

    // [xml] does not apply to a json response: the matching fields are returned, not raw
    req =
        new QueryRequest(
            new ModifiableSolrParams(
                Map.of(
                    "q",
                    new String[] {"*:*"},
                    "fl",
                    new String[] {"au*:[xml]"},
                    "wt",
                    new String[] {"json"},
                    "indent",
                    new String[] {"false"})));
    req.setResponseParser(JSON_STREAM_RESPONSE_PARSER);
    rsp = CLIENT.request(req, "collection1");
    strResponse = InputStreamResponseParser.consumeResponseToString(rsp);
    assertTrue(
        "unexpected serialization of XML field value in JSON response: " + strResponse,
        strResponse.contains("\"author\":\"<root><child1>some</child1>"));

    // a glob cannot be combined with a transformer that is not a raw value transformer
    req =
        new QueryRequest(
            new ModifiableSolrParams(
                Map.of("q", new String[] {"*:*"}, "fl", new String[] {"su*:[docid]"})));
    QueryRequest badReq = req;
    SolrException e =
        expectThrows(SolrException.class, () -> CLIENT.request(badReq, "collection1"));
    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, e.code());
  }

  @Test
  public void testSuffixGlobJsonTransformer() throws Exception {
    // *_json:[json] writes every field whose name ends in _json raw, here payload_json
    QueryRequest req =
        new QueryRequest(
            new ModifiableSolrParams(
                Map.of(
                    "q",
                    new String[] {"*:*"},
                    "fl",
                    new String[] {"id,*_json:[json]"},
                    "wt",
                    new String[] {"json"},
                    "indent",
                    new String[] {"false"})));
    req.setResponseParser(JSON_STREAM_RESPONSE_PARSER);
    NamedList<Object> rsp = CLIENT.request(req, "collection1");
    String strResponse = InputStreamResponseParser.consumeResponseToString(rsp);

    Object parsed = Utils.fromJSONString(strResponse);
    List<?> docs = (List<?>) ((Map<?, ?>) ((Map<?, ?>) parsed).get("response")).get("docs");
    int withPayload = 0;
    for (Object doc : docs) {
      Map<?, ?> fields = (Map<?, ?>) doc;
      if (fields.containsKey("payload_json")) {
        withPayload++;
        assertEquals(
            "payload_json was not written raw as a JSON array: " + strResponse,
            "[{seq=[1, 2]}, {seq=[3, 4]}]",
            String.valueOf(fields.get("payload_json")));
      }
    }
    assertTrue("no payload_json field was returned: " + strResponse, withPayload > 0);
    assertFalse(
        "field not matching the glob was returned: " + strResponse,
        strResponse.contains("\"subject\""));

    QueryRequest starReq =
        new QueryRequest(
            new ModifiableSolrParams(
                Map.of("q", new String[] {"*:*"}, "fl", new String[] {"*:[json]"})));
    SolrException e =
        expectThrows(SolrException.class, () -> CLIENT.request(starReq, "collection1"));
    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, e.code());
  }

  private static final InputStreamResponseParser XML_STREAM_RESPONSE_PARSER =
      new InputStreamResponseParser("xml");
  private static final InputStreamResponseParser JSON_STREAM_RESPONSE_PARSER =
      new InputStreamResponseParser("json");
}
