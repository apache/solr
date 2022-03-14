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

import org.apache.commons.io.FileUtils;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.impl.NoOpResponseParser;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.lang.invoke.MethodHandles;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

/**
 * Tests Raw JSON output for fields when used with and without the unique key field.
 *
 * See SOLR-7993
 */
public class TestRawTransformer extends SolrCloudTestCase {

  private static final String DEBUG_LABEL = MethodHandles.lookup().lookupClass().getName();

  /** A basic client for operations at the cloud level, default collection will be set */
  private static JettySolrRunner JSR;
  private static HttpSolrClient CLIENT;

  @BeforeClass
  public static void beforeClass() throws Exception {
    if (random().nextBoolean()) {
      initStandalone();
      JSR.start();
      CLIENT = (HttpSolrClient) JSR.newClient();
    } else {
      initCloud();
      CLIENT = (HttpSolrClient) JSR.newClient();
      JSR = null;
    }
    initIndex();
  }

  private static void initStandalone() throws Exception {
    initCore("solrconfig-minimal.xml", "schema_latest.xml");
    File homeDir = createTempDir().toFile();
    final File collDir = new File(homeDir, "collection1");
    final File confDir = collDir.toPath().resolve("conf").toFile();
    confDir.mkdirs();
    FileUtils.copyFile(new File(SolrTestCaseJ4.TEST_HOME(), "solr.xml"), new File(homeDir, "solr.xml"));
    String src_dir = TEST_HOME() + "/collection1/conf";
    FileUtils.copyFile(new File(src_dir, "schema_latest.xml"),
            new File(confDir, "schema.xml"));
    FileUtils.copyFile(new File(src_dir, "solrconfig-minimal.xml"),
            new File(confDir, "solrconfig.xml"));
    for (String file : new String[] {"solrconfig.snippet.randomindexconfig.xml",
            "stopwords.txt", "synonyms.txt", "protwords.txt", "currency.xml"}) {
      FileUtils.copyFile(new File(src_dir, file), new File(confDir, file));
    }
    Files.createFile(collDir.toPath().resolve("core.properties"));
    Properties nodeProperties = new Properties();
    nodeProperties.setProperty("solr.data.dir", h.getCore().getDataDir());
    JSR = new JettySolrRunner(homeDir.getAbsolutePath(), nodeProperties, buildJettyConfig("/solr"));
  }

  private static void initCloud() throws Exception {
    final String configName = DEBUG_LABEL + "_config-set";
    final Path configDir = Paths.get(TEST_HOME(), "collection1", "conf");

    final int numNodes = 3;
    MiniSolrCloudCluster cloud = configureCluster(numNodes).addConfig(configName, configDir).configure();

    Map<String, String> collectionProperties = new LinkedHashMap<>();
    collectionProperties.put("config", "solrconfig-minimal.xml");
    collectionProperties.put("schema", "schema_latest.xml");
    CloudSolrClient cloudSolrClient = cloud.getSolrClient();
    CollectionAdminRequest.createCollection("collection1", configName, numNodes, 1)
            .setPerReplicaState(SolrCloudTestCase.USE_PER_REPLICA_STATE)
            .setProperties(collectionProperties)
            .process(cloudSolrClient);

    JSR = cloud.getRandomJetty(random());
  }

  @AfterClass
  private static void afterClass() throws Exception{
    if (JSR != null) {
      JSR.stop();
    }
    // NOTE: CLOUD_CLIENT should be stopped automatically in `SolrCloudTestCase.shutdownCluster()`
  }

  @After
  public void cleanup() throws Exception {
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
      sdoc.addField("subject", "{poffL:[{offL:[{oGUID:\"79D5A31D-B3E4-4667-B812-09DF4336B900\",oID:\"OO73XRX\",prmryO:1,oRank:1,addTp:\"Office\",addCd:\"AA4GJ5T\",ad1:\"102 S 3rd St Ste 100\",city:\"Carson City\",st:\"MI\",zip:\"48811\",lat:43.176885,lng:-84.842919,phL:[\"(989) 584-1308\"],faxL:[\"(989) 584-6453\"]}]}]}");
      sdoc.addField("author", "<root><child1>some</child1><child2>trivial</child2><child3>xml</child3></root>");
      // below are multiValued fields
      sdoc.addField("links", "{an_array:[1,2,3]}");
      sdoc.addField("links", "{an_array:[4,5,6]}");
      sdoc.addField("content_type", "<root>one</root>");
      sdoc.addField("content_type", "<root>two</root>");
      CLIENT.add("collection1", sdoc);
    }
    CLIENT.commit("collection1");
    assertEquals(MAX, CLIENT.query("collection1", new ModifiableSolrParams(Map.of("q", new String[]{"*:*"}))).getResults().getNumFound());
  }

  @Test
  public void testXmlTransformer() throws Exception {
    QueryRequest req = new QueryRequest(new ModifiableSolrParams(
            Map.of("q", new String[]{"*:*"}, "fl", new String[]{"author:[xml],content_type:[xml]"}, "wt", new String[]{"xml"})
    ));
    req.setResponseParser(XML_NOOP_RESPONSE_PARSER);
    String strResponse = (String) CLIENT.request(req,"collection1").get("response");
    assertTrue("response does not contain raw XML encoding: " + strResponse,
            strResponse.contains("<raw name=\"author\"><root><child1>some</child1><child2>trivial</child2><child3>xml</child3></root></raw>"));
    assertTrue("response (multiValued) does not contain raw XML encoding: " + strResponse,
            Pattern.compile("<arr name=\"content_type\">\\s*<raw><root>one</root></raw>\\s*<raw><root>two</root></raw>\\s*</arr>").matcher(strResponse).find());

    req = new QueryRequest(new ModifiableSolrParams(
            Map.of("q", new String[]{"*:*"}, "fl", new String[]{"author,content_type"}, "wt", new String[]{"xml"})
    ));
    req.setResponseParser(XML_NOOP_RESPONSE_PARSER);
    strResponse = (String) CLIENT.request(req, "collection1").get("response");
    assertTrue("response does not contain escaped XML encoding: " + strResponse,
            strResponse.contains("<str name=\"author\">&lt;root&gt;&lt;child1"));
    assertTrue("response (multiValued) does not contain escaped XML encoding: " + strResponse,
            Pattern.compile("<arr name=\"content_type\">\\s*<str>&lt;root&gt;").matcher(strResponse).find());

    req = new QueryRequest(new ModifiableSolrParams(
            Map.of("q", new String[]{"*:*"}, "fl", new String[]{"author:[xml],content_type:[xml]"}, "wt", new String[]{"json"})
    ));
    req.setResponseParser(JSON_NOOP_RESPONSE_PARSER);
    strResponse = (String) CLIENT.request(req, "collection1").get("response");
    assertTrue("unexpected serialization of XML field value in JSON response: " + strResponse,
            strResponse.contains("\"author\":\"<root><child1>some</child1>"));
    assertTrue("unexpected (multiValued) serialization of XML field value in JSON response: " + strResponse,
            strResponse.contains("\"content_type\":[\"<root>one</root>"));
  }

  @Test
  public void testJsonTransformer() throws Exception {
    QueryRequest req = new QueryRequest(new ModifiableSolrParams(
      Map.of("q", new String[]{"*:*"}, "fl", new String[]{"subject:[json],links:[json]"}, "wt", new String[]{"json"})
    ));
    req.setResponseParser(JSON_NOOP_RESPONSE_PARSER);
    String strResponse = (String) CLIENT.request(req,"collection1").get("response");
    assertTrue("response does not contain right JSON encoding: " + strResponse,
        strResponse.contains("\"subject\":{poffL:[{offL:[{oGUID:\"7"));
    assertTrue("response (multiValued) does not contain right JSON encoding: " + strResponse,
            Pattern.compile("\"links\":\\[\\{an_array:\\[1,2,3]},\\s*\\{an_array:\\[4,5,6]}]").matcher(strResponse).find());

    req = new QueryRequest(new ModifiableSolrParams(
      Map.of("q", new String[]{"*:*"}, "fl", new String[]{"id", "subject,links"}, "wt", new String[]{"json"})
    ));
    req.setResponseParser(JSON_NOOP_RESPONSE_PARSER);
    strResponse = (String) CLIENT.request(req, "collection1").get("response");
    assertTrue("response does not contain right JSON encoding: " + strResponse,
        strResponse.contains("subject\":\""));
    assertTrue("response (multiValued) does not contain right JSON encoding: " + strResponse,
            strResponse.contains("\"links\":[\""));
  }

  private static final NoOpResponseParser XML_NOOP_RESPONSE_PARSER = new NoOpResponseParser();
  private static final NoOpResponseParser JSON_NOOP_RESPONSE_PARSER = new NoOpResponseParser() {
    @Override
    public String getWriterType() {
      return "json";
    }
  };

}

