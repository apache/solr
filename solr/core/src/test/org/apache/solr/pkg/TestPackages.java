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

package org.apache.solr.pkg;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.Callable;

import org.apache.commons.codec.digest.DigestUtils;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.ResourceLoader;
import org.apache.lucene.util.ResourceLoaderAware;
import org.apache.solr.SolrTestUtil;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.BaseHttpSolrClient;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.client.solrj.request.RequestWriter;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.request.V2Request;
import org.apache.solr.client.solrj.request.beans.Package;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.MapWriterMap;
import org.apache.solr.common.NavigableObject;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.annotation.JsonProperty;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ReflectMapWriter;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.SolrCore;
import org.apache.solr.filestore.PackageStoreAPI;
import org.apache.solr.filestore.TestDistribPackageStore;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.search.QParser;
import org.apache.solr.search.QParserPlugin;
import org.apache.solr.util.LogLevel;
import org.apache.solr.util.plugin.SolrCoreAware;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import static org.apache.solr.SolrTestCaseJ4.randomizeNumericTypesProperties;
import static org.apache.solr.common.cloud.ZkStateReader.SOLR_PKGS_PATH;
import static org.apache.solr.common.params.CommonParams.JAVABIN;
import static org.apache.solr.common.params.CommonParams.WT;
import static org.apache.solr.core.TestSolrConfigHandler.getFileContent;
import static org.apache.solr.filestore.TestDistribPackageStore.readFile;
import static org.apache.solr.filestore.TestDistribPackageStore.uploadKey;
import static org.apache.solr.filestore.TestDistribPackageStore.waitForAllNodesHaveFile;
import static org.hamcrest.CoreMatchers.containsString;

@LogLevel("org.apache.solr.pkg.PackageLoader=DEBUG;org.apache.solr.pkg.PackageAPI=DEBUG")
//@org.apache.lucene.util.LuceneTestCase.AwaitsFix(bugUrl="https://issues.apache.org/jira/browse/SOLR-13822") // leaks files
@LuceneTestCase.Nightly
public class TestPackages extends SolrCloudTestCase {

  @BeforeClass
  public static void beforeTestPackages() throws Exception {
    useFactory(null);
    randomizeNumericTypesProperties();
  }

  @Before
  public void setup() {
    System.setProperty("enable.packages", "true");
  }
  
  @After
  public void teardown() {
    System.clearProperty("enable.packages");
  }
  public static class ConfigPlugin implements ReflectMapWriter {
    @JsonProperty
    public String name;

    @JsonProperty("class")
    public String klass;
  }
  @Test
  public void testPluginLoading() throws Exception {
    MiniSolrCloudCluster cluster =
        configureCluster(4)
            .withJettyConfig(jetty -> jetty.enableV2(true))
            .addConfig("conf", SolrTestUtil.configset("cloud-minimal"))
            .configure();
    try {
      String FILE1 = "/mypkg/runtimelibs.jar";
      String FILE2 = "/mypkg/runtimelibs_v2.jar";
      String FILE3 = "/mypkg/runtimelibs_v3.jar";
      String URP1 = "/mypkg/testurpv1.jar";
      String URP2 = "/mypkg/testurpv2.jar";
      String EXPR1 = "/mypkg/expressible.jar";
      String COLLECTION_NAME = "testPluginLoadingColl";
      byte[] derFile = readFile("cryptokeys/pub_key2048.der");
      uploadKey(derFile, PackageStoreAPI.KEYS_DIR+"/pub_key2048.der", cluster);
      postFileAndWait(cluster, "runtimecode/runtimelibs.jar.bin", FILE1,
          "HkyPD6HyDIjMuThtnILZPlyBfownzPkzlmignCPQjfpgitVWpf/sNAy33kzmeQP+F354IeTyNAvbBMiepzCbjlk6JLjCioDsmFRsROjYcZI/q0117NOortch37dQLKbRGiL7An1d6HtaXwCuLwsrfIPUJA+P8gkJ0H1gXSVJKhY6WWw3SqE5D2uwugy0e19GeZmbEffxSRw8Uar6kskm6WBjDYNlnIktQv9bal5o8n81SWgwBmul7MiVE6m0DE51KeVt/LreCMTGcOxjaFBMF427xCQH/xvbAc70uRi/foPNgKhHzS6f/ehQSIoAAi+Yo72HwSlKnawvgBhnt3IKYQ==");

      postFileAndWait(cluster, "runtimecode/testurp_v1.jar.bin", URP1,
          "MZsor+9M3g/gW8MSIUg9xrp7WV/X/rycQG9BCuD6gx8Q4uc7fPfA0eSaMYEj7Tr5jswbMGaysRRXu/bwZcVZBnwYRNnR1MDqgyjawyIX/zQ0LSkROf/1l8cEVSmq+FBkBcSKihrPssUWaR7Oh53liyX89RHJp7mj7+l7zmA0OM7uvnsp0uG5d2OXbCAd5hrb0YkSI9FRrf+mDpPdSeuEWeRl4E4/4mKgvAbmwn4HvzY5JeU/tIRGgTrEbcdqHx9ykmaifg7wbPbvyoPw73rafoKdeMpHNpTSinWDKmZXpgTqGKzsY+Bo4/RMT/8IElL2DlikP/2zgSQTWxhtX5hbYQ==");

      postFileAndWait(cluster, "runtimecode/expressible.jar.bin", EXPR1,
          "F5qbILCrTjIsUqdEfeEBPc2//O2Bnq8TgKChMwWesmU+yUA+NN/d+FcovjAw++IQ4zqTsPiepX4VD1su5QGTf8qAITMqUd7aVrjSKY2/Xhc78eOTMbkqmHjDsMJo2zM0Ncq6Fodvixm7VKZZDBH0YFTaQ/pbAciGR436VtVTozycHeEE+Epd+f7HkjLA5ojOplHmlurSg64xY7C9xIS/POKMpwIHLHoE1ZVOHHPIJVTXppxehDghIF01zo22ZFRHdDANP4hmRMUq+jd0g7eMVqU16G4d1NUhLZuO0Pw1gaWJwj7nwi1K8xevuWlSlLX1Y3Vw+4v6tLfGDa/iSONzLg==");

      Package.AddVersion add = new Package.AddVersion();
      add.version = "1.0";
      add.pkg = "mypkg";
      add.files = Arrays.asList(new String[]{FILE1, URP1, EXPR1});
      V2Request req = new V2Request.Builder("/cluster/package")
          .forceV2(true)
          .withMethod(SolrRequest.METHOD.POST)
          .withPayload(Collections.singletonMap("add", add))
          .build();

      req.process(cluster.getSolrClient());


      CollectionAdminRequest
          .createCollection(COLLECTION_NAME, "conf", 2, 2)
          .setMaxShardsPerNode(100)
          .process(cluster.getSolrClient());

      TestDistribPackageStore.assertResponseValues(10,
          () -> new V2Request.Builder("/cluster/package").
              withMethod(SolrRequest.METHOD.GET)
              .build().process(cluster.getSolrClient()),
          Utils.makeMap(
              ":result:packages:mypkg[0]:version", "1.0",
              ":result:packages:mypkg[0]:files[0]", FILE1
          ));
      Map<String,ConfigPlugin> plugins = new LinkedHashMap<>();
      ConfigPlugin p = new ConfigPlugin();
      p.klass = "mypkg:org.apache.solr.core.RuntimeLibReqHandler";
      p.name = "/runtime";
      plugins.put("create-requesthandler", p);

      p = new ConfigPlugin();
      p.klass = "mypkg:org.apache.solr.core.RuntimeLibSearchComponent";
      p.name = "get";
      plugins.put("create-searchcomponent", p);

      p = new ConfigPlugin();
      p.klass = "mypkg:org.apache.solr.core.RuntimeLibResponseWriter";
      p.name = "json1";
      plugins.put("create-queryResponseWriter", p);

      p = new ConfigPlugin();
      p.klass = "mypkg:org.apache.solr.update.TestVersionedURP";
      p.name = "myurp";
      plugins.put("create-updateProcessor", p);

      p = new ConfigPlugin();
      p.klass = "mypkg:org.apache.solr.client.solrj.io.stream.metrics.MinCopyMetric";
      p.name = "mincopy";
      plugins.put("create-expressible", p);


      V2Request v2r = new V2Request.Builder( "/c/"+COLLECTION_NAME+ "/config")
              .withMethod(SolrRequest.METHOD.POST)
              .withPayload(plugins)
              .forceV2(true)
              .build();
      cluster.getSolrClient().request(v2r);

      verifyCmponent(cluster.getSolrClient(),
          COLLECTION_NAME, "queryResponseWriter", "json1",
          "mypkg", "1.0" );

      verifyCmponent(cluster.getSolrClient(),
          COLLECTION_NAME, "searchComponent", "get",
          "mypkg", "1.0" );

      verifyCmponent(cluster.getSolrClient(),
          COLLECTION_NAME, "requestHandler", "/runtime",
          "mypkg", "1.0" );

      verifyCmponent(cluster.getSolrClient(),
          COLLECTION_NAME, "updateProcessor", "myurp",
          "mypkg", "1.0" );

      verifyCmponent(cluster.getSolrClient(),
          COLLECTION_NAME, "expressible", "mincopy",
          "mypkg", "1.0" );

      TestDistribPackageStore.assertResponseValues(10,
          cluster.getSolrClient() ,
          new GenericSolrRequest(SolrRequest.METHOD.GET,
              "/stream", new MapSolrParams((Map) Utils.makeMap("collection", COLLECTION_NAME,
                  WT, JAVABIN,
                  "action", "plugins"
                  ))), Utils.makeMap(
              ":plugins:mincopy", "org.apache.solr.client.solrj.io.stream.metrics.MinCopyMetric"
          ));

      UpdateRequest ur = new UpdateRequest();
      ur.add(new SolrInputDocument("id", "1"));
      ur.setParam("processor", "myurp");
      ur.process(cluster.getSolrClient(), COLLECTION_NAME);
      cluster.getSolrClient().commit(COLLECTION_NAME, true, true);

      QueryResponse result = cluster.getSolrClient()
          .query(COLLECTION_NAME, new SolrQuery( "id:1"));

      assertEquals("Version 1", result.getResults().get(0).getFieldValue("TestVersionedURP.Ver_s"));

      executeReq( "/" + COLLECTION_NAME + "/runtime?wt=javabin", cluster.getRandomJetty(random()),
          Utils.JAVABINCONSUMER,
          Utils.makeMap("class", "org.apache.solr.core.RuntimeLibReqHandler"));

      executeReq( "/" + COLLECTION_NAME + "/get?wt=json", cluster.getRandomJetty(random()),
          Utils.JSONCONSUMER,
          Utils.makeMap("class", "org.apache.solr.core.RuntimeLibSearchComponent",
              "Version","1"));


      executeReq( "/" + COLLECTION_NAME + "/runtime?wt=json1", cluster.getRandomJetty(random()),
          Utils.JSONCONSUMER,
          Utils.makeMap("wt", "org.apache.solr.core.RuntimeLibResponseWriter"));

      //now upload the second jar
      postFileAndWait(cluster, "runtimecode/runtimelibs_v2.jar.bin", FILE2,
          "YLIdPpBif2U2Y73TkNrz4wLKfYqGKkhiWPcDS6AqP69t9eXFHFx7lg+B5nrKViGKrjBHf0ISfL3eX9bORsq6HU3aLgMaSMOkIagBVSwk2bnJIlVmi484wtIJhR3ftT6TJSI8YMxl/g+6KyY9xMQuPD4zgKGMWTzyqxjDIgPMrqqnIabdgnQnGiq1WXpjOQSmW6g6SBMCli72gQqe8lmFwqxL9qWR4kUVb/9s6C4wB/a1QZz6Vp1OVoR0wjQhatZ1G++I4AhKPcQuWhqQj0ct6jhp2hZevBNRLPExoShq1A7BbkKToTbRuDDSPGXJA0FDISEsEA4WFUrpL2tIywaYEA==");

      postFileAndWait(cluster, "runtimecode/testurp_v2.jar.bin", URP2,
          "Zew5ZA5ffaaLDq5xNy2eoCcWh3PpYRTI4hVWONu7m9CbcszfAg7gUGrZzIm+IE9pI6W/qSnEFjCCc7XKp5W6x3IhfHqlzkqGjcTY69xbUb6OZhgJSc6n5JkSvhKdcRtnUuSaiKnx7vE9DdcB58rPIxAoZof7m9kygzZobyFQMBRSt40+I7gHzS2oly+aAP8oswOfeKijM2C41xTXxFtYBghhbbzimdp+5eV2upWPtapSEIopGKO1Yd0kTQXHl4OB/NWn5Vg60OidsIngUzfzC35HqhZS3MS0rxLjS3OHvFHG+2KxzR/AAv54ulsNiR7CQLolgUgycJyGCxeey/WDxQ==");
      //add the version using package API
      add.version = "1.1";
      add.files = Arrays.asList(new String[]{FILE2,URP2, EXPR1});
      req.process(cluster.getSolrClient());

      verifyCmponent(cluster.getSolrClient(),
          COLLECTION_NAME, "queryResponseWriter", "json1",
          "mypkg", "1.1" );

      verifyCmponent(cluster.getSolrClient(),
          COLLECTION_NAME, "searchComponent", "get",
          "mypkg", "1.1" );

      verifyCmponent(cluster.getSolrClient(),
          COLLECTION_NAME, "requestHandler", "/runtime",
          "mypkg", "1.1" );

      verifyCmponent(cluster.getSolrClient(),
          COLLECTION_NAME, "updateProcessor", "myurp",
          "mypkg", "1.1" );


      executeReq( "/" + COLLECTION_NAME + "/get?wt=json", cluster.getRandomJetty(random()),
          Utils.JSONCONSUMER,
          Utils.makeMap(  "Version","2"));


      //now upload the third jar
      postFileAndWait(cluster, "runtimecode/runtimelibs_v3.jar.bin", FILE3,
          "gtJ3CM/bLITapMU9huAC5crPYEyiR/pJxWWfhZB6/bma8Rvpcs9ljFDO741OEc702hHqWYsaIrYiHgkVVFrFirCtxLNeAqjsPftsY4exKxX2HZyajQYBxJJxCTJCTsak0v1Bst2vZAjPGXbZYIIGdJMBX/dbxgW2F34E7RI816F/cDjB2W1ktWESuK1liKo+//EW3OAeBz1qIX8srERpcjENAfORBw921qOdT4XaRq+AETJywRVDd67HKAfOXS5cPL4rRMGD3gSV3Gpa2PLiNO5tuhKDxnNG7sKVght//0yU7ypWL1jbLLU5CBXtj4Va7+BdqE1Epm2tgyCSeYv0NQ==");

      add.version = "2.1";
      add.files = Arrays.asList(new String[]{FILE3, URP2, EXPR1});
      req.process(cluster.getSolrClient());

      //now let's verify that the classes are updated
      verifyCmponent(cluster.getSolrClient(),
          COLLECTION_NAME, "queryResponseWriter", "json1",
          "mypkg", "2.1" );

      verifyCmponent(cluster.getSolrClient(),
          COLLECTION_NAME, "searchComponent", "get",
          "mypkg", "2.1" );

      verifyCmponent(cluster.getSolrClient(),
          COLLECTION_NAME, "requestHandler", "/runtime",
          "mypkg", "2.1" );

      executeReq( "/" + COLLECTION_NAME + "/runtime?wt=json", cluster.getRandomJetty(random()),
          Utils.JSONCONSUMER,
          Utils.makeMap("Version","2"));

      //insert a doc with urp
      ur = new UpdateRequest();
      ur.add(new SolrInputDocument("id", "2"));
      ur.setParam("processor", "myurp");
      ur.process(cluster.getSolrClient(), COLLECTION_NAME);
      cluster.getSolrClient().commit(COLLECTION_NAME, true, true);

      result = cluster.getSolrClient()
          .query(COLLECTION_NAME, new SolrQuery( "id:2"));

      assertEquals("Version 2", result.getResults().get(0).getFieldValue("TestVersionedURP.Ver_s"));


      Package.DelVersion delVersion = new Package.DelVersion();
      delVersion.pkg = "mypkg";
      delVersion.version = "1.0";
      V2Request delete = new V2Request.Builder("/cluster/package")
          .withMethod(SolrRequest.METHOD.POST)
          .forceV2(true)
          .withPayload(Collections.singletonMap("delete", delVersion))
          .build();
      delete.process(cluster.getSolrClient());

      verifyCmponent(cluster.getSolrClient(),
          COLLECTION_NAME, "queryResponseWriter", "json1",
          "mypkg", "2.1" );

      verifyCmponent(cluster.getSolrClient(),
          COLLECTION_NAME, "searchComponent", "get",
          "mypkg", "2.1" );

      verifyCmponent(cluster.getSolrClient(),
          COLLECTION_NAME, "requestHandler", "/runtime",
          "mypkg", "2.1" );

      // now remove the hughest version. So, it will roll back to the next highest one
      delVersion.version = "2.1";
      delete.process(cluster.getSolrClient());

      verifyCmponent(cluster.getSolrClient(),
          COLLECTION_NAME, "queryResponseWriter", "json1",
          "mypkg", "1.1" );

      verifyCmponent(cluster.getSolrClient(),
          COLLECTION_NAME, "searchComponent", "get",
          "mypkg", "1.1" );

      verifyCmponent(cluster.getSolrClient(),
          COLLECTION_NAME, "requestHandler", "/runtime",
          "mypkg", "1.1" );

      ModifiableSolrParams params = new ModifiableSolrParams();
      params.add("collection", COLLECTION_NAME);
      new GenericSolrRequest(SolrRequest.METHOD.POST, "/config/params", params ){
        @Override
        public RequestWriter.ContentWriter getContentWriter(String expectedType) {
          return new RequestWriter.StringPayloadContentWriter("{set:{PKG_VERSIONS:{mypkg : '1.1'}}}",
              ClientUtils.TEXT_JSON);
        }
      }.process(cluster.getSolrClient()) ;

      add.version = "2.1";
      add.files = Arrays.asList(new String[]{FILE3, URP2, EXPR1});
      req.process(cluster.getSolrClient());

      //the collections mypkg is set to use version 1.1
      //so no upgrade

      verifyCmponent(cluster.getSolrClient(),
          COLLECTION_NAME, "queryResponseWriter", "json1",
          "mypkg", "1.1" );

      verifyCmponent(cluster.getSolrClient(),
          COLLECTION_NAME, "searchComponent", "get",
          "mypkg", "1.1" );

      verifyCmponent(cluster.getSolrClient(),
          COLLECTION_NAME, "requestHandler", "/runtime",
          "mypkg", "1.1" );

      new GenericSolrRequest(SolrRequest.METHOD.POST, "/config/params", params ){
        @Override
        public RequestWriter.ContentWriter getContentWriter(String expectedType) {
          return new RequestWriter.StringPayloadContentWriter("{set:{PKG_VERSIONS:{mypkg : '2.1'}}}",
              ClientUtils.TEXT_JSON);
        }
      }.process(cluster.getSolrClient()) ;

      //now, let's force every collection using 'mypkg' to refresh
      //so that it uses version 2.1
      new V2Request.Builder("/cluster/package")
          .withMethod(SolrRequest.METHOD.POST)
          .withPayload("{refresh : mypkg}")
          .forceV2(true)
          .build()
          .process(cluster.getSolrClient());


      verifyCmponent(cluster.getSolrClient(),
          COLLECTION_NAME, "queryResponseWriter", "json1",
          "mypkg", "2.1" );

      verifyCmponent(cluster.getSolrClient(),
          COLLECTION_NAME, "searchComponent", "get",
          "mypkg", "2.1" );

      verifyCmponent(cluster.getSolrClient(),
          COLLECTION_NAME, "requestHandler", "/runtime",
          "mypkg", "2.1" );

      plugins.clear();
      p = new ConfigPlugin();
      p.name = "/rt_2";
      p.klass = "mypkg:"+ C.class.getName();
      plugins.put("create-requesthandler", p);

      p = new ConfigPlugin();
      p.name = "qp1";
      p.klass = "mypkg:"+ C2.class.getName();
      plugins.put("create-queryparser", p);

      v2r = new V2Request.Builder( "/c/"+COLLECTION_NAME+ "/config")
              .withMethod(SolrRequest.METHOD.POST)
              .withPayload(plugins)
              .forceV2(true)
              .build();
      cluster.getSolrClient().request(v2r);
      assertTrue(C.informCalled);
      assertTrue(C2.informCalled);

      //we create a new node. This node does not have the packages. But it should download it from another node
      JettySolrRunner jetty = cluster.startJettySolrRunner();
      //create a new replica for this collection. it should end up
      CollectionAdminRequest.addReplicaToShard(COLLECTION_NAME, "s1")
          .setNrtReplicas(1)
          .setNode(jetty.getNodeName())
          .process(cluster.getSolrClient());

      waitForAllNodesHaveFile(cluster,FILE3,
          Utils.makeMap(":files:" + FILE3 + ":name", "runtimelibs_v3.jar"),
          false);

    } finally {
      cluster.shutdown();
    }

  }

  private void executeReq(String uri, JettySolrRunner jetty, Utils.InputStreamConsumer parser, Map expected) throws Exception {
    try(Http2SolrClient client = (Http2SolrClient) jetty.newHttp2Client()){
      TestDistribPackageStore.assertResponseValues(10,
          () -> {
            Object o = Utils.executeGET(client,
                jetty.getBaseUrl() + uri, parser);
            if(o instanceof NavigableObject) return (NavigableObject) o;
            if(o instanceof Map) return new MapWriterMap((Map) o);
            throw new RuntimeException("Unknown response");
          }, expected);

    }
  }

  private void verifyCmponent(SolrClient client, String COLLECTION_NAME,
  String componentType, String componentName, String pkg, String version) throws Exception {
    SolrParams params = new MapSolrParams((Map) Utils.makeMap("collection", COLLECTION_NAME,
        WT, JAVABIN,
        "componentName", componentName,
        "meta", "true"));

    GenericSolrRequest req1 = new GenericSolrRequest(SolrRequest.METHOD.GET,
        "/config/" + componentType, params);
    TestDistribPackageStore.assertResponseValues(10,
        client,
        req1, Utils.makeMap(
            ":config:" + componentType + ":" + componentName + ":_packageinfo_:package", pkg,
            ":config:" + componentType + ":" + componentName + ":_packageinfo_:version", version
        ));
  }

  @Test
  public void testAPI() throws Exception {
    System.setProperty("enable.packages", "true");
    MiniSolrCloudCluster cluster =
        configureCluster(4)
            .withJettyConfig(jetty -> jetty.enableV2(true))
            .addConfig("conf", SolrTestUtil.configset("cloud-minimal"))
            .configure();
    try {
      String errPath = "/error/details[0]/errorMessages[0]";
      String FILE1 = "/mypkg/v.0.12/jar_a.jar";
      String FILE2 = "/mypkg/v.0.12/jar_b.jar";
      String FILE3 = "/mypkg/v.0.13/jar_a.jar";

      Package.AddVersion add = new Package.AddVersion();
      add.version = "0.12";
      add.pkg = "test_pkg";
      add.files = Arrays.asList(new String[]{FILE1, FILE2});
      V2Request req = new V2Request.Builder("/cluster/package")
          .forceV2(true)
          .withMethod(SolrRequest.METHOD.POST)
          .withPayload(Collections.singletonMap("add", add))
          .build();

      //the files is not yet there. The command should fail with error saying "No such file"
      expectError(req, cluster.getSolrClient(), errPath, "No such file:");


      //post the jar file. No signature is sent
      postFileAndWait(cluster, "runtimecode/runtimelibs.jar.bin", FILE1, null);


      add.files = Arrays.asList(new String[]{FILE1});
      expectError(req, cluster.getSolrClient(), errPath,
          FILE1 + " has no signature");
      //now we upload the keys
      byte[] derFile = readFile("cryptokeys/pub_key2048.der");
      uploadKey(derFile, PackageStoreAPI.KEYS_DIR+"/pub_key2048.der", cluster);
      //and upload the same file with a different name but it has proper signature
      postFileAndWait(cluster, "runtimecode/runtimelibs.jar.bin", FILE2,
          "HkyPD6HyDIjMuThtnILZPlyBfownzPkzlmignCPQjfpgitVWpf/sNAy33kzmeQP+F354IeTyNAvbBMiepzCbjlk6JLjCioDsmFRsROjYcZI/q0117NOortch37dQLKbRGiL7An1d6HtaXwCuLwsrfIPUJA+P8gkJ0H1gXSVJKhY6WWw3SqE5D2uwugy0e19GeZmbEffxSRw8Uar6kskm6WBjDYNlnIktQv9bal5o8n81SWgwBmul7MiVE6m0DE51KeVt/LreCMTGcOxjaFBMF427xCQH/xvbAc70uRi/foPNgKhHzS6f/ehQSIoAAi+Yo72HwSlKnawvgBhnt3IKYQ==");
      // with correct signature
      //after uploading the file, let's delete the keys to see if we get proper error message
      add.files = Arrays.asList(new String[]{FILE2});
      /*expectError(req, cluster.getSolrClient(), errPath,
          "ZooKeeper does not have any public keys");*/

      //Now lets' put the keys back

      //this time we have a file with proper signature, public keys are in ZK
      // so the add {} command should succeed
      req.process(cluster.getSolrClient());

      //Now verify the data in ZK
      TestDistribPackageStore.assertResponseValues(1,
          () -> new MapWriterMap((Map) Utils.fromJSON(cluster.getZkClient().getData(SOLR_PKGS_PATH,
              null, new Stat()))),
          Utils.makeMap(
              ":packages:test_pkg[0]:version", "0.12",
              ":packages:test_pkg[0]:files[0]", FILE1
          ));

      //post a new jar with a proper signature
      postFileAndWait(cluster, "runtimecode/runtimelibs_v2.jar.bin", FILE3,
          "YLIdPpBif2U2Y73TkNrz4wLKfYqGKkhiWPcDS6AqP69t9eXFHFx7lg+B5nrKViGKrjBHf0ISfL3eX9bORsq6HU3aLgMaSMOkIagBVSwk2bnJIlVmi484wtIJhR3ftT6TJSI8YMxl/g+6KyY9xMQuPD4zgKGMWTzyqxjDIgPMrqqnIabdgnQnGiq1WXpjOQSmW6g6SBMCli72gQqe8lmFwqxL9qWR4kUVb/9s6C4wB/a1QZz6Vp1OVoR0wjQhatZ1G++I4AhKPcQuWhqQj0ct6jhp2hZevBNRLPExoShq1A7BbkKToTbRuDDSPGXJA0FDISEsEA4WFUrpL2tIywaYEA==");


      //this time we are adding the second version of the package (0.13)
      add.version = "0.13";
      add.pkg = "test_pkg";
      add.files = Arrays.asList(new String[]{FILE3});

      //this request should succeed
      req.process(cluster.getSolrClient());
      //no verify the data (/packages.json) in ZK
      TestDistribPackageStore.assertResponseValues(1,
          () -> new MapWriterMap((Map) Utils.fromJSON(cluster.getZkClient().getData(SOLR_PKGS_PATH,
              null, new Stat()))),
          Utils.makeMap(
              ":packages:test_pkg[1]:version", "0.13",
              ":packages:test_pkg[1]:files[0]", FILE3
          ));

      //Now we will just delete one version
      Package.DelVersion delVersion = new Package.DelVersion();
      delVersion.version = "0.1";//this version does not exist
      delVersion.pkg = "test_pkg";
      req = new V2Request.Builder("/cluster/package")
          .forceV2(true)
          .withMethod(SolrRequest.METHOD.POST)
          .withPayload(Collections.singletonMap("delete", delVersion))
          .build();

      //we are expecting an error
      expectError(req, cluster.getSolrClient(), errPath, "No such version:");

      delVersion.version = "0.12";//correct version. Should succeed
      req.process(cluster.getSolrClient());
      //Verify with ZK that the data is correcy
      TestDistribPackageStore.assertResponseValues(1,
          () -> new MapWriterMap((Map) Utils.fromJSON(cluster.getZkClient().getData(SOLR_PKGS_PATH,
              null, new Stat()))),
          Utils.makeMap(
              ":packages:test_pkg[0]:version", "0.13",
              ":packages:test_pkg[0]:files[0]", FILE2
          ));


      //So far we have been verifying the details with  ZK directly
      //use the package read API to verify with each node that it has the correct data
      for (JettySolrRunner jetty : cluster.getJettySolrRunners()) {
        String path = jetty.getBaseUrl().toString().replace("/solr", "/api") + "/cluster/package?wt=javabin";
        TestDistribPackageStore.assertResponseValues(10, new Callable<NavigableObject>() {
          @Override
          public NavigableObject call() throws Exception {
            try (Http2SolrClient solrClient = (Http2SolrClient) jetty.newHttp2Client()) {
              return (NavigableObject) Utils.executeGET(solrClient, path, Utils.JAVABINCONSUMER);
            }
          }
        }, Utils.makeMap(
            ":result:packages:test_pkg[0]:version", "0.13",
            ":result:packages:test_pkg[0]:files[0]", FILE3
        ));
      }
    } finally {
      cluster.shutdown();
    }
  }
  public static class C extends RequestHandlerBase implements SolrCoreAware   {
    static boolean informCalled = false;

    @Override
    public void inform(SolrCore core) {
      informCalled = true;

    }

    @Override
    public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) {

    }

    @Override
    public String getDescription() {
      return "test";
    }
  }

  public static class C2 extends QParserPlugin implements ResourceLoaderAware {
    static boolean informCalled = false;


    @Override
    public void inform(ResourceLoader loader) throws IOException {
      informCalled = true;

    }

    @Override
    public QParser createParser(String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
      return null;
    }
  }

  static void postFileAndWait(MiniSolrCloudCluster cluster, String fname, String path, String sig) throws Exception {
    ByteBuffer fileContent = getFileContent(fname);
    String sha512 = DigestUtils.sha512Hex(fileContent.array());

    TestDistribPackageStore.postFile(cluster.getSolrClient(),
        fileContent,
        path, sig);// has file, but no signature

    TestDistribPackageStore.waitForAllNodesHaveFile(cluster, path, Utils.makeMap(
        ":files:" + path + ":sha512",
        sha512
    ), false);
  }

  private void expectError(V2Request req, SolrClient client, String errPath, String expectErrorMsg) throws IOException, SolrServerException {
    try {
      req.process(client);
      fail("should have failed with message : " + expectErrorMsg);
    } catch (BaseHttpSolrClient.RemoteExecutionException e) {
      String msg = e.getMetaData()._getStr(errPath, "");
      assertThat(msg, containsString(expectErrorMsg));
    }
  }
}
