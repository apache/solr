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

package org.apache.solr.cli;

import static org.apache.solr.cli.SolrCLI.findTool;
import static org.apache.solr.cli.SolrCLI.parseCmdLine;

import java.io.File;
import java.io.FileWriter;
import org.apache.commons.cli.CommandLine;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.client.solrj.request.AbstractUpdateRequest;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.util.SecurityJson;
import org.junit.BeforeClass;
import org.junit.Test;

public class StreamToolBasicAuthTest extends SolrCloudTestCase {

  private static final String collectionName = "testCreateCollectionWithBasicAuth";

  @BeforeClass
  public static void setupClusterWithSecurityEnabled() throws Exception {
    configureCluster(2)
        .addConfig("conf", configset("cloud-minimal"))
        .withSecurityJson(SecurityJson.SIMPLE)
        .configure();
  }

  private <T extends SolrRequest<? extends SolrResponse>> T withBasicAuth(T req) {
    req.setBasicAuthCredentials(SecurityJson.USER, SecurityJson.PASS);
    return req;
  }

  @Test
  public void testStreamAgainstSolrWithBasicAuth() throws Exception {
    String COLLECTION_NAME = "testStreamSolrWithBasicAuth";
    withBasicAuth(CollectionAdminRequest.createCollection(COLLECTION_NAME, "conf", 1, 1))
        .processAndWait(cluster.getSolrClient(), 10);
    waitForState(
        "Expected collection to be created with 1 shard and 1 replicas",
        COLLECTION_NAME,
        clusterShape(1, 1));

    UpdateRequest ur = new UpdateRequest();
    ur.setAction(AbstractUpdateRequest.ACTION.COMMIT, true, true);

    for (int i = 0; i < 10; i++) {
      withBasicAuth(ur)
          .add(
              "id",
              String.valueOf(i),
              "desc_s",
              TestUtil.randomSimpleString(random(), 10, 50),
              "a_dt",
              "2019-09-30T05:58:03Z");
    }

    String expression = "search(" + COLLECTION_NAME + ",q='*:*',fl='id, desc_s',sort='id desc')";
    File expressionFile = File.createTempFile("expression", ".EXPR");
    FileWriter writer = new FileWriter(expressionFile);
    writer.write(expression);
    writer.close();

    // test passing in the file
    String[] args = {
      "stream",
      "-zkHost",
      cluster.getZkClient().getZkServerAddress(),
      "-credentials",
      SecurityJson.USER_PASS,
      "-verbose",
      expressionFile.getAbsolutePath()
    };

    assertEquals(0, runTool(args));
  }

  private int runTool(String[] args) throws Exception {
    Tool tool = findTool(args);
    assertTrue(tool instanceof StreamTool);
    CommandLine cli = parseCmdLine(tool.getName(), args, tool.getOptions());
    return tool.runTool(cli);
  }
}
