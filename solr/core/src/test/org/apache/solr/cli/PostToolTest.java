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

import org.apache.commons.cli.CommandLine;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.junit.BeforeClass;
import org.junit.Test;

public class PostToolTest extends SolrCloudTestCase {

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(1)
        .addConfig(
            "config", TEST_PATH().resolve("configsets").resolve("cloud-minimal").resolve("conf"))
        .configure();
  }

  @Test
  public void testBasicRun() throws Exception {
    final String collection = "aliasedCollection";
    CollectionAdminRequest.createCollection(collection, "config", 1, 1)
        .process(cluster.getSolrClient());

    String[] args = {"post", "-url", "http://localhost:8983/solr/aliasedCollection", "blah.json"};
    assertEquals(0, runTool(args));
  }

  private int runTool(String[] args) throws Exception {
    Tool tool = findTool(args);
    assertTrue(tool instanceof PostTool);
    CommandLine cli = parseCmdLine(tool.getName(), args, tool.getOptions());
    return tool.runTool(cli);
  }
}
