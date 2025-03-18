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

import java.util.Map;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.util.Utils;
import org.apache.solr.embedded.JettySolrRunner;
import org.junit.BeforeClass;
import org.junit.Test;

public class StatusToolTest extends SolrCloudTestCase {

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(2).configure();
  }

  /** Check the tool returns expected details by specifying Solr URL on the command line. */
  @Test
  public void testSolrUrlStatus() throws Exception {

    JettySolrRunner randomJetty = cluster.getRandomJetty(random());
    String baseUrl = randomJetty.getBaseUrl().toString();

    String[] toolArgs = new String[] {"status", "--solr-url", baseUrl};
    CLITestHelper.TestingRuntime runtime = new CLITestHelper.TestingRuntime(true);
    StatusTool tool = new StatusTool(runtime);
    tool.runTool(SolrCLI.processCommandLineArgs(tool, toolArgs));

    Map<?, ?> obj = (Map<?, ?>) Utils.fromJSON(runtime.getReader());
    assertTrue(obj.containsKey("version"));
    assertTrue(obj.containsKey("startTime"));
    assertTrue(obj.containsKey("uptime"));
    assertTrue(obj.containsKey("memory"));
  }
}
