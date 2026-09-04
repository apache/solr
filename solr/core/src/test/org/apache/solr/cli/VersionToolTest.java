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

import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.embedded.JettySolrRunner;
import org.junit.BeforeClass;
import org.junit.Test;

public class VersionToolTest extends SolrCloudTestCase {

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(1).configure();
  }

  @Test
  public void testClientVersionOnly() throws Exception {
    String[] toolArgs = new String[] {"version"};
    CLITestHelper.TestingRuntime runtime = new CLITestHelper.TestingRuntime(true);
    VersionTool tool = new VersionTool(runtime);
    tool.runTool(SolrCLI.processCommandLineArgs(tool, toolArgs));

    String output = runtime.getOutput();
    assertTrue("Output should contain 'Client version:'", output.contains("Client version:"));
    assertFalse("Output should not contain 'Server version:'", output.contains("Server version:"));
  }

  @Test
  public void testClientAndServerVersion() throws Exception {
    JettySolrRunner randomJetty = cluster.getRandomJetty(random());
    String baseUrl = randomJetty.getBaseUrl().toString();

    String[] toolArgs = new String[] {"version", "--solr-url", baseUrl};
    CLITestHelper.TestingRuntime runtime = new CLITestHelper.TestingRuntime(true);
    VersionTool tool = new VersionTool(runtime);
    tool.runTool(SolrCLI.processCommandLineArgs(tool, toolArgs));

    String output = runtime.getOutput();
    assertTrue("Output should contain 'Client version:'", output.contains("Client version:"));
    assertTrue("Output should contain 'Server version:'", output.contains("Server version:"));
  }
}
