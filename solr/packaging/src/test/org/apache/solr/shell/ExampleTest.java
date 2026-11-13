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

package org.apache.solr.shell;

import org.apache.solr.shell.exec.Shell;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Port of {@code test_example.bats}.
 *
 * <p>The example is given {@code --solr-home ${SOLR_HOME}} so that it writes its node directories
 * into this class's own {@code SOLR_HOME} rather than {@code ${SOLR_TIP}/example/cloud}, which is
 * shared with every class running concurrently.
 *
 * <p><b>Please keep the {@code solr start -e} invocation below.</b> {@code CompressionTest}, {@code
 * SnapshotsTest} and {@code StreamTest} use a plain {@code solr start} plus {@code create} and
 * {@code post} instead, the example having been only a convenient way to get a populated
 * collection. Driving {@code RunExampleTool} from the command line is what this suite is
 * <em>for</em>, and it is now the only shell suite that does it at all.
 */
@EnabledOnShell(Shell.BASH)
// Bash-only: on Windows RunExampleTool#startSolr drops every SOLR_* variable from the spawned
// nodes' environment, so they get neither the process-scan marker nor SOLR_PID_DIR and cannot be
// discovered or stopped (SOLR-18650). Both tests depend on the cluster the example starts, hence a
// class gate rather than @AwaitsFix, which would skip the bash run too.
public class ExampleTest extends SolrShellTestCase {

  @BeforeClass
  public static void startSolr() {
    // The two nodes live for the whole class and are stopped automatically; the
    // example data lands under SOLR_HOME, which the next class wipes.
    classCli.run(
        "solr start -e cloud --no-prompt --jvm-opts \"-Dcustom.prop=helloworld\""
            + " --solr-home \"${SOLR_HOME}\"");
    classCli.run(
        "solr assert --started http://localhost:${SOLR_PORT}"
            + " --cloud http://localhost:${SOLR_PORT} --timeout 60000");
    classCli.run(
        "solr assert --started http://localhost:${SOLR2_PORT}"
            + " --cloud http://localhost:${SOLR2_PORT} --timeout 60000");
  }

  @Test
  public void startECloudWorksWithNoPrompt() {
    run(
        "solr assert --started http://localhost:${SOLR_PORT}"
            + " --cloud http://localhost:${SOLR_PORT} --timeout 10000");
    run(
        "solr assert --started http://localhost:${SOLR2_PORT}"
            + " --cloud http://localhost:${SOLR2_PORT} --timeout 10000");
  }

  @Test
  public void startECloudWorksWithJvmOpts() {
    http("/solr/admin/info/properties").assertContains("helloworld");
    http(ports().solr2(), "/solr/admin/info/properties").assertContains("helloworld");
  }
}
