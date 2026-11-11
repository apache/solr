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
 * <p>Kept close to the original, including the order of the tests and the text each asserts on.
 * Where it differs: {@code curl} becomes {@link #http}, and the example is given {@code --solr-home
 * ${SOLR_HOME}} so that it writes its node directories into this class's own {@code SOLR_HOME}
 * instead of {@code ${SOLR_TIP}/example/cloud}, which is shared with every class running
 * concurrently. The BATS {@code test_stream.bats} passed {@code --solr-home} for the same reason,
 * though its port no longer uses an example at all.
 *
 * <p><b>Please keep the {@code solr start -e} invocation below.</b> Several other ports replaced
 * theirs with {@code solr start} plus {@code create} and {@code post} — {@code CompressionTest},
 * {@code SnapshotsTest} and {@code StreamTest} all say so in their own javadoc — because the
 * example was only a convenient way to get a populated collection, and using it cost them their
 * Windows coverage for no benefit. That reasoning does not apply here: driving {@code
 * RunExampleTool} from the command line is what this suite is for, and this is now the only place
 * in the shell suites that does it at all. {@code RunExampleTool} is covered by ordinary unit tests
 * as well, so this does not need to be exhaustive — but it should not reach zero.
 */
@EnabledOnShell(Shell.BASH)
// RunExampleTool#startSolr drops every SOLR_* variable from the environment of the nodes it spawns
// on Windows, so the example's nodes would get neither the harness' process-scan marker nor
// SOLR_PID_DIR, and could not be discovered or stopped afterwards. Both tests here depend on the
// cluster the example starts, so the restriction is the class's.
public class ExampleTest extends SolrShellTestCase {

  @BeforeClass
  public static void startSolr() {
    // BATS' setup_file. The two nodes live for the whole class and are stopped automatically; the
    // example data lands under SOLR_HOME, which the next class wipes.
    classCli
        .run(
            "solr start -e cloud --no-prompt --jvm-opts \"-Dcustom.prop=helloworld\""
                + " --solr-home \"${SOLR_HOME}\"")
        .assertSuccess();
    classCli
        .run(
            "solr assert --started http://localhost:${SOLR_PORT}"
                + " --cloud http://localhost:${SOLR_PORT} --timeout 60000")
        .assertSuccess();
    classCli
        .run(
            "solr assert --started http://localhost:${SOLR2_PORT}"
                + " --cloud http://localhost:${SOLR2_PORT} --timeout 60000")
        .assertSuccess();
  }

  @Test
  public void startECloudWorksWithNoPrompt() {
    run("solr assert --started http://localhost:${SOLR_PORT}"
            + " --cloud http://localhost:${SOLR_PORT} --timeout 10000")
        .assertSuccess();
    run("solr assert --started http://localhost:${SOLR2_PORT}"
            + " --cloud http://localhost:${SOLR2_PORT} --timeout 10000")
        .assertSuccess();
  }

  @Test
  public void startECloudWorksWithJvmOpts() {
    http("/solr/admin/info/properties").assertContains("helloworld");
    http(ports().solr2(), "/solr/admin/info/properties").assertContains("helloworld");
  }
}
