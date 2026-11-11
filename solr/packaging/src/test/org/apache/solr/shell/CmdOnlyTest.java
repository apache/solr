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
 * A whole class gated to one shell, which is the shape a {@code bin/solr.cmd}-specific suite will
 * take. On bash it is skipped before any Solr starts — including the {@code @BeforeClass} below,
 * which would otherwise start a node and leak one.
 */
@EnabledOnShell(Shell.CMD)
public class CmdOnlyTest extends SolrShellTestCase {

  @BeforeClass
  public static void startNodeForTheClass() {
    // Must not run on bash: proof that the class-level gate fires before setup_file-style work.
    classCli.run("solr start").assertSuccess();
  }

  @Test
  public void solrInCmdIsTheWindowsConfigFile() {
    assertEquals(Shell.CMD, cli.shell());
  }
}
