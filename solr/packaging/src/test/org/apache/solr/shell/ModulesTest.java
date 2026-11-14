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

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.Test;

/**
 * Port of {@code test_modules.bats}.
 *
 * <p>Both tests clean up the collection they created, {@code SOLR_HOME} being class-scoped, and the
 * {@code solr.include} file is written in the syntax of the shell that will read it.
 */
public class ModulesTest extends SolrShellTestCase {

  @Test
  public void sqlModule() {
    run("solr start -Dsolr.modules=sql");
    // Its own name, which no assertion here depends on, so that this test
    // and the next cannot collide in the SOLR_HOME the whole class shares.
    run("solr create -c sql_coll");
    run("solr api --solr-url \"http://localhost:${SOLR_PORT}/solr/sql_coll/sql"
            + "?stmt=select+id+from+sql_coll+limit+10\"")
        .assertContains("\"docs\":")
        .assertContains("\"EOF\":true")
        .assertContains("\"RESPONSE_TIME\":")
        .assertNotContains("\"EXCEPTION\"");

    // Without this the collection is still in this class's SOLR_HOME when
    // the next test's node starts, which runs with a different set of modules.
    run("solr delete -c sql_coll --delete-config");
  }

  @Test
  public void icuCollationInAnalysisExtrasModule() {
    // bin/solr sources SOLR_INCLUDE while bin\solr.cmd CALLs it, so the line has to be
    // written in the syntax — and under the extension — of the shell that will read it.
    boolean cmd = "cmd".equals(cli.shell().id());
    Path solrIncludeFile =
        write(
            cmd ? "solr.include.cmd" : "solr.include",
            cmd ? "set \"SOLR_MODULES=analysis-extras\"\r\n" : "SOLR_MODULES=analysis-extras\n");

    env("SOLR_INCLUDE", solrIncludeFile.toString());
    run("solr start");
    run("solr create -c COLL_NAME -d \"" + analysisExtrasConf() + "\"")
        .assertContains("Created collection 'COLL_NAME'");

    run("solr delete -c COLL_NAME --delete-config");
  }

  /**
   * The config set BATS passed as {@code test/analysis_extras_config/conf}, which is relative to
   * {@code solr/packaging}. A session's working directory is its own scratch space, so the port
   * hands {@code solr create} an absolute path: the Gradle test JVM's own working directory first,
   * then the distribution's place inside {@code solr/packaging/build}.
   */
  private static Path analysisExtrasConf() {
    List<Path> candidates =
        List.of(
            Path.of("test/analysis_extras_config/conf").toAbsolutePath().normalize(),
            harnessEnv
                .solrTip()
                .getParent()
                .getParent()
                .resolve("test/analysis_extras_config/conf"));
    for (Path candidate : candidates) {
      if (Files.isDirectory(candidate)) {
        return candidate;
      }
    }
    throw new AssertionError("Could not find the analysis_extras_config config set: " + candidates);
  }
}
