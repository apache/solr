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

import java.nio.file.Path;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Port of {@code test_stream.bats}.
 *
 * <p>A plain {@code solr start} plus the {@code create} and {@code post} that {@code
 * RunExampleTool} would have run for the techproducts example — same config set, same documents,
 * same {@code --filetypes xml}. On Windows the example tool drops every {@code SOLR_*} variable
 * from the spawned node's environment (SOLR-18650), so {@code -e} would have made the whole suite
 * bash-only; the subject here is the streaming expression, not the example.
 *
 * <p>{@code SOLR_INCLUDE} points at a non-existent path inside the work directory, as in {@code
 * BasicAuthTest}: {@code auth enable} then writes neither the include file nor {@code
 * basicAuth.conf}, so every command has to supply the credentials itself, and the distribution's
 * own include file — shared with every concurrent class — is left alone.
 */
public class StreamTest extends SolrCatsTestCase {

  private static final String EXPR_FILE = "search.expr";

  private static final String SEARCH_MEMORY =
      """
      search(techproducts,
      q="name:memory",
      fl="name,price",
      sort="price desc"
      )
      """;

  @BeforeClass
  public static void startSolr() {
    // The node lives for the whole class and is stopped automatically.
    classCli.run("solr start");
    classCli.run("solr create -c techproducts -d sample_techproducts_configs");
    classCli
        .run(
            "solr post --solr-url http://localhost:${SOLR_PORT} --name techproducts"
                + " --type application/xml --filetypes xml"
                + " \"${SOLR_TIP}/example/exampledocs\"")
        .assertContains("files indexed.")
        .assertNotContains("ERROR");

    // The auth command exports some system variables that are injected as basic auth username and
    // password, however that defeats our test so fake that out by pointing SOLR_INCLUDE at a path
    // that does not exist.
    Path bogusInclude =
        classCli.workDir().resolve("force-credentials-to-be-supplied").resolve("solr.in.sh");
    classCli.env("SOLR_INCLUDE", bogusInclude.toString());
    classCli.run("solr auth enable --type basicAuth --credentials name:password");
  }

  @Test
  public void searchingSolrViaLocallyExecutedStreamingExpression() {
    write(EXPR_FILE, SEARCH_MEMORY);

    run("solr stream --execution local --header --credentials name:password " + EXPR_FILE)
        .assertContains("name   price")
        .assertContains("CORSAIR  XMS")
        .assertNotContains("ERROR");
  }

  @Test
  public void searchingSolrViaRemotelyExecutedStreamingExpressionBySolrUrl() {
    write(EXPR_FILE, SEARCH_MEMORY);

    run("solr stream --name techproducts --solr-url http://localhost:${SOLR_PORT} --header"
            + " --credentials name:password "
            + EXPR_FILE)
        .assertContains("name   price")
        .assertContains("CORSAIR  XMS")
        .assertNotContains("ERROR");
  }

  @Test
  public void searchingSolrViaRemotelyExecutedStreamingExpressionBySolrConnection() {
    write(EXPR_FILE, SEARCH_MEMORY);

    run("solr stream --name techproducts --solr-connection http://localhost:${SOLR_PORT}/solr"
            + " --header --credentials name:password "
            + EXPR_FILE)
        .assertContains("name   price")
        .assertContains("CORSAIR  XMS")
        .assertNotContains("ERROR");
  }

  @Test
  public void variableInterpolation() {
    // $1 and $2 are substituted by the stream tool, not by a shell, so they stay literal here.
    write(
        EXPR_FILE,
        """
        search(techproducts,
        q="name:$1",
        fl="name,price",
        sort="price $2"
        )
        """);

    run("solr stream --execution local --header --credentials name:password "
            + EXPR_FILE
            + " apple asc")
        .assertContains("name   price")
        .assertContains("Apple 60 GB iPod")
        .assertNotContains("ERROR");
  }

  @Test
  public void searchingSolrWithoutCredentialsFailsWithError() {
    write(EXPR_FILE, SEARCH_MEMORY);

    runFail("solr stream --execution local --header " + EXPR_FILE).assertContains("ERROR");
  }
}
