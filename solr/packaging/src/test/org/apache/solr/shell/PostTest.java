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

import static org.apache.solr.cats.EnabledOnShell.BASH;

import java.util.List;
import org.apache.solr.cats.EnabledOnShell;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Port of {@code test_post.bats}.
 *
 * <p>The two tests that need a pipe or an embedded newline say so explicitly rather than relying on
 * bash syntax inside a shared command line.
 */
public class PostTest extends SolrCatsTestCase {

  private static final String DOCS = "${SOLR_TIP}/example/exampledocs";

  @BeforeClass
  public static void startSolr() {
    classCli.run("solr start -Dsolr.modules=extraction");
  }

  @Test
  public void checkHelpCommand() {
    runFail("solr post")
        .assertContains(
            forCli("Missing required option: c", "Missing required option: '--name=name'"));

    run("solr post -h").assertContains("usage: bin/solr post").assertNotContains("ERROR");
    run("solr post --help").assertContains("usage: bin/solr post").assertNotContains("ERROR");
  }

  @Test
  public void basicPostWithATypeSpecified() {
    run("solr create -c monitors -d _default").assertContains("Created collection 'monitors'");

    run("solr post --type application/xml --solr-url http://localhost:${SOLR_PORT}"
            + " -c monitors \""
            + DOCS
            + "/monitor.xml\"")
        .assertContains("1 files indexed.")
        .assertNotContains("ERROR");
  }

  @Test
  public void basicPostWithCollectionInsteadOfUrlParameter() {
    run("solr create -c monitors_c_param -d _default")
        .assertContains("Created collection 'monitors_c_param'");

    run("solr post --type application/xml -c monitors_c_param \"" + DOCS + "/monitor.xml\"")
        .assertContains("1 files indexed.")
        .assertNotContains("ERROR");
  }

  @Test
  public void basicPostWithSolrUrlAndCollection() {
    run("solr create -c monitors_solr_url_param -d _default")
        .assertContains("Created collection 'monitors_solr_url_param'");

    run("solr post --type application/xml -c monitors_solr_url_param"
            + " --solr-url http://localhost:${SOLR_PORT} \""
            + DOCS
            + "/monitor.xml\"")
        .assertContains("1 files indexed.")
        .assertNotContains("ERROR");
  }

  @Test
  public void basicPostWithShortSolrConnectionFormAndCollection() {
    run("solr create -c monitors_s_param -d _default")
        .assertContains("Created collection 'monitors_s_param'");

    run("solr post --type application/xml -c monitors_s_param"
            + " -s http://localhost:${SOLR_PORT} \""
            + DOCS
            + "/monitor.xml\"")
        .assertContains("1 files indexed.")
        .assertNotContains("ERROR");
  }

  @Test
  public void basicPostWithoutATypeSpecified() {
    run("solr create -c monitors_no_type -d _default");
    run("solr post --solr-url http://localhost:${SOLR_PORT} -c monitors_no_type"
            + " \""
            + DOCS
            + "/monitor.xml\"")
        .assertContains("1 files indexed.")
        .assertNotContains("ERROR");
    http("/solr/monitors_no_type/select?q=*:*").assertContains("\"numFound\":1");

    run("solr create -c books_no_type -d _default");
    run("solr post --solr-url http://localhost:${SOLR_PORT} -c books_no_type"
            + " \""
            + DOCS
            + "/books.json\"")
        .assertContains("1 files indexed.")
        .assertNotContains("ERROR");
    http("/solr/books_no_type/select?q=*:*").assertContains("\"numFound\":4");

    run("solr create -c books_csv_no_type -d _default");
    run("solr post --solr-url http://localhost:${SOLR_PORT} -c books_csv_no_type"
            + " \""
            + DOCS
            + "/books.csv\"")
        .assertContains("1 files indexed.")
        .assertNotContains("ERROR");
    http("/solr/books_csv_no_type/select?q=*:*").assertContains("\"numFound\":10");
  }

  @Test
  public void crawlingADirectoryAsADryRun() {
    // Filtered to xml, json and csv so the Extract handler is not invoked.
    run("solr post --dry-run --filetypes xml,json,csv --solr-url http://localhost:${SOLR_PORT}"
            + " -c foobar --skip-commit \""
            + DOCS
            + "\"")
        .assertContains("Dry run complete. 15 would have been indexed.")
        .assertNotContains("15 files indexed.")
        .assertNotContains("ERROR");
  }

  @Test
  public void crawlingADirectory() {
    run("solr create -c mixed_content -d _default");

    run("solr post --filetypes xml,json,csv --solr-url http://localhost:${SOLR_PORT}"
            + " -c mixed_content \""
            + DOCS
            + "\"")
        .assertContains("15 files indexed.")
        .assertNotContains("ERROR");
    http("/solr/mixed_content/select?q=*:*").assertContains("\"numFound\":45");
  }

  @Test
  public void skipCommitAndOptimizeAndDelete() {
    run("solr create -c monitors2 -d _default").assertContains("Created collection 'monitors2'");

    run("solr post --solr-url http://localhost:${SOLR_PORT} -c monitors2 --type application/xml"
            + " --skip-commit --optimize \""
            + DOCS
            + "/monitor.xml\"")
        .assertContains("1 files indexed.")
        .assertNotContains("COMMITting Solr index")
        .assertContains("Performing an OPTIMIZE")
        .assertNotContains("ERROR");
  }

  @Test
  public void argsMode() {
    run("solr create -c test_args -d _default").assertContains("Created collection 'test_args'");

    // argv form: these bodies carry quotes and angle brackets, so the harness quotes them for the
    // shell in use rather than the test guessing at escaping.
    run(List.of(
            "solr",
            "post",
            "--solr-url",
            "http://localhost:${SOLR_PORT}",
            "-c",
            "test_args",
            "--mode",
            "args",
            "--type",
            "application/xml",
            "--verbose",
            "<delete><query>*:*</query></delete>"))
        .assertContains("<int name=\"status\">0</int>");

    // Confirms the default type.
    run(List.of(
            "solr",
            "post",
            "--solr-url",
            "http://localhost:${SOLR_PORT}",
            "-c",
            "test_args",
            "--mode",
            "args",
            "--verbose",
            "{'delete': {'query': '*:*'}}"))
        .assertContains("\"status\":0");

    // Confirms nothing comes back without --verbose.
    run(List.of(
            "solr",
            "post",
            "--solr-url",
            "http://localhost:${SOLR_PORT}",
            "-c",
            "test_args",
            "--mode",
            "args",
            "{'delete': {'query': '*:*'}}"))
        // Without the status, a post that failed and printed nothing recognisable
        // would satisfy the refutation.

        .assertNotContains("\"status\":0");
  }

  @Test
  public void verboseEchoTheSolrResponse() {
    run("solr create -c monitors_verbose -d _default")
        .assertContains("Created collection 'monitors_verbose'");

    run("solr post --verbose --type application/xml --solr-url http://localhost:${SOLR_PORT}"
            + " -c monitors_verbose \""
            + DOCS
            + "/monitor.xml\"")
        .assertContains("1 files indexed.")
        .assertContains("<lst name=\"responseHeader\">")
        .assertNotContains("ERROR");
  }

  @Test
  @EnabledOnShell(BASH) // An argument containing a newline is not expressible on cmd.exe.
  public void argsModeWithACsvBodyContainingANewline() {
    // In the argv form the newline is
    // simply part of the Java string and the harness quotes it for the shell.
    run("solr create -c test_args_csv -d _default");
    run(List.of(
            "solr",
            "post",
            "--solr-url",
            "http://localhost:${SOLR_PORT}",
            "-c",
            "test_args_csv",
            "--mode",
            "args",
            "--type",
            "text/csv",
            "--verbose",
            "id,value\nROW1,0.47"))
        .assertContains("\"status\":0");
    http("/solr/test_args_csv/select?q=id:ROW1").assertContains("\"numFound\":1");
  }

  @Test
  public void stdinMode() {
    run("solr create -c test_stdin -d _default").assertContains("Created collection 'test_stdin'");

    runWithStdin(
            "{'commit': {}}",
            "solr post --solr-url http://localhost:${SOLR_PORT} -c test_stdin --mode stdin"
                + " --type application/json --verbose")
        .assertContains("\"status\":0");
  }
}
