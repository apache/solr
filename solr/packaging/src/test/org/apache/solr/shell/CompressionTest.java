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

import java.util.Map;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Port of {@code test_compression.bats}.
 *
 * <p>{@code solr start -e films} is deliberately not used: on Windows {@code
 * RunExampleTool#startSolr} drops every {@code SOLR_*} variable from the spawned node's
 * environment, so the node carries neither the harness' process-scan marker nor {@code
 * SOLR_PID_DIR} and cannot be discovered or stopped (SOLR-18650). A plain start, {@code solr
 * create} and {@code solr post} of the example docs give these tests what they need: a collection
 * whose query response exceeds Jetty's 2048-byte {@code minGzipSize}.
 */
public class CompressionTest extends SolrShellTestCase {

  private static final String COLLECTION = "compression_docs";
  private static final String QUERY = "/solr/" + COLLECTION + "/select?q=*:*&rows=100";

  @BeforeClass
  public static void startSolr() {
    // The node lives for the whole class and is stopped automatically.
    classCli.run("solr start");
    classCli.run("solr create -c " + COLLECTION + " -d _default");
    // Filtered to xml, json and csv so the Extract handler is not invoked. 45 documents, whose
    // response is well over the 2048 bytes Jetty wants before it will compress one at all.
    classCli
        .run(
            "solr post --filetypes xml,json,csv --solr-url http://localhost:${SOLR_PORT}"
                + " -c "
                + COLLECTION
                + " \"${SOLR_TIP}/example/exampledocs\"")
        .assertContains("15 files indexed.")
        .assertNotContains("ERROR");
    classCli.helpers().http(QUERY).assertStatus(200).assertContains("\"numFound\":45");
  }

  @Test
  public void serverDoesNotCompressResponseWithoutAcceptEncodingHeader() {
    http(QUERY).assertStatus(200).assertHeaderNotContains("Content-Encoding:");
  }

  @Test
  public void serverCompressesResponseWhenAcceptEncodingGzipIsRequested() {
    http(QUERY, Map.of("Accept-Encoding", "gzip"))
        .assertStatus(200)
        .assertHeaderContains("Content-Encoding: gzip");
  }

  @Test
  public void compressedResponseCanBeDecompressedAndParsed() {
    // curl --compressed; the harness decompresses a gzip body the same way.
    http(QUERY, Map.of("Accept-Encoding", "gzip, deflate"))
        .assertHeaderContains("Content-Encoding: gzip")
        .assertContains("\"status\":0");
  }
}
