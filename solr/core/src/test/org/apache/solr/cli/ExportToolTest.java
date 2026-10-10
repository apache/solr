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

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.solr.client.solrj.request.AbstractUpdateRequest;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.junit.BeforeClass;
import org.junit.Test;

public class ExportToolTest extends SolrCloudTestCase {
  private static final String COLLECTION = "exportToolColl";
  private static final int DOCS = 10;

  /** Runs the tool. Overridden by the picocli variant of this test. */
  protected int runTool(String[] args, Class<? extends ToolBase> clazz) throws Exception {
    return CLITestHelper.runTool(args, clazz);
  }

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(1).addConfig("conf", configset("cloud-minimal")).configure();
    CollectionAdminRequest.createCollection(COLLECTION, "conf", 1, 1)
        .process(cluster.getSolrClient());
    cluster.waitForActiveCollection(COLLECTION, 1, 1);

    UpdateRequest ur = new UpdateRequest();
    ur.setAction(AbstractUpdateRequest.ACTION.COMMIT, true, true);
    for (int i = 0; i < DOCS; i++) {
      ur.add("id", String.valueOf(i), "desc_s", "doc" + i);
    }
    ur.process(cluster.getSolrClient(), COLLECTION);
  }

  private String solrUrl() {
    return cluster.getJettySolrRunner(0).getBaseUrl().toString();
  }

  private List<String> exportLines(String... extraArgs) throws Exception {
    Path outDir = createTempDir();
    String[] fixed = {
      "export",
      "-c",
      COLLECTION,
      "--solr-url",
      solrUrl(),
      "--format",
      "jsonl",
      "--output",
      outDir.toString()
    };
    String[] args = new String[fixed.length + extraArgs.length];
    System.arraycopy(fixed, 0, args, 0, fixed.length);
    System.arraycopy(extraArgs, 0, args, fixed.length, extraArgs.length);

    assertEquals(0, runTool(args, ExportTool.class));
    return Files.readAllLines(outDir.resolve(COLLECTION + ".jsonl"));
  }

  @Test
  public void testExportAllDocs() throws Exception {
    assertEquals(DOCS, exportLines("--limit", "-1").size());
  }

  @Test
  public void testLimitDefaultsToOneHundredAndIsHonoured() throws Exception {
    assertEquals(DOCS, exportLines().size());
    assertEquals(3, exportLines("--limit", "3").size());
  }

  @Test
  public void testQueryAndFields() throws Exception {
    List<String> lines = exportLines("--query", "id:7", "--fields", "id");
    assertEquals(1, lines.size());
    assertTrue(lines.get(0), lines.get(0).contains("\"id\":\"7\""));
    assertFalse(lines.get(0), lines.get(0).contains("desc_s"));
  }

  @Test
  public void testFailsWithoutAConnectionTarget() throws Exception {
    // commons-cli reports 1; picocli reports its usage-error code 2
    assertNotEquals(0, runTool(new String[] {"export", "-c", COLLECTION}, ExportTool.class));
  }

  int exportTo(Path outDir, String... extraArgs) throws Exception {
    String[] fixed = {
      "export", "-c", COLLECTION, "--solr-url", solrUrl(), "--output", outDir.toString()
    };
    String[] args = new String[fixed.length + extraArgs.length];
    System.arraycopy(fixed, 0, args, 0, fixed.length);
    System.arraycopy(extraArgs, 0, args, fixed.length, extraArgs.length);
    return runTool(args, ExportTool.class);
  }

  @Test
  public void testFormatDefaultsToJson() throws Exception {
    Path outDir = createTempDir();
    assertEquals(0, exportTo(outDir));
    String json = Files.readString(outDir.resolve(COLLECTION + ".json"));
    assertTrue(json, json.contains("\"id\":\"1\""));
  }

  @Test
  public void testJavabinFormat() throws Exception {
    Path outDir = createTempDir();
    assertEquals(0, exportTo(outDir, "--format", "javabin"));
    assertTrue(Files.size(outDir.resolve(COLLECTION + ".javabin")) > 0);
  }

  @Test
  public void testUnknownFormatFails() throws Exception {
    Path outDir = createTempDir();
    // commons-cli reports 1; picocli reports its usage-error code 2
    assertNotEquals(0, exportTo(outDir, "--format", "xml"));
    try (var written = Files.list(outDir)) {
      assertEquals(List.of(), written.toList());
    }
  }

  @Test
  public void testNonNumericLimitFails() throws Exception {
    Path outDir = createTempDir();
    // commons-cli fails on parsing the number (1); picocli rejects it up front (2)
    assertNotEquals(0, exportTo(outDir, "--limit", "abc"));
    try (var written = Files.list(outDir)) {
      assertEquals(List.of(), written.toList());
    }
  }
}
