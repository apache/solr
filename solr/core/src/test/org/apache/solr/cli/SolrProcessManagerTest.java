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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.commons.math3.util.Pair;
import org.apache.solr.SolrTestCase;
import org.apache.solr.cli.SolrProcessManager.SolrProcess;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class SolrProcessManagerTest extends SolrTestCase {
  /**
   * Restricts discovery to this class's own mock processes, which is what the marker is for. Unique
   * per run so a developer's own Solr, or another suite on the same machine, is not discovered.
   */
  private static final String SCAN_MARKER = "SolrProcessManagerTest-" + UUID.randomUUID();

  private static SolrProcessManager solrProcessManager;
  private static Pair<Integer, Process> processHttp;
  private static Pair<Integer, Process> processHttps;
  private static Path pidDir;

  @BeforeClass
  public static void beforeClass() throws Exception {
    processHttp = createProcess(findAvailablePort(), false);
    processHttps = createProcess(findAvailablePort(), true);
    System.setProperty(SolrProcessManager.PROCESS_SCAN_MARKER, SCAN_MARKER);
    System.setProperty("solr.port.listen", Integer.toString(processHttp.getKey()));
    pidDir = createTempDir("solr-pid-dir");
    System.setProperty("solr.pid.dir", pidDir.toString());
    // Both platforms name the file after the port and store the PID in it
    writePidFile(processHttp);
    writePidFile(processHttps);
    Files.writeString(pidDir.resolve("solr-99999.pid"), "99999"); // Stale
    // The legacy Windows port file is no longer read
    Files.writeString(
        pidDir.resolve("solr-" + processHttp.getKey() + ".port"),
        Integer.toString(processHttp.getKey()));
    solrProcessManager = new SolrProcessManager();
  }

  private static void writePidFile(Pair<Integer, Process> process) throws IOException {
    Files.writeString(
        pidDir.resolve("solr-" + process.getKey() + ".pid"),
        Long.toString(process.getValue().pid()));
  }

  @AfterClass
  public static void afterClass() throws Exception {
    processHttp.getValue().destroyForcibly();
    processHttps.getValue().destroyForcibly();
  }

  private static int findAvailablePort() throws IOException {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    }
  }

  @SuppressWarnings("SystemGetProperty")
  private static Pair<Integer, Process> createProcess(int port, boolean https) throws IOException {
    // Get the path to the java executable from the current JVM

    String pathSeparator = System.getProperty("path.separator");
    String classPath =
        Arrays.stream(System.getProperty("java.class.path").split(pathSeparator))
            .filter(p -> p.contains("solr") && p.contains("core") && p.contains("build"))
            .collect(Collectors.joining(pathSeparator));

    ProcessBuilder processBuilder =
        new ProcessBuilder(
            System.getProperty("java.home") + "/bin/java",
            "-Dsolr.port.listen=" + port,
            "-DisHttps=" + https,
            "-D" + SolrProcessManager.PROCESS_SCAN_MARKER + "=" + SCAN_MARKER,
            "-cp",
            classPath,
            "org.apache.solr.cli.SolrProcessManagerTest$MockSolrProcess",
            https ? "--module=https" : "--module=http");

    // Start the process and read first line of output
    Process process = processBuilder.start();
    try (InputStream is = process.getInputStream();
        InputStreamReader isr = new InputStreamReader(is, StandardCharsets.UTF_8);
        BufferedReader br = new BufferedReader(isr)) {
      System.out.println(br.readLine());
    }
    return new Pair<>(port, process);
  }

  public void testGetLocalUrl() {
    assertFalse(solrProcessManager.getAllRunning().isEmpty());
    solrProcessManager
        .getAllRunning()
        .forEach(
            p ->
                assertEquals(
                    (p.isHttps() ? "https" : "http") + "://localhost:" + p.port() + "/solr",
                    p.getLocalUrl()));
  }

  public void testIsRunningWithPort() {
    assertFalse(solrProcessManager.isRunningWithPort(0));
    assertTrue(solrProcessManager.isRunningWithPort(processHttp.getKey()));
    assertTrue(solrProcessManager.isRunningWithPort(processHttps.getKey()));
  }

  public void testIsRunningWithPid() {
    assertFalse(solrProcessManager.isRunningWithPid(0L));
    assertTrue(solrProcessManager.isRunningWithPid(processHttp.getValue().pid()));
    assertTrue(solrProcessManager.isRunningWithPid(processHttps.getValue().pid()));
  }

  public void testProcessForPort() {
    assertEquals(
        processHttp.getKey().intValue(),
        (solrProcessManager.processForPort(processHttp.getKey()).orElseThrow().port()));
    assertEquals(
        processHttps.getKey().intValue(),
        (solrProcessManager.processForPort(processHttps.getKey()).orElseThrow().port()));
  }

  public void testGetProcessForPid() {
    assertEquals(
        processHttp.getValue().pid(),
        (solrProcessManager.getProcessForPid(processHttp.getValue().pid()).orElseThrow().pid()));
    assertEquals(
        processHttps.getValue().pid(),
        (solrProcessManager.getProcessForPid(processHttps.getValue().pid()).orElseThrow().pid()));
  }

  public void testScanSolrPidFiles() throws IOException {
    Collection<SolrProcess> processes = solrProcessManager.scanSolrPidFiles();
    assertEquals(2, processes.size());
    assertFalse(
        "Stale PID file should have been deleted", Files.exists(pidDir.resolve("solr-99999.pid")));
    assertTrue(
        "Legacy .port file should be left alone, not scanned",
        Files.exists(pidDir.resolve("solr-" + processHttp.getKey() + ".port")));
  }

  public void testGetAllRunning() {
    Collection<SolrProcess> processes = solrProcessManager.getAllRunning();
    assertEquals(2, processes.size());
  }

  public void testSolrProcessMethods() {
    SolrProcess http = solrProcessManager.processForPort(processHttp.getKey()).orElseThrow();
    assertEquals(processHttp.getValue().pid(), http.pid());
    assertEquals(processHttp.getKey().intValue(), http.port());
    assertFalse(http.isHttps());
    assertEquals("http://localhost:" + processHttp.getKey() + "/solr", http.getLocalUrl());

    SolrProcess https = solrProcessManager.processForPort(processHttps.getKey()).orElseThrow();
    assertEquals(processHttps.getValue().pid(), https.pid());
    assertEquals(processHttps.getKey().intValue(), https.port());
    assertTrue(https.isHttps());
    assertEquals("https://localhost:" + processHttps.getKey() + "/solr", https.getLocalUrl());
  }

  /**
   * This class is started as new java process by {@link SolrProcessManagerTest#createProcess}, and
   * it listens to an HTTP(s) port to simulate a real Solr process.
   */
  @SuppressWarnings("NewClassNamingConvention")
  public static class MockSolrProcess {
    public static void main(String[] args) {
      int port = Integer.parseInt(System.getProperty("solr.port.listen"));
      boolean https = System.getProperty("isHttps").equals("true");
      try (ServerSocket serverSocket = new ServerSocket(port)) {
        System.out.println("Listening on " + (https ? "https" : "http") + " port " + port);
        serverSocket.accept();
      } catch (IOException e) {
        System.err.println("Error listening to port: " + e.getMessage());
      }
    }
  }
}
