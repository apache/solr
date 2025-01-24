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
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.invoke.MethodHandles;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Collectors;
import org.apache.commons.math3.util.Pair;
import org.apache.solr.SolrTestCase;
import org.apache.solr.cli.SolrProcessManager.SolrProcess;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SolrProcessManagerTest extends SolrTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static SolrProcessManager solrProcessManager;
  private static Pair<Integer, Process> processHttp;
  private static Pair<Integer, Process> processHttps;

  @BeforeClass
  public static void beforeClass() throws Exception {
    boolean isWindows = random().nextBoolean();
    String PID_SUFFIX = isWindows ? ".port" : ".pid";
    log.info("Simulating pid file on {}", isWindows ? "Windows" : "Linux");
    processHttp = createProcess(findAvailablePort(), false);
    processHttps = createProcess(findAvailablePort(), true);
    long processHttpValue = isWindows ? processHttp.getKey() : processHttp.getValue().pid();
    long processHttpsValue = isWindows ? processHttps.getKey() : processHttps.getValue().pid();
    SolrProcessManager.enableTestingMode = true;
    System.setProperty("jetty.port", Integer.toString(processHttp.getKey()));
    Path pidDir = Files.createTempDirectory("solr-pid-dir").toAbsolutePath();
    pidDir.toFile().deleteOnExit();
    System.setProperty("solr.pid.dir", pidDir.toString());
    Files.writeString(
        pidDir.resolve("solr-" + processHttpValue + PID_SUFFIX), Long.toString(processHttpValue));
    Files.writeString(
        pidDir.resolve("solr-" + processHttpsValue + PID_SUFFIX), Long.toString(processHttpsValue));
    Files.writeString(pidDir.resolve("solr-99999" + PID_SUFFIX), "99999"); // Invalid
    solrProcessManager = new SolrProcessManager();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    processHttp.getValue().destroyForcibly();
    processHttps.getValue().destroyForcibly();
    SolrProcessManager.enableTestingMode = false;
    System.clearProperty("jetty.port");
    System.clearProperty("solr.pid.dir");
  }

  private static int findAvailablePort() throws IOException {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    }
  }

  private static Pair<Integer, Process> createProcess(int port, boolean https) throws IOException {
    // Get the path to the java executable from the current JVM
    String classPath =
        Arrays.stream(System.getProperty("java.class.path").split(File.pathSeparator))
            .filter(p -> p.contains("solr") && p.contains("core") && p.contains("build"))
            .collect(Collectors.joining(File.pathSeparator));
    ProcessBuilder processBuilder =
        new ProcessBuilder(
            System.getProperty("java.home") + "/bin/java",
            "-Djetty.port=" + port,
            "-DisHttps=" + https,
            "-DmockSolr=true",
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
                    (p.isHttps() ? "https" : "http") + "://localhost:" + p.getPort() + "/solr",
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
        (solrProcessManager.processForPort(processHttp.getKey()).orElseThrow().getPort()));
    assertEquals(
        processHttps.getKey().intValue(),
        (solrProcessManager.processForPort(processHttps.getKey()).orElseThrow().getPort()));
  }

  public void testGetProcessForPid() {
    assertEquals(
        processHttp.getValue().pid(),
        (solrProcessManager.getProcessForPid(processHttp.getValue().pid()).orElseThrow().getPid()));
    assertEquals(
        processHttps.getValue().pid(),
        (solrProcessManager
            .getProcessForPid(processHttps.getValue().pid())
            .orElseThrow()
            .getPid()));
  }

  public void testScanSolrPidFiles() throws IOException {
    Collection<SolrProcess> processes = solrProcessManager.scanSolrPidFiles();
    assertEquals(2, processes.size());
  }

  public void testGetAllRunning() {
    Collection<SolrProcess> processes = solrProcessManager.getAllRunning();
    assertEquals(2, processes.size());
  }

  public void testSolrProcessMethods() {
    SolrProcess http = solrProcessManager.processForPort(processHttp.getKey()).orElseThrow();
    assertEquals(processHttp.getValue().pid(), http.getPid());
    assertEquals(processHttp.getKey().intValue(), http.getPort());
    assertFalse(http.isHttps());
    assertEquals("http://localhost:" + processHttp.getKey() + "/solr", http.getLocalUrl());

    SolrProcess https = solrProcessManager.processForPort(processHttps.getKey()).orElseThrow();
    assertEquals(processHttps.getValue().pid(), https.getPid());
    assertEquals(processHttps.getKey().intValue(), https.getPort());
    assertTrue(https.isHttps());
    assertEquals("https://localhost:" + processHttps.getKey() + "/solr", https.getLocalUrl());
  }

  /**
   * This class is started as new java process by {@link SolrProcessManagerTest#createProcess}, and
   * it listens to a HTTP(s) port to simulate a real Solr process.
   */
  @SuppressWarnings("NewClassNamingConvention")
  public static class MockSolrProcess {
    public static void main(String[] args) {
      int port = Integer.parseInt(System.getProperty("jetty.port"));
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
