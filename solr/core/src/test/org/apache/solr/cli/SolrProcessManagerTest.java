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

import com.fasterxml.jackson.core.JsonProcessingException;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.invoke.MethodHandles;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
  private static Pair<Integer, Process> processBoundHttp;
  private static Pair<Integer, Process> processAdvertisedHttps;

  @BeforeClass
  public static void beforeClass() throws Exception {
    boolean isWindows = random().nextBoolean();
    String PID_SUFFIX = isWindows ? ".port" : ".pid";
    log.info("Simulating pid file on {}", isWindows ? "Windows" : "Linux");
    processHttp = createProcess(findAvailablePort(), false, null, null);
    processHttps = createProcess(findAvailablePort(), true, "127.0.0.1", null);
    // The mock process does not actually bind to these hosts, they are only command-line markers
    processBoundHttp = createProcess(findAvailablePort(), false, "10.99.99.99", null);
    processAdvertisedHttps =
        createProcess(findAvailablePort(), true, "10.99.99.99", "myhost.example.com");
    for (Pair<Integer, Process> p :
        List.of(processHttp, processHttps, processBoundHttp, processAdvertisedHttps)) {
      awaitReady(p.getValue());
    }
    SolrProcessManager.enableTestingMode = true;
    System.setProperty("solr.port.listen", Integer.toString(processHttp.getKey()));
    Path pidDir = createTempDir("solr-pid-dir");
    System.setProperty("solr.pid.dir", pidDir.toString());
    for (Pair<Integer, Process> p :
        List.of(processHttp, processHttps, processBoundHttp, processAdvertisedHttps)) {
      long pidFileValue = isWindows ? p.getKey() : p.getValue().pid();
      Files.writeString(
          pidDir.resolve("solr-" + pidFileValue + PID_SUFFIX), Long.toString(pidFileValue));
    }
    Files.writeString(pidDir.resolve("solr-99999" + PID_SUFFIX), "99999"); // Invalid
    solrProcessManager = new SolrProcessManager();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    processHttp.getValue().destroyForcibly();
    processHttps.getValue().destroyForcibly();
    processBoundHttp.getValue().destroyForcibly();
    processAdvertisedHttps.getValue().destroyForcibly();
    SolrProcessManager.enableTestingMode = false;
  }

  private static int findAvailablePort() throws IOException {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    }
  }

  @SuppressWarnings("SystemGetProperty")
  private static Pair<Integer, Process> createProcess(
      int port, boolean https, String bindHost, String advertiseHost) throws IOException {
    // Get the path to the java executable from the current JVM

    String pathSeparator = System.getProperty("path.separator");
    String classPath =
        Arrays.stream(System.getProperty("java.class.path").split(pathSeparator))
            .filter(p -> p.contains("solr") && p.contains("core") && p.contains("build"))
            .collect(Collectors.joining(pathSeparator));

    List<String> command = new ArrayList<>();
    command.add(System.getProperty("java.home") + "/bin/java");
    command.add("-Dsolr.port.listen=" + port);
    command.add("-DisHttps=" + https);
    command.add("-DmockSolr=true");
    if (bindHost != null) {
      command.add("-Dsolr.host.bind=" + bindHost);
    }
    if (advertiseHost != null) {
      command.add("-Dsolr.host.advertise=" + advertiseHost);
    }
    command.add("-cp");
    command.add(classPath);
    command.add("org.apache.solr.cli.SolrProcessManagerTest$MockSolrProcess");
    command.add(https ? "--module=https" : "--module=http");
    return new Pair<>(port, new ProcessBuilder(command).start());
  }

  /** Waits for the mock process to print its ready line, so the processes can start in parallel */
  private static void awaitReady(Process process) throws IOException {
    try (InputStream is = process.getInputStream();
        InputStreamReader isr = new InputStreamReader(is, StandardCharsets.UTF_8);
        BufferedReader br = new BufferedReader(isr)) {
      System.out.println(br.readLine());
    }
  }

  public void testGetLocalUrl() {
    assertFalse(solrProcessManager.getAllRunning().isEmpty());
    SolrProcess http = solrProcessManager.processForPort(processHttp.getKey()).orElseThrow();
    assertEquals("http://localhost:" + http.port() + "/solr", http.getLocalUrl());
    SolrProcess https = solrProcessManager.processForPort(processHttps.getKey()).orElseThrow();
    assertEquals("https://localhost:" + https.port() + "/solr", https.getLocalUrl());
    // Non-loopback bind host is used for the local URL
    SolrProcess bound = solrProcessManager.processForPort(processBoundHttp.getKey()).orElseThrow();
    assertEquals("http://10.99.99.99:" + bound.port() + "/solr", bound.getLocalUrl());
    // Advertised host wins over the bind host
    SolrProcess advertised =
        solrProcessManager.processForPort(processAdvertisedHttps.getKey()).orElseThrow();
    assertEquals(
        "https://myhost.example.com:" + advertised.port() + "/solr", advertised.getLocalUrl());
  }

  public void testLocalConnectHost() {
    // No advertise host: bind host decides, loopback and wildcard binds map to localhost
    assertEquals("localhost", localConnectHost(null, null));
    assertEquals("localhost", localConnectHost(null, ""));
    assertEquals("localhost", localConnectHost(null, "0.0.0.0"));
    assertEquals("localhost", localConnectHost(null, "::"));
    assertEquals("localhost", localConnectHost(null, "127.0.0.1"));
    assertEquals("localhost", localConnectHost(null, "::1"));
    assertEquals("localhost", localConnectHost(null, "localhost"));
    assertEquals("10.0.0.5", localConnectHost(null, "10.0.0.5"));
    assertEquals("myhost.example.com", localConnectHost(null, "myhost.example.com"));
    assertEquals("[fe80::1]", localConnectHost(null, "fe80::1"));
    // Advertise host is preferred over the bind host when set
    assertEquals("myhost.example.com", localConnectHost("myhost.example.com", "10.0.0.5"));
    assertEquals("myhost.example.com", localConnectHost("myhost.example.com", null));
    assertEquals("localhost", localConnectHost("localhost", "10.0.0.5"));
    // Blank advertise host falls back to the bind host
    assertEquals("10.0.0.5", localConnectHost("", "10.0.0.5"));
    assertEquals("10.0.0.5", localConnectHost(" ", "10.0.0.5"));
  }

  private static String localConnectHost(String advertiseHost, String bindHost) {
    return SolrProcessManager.localConnectHost(
        Optional.ofNullable(advertiseHost), Optional.ofNullable(bindHost));
  }

  public void testWaitForProcessOnPort() throws Exception {
    assertTrue(solrProcessManager.waitForProcessOnPort(processHttp.getKey(), 0).isPresent());
    assertTrue(solrProcessManager.waitForProcessOnPort(0, 0).isEmpty());
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
    assertEquals(4, processes.size());
  }

  public void testGetAllRunning() {
    Collection<SolrProcess> processes = solrProcessManager.getAllRunning();
    assertEquals(4, processes.size());
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

  public void testParseWindowsPidToCommandLineJson() throws JsonProcessingException {
    String jsonResponseFromPowershell =
        "[{\"ProcessId\": 9356, \"CommandLine\":  \"date\"}, {\"ProcessId\": 4736, \"CommandLine\":  null}\n]";
    Map<Long, String> pidToCommandLine =
        SolrProcessManager.parseWindowsPidToCommandLineJson(jsonResponseFromPowershell);
    assertEquals(1, pidToCommandLine.size());
    assertEquals("date", pidToCommandLine.get(9356L));
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
