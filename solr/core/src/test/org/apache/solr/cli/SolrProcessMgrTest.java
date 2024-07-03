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

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import org.apache.commons.math3.util.Pair;
import org.apache.solr.SolrTestCase;
import org.apache.solr.cli.SolrProcessMgr.SolrProcess;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class SolrProcessMgrTest extends SolrTestCase {
  private static SolrProcessMgr solrProcessMgr;
  private static Pair<Integer, Process> processHttp;
  private static Pair<Integer, Process> processHttps;
  private static Path pidDir;

  @BeforeClass
  public static void beforeClass() throws Exception {
    processHttp = createProcess(findAvailablePort(), false);
    processHttps = createProcess(findAvailablePort(), true);
    SolrProcessMgr.enableTestingMode = true;
    System.setProperty("jetty.port", Integer.toString(processHttp.getKey()));
    pidDir = Files.createTempDirectory("solr-pid-dir").toAbsolutePath();
    pidDir.toFile().deleteOnExit();
    System.setProperty("solr.pid.dir", pidDir.toString());
    Files.writeString(
        pidDir.resolve("solr-" + processHttp.getKey() + ".pid"),
        Long.toString(processHttp.getValue().pid()));
    Files.writeString(
        pidDir.resolve("solr-" + processHttps.getKey() + ".pid"),
        Long.toString(processHttps.getValue().pid()));
    Files.writeString(pidDir.resolve("solr-99999.pid"), "99999"); // Invalid
    solrProcessMgr = new SolrProcessMgr();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    processHttp.getValue().destroyForcibly();
    processHttps.getValue().destroyForcibly();
    SolrProcessMgr.enableTestingMode = false;
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
    ProcessBuilder processBuilder =
        new ProcessBuilder(
            System.getProperty("java.home") + "/bin/java",
            "-cp",
            System.getProperty("java.class.path"),
            "-Djetty.port=" + port,
            "-DisHttps=" + https,
            "-DmockSolr=true",
            https ? "--module=http" : "--module=https",
            "SolrProcessMgrTest$MockSolrProcess");

    // Start the process
    Process process = processBuilder.start();
    return new Pair<>(port, process);
  }

  public void testGetLocalUrl() {
    solrProcessMgr
        .getAllRunning()
        .forEach(
            p -> {
              assertEquals(
                  (p.isHttps() ? "https" : "http") + "://localhost:" + p.getPort() + "/solr",
                  p.getLocalUrl());
            });
  }

  public void testIsRunningWithPort() {
    assertFalse(solrProcessMgr.isRunningWithPort(0));
    assertTrue(solrProcessMgr.isRunningWithPort(processHttp.getKey()));
    assertTrue(solrProcessMgr.isRunningWithPort(processHttps.getKey()));
  }

  public void testIsRunningWithPid() {
    assertFalse(solrProcessMgr.isRunningWithPid(0L));
    assertTrue(solrProcessMgr.isRunningWithPid(processHttp.getValue().pid()));
    assertTrue(solrProcessMgr.isRunningWithPid(processHttps.getValue().pid()));
  }

  public void testProcessForPort() {
    assertEquals(
        processHttp.getKey().intValue(),
        (solrProcessMgr.processForPort(processHttp.getKey()).orElseThrow().getPort()));
    assertEquals(
        processHttps.getKey().intValue(),
        (solrProcessMgr.processForPort(processHttps.getKey()).orElseThrow().getPort()));
  }

  public void testGetProcessForPid() {
    assertEquals(
        processHttp.getValue().pid(),
        (solrProcessMgr.getProcessForPid(processHttp.getValue().pid()).orElseThrow().getPid()));
    assertEquals(
        processHttps.getValue().pid(),
        (solrProcessMgr.getProcessForPid(processHttps.getValue().pid()).orElseThrow().getPid()));
  }

  public void testScanSolrPidFiles() throws IOException {
    Collection<SolrProcess> processes = solrProcessMgr.scanSolrPidFiles();
    assertEquals(2, processes.size());
  }

  public void testGetAllRunning() {
    Collection<SolrProcess> processes = solrProcessMgr.getAllRunning();
    assertEquals(2, processes.size());
  }

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
