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

import static org.apache.solr.servlet.SolrDispatchFilter.SOLR_INSTALL_DIR_ATTRIBUTE;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.solr.common.util.EnvUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Class to interact with Solr OS processes */
public class SolrProcessMgr {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final Map<Long, SolrProcess> pidProcessMap;
  private final Map<Integer, SolrProcess> portProcessMap;
  private final Path pidDir;
  private static final Pattern pidFilePattern = Pattern.compile("^solr-([0-9]+)\\.(pid|port)$");
  // Set this to true during testing to allow the SolrProcessMgr to find only mock Solr processes
  public static boolean enableTestingMode = false;

  public SolrProcessMgr() {
    pidProcessMap =
        ProcessHandle.allProcesses()
            .filter(p -> p.info().command().orElse("").contains("java"))
            .filter(p -> p.info().commandLine().orElse("").contains("-Djetty.port="))
            .filter(
                p ->
                    !enableTestingMode
                        || p.info().commandLine().orElse("").contains("-DmockSolr=true"))
            .collect(
                Collectors.toUnmodifiableMap(
                    ProcessHandle::pid,
                    ph ->
                        new SolrProcess(
                            ph.pid(), parsePortFromProcess(ph).orElseThrow(), isProcessSsl(ph))));
    portProcessMap =
        pidProcessMap.values().stream().collect(Collectors.toUnmodifiableMap(p -> p.port, p -> p));
    pidDir =
        Paths.get(
            EnvUtils.getProperty(
                "solr.pid.dir", EnvUtils.getProperty(SOLR_INSTALL_DIR_ATTRIBUTE + "/bin", "/tmp")));
  }

  public boolean isRunningWithPort(Integer port) {
    return portProcessMap.containsKey(port);
  }

  public boolean isRunningWithPid(Long pid) {
    return pidProcessMap.containsKey(pid);
  }

  public Optional<SolrProcess> processForPort(Integer port) {
    return portProcessMap.containsKey(port)
        ? Optional.of(portProcessMap.get(port))
        : Optional.empty();
  }

  /** Return the SolrProcess for a given PID, if it is running */
  public Optional<SolrProcess> getProcessForPid(Long pid) {
    return pidProcessMap.containsKey(pid) ? Optional.of(pidProcessMap.get(pid)) : Optional.empty();
  }

  public Collection<SolrProcess> scanSolrPidFiles() throws IOException {
    List<SolrProcess> processes = new ArrayList<>();
    try (Stream<Path> pidFiles =
        Files.list(pidDir)
            .filter(p -> pidFilePattern.matcher(p.getFileName().toString()).matches())) {
      for (Path p : pidFiles.collect(Collectors.toList())) {
        Long pid = Long.valueOf(Files.readAllLines(p).get(0));
        Optional<SolrProcess> process = getProcessForPid(pid);
        if (process.isPresent()) {
          processes.add(process.get());
        } else {
          log.warn("PID file found for PID {}, but no process found. Deleting PID file", pid);
          Files.deleteIfExists(p);
        }
      }
      return processes;
    }
  }

  public Collection<SolrProcess> getAllRunning() {
    return pidProcessMap.values();
  }

  private Optional<Integer> parsePortFromProcess(ProcessHandle ph) {
    Optional<String> portStr =
        Arrays.stream(ph.info().arguments().orElse(new String[] {}))
            .filter(a -> a.contains("-Djetty.port="))
            .map(s -> s.split("=")[1])
            .findFirst();
    return portStr.isPresent() ? portStr.map(Integer::parseInt) : Optional.empty();
  }

  private boolean isProcessSsl(ProcessHandle ph) {
    return Arrays.stream(ph.info().arguments().orElse(new String[] {}))
        .anyMatch(
            arg -> List.of("--module=https", "--module=ssl", "--module=ssl-reload").contains(arg));
  }

  /** Represents a running Solr process */
  public static class SolrProcess {
    private final long pid;
    private final int port;
    private final boolean isHttps;

    public SolrProcess(long pid, int port, boolean isHttps) {
      this.pid = pid;
      this.port = port;
      this.isHttps = isHttps;
    }

    public long getPid() {
      return pid;
    }

    public int getPort() {
      return port;
    }

    public boolean isHttps() {
      return isHttps;
    }

    public String getLocalUrl() {
      return String.format(Locale.ROOT, "%s://localhost:%s/solr", isHttps ? "https" : "http", port);
    }
  }
}
