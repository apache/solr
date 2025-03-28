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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.lucene.util.Constants;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.EnvUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Class to interact with Solr OS processes */
public class SolrProcessManager {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final Map<Long, SolrProcess> pidProcessMap;
  private final Map<Integer, SolrProcess> portProcessMap;
  private final Path pidDir;
  private static final Pattern pidFilePattern = Pattern.compile("^solr-([0-9]+)\\.(pid|port)$");
  // Set this to true during testing to allow the SolrProcessManager to find only mock Solr
  // processes
  public static boolean enableTestingMode = false;
  private static final Map<Long, String> pidToWindowsCommandLineMap = new HashMap<>();

  public SolrProcessManager() {
    if (Constants.WINDOWS) {
      pidToWindowsCommandLineMap.putAll(commandLinesWindows());
    }
    pidProcessMap =
        ProcessHandle.allProcesses()
            .filter(p -> p.info().command().orElse("").contains("java"))
            .filter(p -> commandLine(p).orElse("").contains("-Djetty.port="))
            .filter(
                p -> !enableTestingMode || commandLine(p).orElse("").contains("-DmockSolr=true"))
            .collect(
                Collectors.toUnmodifiableMap(
                    ProcessHandle::pid,
                    ph ->
                        new SolrProcess(
                            ph.pid(), parsePortFromProcess(ph).orElseThrow(), isProcessSsl(ph))));
    portProcessMap =
        pidProcessMap.values().stream().collect(Collectors.toUnmodifiableMap(p -> p.port, p -> p));
    String solrInstallDir = EnvUtils.getProperty(SOLR_INSTALL_DIR_ATTRIBUTE);
    pidDir =
        Paths.get(
            EnvUtils.getProperty(
                "solr.pid.dir",
                solrInstallDir != null
                    ? solrInstallDir + "/bin"
                    : System.getProperty("java.io.tmpdir")));
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

  /**
   * Scans the PID directory for Solr PID files and returns a list of SolrProcesses for each running
   * Solr instance. If a PID file is found but no process is running, the PID file is deleted. On
   * Windows, the file is a 'PORT' file containing the port number.
   *
   * @return a list of SolrProcesses for each running Solr instance
   */
  public Collection<SolrProcess> scanSolrPidFiles() throws IOException {
    List<SolrProcess> processes = new ArrayList<>();
    try (Stream<Path> pidFiles =
        Files.list(pidDir)
            .filter(p -> pidFilePattern.matcher(p.getFileName().toString()).matches())) {
      for (Path p : pidFiles.collect(Collectors.toList())) {
        Optional<SolrProcess> process;
        if (p.toString().endsWith(".port")) {
          // On Windows, the file is a 'PORT' file containing the port number.
          Integer port = Integer.valueOf(Files.readAllLines(p).get(0));
          process = processForPort(port);
        } else {
          // On Linux, the file is a 'PID' file containing the process ID.
          Long pid = Long.valueOf(Files.readAllLines(p).get(0));
          process = getProcessForPid(pid);
        }
        if (process.isPresent()) {
          processes.add(process.get());
        } else {
          log.warn("PID file {} found, but no process running. Deleting PID file", p.getFileName());
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
        arguments(ph).stream()
            .filter(a -> a.contains("-Djetty.port="))
            .map(s -> s.split("=")[1])
            .findFirst();
    return portStr.isPresent() ? portStr.map(Integer::parseInt) : Optional.empty();
  }

  private boolean isProcessSsl(ProcessHandle ph) {
    return arguments(ph).stream()
        .anyMatch(
            arg -> List.of("--module=https", "--module=ssl", "--module=ssl-reload").contains(arg));
  }

  /**
   * Gets the command line of a process as a string. For Windows we need to fetch command lines
   * using a PowerShell command.
   *
   * @param ph the process handle
   * @return the command line of the process
   */
  private static Optional<String> commandLine(ProcessHandle ph) {
    if (!Constants.WINDOWS) {
      return ph.info().commandLine();
    } else {
      return Optional.ofNullable(pidToWindowsCommandLineMap.get(ph.pid()));
    }
  }

  /**
   * Gets the command lines of all java processes on Windows using PowerShell.
   *
   * @return a map of process IDs to command lines
   */
  private static Map<Long, String> commandLinesWindows() {
    try {
      StopWatch stopWatch = StopWatch.createStarted();
      Process process =
          new ProcessBuilder(
                  "powershell.exe",
                  "-Command",
                  "Get-CimInstance -ClassName Win32_Process | Where-Object { $_.Name -like '*java*' } | Select-Object ProcessId, CommandLine | ConvertTo-Json -Depth 1")
              .redirectErrorStream(true)
              .start();
      try (InputStreamReader inputStreamReader =
              new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8);
          BufferedReader reader = new BufferedReader(inputStreamReader)) {
        StringBuilder stringResponse = new StringBuilder();
        String line;
        while ((line = reader.readLine()) != null) {
          stringResponse.append(line);
        }
        log.debug("Looking up PIDs on Windows took {} ms", stopWatch.getTime());
        return parsePidToCommandLineJson(stringResponse.toString());
      }
    } catch (IOException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Error getting command lines for Windows");
    }
  }

  static Map<Long, String> parsePidToCommandLineJson(String jsonString)
      throws JsonProcessingException {
    // Json format: [{"ProcessId": 1234, "CommandLine": "java foo"}]
    ObjectMapper mapper = new ObjectMapper();
    List<ProcessInfo> processInfoList =
        mapper.readValue(jsonString, new TypeReference<List<ProcessInfo>>() {});
    return processInfoList.stream()
        .filter(p -> p.CommandLine != null)
        .collect(Collectors.toMap(p -> p.ProcessId, p -> p.CommandLine));
  }

  public static class ProcessInfo {
    public long ProcessId;
    public String CommandLine;
  }

  /**
   * Gets the arguments of a process as a list of strings. With workaround for Windows.
   *
   * @param ph the process handle
   * @return the arguments of the process
   */
  private static List<String> arguments(ProcessHandle ph) {
    if (!Constants.WINDOWS) {
      return Arrays.asList(ph.info().arguments().orElse(new String[] {}));
    } else {
      return Arrays.asList(commandLine(ph).orElse("").split("\\s+"));
    }
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
