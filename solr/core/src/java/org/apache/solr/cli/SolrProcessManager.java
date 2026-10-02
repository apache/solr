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

import static org.apache.solr.servlet.CoreContainerProvider.SOLR_INSTALL_DIR;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.io.IOUtils;
import org.apache.lucene.util.Constants;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.EnvUtils;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.util.TimeOut;
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
            .filter(p -> commandLine(p).orElse("").contains("-Dsolr.port.listen="))
            .filter(
                p -> !enableTestingMode || commandLine(p).orElse("").contains("-DmockSolr=true"))
            .collect(
                Collectors.toUnmodifiableMap(
                    ProcessHandle::pid,
                    ph ->
                        new SolrProcess(
                            ph.pid(),
                            parseSyspropFromProcess(ph, "solr.port.listen")
                                .map(Integer::parseInt)
                                .orElseThrow(),
                            isProcessSsl(ph),
                            localConnectHost(
                                parseSyspropFromProcess(ph, "solr.host.advertise"),
                                parseSyspropFromProcess(ph, "solr.host.bind")))));
    portProcessMap =
        pidProcessMap.values().stream().collect(Collectors.toUnmodifiableMap(p -> p.port, p -> p));
    String solrInstallDir = EnvUtils.getProperty(SOLR_INSTALL_DIR);
    pidDir =
        Path.of(
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
      for (Path p : pidFiles.toList()) {
        Optional<SolrProcess> process;
        if (p.toString().endsWith(".port")) {
          // On Windows, the file is a 'PORT' file containing the port number.
          Integer port = Integer.valueOf(Files.readAllLines(p).getFirst());
          process = processForPort(port);
        } else {
          // On Linux, the file is a 'PID' file containing the process ID.
          Long pid = Long.valueOf(Files.readAllLines(p).getFirst());
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

  /** Parses the value of the given system property from the process' command line arguments */
  private static Optional<String> parseSyspropFromProcess(ProcessHandle ph, String sysprop) {
    return arguments(ph).stream()
        .filter(a -> a.contains("-D" + sysprop + "="))
        .map(s -> s.split("=", 2)[1])
        .findFirst();
  }

  /**
   * Returns the process listening on the given port, if found, waiting up to {@code maxWaitSecs}
   * for it to appear. A newly started process may not be visible in the process table right away,
   * so the table is re-scanned once a second until the deadline.
   */
  public Optional<SolrProcess> waitForProcessOnPort(int port, int maxWaitSecs)
      throws InterruptedException {
    Optional<SolrProcess> proc = processForPort(port);
    TimeOut timeOut = new TimeOut(maxWaitSecs, TimeUnit.SECONDS, TimeSource.NANO_TIME);
    while (proc.isEmpty() && !timeOut.hasTimedOut()) {
      timeOut.sleep(1000);
      proc = new SolrProcessManager().processForPort(port);
    }
    return proc;
  }

  /**
   * Resolves the host to use when connecting locally to a Solr process. The advertised host is
   * preferred when set, as that is the name the node is reachable by and, with SSL, the name its
   * certificate is issued for. Otherwise the bind host is used if it is a specific non-loopback
   * address. Wildcard and loopback binds are reachable as {@code localhost}. IPv6 literals are
   * bracketed for use in URLs.
   */
  static String localConnectHost(Optional<String> advertiseHost, Optional<String> bindHost) {
    String host =
        advertiseHost
            .map(String::trim)
            .filter(h -> !h.isEmpty())
            .orElseGet(() -> bindHost.map(String::trim).orElse(""));
    return switch (host) {
      case "", "0.0.0.0", "::", "[::]", "127.0.0.1", "::1", "[::1]", "localhost" -> "localhost";
      default -> host.contains(":") && !host.startsWith("[") ? "[" + host + "]" : host;
    };
  }

  private boolean isProcessSsl(ProcessHandle ph) {
    return arguments(ph).stream()
        .anyMatch(
            arg -> List.of("--module=https", "--module=ssl", "--module=ssl-reload").contains(arg));
  }

  /**
   * Gets the command line of a process as a string. For Windows, we need to fetch command lines
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
      Process process =
          new ProcessBuilder(
                  "powershell.exe",
                  "-Command",
                  "Get-CimInstance -ClassName Win32_Process | Where-Object { $_.Name -like '*java*' } | Select-Object ProcessId, CommandLine | ConvertTo-Json -Depth 1")
              .redirectErrorStream(true)
              .start();
      String output = IOUtils.toString(process.getInputStream(), StandardCharsets.UTF_8);
      int exitCode = process.waitFor();
      if (exitCode != 0) {
        String errorText = IOUtils.toString(process.getErrorStream(), StandardCharsets.UTF_8);
        throw new SolrException(
            SolrException.ErrorCode.SERVER_ERROR,
            "Error getting command lines for Windows: " + errorText);
      }
      return parseWindowsPidToCommandLineJson(output);
    } catch (IOException e) {
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR, "Error getting command lines for Windows");
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR,
          "Interrupted while getting command lines for Windows");
    }
  }

  static Map<Long, String> parseWindowsPidToCommandLineJson(String jsonString)
      throws JsonProcessingException {
    // Json format: [{"ProcessId": 1234, "CommandLine": "java foo"}]
    ObjectMapper mapper = new ObjectMapper();
    List<WindowsProcessInfo> processInfoList =
        mapper.readValue(jsonString, new TypeReference<>() {});
    return processInfoList.stream()
        .filter(p -> p.CommandLine != null)
        .collect(Collectors.toMap(p -> p.ProcessId, p -> p.CommandLine));
  }

  public static class WindowsProcessInfo {
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

  /**
   * Represents a running Solr process. The {@code host} is the host to use when connecting to the
   * process from the local machine, i.e. the advertised host if set, else the bind host if bound to
   * a specific address, else {@code localhost}.
   */
  public record SolrProcess(long pid, int port, boolean isHttps, String host) {

    public String getLocalUrl() {
      return String.format(Locale.ROOT, "%s://%s:%s/solr", isHttps ? "https" : "http", host, port);
    }
  }
}
