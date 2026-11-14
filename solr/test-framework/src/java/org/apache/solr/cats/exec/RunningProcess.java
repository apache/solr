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

package org.apache.solr.cats.exec;

import java.util.Arrays;
import java.util.List;

/**
 * One process of the program under test that the harness is tracking.
 *
 * <p>A process is identified by its pid, never by a port: a long-running command-line tool and a
 * converter that runs to completion have no ports at all, a server has one, and a backend may have
 * several with different roles. Ports are an attribute, and the list is in the order the program's
 * {@link org.apache.solr.cats.env.PortLayout} declares them.
 *
 * @param pid the operating system process id
 * @param ports the ports it serves on, empty when it serves none
 */
public record RunningProcess(long pid, List<Integer> ports) {

  public RunningProcess {
    ports = List.copyOf(ports);
  }

  /** A process on {@code ports}, or on none when none are given. */
  public static RunningProcess of(long pid, int... ports) {
    return new RunningProcess(pid, Arrays.stream(ports).boxed().toList());
  }

  /** Whether any of this process' ports is {@code port}. */
  public boolean hasPort(int port) {
    return ports.contains(port);
  }

  /** As it reads in a message or a log line. */
  @Override
  public String toString() {
    return ports.isEmpty() ? "pid " + pid : "pid " + pid + " on " + ports;
  }
}
