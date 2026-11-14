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

import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Which processes of the program under test the harness has seen, so that each one is owned by
 * exactly one session and none outlives the JVM.
 *
 * <p>Two kinds are recorded, and cleanup treats them alike. A <em>child</em> is a direct child of a
 * shell the harness spawned, tracked by the {@link Process} handle the executor already holds — a
 * long-running command-line tool, or a converter that runs to completion. A <em>discovered</em>
 * process is one that daemonised itself away from that shell, and is found again by the marker its
 * command line carries.
 *
 * <p>One registry per harness environment rather than one per JVM: a library may be instantiated
 * twice in a process, and static bookkeeping would then have two harnesses stopping each other's
 * processes.
 */
public final class ProgramRegistry {

  /** Every pid the registry has ever seen, so the shutdown hook can finish the job. */
  private final Set<Long> recordedPids = ConcurrentHashMap.newKeySet();

  /** Children still tracked, by pid, so that a handle is at hand to kill the whole tree. */
  private final Map<Long, Process> children = new ConcurrentHashMap<>();

  /** Discovered processes already accounted for, so one is owned by the session that started it. */
  private final Map<Long, RunningProcess> discovered = new ConcurrentHashMap<>();

  public ProgramRegistry() {
    Runtime.getRuntime().addShutdownHook(new Thread(this::killRecorded, "cats-cleanup"));
  }

  /** Records a process the harness spawned as a direct child of one of its shells. */
  public void recordChild(Process child) {
    children.put(child.pid(), child);
    recordedPids.add(child.pid());
  }

  /** Drops a child a session has just stopped, or that has exited on its own. */
  public void forgetChild(Process child) {
    children.remove(child.pid(), child);
    recordedPids.remove(child.pid());
  }

  /**
   * Records a process discovery has just reported, and says whether it is new — a pid already
   * recorded has been accounted for and needs no second owner.
   */
  public boolean recordIfNew(RunningProcess process) {
    if (discovered.putIfAbsent(process.pid(), process) != null) {
      return false;
    }
    recordedPids.add(process.pid());
    return true;
  }

  /** Drops a discovered process a session has just stopped. */
  public void forget(RunningProcess process) {
    discovered.remove(process.pid());
    recordedPids.remove(process.pid());
  }

  /**
   * Last resort, from the JVM shutdown hook: kill everything this registry still tracks, a child by
   * its handle and a discovered process by its pid.
   */
  void killRecorded() {
    for (Long pid : recordedPids) {
      Process child = children.get(pid);
      if (child != null) {
        if (child.isAlive()) {
          ProcessKill.killTree(child.toHandle(), Duration.ofSeconds(5));
        }
        continue;
      }
      if (ProcessHandle.of(pid).map(ProcessHandle::isAlive).orElse(false)) {
        ProcessKill.killTree(pid, Duration.ofSeconds(5));
      }
    }
  }
}
