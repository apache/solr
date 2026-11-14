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
 * Which instances of the program under test the harness has seen, so that each one is owned by
 * exactly one session and none outlives the JVM.
 *
 * <p>One registry per harness environment rather than one per JVM: a library may be instantiated
 * twice in a process, and static bookkeeping would then have two harnesses stopping each other's
 * processes.
 */
public final class ProgramRegistry {

  /** Every pid the registry has ever seen, so the shutdown hook can finish the job. */
  private final Set<Long> recordedPids = ConcurrentHashMap.newKeySet();

  /** Instances already accounted for, so one is owned by the session that actually started it. */
  private final Map<Integer, Long> known = new ConcurrentHashMap<>();

  public ProgramRegistry() {
    Runtime.getRuntime().addShutdownHook(new Thread(this::killRecordedPids, "cats-cleanup"));
  }

  /**
   * Records an instance discovery has just reported, and says whether it is new — a port whose pid
   * is already recorded has been accounted for and needs no second owner.
   */
  public boolean recordIfNew(RunningProgram program) {
    Long previous = known.get(program.port());
    if (previous != null && previous == program.pid()) {
      return false;
    }
    known.put(program.port(), program.pid());
    recordedPids.add(program.pid());
    return true;
  }

  /** Drops an instance a session has just stopped. */
  public void forget(int port, long pid) {
    recordedPids.remove(pid);
    known.remove(port, pid);
  }

  /** Last resort, from the JVM shutdown hook: kill everything this registry ever recorded. */
  private void killRecordedPids() {
    for (Long pid : recordedPids) {
      if (ProcessHandle.of(pid).map(ProcessHandle::isAlive).orElse(false)) {
        ProcessKill.killTree(pid, Duration.ofSeconds(5));
      }
    }
  }
}
