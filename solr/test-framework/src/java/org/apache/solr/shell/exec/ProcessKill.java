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

package org.apache.solr.shell.exec;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;

/** Force-kills a process and everything below it. */
public final class ProcessKill {

  private ProcessKill() {}

  /**
   * Kills {@code handle}'s descendants first, then the process itself, and waits up to {@code
   * timeout} for it to be gone.
   *
   * <p>A detached Solr node ({@code nohup} on Unix, {@code start /B} on Windows) is re-parented
   * once its launching shell exits, so it will generally <em>not</em> be a descendant of anything
   * the harness started — {@link SolrNodes} is how the harness finds Solr. This method is for a
   * command process that overran its timeout, and for a PID discovery returned.
   *
   * @return whether the process is no longer alive
   */
  public static boolean killTree(ProcessHandle handle, Duration timeout) {
    List<ProcessHandle> descendants = handle.descendants().toList();
    for (ProcessHandle child : descendants) {
      child.destroyForcibly();
    }
    handle.destroyForcibly();
    long deadline = System.nanoTime() + timeout.toNanos();
    while (handle.isAlive() && System.nanoTime() < deadline) {
      try {
        TimeUnit.MILLISECONDS.sleep(50);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
    }
    return !handle.isAlive();
  }

  /** Kills the given PID and its descendants, if that PID is still alive. */
  public static boolean killTree(long pid, Duration timeout) {
    return ProcessHandle.of(pid).map(h -> killTree(h, timeout)).orElse(true);
  }
}
