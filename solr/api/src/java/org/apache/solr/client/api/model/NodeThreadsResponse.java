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
package org.apache.solr.client.api.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.v3.oas.annotations.media.Schema;
import java.util.List;

/** Response body for the {@code /api/node/threads} endpoint. */
public class NodeThreadsResponse extends SolrJerseyResponse {

  @Schema(description = "Thread information for the receiving JVM.")
  @JsonProperty("system")
  public SystemInfo system;

  public static class SystemInfo {

    @Schema(description = "Live and peak platform-thread counts.")
    @JsonProperty("threadCount")
    public ThreadCount threadCount;

    @Schema(description = "Threads detected in a deadlock. Omitted when no deadlock is detected.")
    @JsonProperty("deadlocks")
    public List<ThreadEntry> deadlocks;

    @Schema(description = "Snapshot of live platform threads, each wrapped in a thread property.")
    @JsonProperty("threadDump")
    public List<ThreadEntry> threadDump;
  }

  public static class ThreadCount {

    @Schema(description = "Current number of live platform threads.")
    @JsonProperty("current")
    public long current;

    @Schema(
        description =
            "Peak live platform-thread count since JVM startup or the last peak-count reset.")
    @JsonProperty("peak")
    public long peak;

    @Schema(description = "Current number of live daemon platform threads.")
    @JsonProperty("daemon")
    public long daemon;
  }

  public static class ThreadEntry {

    @Schema(description = "Details of one platform thread.")
    @JsonProperty("thread")
    public ThreadInfo thread;
  }

  public static class ThreadInfo {

    @Schema(description = "JVM thread identifier.")
    @JsonProperty("id")
    public long id;

    @Schema(description = "Thread name.")
    @JsonProperty("name")
    public String name;

    @Schema(description = "JVM thread state, such as RUNNABLE, BLOCKED, or WAITING.")
    @JsonProperty("state")
    public String state;

    @Schema(description = "Name of the lock the thread is waiting for, if any.")
    @JsonProperty("lock")
    public String lock;

    @Schema(description = "Lock the thread is waiting for, if any, and its owner when known.")
    @JsonProperty("lock-waiting")
    public LockWaiting lockWaiting;

    @Schema(description = "Ownable synchronizers held by this thread. Omitted when none are held.")
    @JsonProperty("synchronizers-locked")
    public List<String> synchronizersLocked;

    @Schema(description = "Object monitors held by this thread. Omitted when none are held.")
    @JsonProperty("monitors-locked")
    public List<String> monitorsLocked;

    @Schema(description = "Present and true when the thread is suspended.")
    @JsonProperty("suspended")
    public Boolean suspended;

    @Schema(description = "Present and true when the thread is executing native code.")
    @JsonProperty("native")
    public Boolean nativeThread;

    @Schema(
        description =
            "Total thread CPU time formatted in milliseconds with an ms suffix. Omitted when unsupported; a negative value indicates unavailable timing.")
    @JsonProperty("cpuTime")
    public String cpuTime;

    @Schema(
        description =
            "Thread CPU time in user mode formatted in milliseconds with an ms suffix. Omitted when unsupported; a negative value indicates unavailable timing.")
    @JsonProperty("userTime")
    public String userTime;

    @Schema(description = "Stack frames, starting with the most recent method invocation.")
    @JsonProperty("stackTrace")
    public List<String> stackTrace;
  }

  public static class LockWaiting {

    @Schema(description = "Lock class name and identity hash code.")
    @JsonProperty("name")
    public String name;

    @Schema(description = "Thread owning the lock. Omitted when no owner is known.")
    @JsonProperty("owner")
    public LockOwner owner;
  }

  public static class LockOwner {

    @Schema(description = "Name of the thread owning the lock.")
    @JsonProperty("name")
    public String name;

    @Schema(description = "JVM thread identifier.")
    @JsonProperty("id")
    public long id;
  }
}
