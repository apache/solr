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
import java.util.List;

/** Response body for the {@code /api/node/threads} endpoint. */
public class NodeThreadsResponse extends SolrJerseyResponse {

  @JsonProperty("system")
  public SystemInfo system;

  public static class SystemInfo {

    @JsonProperty("threadCount")
    public ThreadCount threadCount;

    @JsonProperty("deadlocks")
    public List<ThreadEntry> deadlocks;

    @JsonProperty("threadDump")
    public List<ThreadEntry> threadDump;
  }

  public static class ThreadCount {

    @JsonProperty("current")
    public long current;

    @JsonProperty("peak")
    public long peak;

    @JsonProperty("daemon")
    public long daemon;
  }

  public static class ThreadEntry {

    @JsonProperty("thread")
    public ThreadInfo thread;
  }

  public static class ThreadInfo {

    @JsonProperty("id")
    public long id;

    @JsonProperty("name")
    public String name;

    @JsonProperty("state")
    public String state;

    @JsonProperty("lock")
    public String lock;

    @JsonProperty("lock-waiting")
    public LockWaiting lockWaiting;

    @JsonProperty("synchronizers-locked")
    public List<String> synchronizersLocked;

    @JsonProperty("monitors-locked")
    public List<String> monitorsLocked;

    @JsonProperty("suspended")
    public Boolean suspended;

    @JsonProperty("native")
    public Boolean nativeThread;

    @JsonProperty("cpuTime")
    public String cpuTime;

    @JsonProperty("userTime")
    public String userTime;

    @JsonProperty("stackTrace")
    public List<String> stackTrace;
  }

  public static class LockWaiting {

    @JsonProperty("name")
    public String name;

    @JsonProperty("owner")
    public LockOwner owner;
  }

  public static class LockOwner {

    @JsonProperty("name")
    public String name;

    @JsonProperty("id")
    public long id;
  }
}
