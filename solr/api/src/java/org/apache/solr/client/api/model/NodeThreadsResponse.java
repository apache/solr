package org.apache.solr.client.api.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

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
