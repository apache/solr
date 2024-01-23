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

package org.apache.solr.zero.process;

import java.lang.invoke.MethodHandles;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.solr.zero.client.ZeroFile;
import org.apache.solr.zero.client.ZeroStoreClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Generic deletion task for files located on the Zero store */
public abstract class DeleterTask implements Callable<DeleterTask.Result> {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  protected final ZeroStoreClient zeroStoreClient;

  private final Set<ZeroFile> zeroFiles;
  private final AtomicInteger attempt;
  private final Instant queuedTime;
  private final int maxAttempts;
  private final boolean allowRetry;
  private Throwable err;

  public DeleterTask(
      ZeroStoreClient zeroStoreClient,
      Set<ZeroFile> zeroFiles,
      boolean allowRetry,
      int maxAttempts) {
    this.zeroStoreClient = zeroStoreClient;
    this.zeroFiles = zeroFiles;
    this.attempt = new AtomicInteger(0);
    this.queuedTime = Instant.now();
    this.allowRetry = allowRetry;
    this.maxAttempts = maxAttempts;
  }

  public abstract String getBasePath();

  /** Return a String representing the action performed by the DeleterTask for logging purposes */
  public abstract String getActionName();

  public abstract void setMDCContext();

  @Override
  public DeleterTask.Result call() {
    List<ZeroFile> filesDeleted = new ArrayList<>();
    final Instant startTime = Instant.now();
    boolean isSuccess = true;
    boolean shouldRetry = false;
    try {
      filesDeleted.addAll(doDelete());
      attempt.incrementAndGet();
      return new DeleterTask.Result(this, filesDeleted, isSuccess, shouldRetry, err);
    } catch (Exception ex) {
      if (err == null) {
        err = ex;
      } else {
        err.addSuppressed(ex);
      }
      int attempts = attempt.incrementAndGet();
      isSuccess = false;
      log.warn("{} DeleterTask failed on attempt={}", this, attempts, ex);
      if (allowRetry) {
        if (attempts < maxAttempts) {
          shouldRetry = true;
        } else {
          log.warn("{} reached {} attempt limit. This task won't be retried.", this, maxAttempts);
        }
      }
    } finally {
      Instant now = Instant.now();
      long runTime = startTime.until(now, ChronoUnit.MILLIS);
      long startLatency = this.queuedTime.until(now, ChronoUnit.MILLIS);
      if (log.isInfoEnabled()) {
        log.info(
            "{} path={} runTime={} startLatency={} isSuccess={}",
            this,
            getBasePath(),
            runTime,
            startLatency,
            isSuccess);
      }
    }
    return new DeleterTask.Result(this, filesDeleted, isSuccess, shouldRetry, err);
  }

  public Collection<ZeroFile> doDelete() throws Exception {
    zeroStoreClient.deleteZeroFiles(zeroFiles);
    return zeroFiles;
  }

  /** Represents the result of a deletion task */
  public static class Result {
    private final DeleterTask task;
    private final Collection<ZeroFile> filesDeleted;
    private boolean isSuccess;
    private final boolean shouldRetry;
    private final Throwable err;

    public Result(
        DeleterTask task,
        Collection<ZeroFile> filesDeleted,
        boolean isSuccess,
        boolean shouldRetry,
        Throwable errs) {
      this.task = task;
      this.filesDeleted = filesDeleted;
      this.isSuccess = isSuccess;
      this.shouldRetry = shouldRetry;
      this.err = errs;
    }

    public boolean isSuccess() {
      return isSuccess;
    }

    public void updateSuccess(boolean s) {
      isSuccess = s;
    }

    public boolean shouldRetry() {
      return shouldRetry;
    }

    public DeleterTask getTask() {
      return task;
    }

    /**
     * @return the files that are being deleted. Note if the task wasn't successful there is no
     *     guarantee all of these files were in fact deleted from the Zero store
     */
    public Collection<ZeroFile> getFilesDeleted() {
      return filesDeleted;
    }

    public Throwable getError() {
      return err;
    }
  }

  @Override
  public String toString() {
    return String.format(
        Locale.ROOT,
        "action=%s totalFilesSpecified=%d allowRetry=%b queuedTime=%s attemptsTried=%d",
        getActionName(),
        zeroFiles.size(),
        allowRetry,
        queuedTime.toString(),
        attempt.get());
  }

  public int getAttempts() {
    return attempt.get();
  }
}
