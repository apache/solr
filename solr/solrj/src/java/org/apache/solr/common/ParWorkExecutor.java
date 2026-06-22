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
package org.apache.solr.common;

import org.apache.solr.common.util.CloseTracker;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.SolrQTP;
import org.apache.solr.logging.MDCLoggingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class ParWorkExecutor extends ThreadPoolExecutor {
  private static final Logger log = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  private static class ParWorkFutureTask<T> extends FutureTask {

    final String threadName;

    public ParWorkFutureTask(String threadName, Callable callable) {
      super(callable);
      this.threadName = threadName;
    }

    public ParWorkFutureTask(String threadName, Runnable runnable, Object value) {
      super(runnable, value);
      this.threadName = threadName;
    }
  }

  public static final int KEEP_ALIVE_TIME = 1000;


  private CloseTracker closeTracker;

  public ParWorkExecutor(String name, int maxPoolsSize) {
    this(name, Math.min(6, maxPoolsSize), maxPoolsSize, KEEP_ALIVE_TIME, new LinkedTransferQueue<>());
  }

  public ParWorkExecutor(String name, int corePoolsSize, int maxPoolsSize,
      int keepalive, BlockingQueue<Runnable> workQueue) {
    super(corePoolsSize, Math.max(corePoolsSize, maxPoolsSize), keepalive, TimeUnit.MILLISECONDS, workQueue, new ParWorkThreadFactory(name));
   // if (workQueue instanceof LinkedTransferQueue) {
      setRejectedExecutionHandler(new AbortPolicy() {
        public void rejectedExecution(Runnable r, ThreadPoolExecutor e) {
          // Do NOT block-enqueue onto a pool that is shutting down: the queue would never drain and
          // the task would be stranded forever. Surface the rejection so callers (e.g. ParWork.close)
          // run the task inline instead.
          if (e.isShutdown()) {
            throw new RejectedExecutionException("Task " + r + " rejected from shut down executor " + e);
          }
          try {
            e.getQueue().put(r);
          } catch (InterruptedException e1) {
            Thread.currentThread().interrupt();
            throw new RejectedExecutionException(e1);
          }
        }

      });
   // } else {
   //   setRejectedExecutionHandler(new CallerRunsPolicy());
  //  }
    assert (closeTracker = new CloseTracker(false)) != null;
  }

  public void shutdown() {

    assert closeTracker != null ? closeTracker.close() : true;

    super.shutdown();
  }

  public List<Runnable> shutdownNow() {
    return super.shutdownNow();
  }

  public void enableCloseLock() {
    if (this.closeTracker != null) {
      this.closeTracker.enableCloseLock();
    }
  }

  public void disableCloseLock() {
    if (this.closeTracker != null) {
      this.closeTracker.disableCloseLock();
    }
  }

  protected static <T> RunnableFuture<T> newTaskFor(String threadName, Runnable runnable, T value) {
    return new ParWorkFutureTask<T>(threadName, runnable, value);
  }

  protected static <T> RunnableFuture<T> newTaskFor(String threadName, Callable<T> callable) {
    return new ParWorkFutureTask<T>(threadName, callable);
  }

  @Override
  protected void beforeExecute(Thread t, Runnable r) {
    ExecutorUtil.setServerThreadFlag(Boolean.TRUE);
    if (r instanceof ParWorkFutureTask) {
      Thread.currentThread().setName(((ParWorkFutureTask) r).threadName);
    }
  }

  @Override
  protected void afterExecute(Runnable r, Throwable t) {
    ExecutorUtil.setServerThreadFlag(null);
    if (r instanceof ParWorkFutureTask) {
      ((SolrThread)Thread.currentThread()).resetName();
      //((SolrThread)Thread.currentThread()).clearExecutor();
    }
    // NOTE: JavaBinCodec.THREAD_LOCAL_ARR no longer exists in this fork, so that cleanup is gone.
    // Clear thread-locals registered for cleanup so they are not retained on this pooled thread
    // between unrelated tasks. SolrQTP.threadLocals is the registry built precisely for this.
    for (ThreadLocal tl : SolrQTP.threadLocals) {
      tl.remove();
    }
    MDCLoggingContext.reset();
  }

  @Override
  public void execute(Runnable runnable) {
    try {
      super.execute(runnable);
    } catch (RejectedExecutionException t) {
      throw t;
    } catch (Throwable t) {
      log.error("ParWorkExecutor exception in execute", t);
      throw t;
    }
  }

  private static class ParWorkThreadFactory implements ThreadFactory {
    final AtomicInteger threadNumber;
    final ThreadGroup group;
    private final String name;

    public ParWorkThreadFactory(String name) {
      this.name = name;
      threadNumber = new AtomicInteger(1);
      SecurityManager s = System.getSecurityManager();
      group = (s != null) ? s.getThreadGroup() : Thread.currentThread().getThreadGroup();
    }

    @Override
    public Thread newThread(Runnable r) {
      SolrThread thread = new SolrThread(group, r, name + threadNumber.getAndIncrement());
      thread.setDaemon(true);
      return thread;
    }
  }
}
