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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.TimeUnit;

//ForkJoinPool(int parallelism,
//    ForkJoinWorkerThreadFactory factory,
//    UncaughtExceptionHandler handler,
//    boolean asyncMode)

public class ForJoinParWorkExecutor extends ForkJoinPool {
  private static final Logger log = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  public void prestartAllCoreThreads() {
    // noop
  }

  public void setKeepAliveTime(int i, TimeUnit nanoseconds) {
  }

  public void allowCoreThreadTimeOut(boolean b) {
  }

  private static class ParWorkFutureTask<T> extends FutureTask {

    final String threadName;
    private volatile String oldThreadName;

    public ParWorkFutureTask(String threadName, Callable callable) {
      super(callable);
      this.threadName = threadName;
    }

    public ParWorkFutureTask(String threadName, Runnable runnable, Object value) {
      super(runnable, value);
      this.threadName = threadName;
    }

    public void updateThreadName() {
      if (oldThreadName == null) {
        this.oldThreadName = Thread.currentThread().getName();
        Thread.currentThread().setName(threadName);
      } else {
        Thread.currentThread().setName(oldThreadName);
        oldThreadName = null;
      }
    }
  }

  public static final int KEEP_ALIVE_TIME = 5000;


  private CloseTracker closeTracker;

//  public ParWorkExecutor(String name, int maxPoolsSize) {
//    this(name, 4, maxPoolsSize, KEEP_ALIVE_TIME, new LinkedBlockingDeque<>());
//  }

//  public ParWorkExecutor(String name, int corePoolsSize, int maxPoolsSize,
//      int keepalive, BlockingQueue<Runnable> workQueue) {
//    super(corePoolsSize, Math.max(corePoolsSize, maxPoolsSize), keepalive, TimeUnit.MILLISECONDS, workQueue, new ThreadFactory() {
//      AtomicInteger threadNumber = new AtomicInteger(1);
//      ThreadGroup group;
//
//      {
//        SecurityManager s = System.getSecurityManager();
//        group = (s != null) ? s.getThreadGroup() : Thread.currentThread().getThreadGroup();
//      }
//
//      @Override
//      public Thread newThread(Runnable r) {
//        SolrThread thread = new SolrThread(group, r, name + threadNumber.getAndIncrement());
//        thread.setDaemon(false);
//        return thread;
//      }
//    });
//    if (workQueue instanceof TransferQueue) {
//      setRejectedExecutionHandler(new AbortPolicy() {
//        public void rejectedExecution(Runnable r, ThreadPoolExecutor e) {
//          try {
//            getQueue().put(r);
//          } catch (InterruptedException e1) {
//            Thread.currentThread().interrupt();
//          }
//        }
//
//      });
//    } else {
//      setRejectedExecutionHandler(new CallerRunsPolicy());
//    }
//    assert (closeTracker = new CloseTracker(false)) != null;
//  }


  private static class SolrForkJoinWorkerThread extends ForkJoinWorkerThread {

    /**
     * Creates a ForkJoinWorkerThread operating in the given pool.
     *
     * @param pool the pool this thread works in
     * @throws NullPointerException if pool is null
     */
    protected SolrForkJoinWorkerThread(ForkJoinPool pool) {
      super(pool);
    }
  }

  public ForJoinParWorkExecutor(String name, int maxPoolsSize) {
    super(64, new MyForkJoinWorkerThreadFactory(), (t, e) -> {
    }, false);

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

//  @Override
//  protected void beforeExecute(Thread t, Runnable r) {
//    if (r instanceof ParWorkFutureTask) {
//      ((ParWorkFutureTask) r).updateThreadName();
//    }
//  }
//
//  @Override
//  protected void afterExecute(Runnable r, Throwable t) {
//    if (r instanceof ParWorkFutureTask) {
//      ((ParWorkFutureTask) r).updateThreadName();
//    }
//  }

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

  private static class MyForkJoinWorkerThreadFactory implements ForkJoinWorkerThreadFactory {
    @Override public ForkJoinWorkerThread newThread(ForkJoinPool pool) {
      return new SolrForkJoinWorkerThread(pool);
    }
  }
}
