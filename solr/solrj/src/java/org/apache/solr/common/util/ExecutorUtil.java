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
package org.apache.solr.common.util;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

public class ExecutorUtil {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  static final ThreadLocal<Throwable> submitter = new ThreadLocal<>();

  private static volatile List<InheritableThreadLocalProvider> providers = new ArrayList<>();

  /**
   * Resets everything added via {@link #addThreadLocalProvider(InheritableThreadLocalProvider)}.
   * Useful to call at the beginning of tests.
   */
  public static void resetThreadLocalProviders() {
    providers = new ArrayList<>();
  }

  public static synchronized void addThreadLocalProvider(InheritableThreadLocalProvider provider) {
    for (InheritableThreadLocalProvider p :
        providers) { // this is to avoid accidental multiple addition of providers in tests
      if (p.getClass().equals(provider.getClass())) return;
    }
    List<InheritableThreadLocalProvider> copy = new ArrayList<>(providers);
    copy.add(provider);
    providers = copy;
  }

  /**
   * Any class which wants to carry forward the threadlocal values to the threads run by threadpools
   * must implement this interface and the implementation should be registered here
   */
  public interface InheritableThreadLocalProvider {
    /**
     * This is invoked in the parent thread which submitted a task. copy the necessary Objects to
     * the ctx. The object that is passed is same across all three methods
     */
    void store(AtomicReference<Object> ctx);

    /**
     * This is invoked in the Threadpool thread. set the appropriate values in the threadlocal of
     * this thread.
     */
    void set(AtomicReference<Object> ctx);

    /**
     * This method is invoked in the threadpool thread after the execution clean all the variables
     * set in the set method
     */
    void clean(AtomicReference<Object> ctx);
  }

  public static boolean isShutdown(ExecutorService pool) {
    try {
      return pool.isShutdown();
    } catch (IllegalStateException e) {
      // JSR-236 ManagedExecutorService cannot query the lifecycle, so just return false
      return false;
    }
  }

  public static boolean isTerminated(ExecutorService pool) {
    try {
      return pool.isTerminated();
    } catch (IllegalStateException e) {
      // JSR-236 ManagedExecutorService cannot query the lifecycle, so just return false
      return false;
    }
  }

  /**
   * Shutdown the {@link ExecutorService} and wait for 60 seconds for the threads to complete. More
   * detail on the waiting can be found in {@link #awaitTermination(ExecutorService)}.
   *
   * @param pool The ExecutorService to shutdown and wait on
   */
  public static void shutdownAndAwaitTermination(ExecutorService pool) {
    if (pool == null) return;
    pool.shutdown(); // Disable new tasks from being submitted
    awaitTermination(pool);
  }

  /**
   * Shutdown the {@link ExecutorService} and wait forever for the threads to complete. More detail
   * on the waiting can be found in {@link #awaitTerminationForever(ExecutorService)}.
   *
   * <p>This should likely not be used in {@code close()} methods, as we want to timebound when
   * shutting down. However, sometimes {@link ExecutorService}s are used to submit a list of tasks
   * and awaiting termination is akin to waiting on the list of {@link Future}s to complete. In that
   * case, this method should be used as there is no inherent time bound to waiting on those tasks
   * to complete.
   *
   * @param pool The ExecutorService to shutdown and wait on
   */
  public static void shutdownAndAwaitTerminationForever(ExecutorService pool) {
    if (pool == null) return;
    pool.shutdown(); // Disable new tasks from being submitted
    awaitTerminationForever(pool);
  }

  public static void shutdownNowAndAwaitTermination(ExecutorService pool) {
    if (pool == null) return;
    pool.shutdownNow(); // Disable new tasks from being submitted; interrupt existing tasks
    awaitTermination(pool);
  }

  /**
   * Await the termination of an {@link ExecutorService} for a default of 60 seconds, then force
   * shutdown the remaining threads and wait another 60 seconds.
   *
   * @param pool the ExecutorService to wait on
   */
  public static void awaitTermination(ExecutorService pool) {
    awaitTermination(pool, 60, TimeUnit.SECONDS);
  }

  // Used in testing to not have to wait the full 60 seconds.
  static void awaitTermination(ExecutorService pool, long timeout, TimeUnit unit) {
    try {
      // Wait a while for existing tasks to terminate.
      if (!pool.awaitTermination(timeout, unit)) {
        // We want to force shutdown any remaining threads.
        pool.shutdownNow();
        // Wait again for forced threads to stop.
        if (!pool.awaitTermination(timeout, unit)) {
          log.error("Threads from pool {} did not forcefully stop.", pool);
          throw new RuntimeException("Timeout waiting for pool " + pool + " to shutdown.");
        }
      }
    } catch (InterruptedException ie) {
      // (Re-)Cancel if current thread also interrupted
      pool.shutdownNow();
      // Preserve interrupt status
      Thread.currentThread().interrupt();
    }
  }

  /**
   * Await the termination of an {@link ExecutorService} until all threads are complete, or until we
   * are interrupted, at which point the {@link ExecutorService} will be interrupted as well.
   *
   * @param pool the ExecutorService to wait on
   */
  public static void awaitTerminationForever(ExecutorService pool) {
    boolean shutdown = false;
    try {
      while (!shutdown) {
        // Wait a while for existing tasks to terminate
        shutdown = pool.awaitTermination(60, TimeUnit.SECONDS);
      }
    } catch (InterruptedException e) {
      // Force cancel if current thread also interrupted
      pool.shutdownNow();
      // Preserve interrupt status
      Thread.currentThread().interrupt();
    }
  }

  /** See {@link java.util.concurrent.Executors#newFixedThreadPool(int, ThreadFactory)} */
  public static ExecutorService newMDCAwareFixedThreadPool(
      int nThreads, ThreadFactory threadFactory) {
    return new MDCAwareThreadPoolExecutor(
        nThreads, nThreads, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(), threadFactory);
  }

  public static ExecutorService newMDCAwareFixedThreadPool(
      int nThreads, int queueCapacity, ThreadFactory threadFactory, Runnable beforeExecute) {
    return new MDCAwareThreadPoolExecutor(
        nThreads,
        nThreads,
        0L,
        TimeUnit.MILLISECONDS,
        new LinkedBlockingQueue<>(queueCapacity),
        threadFactory,
        beforeExecute);
  }

  /**
   * See {@link java.util.concurrent.Executors#newSingleThreadExecutor(ThreadFactory)}. Note the
   * thread is always active, even if no tasks are submitted to the executor.
   */
  public static ExecutorService newMDCAwareSingleThreadExecutor(ThreadFactory threadFactory) {
    return new MDCAwareThreadPoolExecutor(
        1, 1, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(), threadFactory);
  }

  /**
   * Similar to {@link #newMDCAwareSingleThreadExecutor(ThreadFactory)}, but the thread will not be
   * kept active after the specified time if no task is submitted to the executor.
   */
  public static ExecutorService newMDCAwareSingleLazyThreadExecutor(
      ThreadFactory threadFactory, long keepAliveTime, TimeUnit unit) {
    return new MDCAwareThreadPoolExecutor(
        0, 1, keepAliveTime, unit, new LinkedBlockingQueue<>(), threadFactory);
  }

  /** Create a cached thread pool using a named thread factory */
  public static ExecutorService newMDCAwareCachedThreadPool(String name) {
    return newMDCAwareCachedThreadPool(new SolrNamedThreadFactory(name));
  }

  /** See {@link java.util.concurrent.Executors#newCachedThreadPool(ThreadFactory)} */
  public static ExecutorService newMDCAwareCachedThreadPool(ThreadFactory threadFactory) {
    return new MDCAwareThreadPoolExecutor(
        0, Integer.MAX_VALUE, 60L, TimeUnit.SECONDS, new SynchronousQueue<>(), threadFactory);
  }

  /**
   * Create a new pool with cached a reused threads. Thread are created only on-demand, up to the
   * specified {@code maxThreads}.
   */
  public static ExecutorService newMDCAwareCachedThreadPool(
      int maxThreads, int queueCapacity, ThreadFactory threadFactory) {
    // Create an executor with same value of core size and max total size. With an unbounded queue,
    // the ThreadPoolExecutor ignores the configured max value and only considers core pool size.
    // Since we allow core threads to die when idle for too long, this ends in having a pool with
    // lazily-initialized and cached threads.
    MDCAwareThreadPoolExecutor executor =
        new MDCAwareThreadPoolExecutor(
            maxThreads,
            maxThreads,
            60L,
            TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(queueCapacity),
            threadFactory);
    // Allow core threads to die
    executor.allowCoreThreadTimeOut(true);
    return executor;
  }

  @SuppressForbidden(reason = "class customizes ThreadPoolExecutor so it can be used instead")
  public static class MDCAwareThreadPoolExecutor extends ThreadPoolExecutor {

    private static final int MAX_THREAD_NAME_LEN = 512;
    public static final Runnable NOOP = () -> {};

    private final boolean enableSubmitterStackTrace;
    private final Runnable beforeExecuteTask;

    public MDCAwareThreadPoolExecutor(
        int corePoolSize,
        int maximumPoolSize,
        long keepAliveTime,
        TimeUnit unit,
        BlockingQueue<Runnable> workQueue,
        ThreadFactory threadFactory,
        RejectedExecutionHandler handler) {
      super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory, handler);
      this.enableSubmitterStackTrace = true;
      this.beforeExecuteTask = NOOP;
    }

    public MDCAwareThreadPoolExecutor(
        int corePoolSize,
        int maximumPoolSize,
        long keepAliveTime,
        TimeUnit unit,
        BlockingQueue<Runnable> workQueue) {
      super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue);
      this.enableSubmitterStackTrace = true;
      this.beforeExecuteTask = NOOP;
    }

    public MDCAwareThreadPoolExecutor(
        int corePoolSize,
        int maximumPoolSize,
        long keepAliveTime,
        TimeUnit unit,
        BlockingQueue<Runnable> workQueue,
        ThreadFactory threadFactory) {
      this(
          corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory, true, NOOP);
    }

    public MDCAwareThreadPoolExecutor(
        int corePoolSize,
        int maximumPoolSize,
        long keepAliveTime,
        TimeUnit unit,
        BlockingQueue<Runnable> workQueue,
        ThreadFactory threadFactory,
        Runnable beforeExecuteTask) {
      this(
          corePoolSize,
          maximumPoolSize,
          keepAliveTime,
          unit,
          workQueue,
          threadFactory,
          true,
          beforeExecuteTask);
    }

    public MDCAwareThreadPoolExecutor(
        int corePoolSize,
        int maximumPoolSize,
        long keepAliveTime,
        TimeUnit unit,
        BlockingQueue<Runnable> workQueue,
        ThreadFactory threadFactory,
        boolean enableSubmitterStackTrace,
        Runnable beforeExecuteTask) {
      super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory);
      this.enableSubmitterStackTrace = enableSubmitterStackTrace;
      this.beforeExecuteTask = beforeExecuteTask;
    }

    public MDCAwareThreadPoolExecutor(
        int corePoolSize,
        int maximumPoolSize,
        long keepAliveTime,
        TimeUnit unit,
        BlockingQueue<Runnable> workQueue,
        RejectedExecutionHandler handler) {
      super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, handler);
      this.enableSubmitterStackTrace = true;
      this.beforeExecuteTask = NOOP;
    }

    public MDCAwareThreadPoolExecutor(
        int corePoolSize,
        int maximumPoolSize,
        int keepAliveTime,
        TimeUnit timeUnit,
        BlockingQueue<Runnable> blockingQueue,
        SolrNamedThreadFactory httpShardExecutor,
        boolean enableSubmitterStackTrace) {
      super(
          corePoolSize, maximumPoolSize, keepAliveTime, timeUnit, blockingQueue, httpShardExecutor);
      this.enableSubmitterStackTrace = enableSubmitterStackTrace;
      this.beforeExecuteTask = NOOP;
    }

    public MDCAwareThreadPoolExecutor(
        int i,
        int maxValue,
        long l,
        TimeUnit timeUnit,
        BlockingQueue<Runnable> es,
        SolrNamedThreadFactory testExecutor,
        boolean b) {
      this(i, maxValue, l, timeUnit, es, testExecutor, b, NOOP);
    }

    @Override
    protected void beforeExecute(Thread t, Runnable r) {
      this.beforeExecuteTask.run();
    }

    @Override
    public void execute(final Runnable command) {
      final Map<String, String> submitterContext = MDC.getCopyOfContextMap();
      StringBuilder contextString = new StringBuilder();
      if (submitterContext != null) {
        Collection<String> values = submitterContext.values();

        for (String value : values) {
          contextString.append(value).append(' ');
        }
        if (contextString.length() > 1) {
          contextString.setLength(contextString.length() - 1);
        }
      }

      String ctxStr = contextString.toString().replace("/", "//");
      final String submitterContextStr =
          ctxStr.length() <= MAX_THREAD_NAME_LEN
              ? ctxStr
              : ctxStr.substring(0, MAX_THREAD_NAME_LEN);
      final Throwable submitterStackTrace; // Never thrown, only used as stack trace holder
      if (enableSubmitterStackTrace) {
        Throwable grandParentSubmitter = submitter.get();
        submitterStackTrace = new Exception("Submitter stack trace", grandParentSubmitter);
      } else {
        submitterStackTrace = null;
      }
      final List<InheritableThreadLocalProvider> providersCopy = providers;
      final ArrayList<AtomicReference<Object>> ctx =
          providersCopy.isEmpty() ? null : new ArrayList<>(providersCopy.size());
      if (ctx != null) {
        for (int i = 0; i < providers.size(); i++) {
          AtomicReference<Object> reference = new AtomicReference<>();
          ctx.add(reference);
          providersCopy.get(i).store(reference);
        }
      }
      super.execute(
          () -> {
            isServerPool.set(Boolean.TRUE);
            if (ctx != null) {
              for (int i = 0; i < providersCopy.size(); i++) providersCopy.get(i).set(ctx.get(i));
            }
            Map<String, String> threadContext = MDC.getCopyOfContextMap();
            final Thread currentThread = Thread.currentThread();
            final String oldName = currentThread.getName();
            if (submitterContext != null && !submitterContext.isEmpty()) {
              MDC.setContextMap(submitterContext);
              currentThread.setName(oldName + "-processing-" + submitterContextStr);
            } else {
              MDC.clear();
            }
            if (enableSubmitterStackTrace) {
              submitter.set(submitterStackTrace);
            }
            try {
              command.run();
            } catch (Throwable t) {
              if (t instanceof OutOfMemoryError) {
                throw t;
              }
              // Flip around the exception cause tree, because it is in reverse order
              Throwable baseCause = t;
              Throwable nextCause = submitterStackTrace;
              while (nextCause != null) {
                baseCause = new Exception(nextCause.getMessage(), baseCause);
                baseCause.setStackTrace(nextCause.getStackTrace());
                nextCause = nextCause.getCause();
              }
              log.error(
                  "Uncaught exception {} thrown by thread: {}",
                  t,
                  currentThread.getName(),
                  baseCause);
              throw t;
            } finally {
              isServerPool.remove();
              if (threadContext != null && !threadContext.isEmpty()) {
                MDC.setContextMap(threadContext);
              } else {
                MDC.clear();
              }
              if (ctx != null) {
                for (int i = 0; i < providersCopy.size(); i++)
                  providersCopy.get(i).clean(ctx.get(i));
              }
              currentThread.setName(oldName);
            }
          });
    }
  }

  private static final ThreadLocal<Boolean> isServerPool = new ThreadLocal<>();

  /// this tells whether a thread is owned/run by solr or not.
  public static boolean isSolrServerThread() {
    return Boolean.TRUE.equals(isServerPool.get());
  }

  public static void setServerThreadFlag(Boolean flag) {
    if (flag == null) isServerPool.remove();
    else isServerPool.set(flag);
  }

  /**
   * Takes an executor and a list of Callables and executes them returning the results as a list.
   * The method waits for the return of every task even if one of them throws an exception. If any
   * exception happens it will be thrown, wrapped into an IOException, and other following
   * exceptions will be added as `addSuppressed` to the original exception
   *
   * @param <T> the response type
   * @param service executor
   * @param tasks the list of callables to be executed
   * @return results list
   * @throws IOException in case any exceptions happened
   */
  public static <T> Collection<T> submitAllAndAwaitAggregatingExceptions(
      ExecutorService service, List<? extends Callable<T>> tasks) throws IOException {
    List<T> results = new ArrayList<>(tasks.size());
    IOException parentException = null;

    // Could alternatively use service.invokeAll, but this way we can start looping over futures
    // before all are done
    List<Future<T>> futures =
        tasks.stream().map(service::submit).collect(Collectors.toUnmodifiableList());
    for (Future<T> f : futures) {
      try {
        results.add(f.get());
      } catch (ExecutionException e) {
        if (parentException == null) {
          parentException = new IOException(e.getCause());
        } else {
          parentException.addSuppressed(e.getCause());
        }
      } catch (Exception e) {
        if (parentException == null) {
          parentException = new IOException(e);
        } else {
          parentException.addSuppressed(e);
        }
      }
    }
    if (parentException != null) {
      throw parentException;
    }
    return results;
  }
}
