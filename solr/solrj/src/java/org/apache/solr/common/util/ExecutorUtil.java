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

  public static void shutdownAndAwaitTermination(ExecutorService pool) {
    if (pool == null) return;
    pool.shutdown(); // Disable new tasks from being submitted
    awaitTermination(pool);
  }

  public static void shutdownNowAndAwaitTermination(ExecutorService pool) {
    if (pool == null) return;
    pool.shutdownNow(); // Disable new tasks from being submitted; interrupt existing tasks
    awaitTermination(pool);
  }

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

  /** See {@link java.util.concurrent.Executors#newFixedThreadPool(int, ThreadFactory)} */
  public static ExecutorService newMDCAwareFixedThreadPool(
      int nThreads, ThreadFactory threadFactory) {
    return new MDCAwareThreadPoolExecutor(
        nThreads, nThreads, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(), threadFactory);
  }

  /** See {@link java.util.concurrent.Executors#newSingleThreadExecutor(ThreadFactory)} */
  public static ExecutorService newMDCAwareSingleThreadExecutor(ThreadFactory threadFactory) {
    return new MDCAwareThreadPoolExecutor(
        1, 1, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(), threadFactory);
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

  public static ExecutorService newMDCAwareCachedThreadPool(
      int maxThreads, ThreadFactory threadFactory) {
    return new MDCAwareThreadPoolExecutor(
        0, maxThreads, 60L, TimeUnit.SECONDS, new LinkedBlockingQueue<>(maxThreads), threadFactory);
  }

  @SuppressForbidden(reason = "class customizes ThreadPoolExecutor so it can be used instead")
  public static class MDCAwareThreadPoolExecutor extends ThreadPoolExecutor {

    private static final int MAX_THREAD_NAME_LEN = 512;

    private final boolean enableSubmitterStackTrace;

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
    }

    public MDCAwareThreadPoolExecutor(
        int corePoolSize,
        int maximumPoolSize,
        long keepAliveTime,
        TimeUnit unit,
        BlockingQueue<Runnable> workQueue) {
      super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue);
      this.enableSubmitterStackTrace = true;
    }

    public MDCAwareThreadPoolExecutor(
        int corePoolSize,
        int maximumPoolSize,
        long keepAliveTime,
        TimeUnit unit,
        BlockingQueue<Runnable> workQueue,
        ThreadFactory threadFactory) {
      this(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory, true);
    }

    public MDCAwareThreadPoolExecutor(
        int corePoolSize,
        int maximumPoolSize,
        long keepAliveTime,
        TimeUnit unit,
        BlockingQueue<Runnable> workQueue,
        ThreadFactory threadFactory,
        boolean enableSubmitterStackTrace) {
      super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory);
      this.enableSubmitterStackTrace = enableSubmitterStackTrace;
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
              if (enableSubmitterStackTrace) {
                log.error(
                    "Uncaught exception {} thrown by thread: {}",
                    t,
                    currentThread.getName(),
                    submitterStackTrace);
              } else {
                log.error("Uncaught exception {} thrown by thread: {}", t, currentThread.getName());
              }
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
