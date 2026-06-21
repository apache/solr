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

import java.io.Closeable;
import java.lang.invoke.MethodHandles;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.Timer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.ForkJoinParWorkRootExec;
import org.apache.commons.ParWorkRootExec;
import org.apache.commons.ParWorkRootIOExec;
import org.apache.commons.ParWorkUnknownRootIOExec;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.common.util.ObjectReleaseTrackerTestImpl;
import org.apache.solr.common.util.OrderedExecutor;
import org.apache.solr.common.util.SysStats;
import org.apache.zookeeper.KeeperException;
import org.jctools.maps.NonBlockingHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ParWork. A workhorse utility class that tries to use good patterns,
 * parallelism
 * 
 */
public class ParWork implements Closeable {
  private final static boolean TRACK_TIMES = false;

  public static final int PROC_COUNT = ManagementFactory.getOperatingSystemMXBean().getAvailableProcessors();

  public static final String ROOT_EXEC_PREFIX = "SHARED";
  public static final String ROOT_EXEC_NAME = ROOT_EXEC_PREFIX + ':';
  public static final String ROOT_EXEC_IO_NAME = ROOT_EXEC_PREFIX + "IO:";

  private static final String WORK_WAS_INTERRUPTED = "Work was interrupted!";

  private static final String RAN_INTO_AN_ERROR_WHILE_DOING_WORK =
      "Ran into an error while doing work!";

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final int PER_THREAD_MAX = Integer.getInteger("solr.perThreadPoolSize", Math.max(SysStats.PROC_COUNT, 4));
  public static final Long TIMEOUT = Long.getLong("solr.parwork.task_timeout", TimeUnit.MINUTES.toMillis(10));

  private final String rootLabel;

  private Queue<Object> collectSet = new LinkedList<>();

//  private static final RejectedExecutionHandler QUEUE_EXECUTION_HANDLER = (r, e) -> {
//    if (!e.isShutdown()) {
//      e.getQueue().add(r);
//    }
//  };

  public static BlockingQueue<Runnable> getQueue() {
    return new LinkedTransferQueue<>() {
      @Override public boolean offer(Runnable r) {
        return tryTransfer(r);
      }
    };
  }

  public static ParWorkExecutor getRootSharedExecutor() {
    return ParWorkRootExec.RootExecHolder.getExecutor();
  }

  public static ParWorkExecutor getRootSharedIOExecutor() {
    return ParWorkRootIOExec.getExecutor();
  }

  public static ParWorkExecutor getRootSharedForkJoinExecutor() {
    return ForkJoinParWorkRootExec.RootExecHolder.getExecutor();
  }

  public static Future<?> submit(String threadName, Runnable task) {
    if (task == null) throw new NullPointerException();
    ParWorkExecutor exec = getRootSharedExecutor();
    RunnableFuture<Void> ftask = ParWorkExecutor.newTaskFor(threadName, task, null);
    exec.execute(ftask);
    return ftask;
  }

  public static Future<?> submitIO(String threadName, Runnable task) {
    if (task == null) throw new NullPointerException();
    ParWorkExecutor exec = getRootSharedIOExecutor();
    RunnableFuture<Void> ftask = ParWorkExecutor.newTaskFor(threadName, task, null);
    exec.execute(ftask);
    return ftask;
  }

  public static <T> Future<T> submitIO(String threadName, Callable<T> task) {
    if (task == null) throw new NullPointerException();
    ParWorkExecutor exec = getRootSharedIOExecutor();
    RunnableFuture<T> ftask = ParWorkExecutor.newTaskFor(threadName, task);
    exec.execute(ftask);
    return ftask;
  }

  public static <T> Future<T> submit(String threadName, Runnable task, T result) {
    if (task == null) throw new NullPointerException();
    ParWorkExecutor exec = getRootSharedExecutor();
    RunnableFuture<T> ftask = ParWorkExecutor.newTaskFor(threadName, task, result);
    exec.execute(ftask);
    return ftask;
  }


  public static <T> Future<T> submit(String threadName, Callable<T> task) {
    if (task == null) throw new NullPointerException();
    ParWorkExecutor exec = getRootSharedExecutor();
    RunnableFuture<T> ftask = ParWorkExecutor.newTaskFor(threadName, task);
    exec.execute(ftask);
    return ftask;
  }

  public static void shutdownParWorkExecutor() {
    shutdownParWorkExecutor(true);
  }

  public static void shutdownParWorkExecutor(boolean wait) {
    try {
      ParWorkExecutor exec = getRootSharedExecutor();
      shutdownParWorkExecutor(exec, wait);

      ParWorkExecutor execIO = getRootSharedIOExecutor();
      shutdownParWorkExecutor(execIO, wait);
    } finally {
     // ParWorkRootExec.RootExecHolder.reset();
    }

//    try {
//      ParWorkExecutor exec = getRootSharedForkJoinExecutor();
//      shutdownParWorkExecutor(exec, wait);
//    } finally {
//      //ParWorkRootExec.RootExecHolder.reset();
//    }
  }

  public static void shutdownParWorkExecutor(ParWorkExecutor executor, boolean wait) {
   // log.info("shutdownParWorkExecutor CLASSLOADER={}", executor.getClass().getClassLoader());
    if (executor != null) {
      executor.disableCloseLock();
//      executor.setKeepAliveTime(1, TimeUnit.NANOSECONDS);
//      executor.allowCoreThreadTimeOut(true);
     // executor.setKeepAliveTime(1, TimeUnit.NANOSECONDS);
     // executor.allowCoreThreadTimeOut(true);
      executor.shutdownNow();

      if (wait) {
        try {
          executor.awaitTermination(2000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {

        }
      }
    }
  }


  private static final SysStats sysStats = SysStats.getSysStats();

  public static SysStats getSysStats() {
    return sysStats;
  }

    private static class WorkUnit {
    private final Collection<Object> objects;
    private final TimeTracker tracker;

    public WorkUnit(Collection<Object> objects, TimeTracker tracker) {
      this.objects = objects;
      this.tracker = tracker;
    }
  }

  private static final Set<Class> OK_CLASSES;

  static {
    OK_CLASSES = Set.of(ExecutorService.class, OrderedExecutor.class, Closeable.class, AutoCloseable.class, Callable.class, Runnable.class, Timer.class, CloseableHttpClient.class, Map.class);
  }

  private final Queue<WorkUnit> workUnits = new LinkedList<>();

  private volatile TimeTracker tracker;

  private final boolean ignoreExceptions;

  private final Set<Throwable> warns = ConcurrentHashMap.newKeySet(16);

  // TODO should take logger as well
  public static class Exp extends Exception {

    private static final String ERROR_MSG = "Solr ran into an unexpected Exception";

    /**
     * Handles exceptions correctly for you, including logging.
     * 
     * @param msg message to include to clarify the problem
     */
    public Exp(String msg) {
      this(null, msg, null);
    }

    /**
     * Handles exceptions correctly for you, including logging.
     * 
     * @param th the exception to handle
     */
    public Exp(Throwable th) {
      this(null, th.getMessage(), th);
    }

    /**
     * Handles exceptions correctly for you, including logging.
     * 
     * @param msg message to include to clarify the problem
     * @param th  the exception to handle
     */
    public Exp(String msg, Throwable th) {
      this(null, msg, th);
    }

    public Exp(Logger classLog, String msg, Throwable th) {
      super(msg == null ? ERROR_MSG : msg, th);

      Logger logger;
      if (classLog != null) {
        logger = classLog;
      } else {
        logger = log;
      }

      logger.error(ERROR_MSG, th);
      if (th instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      if (th instanceof KeeperException) { // TODO maybe start using ZooKeeperException
        if (((KeeperException) th).code() == KeeperException.Code.SESSIONEXPIRED) {
          log.warn("The session has expired, give up any leadership roles!");
        }
      }
    }
  }

  public ParWork() {
    this("Default", false);
  }

  public ParWork(Object object) {
    this(object, false);
  }

  public ParWork(Object object, boolean ignoreExceptions) {
    this.ignoreExceptions = ignoreExceptions;
    this.rootLabel = object instanceof String ?
        (String) object : object.getClass().getSimpleName();
    if (TRACK_TIMES) tracker = new TimeTracker(object, object.getClass().getName());
    // constructor must stay very light weight
  }

  public void collect(String label, Object object) {
    if (object == null) {
      return;
    }
    if (collectSet == null) {
      collectSet = new LinkedList<>();
    }
    gatherObjects(label, object, collectSet);
  }

  public void collect(Object object) {
   collect(object != null ? object instanceof String ? (String) object : object.getClass().getSimpleName() : null, object);
  }

  public void collect(Object... objects) {
    for (Object object : objects) {
      collect(object);
    }
  }

  /**
   * @param callable A Callable to run. If an object is return, it's toString is
   *                 used to identify it.
   */
  public void collect(String label, Callable<?> callable) {
    collect(label, (Object) callable);
  }

  /**
   * @param runnable A Runnable to run. If an object is return, it's toString is
   *                 used to identify it.
   */
  public void collect(String label, Runnable runnable) {
    collect(label, (Object) runnable);
  }

  public void addCollect() {
    if (collectSet == null || collectSet.isEmpty()) {
      //if (log.isDebugEnabled()) log.debug("No work collected to submit", new RuntimeException());
      return;
    }
    try {
      add(collectSet);
    } finally {
      collectSet = null;
    }
  }

  private static void gatherObjects(String label, Object submittedObject, Collection<Object> collectSet) {
    if (submittedObject != null) {
      if (submittedObject instanceof Collection) {
        for (Object obj : (Collection) submittedObject) {
          verifyValidType(obj);
          collectSet.add(obj);
        }
      } else if (submittedObject instanceof Map) {
        ((Map) submittedObject).forEach((k, v) -> {
          verifyValidType(v);
          collectSet.add(v);
        });
      } else {
        verifyValidType(submittedObject);
        collectSet.add(submittedObject);
      }
    }
  }

  private void add(Collection<Object> objects) {
    WorkUnit workUnit = new WorkUnit(objects, tracker);
    workUnits.add(workUnit);
  }

  private static void verifyValidType(Object object) {
    boolean ok = false;
    for (Class okobject : OK_CLASSES) {
      if (okobject.isAssignableFrom(object.getClass())) {
        ok = true;
        break;
      }
    }
    if (!ok) {
      log.error(" -> I do not know how to close: {}", object.getClass().getName());
      throw new IllegalArgumentException(" -> I do not know how to close: " + object.getClass().getName());
    }
  }

  @Override
  public void close() {

    addCollect();

    boolean needExec = false;
    for (WorkUnit workUnit : workUnits) {
      if (workUnit.objects.size() > 1) {
        needExec = true;
        break;
      }
    }

    VirtualExecutorService executor = null;
    if (needExec) {
      executor = (VirtualExecutorService) getMyPerThreadExecutor();
    }
    //initExecutor();
    AtomicReference<Throwable> exception = new AtomicReference<>();
    try {
      for (WorkUnit workUnit : workUnits) {
        if (log.isTraceEnabled()) log.trace("Process workunit {} {}", rootLabel, workUnit.objects);
        TimeTracker workUnitTracker = null;
        if (TRACK_TIMES) workUnitTracker = workUnit.tracker.startSubClose(workUnit);
        try {
          Collection<Object> objects = workUnit.objects;

          if (objects.size() == 1) {
            handleObject(exception, workUnitTracker, objects.iterator().next(), ignoreExceptions);
          } else {

            List<Callable<Object>> closeCalls = new ArrayList<>(objects.size());

            for (Object object : objects) {
              if (object == null) continue;
              closeCalls.add(new RunCallable(exception, workUnitTracker, object, ignoreExceptions));
            }
            if (closeCalls.size() > 0) {

              List<Future<Object>> results = new ArrayList<>(closeCalls.size());

              for (Callable call : closeCalls) {
                Future future = null;
                try {
                  future = executor.submit(call);
                } catch (RejectedExecutionException rejectedExecutionException) {
                  call.call();
                }
                if (future != null) {
                  results.add(future);
                }
              }

              int i = 0;
              for (Future<Object> future : results) {
                  try {

                    while (true) {
                      try {
                        future.get(TIMEOUT, TimeUnit.MILLISECONDS); // MRM TODO: timeout
                        break;
                      } catch (InterruptedException e) {
                        log.warn("ParWork interrupted", e);
                        break;
                      }
                    }

                  } catch (Error error) {
                    log.error("Error in ParWork", error);
                    throw error;
                  } catch (Throwable t) {
                    exception.updateAndGet(throwable -> {
                      if (throwable == null) {
                        return t;
                      } else {
                        throwable.addSuppressed(t);
                        return throwable;
                      }
                    });
                  }
                  if (!future.isDone() || future.isCancelled()) {
                    log.warn("A task did not finish isDone={} isCanceled={}", future.isDone(), future.isCancelled(), exception.get());
                  }
              }
            }
          }
        } finally {
          if (workUnitTracker != null)
            workUnitTracker.doneClose();
        }

      }
    } catch (Throwable t) {
      log.error(RAN_INTO_AN_ERROR_WHILE_DOING_WORK, t);

      if (exception.get() == null) {
        exception.set(t);
      }
    } finally {

      if (TRACK_TIMES) tracker.doneClose();
      
      //System.out.println("DONE:" + tracker.getElapsedMS());

      // warns.forEach((it) -> log.warn(RAN_INTO_AN_ERROR_WHILE_DOING_WORK, new RuntimeException(it)));

      if (exception.get() != null) {
        Throwable exp = exception.get();
        if (exp instanceof RuntimeException) {
          throw (RuntimeException) exp;
        }
        ParWorkException rte = new ParWorkException(exp);
        //rte.addSuppressed();
        throw rte;
      }
    }
  }

  public static class ParWorkException extends RuntimeException {

    public ParWorkException(Throwable exp) {
      super(exp);
    }
  }

  public static ExecutorService getMyPerThreadExecutor() {
    // section perThreadExec
    Thread thread = Thread.currentThread();

    if (thread instanceof  SolrThread) {
      return getExecutorService("SolrPerThread", PER_THREAD_MAX, false);
    }

    log.debug("Unknown thread {}", thread);
    return new VirtualExecutorService("ExtSolrPerThread", ParWorkUnknownRootIOExec.getExecutor(), Math.max(SysStats.PROC_COUNT, 4), false);

  }

  public static ExecutorService getParExecutorService(String name, int corePoolSize, int maxPoolSize, int keepAliveTime) {
    ExecutorService exec;
    exec = new ParWorkExecutor(name,
            corePoolSize, maxPoolSize, keepAliveTime, getQueue());
    return exec;
  }

  public static ExecutorService getParExecutorServiceCustomQueue(String name, int corePoolSize, int maxPoolSize, int keepAliveTime, BlockingQueue<Runnable> queue) {
    ExecutorService exec;
    exec = new ParWorkExecutor(name,
        corePoolSize, maxPoolSize, keepAliveTime, queue);
    return exec;
  }

  public static ExecutorService getForkJoinParExecutorService(String name, int maxPoolSize) {
    ExecutorService exec;

    exec = new ForJoinParWorkExecutor(name, maxPoolSize);
    return exec;
  }

  public static ExecutorService getExecutorService(String name, int maximumPoolSize, boolean wait) {
    return new VirtualExecutorService(name, getRootSharedIOExecutor(), maximumPoolSize, wait);
  }

  private static void handleObject(AtomicReference<Throwable> exception, final TimeTracker workUnitTracker, Object ob, boolean ignoreExceptions) {
    if (log.isTraceEnabled()) log.trace(
          "handleObject(AtomicReference<Throwable> exception={}, CloseTimeTracker workUnitTracker={}, Object object={}) - start",
          exception, workUnitTracker, ob);

    Object returnObject = null;
    TimeTracker subTracker = null;
    if (TRACK_TIMES) subTracker = workUnitTracker.startSubClose(ob);
    try {
      boolean handled = false;
      if (ob instanceof OrderedExecutor) {
        ((OrderedExecutor) ob).shutdownAndAwaitTermination();
        handled = true;
      } else if (ob instanceof ExecutorService) {
        shutdownAndAwaitTermination((ExecutorService) ob);
        handled = true;
      } else if (ob instanceof CloseableHttpClient) {
        HttpClientUtil.close((CloseableHttpClient) ob);
        handled = true;
      } else if (ob instanceof Closeable) {
        ((Closeable) ob).close();
        handled = true;
      } else if (ob instanceof AutoCloseable) {
        ((AutoCloseable) ob).close();
        handled = true;
      } else if (ob instanceof Callable) {
        returnObject = ((Callable<?>) ob).call();
        handled = true;
      } else if (ob instanceof Runnable) {
        ((Runnable) ob).run();
        handled = true;
      } else if (ob instanceof Timer) {
        ((Timer) ob).cancel();
        handled = true;
      }

      if (!handled) {
        String msg = " -> I do not know how to close " + ob.getClass().getName();
        log.error(msg);
        exception.updateAndGet(throwable -> {
          if (throwable == null) {
            return new IllegalArgumentException(msg);
          } else {
            throwable.addSuppressed(new IllegalArgumentException(msg));
            return throwable;
          }
        });
      }
    } catch (Throwable t) {
      if (ignoreExceptions) {
        log.error("Error handling close for an object: {}", ob.getClass().getSimpleName(), new ObjectReleaseTrackerTestImpl.ObjectTrackerException(t));
        if (t instanceof Error && !(t instanceof AssertionError)) {
          throw (Error) t;
        }
      } else {
        log.error("handleObject(AtomicReference<Throwable>={}, CloseTimeTracker={}), Object={})", exception, workUnitTracker, ob, t);
        if (t instanceof Error) {
          throw (Error) t;
        }
        if (t instanceof RuntimeException) {
          throw (RuntimeException) t;
        } else {
          throw new WorkException(RAN_INTO_AN_ERROR_WHILE_DOING_WORK, t); // TODO, hmm how do I keep zk session timeout and interrupt in play?
        }
      }

    } finally {
      if (TRACK_TIMES) subTracker.doneClose(returnObject instanceof String ? (String) returnObject : (returnObject == null ? "" : returnObject.getClass().getName()));
    }

    if (log.isTraceEnabled()) log.trace("handleObject(AtomicReference<Throwable>, CloseTimeTracker, List<Callable<Object>>, Object) - end");
  }

  public static <K> Set<K> concSetSmallO() {
    return ConcurrentHashMap.newKeySet(50);
  }

  public static <K, V> Map<K, V> concMapSmallO() {
    return new ConcurrentHashMap<K, V>(132);
  }

  public static <K, V> Map<K, V> concMapReqsO() {
    return new ConcurrentHashMap<>(128);
  }

  public static <K, V> Map<K, V> concMapClassesO() {
    return new ConcurrentHashMap<>(132);
  }

  public static void propagateInterrupt(Throwable t) {
    propagateInterrupt(t, false);
  }

  public static void propagateInterrupt(Throwable t, boolean infoLogMsg) {
    if (t instanceof InterruptedException) {
      log.warn("Interrupted {} while doing work", t.getMessage(), t);
      Thread.currentThread().interrupt();
    } else {
      if (infoLogMsg) {
        if (log.isDebugEnabled()) {
          log.debug("{} {}", t.getClass().getName(), t.getMessage(), t);
        } else {
          log.debug("{} {}", t.getClass().getName(), t.getMessage(), t);
        }
      } else {
        log.error("Solr ran into an exception", t);
      }
    }

    if (t instanceof Error) {
      throw (Error) t;
    }
  }

  public static void propagateInterrupt(String msg, Throwable t) {
    propagateInterrupt(msg, t, false);
  }

  public static void propagateInterrupt(String msg, Throwable t, boolean infoLogMsg) {
    if (t instanceof InterruptedException) {
      log.info("Interrupted", t);
      Thread.currentThread().interrupt();
    } else {
      if (infoLogMsg) {
        log.info(msg);
      } else {
        log.warn(msg, t);
      }
    }
    if (t instanceof Error) {
      throw (Error) t;
    }
  }

  public static void shutdownAndAwaitTermination(ExecutorService pool) {
    if (pool == null)
      return;
    pool.shutdown(); // Disable new tasks from being submitted
    awaitTermination(pool);
    if (!(pool.isShutdown())) {
      throw new RuntimeException("Timeout waiting for executor to shutdown");
    }

  }

  public static void awaitTermination(ExecutorService pool) {
    boolean shutdown = false;
    while (!shutdown) {
      try {
        // Wait a while for existing tasks to terminate
        shutdown = pool.awaitTermination(30, TimeUnit.SECONDS);
      } catch (InterruptedException ie) {
        // Preserve interrupt status
        Thread.currentThread().interrupt();
      }
    }
  }

  public static abstract class ParWorkCallableBase<V> implements Callable<Object> {
    @Override
    public abstract Object call() throws Exception;

    public abstract boolean isCallerThreadAllowed();
  }

  private static class RunCallable implements Callable<Object> {
    private final AtomicReference<Throwable> exception;
    private final Object object;
    private final TimeTracker tracker;
    private final boolean ignoreException;

    RunCallable(AtomicReference<Throwable> exception, TimeTracker tracker, Object object, boolean ignoreException) {
      this.exception = exception;
      this.object = object;
      this.tracker = tracker;
      this.ignoreException = ignoreException;
    }

    @Override public Object call() throws Exception {
      handleObject(exception, tracker, object, ignoreException);
      return object;
    }
  }
}
