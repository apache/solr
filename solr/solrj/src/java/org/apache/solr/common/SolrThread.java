package org.apache.solr.common;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.atomic.AtomicInteger;

public class SolrThread extends Thread {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static AtomicInteger COUNT = new AtomicInteger();
  private final String name;


 // private AtomicReference<ExecutorService> executorService = new AtomicReference<>();

 // private static Counter executors = Metrics.MARKS_METRICS.counter("solrthread_executors");

 // private static Counter executorsReused = Metrics.MARKS_METRICS.counter("solrthread_executors_reused");

  public SolrThread(ThreadGroup group, Runnable r, String name) {
    super(group, r, name + '-' + COUNT.incrementAndGet());
    this.name = getName();
    setDaemon(true);
//    Thread currentThread = Thread.currentThread();
//    if (currentThread instanceof SolrThread) {
//      parentThread = (SolrThread) currentThread;
//    } else {
//      parentThread = null;
//    }
  }

  public void resetName() {
    setName(name);
  }

//  private ExecutorService createExecutorService() {
//    // log.info("createExecutorService CLASSLOADER={}", Thread.currentThread().getClass().getClassLoader());
//    executors.inc();
//    return ParWork.getExecutorService(name, Integer.getInteger("solr.perThreadPoolSize", Math.max(SysStats.PROC_COUNT, 4)), false);
//    // log.info("createExecutorService Class instance CLASSLOADER={}", executorService.getClass().getClassLoader());
//  }
//
//  public ExecutorService getExecutorService() {
//    return executorService.updateAndGet(execService -> {
//      if (execService != null) {
//        return execService;
//      }
//      if (parentThread != null) {
//        executorsReused.inc();
//        return parentThread.getExecutorService();
//      }
//
//      return createExecutorService();
//    });
//  }

  public static SolrThread getCurrentThread() {
    return (SolrThread) currentThread();
  }

//  public void clearExecutor() {
//    this.executorService = null;
//  }
}
