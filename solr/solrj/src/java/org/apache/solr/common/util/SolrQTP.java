package org.apache.solr.common.util;

import org.apache.solr.common.ParWork;
import org.apache.solr.common.SolrThread;
import org.apache.solr.logging.MDCLoggingContext;
import org.eclipse.jetty.util.BlockingArrayQueue;
import org.eclipse.jetty.util.annotation.ManagedObject;
import org.eclipse.jetty.util.thread.QueuedThreadPool;

import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.ThreadFactory;

@ManagedObject("A thread pool")
public class SolrQTP extends QueuedThreadPool {

  public SolrQTP() {
    this("jetty", Integer.MAX_VALUE, 12, getSolrQueue());
  }

  public SolrQTP(String name) {
    this(name, Integer.MAX_VALUE, 12, getSolrQueue());
  }

  public SolrQTP(String name, int maxThreads, int minThreads) {
    this(name, maxThreads, minThreads, getSolrQueue());
  }

  public SolrQTP(String name, int maxThreads, int minThreads, BlockingQueue<Runnable> queue) {
    super(maxThreads, minThreads, 3000, -1, queue, new ThreadGroup("jetty"), new MyThreadFactory());
    setName(name);
  }

  @Override public Thread newThread(Runnable runnable) {
    return new JettyThread(getName(), runnable);
  }

  @Override
  protected void doStop() throws Exception
  {
//    ReservedThreadExecutor exec = getBean(ReservedThreadExecutor.class);
//    exec.stop();
    super.doStop();
  }

  public static BlockingQueue<Runnable> getSolrQueue() {
    return new LinkedTransferQueue<>();
    //return new BlockingArrayQueue<>(128, BlockingArrayQueue.DEFAULT_GROWTH);
  }

  public static class JettyThread extends Thread {
    private final Runnable runnable;
    private final String name;
    private volatile Future runnableFuture;

    JettyThread(String name, Runnable runnable) {
      this.runnable = runnable;
      this.name = name;
    }

    @Override
    public void start() {
      long threadId = Thread.currentThread().getId();
      synchronized (this) {
        runnableFuture = ParWork.submitIO(name + threadId, () -> {
          try {
            runnable.run();
          } finally {
       //     JavaBinCodec.THREAD_LOCAL_ARR.remove();

            //FastInputStream.THREAD_LOCAL_BYTEARRAY.remove();
            MDCLoggingContext.reset();
          }
        });
      }
    }

    @Override
    public void interrupt() {
      if (runnableFuture != null) {
        runnableFuture.cancel(true);
      }
    }
  }

  private static class MyThreadFactory implements ThreadFactory {
    @Override public Thread newThread(Runnable r) {
      Runnable runnable = new MyRunnable(r);

      SecurityManager s = System.getSecurityManager();
      ThreadGroup group = (s != null) ? s.getThreadGroup() : Thread.currentThread().getThreadGroup();

      return new SolrThread(group, runnable, "jetty");
    }

    private static class MyRunnable implements Runnable {
      private final Runnable r;

      public MyRunnable(Runnable r) {
        this.r = r;
      }

      @Override public void run() {
        try {
          r.run();
        } finally {
        //  JavaBinCodec.THREAD_LOCAL_ARR.remove();

//          for (ThreadLocal tl : threadLocals) {
//            tl.remove();
//          }
          MDCLoggingContext.reset();
        }
      }
    }
  }

  public static Set<ThreadLocal> threadLocals = ConcurrentHashMap.newKeySet();

  public static void registerThreadLocal(ThreadLocal tl) {
    threadLocals.add(tl);
  }
}
