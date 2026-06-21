package org.apache.commons;

import org.apache.solr.common.ParWork;
import org.apache.solr.common.ParWorkExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.LinkedTransferQueue;

public class ForkJoinParWorkRootExec {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static class RootExecHolder {
    private final static ParWorkExecutor EXEC_HOLDER_INSTANCE;

    static {
      try {
        EXEC_HOLDER_INSTANCE = (ParWorkExecutor) ParWork
            .getForkJoinParExecutorService(ParWork.ROOT_EXEC_NAME, Integer.getInteger("solr.rootSharedThreadPoolCoreSize", 128));
      } catch (Throwable e) {
        log.warn("Could not find object release tracker class", e);
        throw e;
      }
      EXEC_HOLDER_INSTANCE.enableCloseLock();

      EXEC_HOLDER_INSTANCE.prestartAllCoreThreads();
    }

    private static BlockingQueue getQueue() {
      BlockingQueue<Runnable> queue = new LinkedTransferQueue<Runnable>() {
        @Override
        public boolean offer(Runnable e) {
          return tryTransfer(e);
        }
      };
      return queue;
    }

    public static ParWorkExecutor getExecutor() {
      return EXEC_HOLDER_INSTANCE;
    }

    public static class SolrForkJoinThreadFactory implements ForkJoinPool.ForkJoinWorkerThreadFactory {
      @Override
      public ForkJoinWorkerThread newThread(ForkJoinPool pool) {
        return new NotSoInnocuousWorkerThread(pool);
      }

      private static class NotSoInnocuousWorkerThread extends ForkJoinWorkerThread {
        public NotSoInnocuousWorkerThread(ForkJoinPool pool) {
          super(pool);
          setDaemon(true);

          setName("SolrForkJoin");
        }
      }
    }

//    public static void reset() {
//      EXEC_HOLDER_INSTANCE = (ParWorkExecutor) ParWork
//          .getParExecutorService(ParWork.ROOT_EXEC_NAME, Integer.getInteger("solr.rootSharedThreadPoolCoreSize", 128), Integer.MAX_VALUE, 30000,
//              new SynchronousQueue(false));
//    }
  }
}
