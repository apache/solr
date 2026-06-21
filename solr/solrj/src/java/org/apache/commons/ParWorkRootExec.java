package org.apache.commons;

import org.apache.solr.common.ParWork;
import org.apache.solr.common.ParWorkExecutor;
import org.apache.solr.common.util.SysStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;

public class ParWorkRootExec {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static class RootExecHolder {
    private static final ParWorkExecutor EXEC_HOLDER_INSTANCE;

    static {
      try {
        EXEC_HOLDER_INSTANCE = (ParWorkExecutor) ParWork
            .getParExecutorService(ParWork.ROOT_EXEC_NAME,
                Integer.getInteger("solr.rootSharedThreadPoolCoreSize", SysStats.PROC_COUNT),
                Integer.getInteger("solr.rootSharedThreadPoolMaxSize", SysStats.PROC_COUNT * 3), 1000);
      } catch (Throwable e) {
        log.warn("Could not find object release tracker class", e);
        throw e;
      }
      EXEC_HOLDER_INSTANCE.enableCloseLock();
      EXEC_HOLDER_INSTANCE.prestartAllCoreThreads();
    }

    public static ParWorkExecutor getExecutor() {
      return EXEC_HOLDER_INSTANCE;
    }

//        public static void reset() {
//          EXEC_HOLDER_INSTANCE = (ParWorkExecutor) ParWork
//              .getParExecutorService(ParWork.ROOT_EXEC_NAME,
//                  Integer.getInteger("solr.rootSharedThreadPoolCoreSize", 128),
//                  200, 3000);
//        }
  }
}
