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
package org.apache.solr.search;

import com.google.common.annotations.VisibleForTesting;
import java.lang.invoke.MethodHandles;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.lang.reflect.Method;
import org.apache.lucene.index.QueryTimeout;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.request.SolrQueryRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MemAllowedLimit implements QueryTimeout {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final double MEBI = 1024.0 * 1024.0;
  private static final ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();
  private static Method GET_BYTES_METHOD;
  private static boolean supported;

  static {
    boolean testSupported;
    try {
      Class<?> sunThreadBeanClz = Class.forName("com.sun.management.ThreadMXBean");
      if (sunThreadBeanClz.isAssignableFrom(threadBean.getClass())) {
        Method m = sunThreadBeanClz.getMethod("isThreadAllocatedMemorySupported");
        Boolean supported = (Boolean) m.invoke(threadBean);
        if (supported) {
          m = sunThreadBeanClz.getMethod("setThreadAllocatedMemoryEnabled", boolean.class);
          m.invoke(threadBean, Boolean.TRUE);
          testSupported = true;
          GET_BYTES_METHOD = sunThreadBeanClz.getMethod("getCurrentThreadAllocatedBytes");
        } else {
          testSupported = false;
        }
      } else {
        testSupported = false;
      }
    } catch (Exception e) {
      testSupported = false;
    }
    supported = testSupported;
  }

  static final int MIN_COUNT = 100;

  private long limitAtBytes;
  private long initialBytes;

  public MemAllowedLimit(SolrQueryRequest req) {
    if (!supported) {
      throw new IllegalArgumentException(
          "Per-thread memory allocation monitoring not available in this JVM.");
    }
    float reqMemLimit = req.getParams().getFloat(CommonParams.MEM_ALLOWED, -1.0f);
    if (reqMemLimit <= 0.0f) {
      throw new IllegalArgumentException(
          "Check for limit with hasMemLimit(req) before creating a MemAllowedLimit!");
    }

    init(reqMemLimit);
  }

  private void init(float reqMemLimit) {
    try {
      initialBytes = (Long) GET_BYTES_METHOD.invoke(threadBean);
      log.info("init " + Thread.currentThread().getName() + " " + initialBytes);
      if (reqMemLimit > 0) {
        long limitBytes = Math.round(reqMemLimit * MEBI);
        limitAtBytes = initialBytes + limitBytes;
      } else {
        limitAtBytes = -1;
      }
    } catch (Exception e) {
      supported = false;
      throw new IllegalArgumentException(
          "Unexpected error checking thread allocation, disabling!", e);
    }
  }

  @VisibleForTesting
  MemAllowedLimit(float memLimit) {
    init(memLimit);
  }

  @VisibleForTesting
  static boolean isSupported() {
    return supported;
  }

  static boolean hasMemLimit(SolrQueryRequest req) {
    return req.getParams().getFloat(CommonParams.MEM_ALLOWED, -1.0f) > 0.0f
        || req.getParams().getFloat(CommonParams.MEM_ALLOWED_RATIO, -1.0f) > 1.0f;
  }

  @Override
  public boolean shouldExit() {
    try {
      long currentAllocatedBytes = (Long) GET_BYTES_METHOD.invoke(threadBean);
      log.info("mem delta " + Thread.currentThread().getName() + ": " + (currentAllocatedBytes - limitAtBytes));
      if (limitAtBytes < currentAllocatedBytes) {
        return true;
      }
      return false;
    } catch (Exception e) {
      supported = false;
      throw new IllegalArgumentException(
          "Unexpected error checking thread allocation, disabling!", e);
    }
  }
}
