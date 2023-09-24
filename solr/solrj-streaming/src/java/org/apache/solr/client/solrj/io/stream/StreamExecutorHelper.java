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
package org.apache.solr.client.solrj.io.stream;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.SolrNamedThreadFactory;

public class StreamExecutorHelper {

  // this could easily live in ExecutorUtil
  public static <T> List<T> submitAllAndAwaitAggregatingExceptions(
      List<? extends Callable<T>> tasks, String threadsName) throws IOException {
    ExecutorService service =
        ExecutorUtil.newMDCAwareCachedThreadPool(new SolrNamedThreadFactory(threadsName));
    List<T> results = new ArrayList<>();

    try {
      List<Future<T>> futures =
          tasks.stream().map(service::submit).collect(Collectors.toUnmodifiableList());

      AtomicReference<IOException> ex = new AtomicReference<>();
      for (Future<T> f : futures) {
        try {
          T result = f.get();
          if (result != null) {
            results.add(result);
          }
        } catch (Exception e) {
          if (ex.get() != null) {
            ex.get().addSuppressed(e);
          } else {
            ex.set(new IOException(e));
          }
        }
      }
      if (ex.get() != null) {
        throw ex.get();
      }

    } finally {
      ExecutorUtil.shutdownAndAwaitTermination(service);
    }

    return results;
  }
}
