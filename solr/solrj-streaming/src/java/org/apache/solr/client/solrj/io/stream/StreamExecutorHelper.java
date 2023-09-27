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
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.SolrNamedThreadFactory;

public class StreamExecutorHelper {

  /**
   * Takes a list of Callables and executes them returning the results as a list. The method waits
   * for the return of every task even if one of them throws an exception. If any exception happens
   * it will be thrown, wrapped into an IOException, and other following exceptions will be added as
   * `addSuppressed` to the original exception
   *
   * @param <T> the response type
   * @param tasks the list of callables to be executed
   * @param threadsName name to be used by the SolrNamedThreadFactory
   * @return results collection
   * @throws IOException in case any exceptions happened
   */
  public static <T> Collection<T> submitAllAndAwaitAggregatingExceptions(
      List<? extends Callable<T>> tasks, String threadsName) throws IOException {
    ExecutorService service =
        ExecutorUtil.newMDCAwareCachedThreadPool(new SolrNamedThreadFactory(threadsName));
    try {
      return ExecutorUtil.submitAllAndAwaitAggregatingExceptions(service, tasks).stream()
          .collect(Collectors.toList());
    } finally {
      ExecutorUtil.shutdownNowAndAwaitTermination(service);
    }
  }
}
