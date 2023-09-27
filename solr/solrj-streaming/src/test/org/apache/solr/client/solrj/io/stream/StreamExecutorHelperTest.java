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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.solr.SolrTestCase;
import org.junit.Test;

public class StreamExecutorHelperTest extends SolrTestCase {

  @Test
  public void submitAllTest() throws IOException {
    AtomicLong idx = new AtomicLong();
    Callable<Long> c = () -> idx.getAndIncrement();

    List<Callable<Long>> tasks = List.of(c, c, c, c, c);
    List<Long> results = new ArrayList<>();
    results.addAll(StreamExecutorHelper.submitAllAndAwaitAggregatingExceptions(tasks, "test"));
    Collections.sort(results);
    List<Long> expected = List.of(0l, 1l, 2l, 3l, 4l);
    assertEquals(expected, results);
  }

  @Test
  public void submitAllWithExceptionsTest() {
    AtomicLong idx = new AtomicLong();
    Callable<Long> c =
        () -> {
          long id = idx.getAndIncrement();
          if (id % 2 == 0) {
            throw new Exception("TestException" + id);
          }
          return id;
        };
    List<Callable<Long>> tasks = List.of(c, c, c, c, c);
    IOException ex =
        expectThrows(
            IOException.class,
            () -> StreamExecutorHelper.submitAllAndAwaitAggregatingExceptions(tasks, "test"));
    List<String> results = new ArrayList<>();
    results.add(ex.getCause().getMessage());
    for (var s : ex.getSuppressed()) {
      results.add(s.getMessage());
    }
    Collections.sort(results);
    List<String> expected = List.of("TestException0", "TestException2", "TestException4");
    assertEquals(expected, results);
  }
}
