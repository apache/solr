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

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;

public class TestRetryUtil extends SolrTestCaseJ4 {

  public void testRetryOnThrowable() throws Throwable {
    final AtomicInteger executes = new AtomicInteger();
    RetryUtil.retryOnException(
        SolrException.class,
        10000,
        10,
        () -> {
          int calls = executes.incrementAndGet();
          if (calls <= 2) {
            throw new SolrException(ErrorCode.SERVER_ERROR, "Bad Stuff Happened");
          }
        });

    assertEquals(3, executes.get());

    executes.set(0);
    assertThrows(
        SolrException.class,
        () ->
            RetryUtil.retryOnException(
                IllegalStateException.class,
                10000,
                10,
                () -> {
                  int calls = executes.incrementAndGet();
                  if (calls <= 2) {
                    throw new SolrException(ErrorCode.SERVER_ERROR, "Bad Stuff Happened");
                  }
                }));
    assertEquals(1, executes.get());

    executes.set(0);
    assertThrows(
        SolrException.class,
        () ->
            RetryUtil.retryOnException(
                SolrException.class,
                1000,
                10,
                () -> {
                  executes.incrementAndGet();
                  throw new SolrException(ErrorCode.SERVER_ERROR, "Bad Stuff Happened");
                }));

    assertTrue(executes.get() > 1);
  }
}
