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
package org.apache.solr.client.solrj.util;

import java.io.InputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;

import org.apache.solr.SolrTestCase;
import org.apache.solr.client.solrj.impl.HttpClientUtil;

import org.eclipse.jetty.client.HttpResponse;
import org.eclipse.jetty.util.Callback;

import static org.hamcrest.core.StringContains.containsString;

public class TestInputStreamResponseListener extends SolrTestCase {

  public void testNoDataTriggersWaitLimit() throws Exception {
    final long waitLimit = 1000; // millis
    final InputStreamResponseListener listener = new InputStreamResponseListener(waitLimit);

    // nocommit: we should be able to use a null requestTimeout to pass this test....
    // nocommit: (the waitLimit should be enough to trigger failure)
    listener.setRequestTimeout(Instant.now()); // nocommit
    // nocommit: // listener.setRequestTimeout(null);
    
    // emulate low level transport code providing headers, and then nothing else...
    final HttpResponse dummyResponse = new HttpResponse(null /* bogus request */, Collections.emptyList());
    listener.onHeaders(dummyResponse);

    // client tries to consume, but there is never any content...
    assertEquals(dummyResponse, listener.get(0, TimeUnit.SECONDS));
    final ForkJoinTask<IOException> readTask = ForkJoinPool.commonPool().submit(() -> {
        try (final InputStream stream = listener.getInputStream()) {
          return expectThrows(IOException.class, () -> {
              int trash = stream.read();
            });
        }
      });
    final IOException expected = readTask.get(waitLimit * 2L, TimeUnit.MILLISECONDS);
    assertNotNull(expected.getCause());
    assertEquals(TimeoutException.class, expected.getCause().getClass());

    // nocommit: this should be something about waitLimit...
    assertThat(expected.getCause().getMessage(), containsString("requestTimeout exceeded"));
  }


      
  public void testReallySlowDataTriggersRequestTimeout() throws Exception {
    final long writeDelayMillies = 500;
    final InputStreamResponseListener listener = new InputStreamResponseListener(writeDelayMillies * 2);
    
    // emulate low level transport code providing headers, and then writes a (slow) never ending stream of bytes
    final HttpResponse dummyResponse = new HttpResponse(null /* bogus request */, Collections.emptyList());
    listener.onHeaders(dummyResponse);
    final CountDownLatch writeTaskCloseLatch = new CountDownLatch(1);
    try {
      final ForkJoinTask<Boolean> writeTask = ForkJoinPool.commonPool().submit(() -> {
          final ByteBuffer dataToWriteForever = ByteBuffer.allocate(5);
          while (0 < writeTaskCloseLatch.getCount()) {
            dataToWriteForever.position(0);
            listener.onContent(dummyResponse, dataToWriteForever, Callback.NOOP);
            Thread.sleep(writeDelayMillies);
          }
          return true;
        });

      // client reads "forever" ... until read times out because requestTimeout exceeded
      assertEquals(dummyResponse, listener.get(0, TimeUnit.SECONDS));
      final IOException expected = expectThrows(IOException.class, () -> {
          final Instant requestTimeout = Instant.now().plus(1, ChronoUnit.MINUTES);
          listener.setRequestTimeout(requestTimeout);
          final Instant forever = requestTimeout.plusSeconds(60);
          try (final InputStream stream = listener.getInputStream()) {
            while (Instant.now().isBefore(forever)) {
              int trash = stream.read(); // this should eventually throw an exception
            }
          }
        });
      assertNotNull(expected.getCause());
      assertEquals(TimeoutException.class, expected.getCause().getClass());
      assertThat(expected.getCause().getMessage(), containsString("requestTimeout exceeded"));
    } finally {
      writeTaskCloseLatch.countDown();
    }
  }
}
