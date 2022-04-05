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

//  Copyright (c) 1995-2022 Mort Bay Consulting Pty Ltd and others.

package org.apache.solr.client.solrj.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousCloseException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Queue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.Response;
import org.eclipse.jetty.client.api.Response.Listener;
import org.eclipse.jetty.client.api.Result;
import org.eclipse.jetty.client.util.DeferredContentProvider;
import org.eclipse.jetty.util.BufferUtil;
import org.eclipse.jetty.util.Callback;
import org.eclipse.jetty.util.IO;
import org.eclipse.jetty.util.log.Log;
import org.eclipse.jetty.util.log.Logger;

/**
 * Fork of jetty's <code>InputStreamResponseListener</code> that adds support for an (optional)
 * <code>requestTimeout</code> (which defaults to 1 hour from instantiation) as well as a
 * (mandatory) <code>maxWaitLimit</code> (millis) to work around <a
 * href="https://issues.apache.org/jira/browse/SOLR-16099">SOLR-16099</a>
 *
 * <p>Implementation of {@link Listener} that produces an {@link InputStream} that allows
 * applications to read the response content.
 *
 * <p>Typical usage is:
 *
 * <pre>
 * long maxWaitLimit = 5000;
 * InputStreamResponseListener listener = new InputStreamResponseListener(maxWaitLimit);
 * client.newRequest(...).send(listener);
 *
 * // Wait for the response headers to arrive
 * Response response = listener.get(5, TimeUnit.SECONDS);
 * if (response.getStatus() == 200)
 * {
 *     // Obtain the input stream on the response content
 *     try (InputStream input = listener.getInputStream())
 *     {
 *         // Read the response content
 *     }
 * }
 * </pre>
 *
 * <p>The {@link HttpClient} implementation (the producer) will feed the input stream asynchronously
 * while the application (the consumer) is reading from it.
 *
 * <p>If the consumer is faster than the producer, then the consumer will block with the typical
 * {@link InputStream#read()} semantic. If the consumer is slower than the producer, then the
 * producer will block until the client consumes.
 *
 * @see <a href="https://github.com/eclipse/jetty.project/pull/7260">Jetty PR#7260</a>
 */
public class InputStreamResponseListener extends Listener.Adapter {
  private static final Logger log = Log.getLogger(InputStreamResponseListener.class);
  private static final DeferredContentProvider.Chunk EOF =
      new DeferredContentProvider.Chunk(BufferUtil.EMPTY_BUFFER, Callback.NOOP);
  private final AutoLock.WithCondition lock = new AutoLock.WithCondition();
  private final CountDownLatch responseLatch = new CountDownLatch(1);
  private final CountDownLatch resultLatch = new CountDownLatch(1);
  private final AtomicReference<InputStream> stream = new AtomicReference<>();
  private final Queue<DeferredContentProvider.Chunk> chunks = new ArrayDeque<>();
  private final AtomicReference<Instant> requestTimeoutRef =
      new AtomicReference<>(Instant.now().plus(1, ChronoUnit.HOURS));
  private final long maxWaitLimit;
  private Response response;
  private Result result;
  private Throwable failure;
  private boolean closed;

  /**
   * @param maxWaitLimit positive millisecond limit to {@link #wait} for individual response chunks
   *     when callers are {@link InputStream#read}ing from the the <code>InputStream</code> returned
   *     by {@link #getInputStream()}
   */
  public InputStreamResponseListener(final long maxWaitLimit) {
    super();
    if (maxWaitLimit <= 0L) {
      throw new IllegalArgumentException("maxWaitLimit must be greater then 0 milliseconds");
    }
    this.maxWaitLimit = maxWaitLimit;
  }

  /**
   * Change the hard limit on when the entire response must be recieved, or {@link InputStream#read}
   * will throw an {@link IOException} wrapping a {@link TimeoutException}. Defaults to 1 HOUR from
   * when this Listener was constructed.
   *
   * <p><b>NOTE:</b> This timeout is only checked when the caller is blocked waiting for more data
   * to be recieved, it will not cause any failures in situations where the caller is slower to
   * consume the content then the remote server is to provided it.
   *
   * @param requestTimeout Instant past which all response chunks must be recieved, if null then
   *     {@link Instant#MAX} is used
   * @see #getInputStream
   */
  public void setRequestTimeout(final Instant requestTimeout) {
    requestTimeoutRef.set(null == requestTimeout ? Instant.MAX : requestTimeout);
  }

  @Override
  @SuppressWarnings("try")
  public void onHeaders(Response response) {
    try (AutoLock l = lock.lock()) {
      this.response = response;
      responseLatch.countDown();
    }
  }

  @Override
  @SuppressWarnings("try")
  public void onContent(Response response, ByteBuffer content, Callback callback) {
    if (content.remaining() == 0) {
      if (log.isDebugEnabled()) {
        log.debug("Skipped empty content {}", content);
      }
      callback.succeeded();
      return;
    }

    boolean closed;
    try (AutoLock.WithCondition l = lock.lock()) {
      closed = this.closed;
      if (!closed) {
        if (log.isDebugEnabled()) {
          log.debug("Queueing content {}", content);
        }
        chunks.add(new DeferredContentProvider.Chunk(content, callback));
        l.signalAll();
      }
    }

    if (closed) {
      if (log.isDebugEnabled()) {
        log.debug("InputStream closed, ignored content {}", content);
      }
      callback.failed(new AsynchronousCloseException());
    }
  }

  @Override
  @SuppressWarnings("try")
  public void onSuccess(Response response) {
    try (AutoLock.WithCondition l = lock.lock()) {
      if (!closed) chunks.add(EOF);
      l.signalAll();
    }

    if (log.isDebugEnabled()) {
      log.debug("End of content");
    }
  }

  @Override
  @SuppressWarnings("try")
  public void onFailure(Response response, Throwable failure) {
    List<Callback> callbacks;
    try (AutoLock.WithCondition l = lock.lock()) {
      if (this.failure != null) return;
      if (failure == null) {
        failure = new IOException("Generic failure");
        log.warn("Missing failure in onFailure() callback", failure);
      }
      this.failure = failure;
      callbacks = drain();
      l.signalAll();
    }

    if (log.isDebugEnabled()) {
      log.debug("Content failure", failure);
    }

    Throwable f = failure;
    callbacks.forEach(callback -> callback.failed(f));
  }

  @Override
  @SuppressWarnings("try")
  public void onComplete(Result result) {
    Throwable failure = result.getFailure();
    List<Callback> callbacks = Collections.emptyList();
    try (AutoLock.WithCondition l = lock.lock()) {
      this.result = result;
      if (result.isFailed() && this.failure == null) {
        this.failure = failure;
        callbacks = drain();
      }
      // Notify the response latch in case of request failures.
      responseLatch.countDown();
      resultLatch.countDown();
      l.signalAll();
    }

    if (log.isDebugEnabled()) {
      if (failure == null) {
        log.debug("Result success");
      } else {
        log.debug("Result failure", failure);
      }
    }

    callbacks.forEach(callback -> callback.failed(failure));
  }

  /**
   * Waits for the given timeout for the response to be available, then returns it.
   *
   * <p>The wait ends as soon as all the HTTP headers have been received, without waiting for the
   * content. To wait for the whole content, see {@link #await(long, TimeUnit)}.
   *
   * @param timeout the time to wait
   * @param unit the timeout unit
   * @return the response
   * @throws InterruptedException if the thread is interrupted
   * @throws TimeoutException if the timeout expires
   * @throws ExecutionException if a failure happened
   */
  @SuppressWarnings("try")
  public Response get(long timeout, TimeUnit unit)
      throws InterruptedException, TimeoutException, ExecutionException {
    boolean expired = !responseLatch.await(timeout, unit);
    if (expired) throw new TimeoutException();
    try (AutoLock l = lock.lock()) {
      // If the request failed there is no response.
      if (response == null) throw new ExecutionException(failure);
      return response;
    }
  }

  /**
   * Waits for the given timeout for the whole request/response cycle to be finished, then returns
   * the corresponding result.
   *
   * <p>
   *
   * @param timeout the time to wait
   * @param unit the timeout unit
   * @return the result
   * @throws InterruptedException if the thread is interrupted
   * @throws TimeoutException if the timeout expires
   * @see #get(long, TimeUnit)
   */
  @SuppressWarnings("try")
  public Result await(long timeout, TimeUnit unit) throws InterruptedException, TimeoutException {
    boolean expired = !resultLatch.await(timeout, unit);
    if (expired) throw new TimeoutException();
    try (AutoLock l = lock.lock()) {
      return result;
    }
  }

  /**
   * Returns an {@link InputStream} providing the response content bytes.
   *
   * <p>The method may be invoked only once; subsequent invocations will return a closed {@link
   * InputStream}.
   *
   * <p>{@link InputStream#read} calls on this <code>InputStream</code> may block up to the
   * configured <code>maxWaitLimit</code> or until the {@link #setRequestTimeout} (which ever is
   * sooner) if no data is currently available, at which point an {@link IOException} wrapping a
   * {@link TimeoutException} wll be thrown
   *
   * @return an input stream providing the response content
   */
  public InputStream getInputStream() {
    InputStream result = new Input();
    if (stream.compareAndSet(null, result)) return result;
    return IO.getClosedStream();
  }

  @SuppressWarnings("try")
  private List<Callback> drain() {
    List<Callback> callbacks = new ArrayList<>();
    try (AutoLock l = lock.lock()) {
      while (true) {
        DeferredContentProvider.Chunk chunk = chunks.peek();
        if (chunk == null || chunk == EOF) break;
        callbacks.add(chunk.callback);
        chunks.poll();
      }
    }
    return callbacks;
  }

  @Override
  @SuppressWarnings("try")
  public String toString() {
    try (AutoLock l = lock.lock()) {
      return String.format(
          Locale.ROOT,
          "%s@%x[response=%s,result=%s,closed=%b,failure=%s,chunks=%s]",
          getClass().getSimpleName(),
          hashCode(),
          response,
          result,
          closed,
          failure,
          chunks);
    }
  }

  private class Input extends InputStream {
    @Override
    public int read() throws IOException {
      byte[] tmp = new byte[1];
      int read = read(tmp);
      if (read < 0) return read;
      return tmp[0] & 0xFF;
    }

    /**
     * awaits on the condition until either <code>maxWaitLimit</code> or <code>requestTimeout</code>
     * is reached (whichever is sooner)
     *
     * @return an explantion as to why the condition wait expired, or null if the condition was met
     *     in a timely manner
     */
    private final String awaitOrReturnError(final AutoLock.WithCondition condition)
        throws InterruptedException {
      final Instant effectiveNow = Instant.now();
      final Instant requestTimeout = requestTimeoutRef.get();
      assert null != requestTimeout;

      if (effectiveNow.isBefore(requestTimeout)) {
        // NOTE: convert maxWaitLimit to Instant for comparison, rather then vice-versa, so we
        // don't risk ArithemticException.  (await in MILLIS instead of NANOS for same reason)
        final long awaitAmountMillis =
            effectiveNow.plusMillis(maxWaitLimit).isBefore(requestTimeout)
                ? maxWaitLimit
                : Math.min(1L, Duration.between(effectiveNow, requestTimeout).toMillis());

        if (condition.await(awaitAmountMillis, TimeUnit.MILLISECONDS)) {
          return null;
        } else {
          return (awaitAmountMillis < maxWaitLimit ? "requestTimeout" : "maxWaitLimit")
              + " exceeded";
        }
      } // else...

      // we've already reached (or exceeded) requestTimeout w/o any waiting
      return "requestTimeout exceeded";
    }

    @Override
    public int read(byte[] b, int offset, int length) throws IOException {
      try {
        int result = 0;
        Callback callback = null;
        List<Callback> callbacks = Collections.emptyList();
        Throwable timeoutFailure = null;
        try (AutoLock.WithCondition l = lock.lock()) {
          DeferredContentProvider.Chunk chunk;
          while (true) {
            chunk = chunks.peek();
            if (chunk == EOF) return -1;

            if (chunk != null) break;

            if (failure != null) throw toIOException(failure);

            if (closed) throw new AsynchronousCloseException();

            final String expirationReason = awaitOrReturnError(l);
            if (null != expirationReason) {
              if (log.isDebugEnabled()) {
                log.debug(
                    "Read timed out: {}, {}", expirationReason, InputStreamResponseListener.this);
              }
              failure = timeoutFailure = new TimeoutException("Read timeout: " + expirationReason);
              callbacks = drain();
              break;
            }
          }

          if (timeoutFailure == null) {
            ByteBuffer buffer = chunk.buffer;
            result = Math.min(buffer.remaining(), length);
            buffer.get(b, offset, result);
            if (!buffer.hasRemaining()) {
              callback = chunk.callback;
              chunks.poll();
            }
          }
        }
        if (timeoutFailure == null) {
          if (callback != null) callback.succeeded();
          return result;
        } else {
          Throwable f = timeoutFailure;
          callbacks.forEach(c -> c.failed(f));
          throw toIOException(f);
        }
      } catch (InterruptedException x) {
        throw new InterruptedIOException();
      }
    }

    private IOException toIOException(Throwable failure) {
      if (failure instanceof IOException) return (IOException) failure;
      else return new IOException(failure);
    }

    @Override
    public void close() throws IOException {
      List<Callback> callbacks;
      try (AutoLock.WithCondition l = lock.lock()) {
        if (closed) return;
        closed = true;
        callbacks = drain();
        l.signalAll();
      }

      if (log.isDebugEnabled()) {
        log.debug("InputStream close");
      }

      Throwable failure = new AsynchronousCloseException();
      callbacks.forEach(callback -> callback.failed(failure));

      super.close();
    }
  }
}
