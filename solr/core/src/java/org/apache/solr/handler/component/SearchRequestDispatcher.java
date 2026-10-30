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
package org.apache.solr.handler.component;

import java.io.Closeable;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import org.apache.solr.common.util.EnvUtils;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Holds the {@link SearchRequestListener}s of a node and delivers each reported search to them. */
public class SearchRequestDispatcher implements Closeable {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final String MARKER = SearchRequestDispatcher.class.getName();

  public static final String QUEUE_SIZE_PROP = "solr.search.listener.queue.size";

  static final int DEFAULT_QUEUE_SIZE = 1000;

  static final long CLOSE_TIMEOUT_MS = 5000;

  static final long LOST_WARN_INTERVAL_MS = 60_000;

  private record Marker(ResponseBuilder rb) {}

  record Delivery(SearchRequestListener listener, Object captured) {}

  private record Event(SearchRequestInfo info, List<Delivery> deliveries) {}

  private final List<SearchRequestListener> listeners = new CopyOnWriteArrayList<>();

  private final BlockingQueue<Event> queue;

  private final LongAdder lost = new LongAdder();

  private final AtomicLong lastWarnNanos;

  private volatile boolean closed;

  private Thread worker;

  public SearchRequestDispatcher() {
    this(EnvUtils.getPropertyAsInteger(QUEUE_SIZE_PROP, DEFAULT_QUEUE_SIZE));
  }

  SearchRequestDispatcher(int queueSize) {
    queue = new ArrayBlockingQueue<>(Math.max(1, queueSize));
    lastWarnNanos =
        new AtomicLong(System.nanoTime() - TimeUnit.MILLISECONDS.toNanos(LOST_WARN_INTERVAL_MS));
  }

  /**
   * Adds a listener, starting the delivery thread on first use.
   *
   * @throws IllegalStateException if this dispatcher is closed
   */
  public synchronized void add(SearchRequestListener listener) {
    if (closed) {
      throw new IllegalStateException("SearchRequestDispatcher is closed");
    }
    listeners.add(listener);
    if (worker == null) {
      worker = new Thread(this::deliver, "searchRequestListener");
      worker.setDaemon(true);
      worker.start();
    }
  }

  public boolean remove(SearchRequestListener listener) {
    return listeners.remove(listener);
  }

  public List<SearchRequestListener> listeners() {
    return Collections.unmodifiableList(listeners);
  }

  static void mark(SolrQueryRequest req, ResponseBuilder rb) {
    req.getContext().put(MARKER, new Marker(rb));
  }

  /** Drops the report a {@link SearchHandler} requested for {@code req}, if any. */
  public static void unmark(SolrQueryRequest req) {
    req.getContext().remove(MARKER);
  }

  /**
   * Reports {@code req} to the listeners if a {@link SearchHandler} marked it. Called once the
   * response header has its status and QTime.
   */
  public void afterResponse(SolrQueryRequest req, SolrQueryResponse rsp, long qTimeMs) {
    if (!(req.getContext().remove(MARKER) instanceof Marker marker)) {
      return;
    }
    SearchRequestInfo info = SearchRequestInfo.from(req, rsp, marker.rb(), qTimeMs);
    List<Delivery> deliveries = null;
    for (SearchRequestListener listener : listeners) {
      Object captured;
      try {
        captured = listener.capture(info, req, rsp, marker.rb());
      } catch (Exception e) {
        log.warn("SearchRequestListener {} capture failed", listener.getClass().getName(), e);
        continue;
      }
      if (captured != null) {
        if (deliveries == null) {
          deliveries = new ArrayList<>(listeners.size());
        }
        deliveries.add(new Delivery(listener, captured));
      }
    }
    if (deliveries != null) {
      offer(info, deliveries);
    }
  }

  void offer(SearchRequestInfo info, List<Delivery> deliveries) {
    Event event = new Event(info, deliveries);
    if (closed) {
      lost.increment();
    } else if (!queue.offer(event)) {
      lost.increment();
      warnLost();
    } else if (closed && queue.remove(event)) {
      lost.increment();
    }
  }

  private void warnLost() {
    long last = lastWarnNanos.get();
    long now = System.nanoTime();
    if (now - last >= TimeUnit.MILLISECONDS.toNanos(LOST_WARN_INTERVAL_MS)
        && lastWarnNanos.compareAndSet(last, now)) {
      log.warn(
          "SearchRequestListener queue is full, {} searches not delivered so far; see {}",
          lost.sum(),
          QUEUE_SIZE_PROP);
    }
  }

  private void deliver() {
    while (!closed) {
      Event event;
      try {
        event = queue.take();
      } catch (InterruptedException e) {
        return;
      }
      if (closed) {
        lost.increment();
        return;
      }
      for (Delivery delivery : event.deliveries()) {
        if (closed) {
          return;
        }
        SearchRequestListener listener = delivery.listener();
        if (!listeners.contains(listener)) {
          continue;
        }
        try {
          listener.onRequestEnd(event.info(), delivery.captured());
        } catch (VirtualMachineError e) {
          throw e;
        } catch (Throwable t) {
          log.warn("SearchRequestListener {} failed", listener.getClass().getName(), t);
        }
      }
    }
  }

  /** The number of searches not delivered because the queue was full or the node was closing. */
  public long lost() {
    return lost.sum();
  }

  synchronized Thread worker() {
    return worker;
  }

  /**
   * Stops the delivery thread, waiting up to {@link #CLOSE_TIMEOUT_MS} for the listener call in
   * progress. Queued searches are discarded and counted in {@link #lost()}.
   */
  @Override
  public void close() {
    Thread thread;
    synchronized (this) {
      if (closed) {
        return;
      }
      closed = true;
      thread = worker;
    }
    if (thread != null) {
      thread.interrupt();
      try {
        thread.join(CLOSE_TIMEOUT_MS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
    List<Event> discarded = new ArrayList<>();
    queue.drainTo(discarded);
    lost.add(discarded.size());
  }
}
