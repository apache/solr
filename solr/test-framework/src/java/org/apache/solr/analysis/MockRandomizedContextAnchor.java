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
package org.apache.solr.analysis;

import java.lang.invoke.MethodHandles;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayDeque;
import java.util.IdentityHashMap;
import java.util.Map;

import com.carrotsearch.randomizedtesting.RandomizedContext;
import com.carrotsearch.randomizedtesting.Randomness;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test-only bridge that lets {@link org.apache.lucene.analysis.MockTokenizer} (and any other
 * test analyzer that calls {@link RandomizedContext#current()}) be constructed on threads that
 * are NOT owned by the current {@code RandomizedRunner} suite.
 *
 * <p>This fork routes embedded Jetty / cluster request handling through long-lived shared thread
 * pools (ParWork / SolrQTP). Those worker threads are created once and then reused across many
 * test classes within a single test JVM, so their {@link ThreadGroup} belongs to whichever test
 * class happened to start the pool first. After that class finishes, the runner disposes its
 * context, and {@code RandomizedContext.current()} throws
 * {@code "No context information for thread"} for every subsequent class whose analysis runs on
 * one of those reused threads. {@code MockTokenizer}'s constructor unconditionally calls
 * {@code RandomizedContext.current().getRandom()}, so indexing fails with a spurious
 * "possible analysis error" (HTTP 400/500).
 *
 * <p>To repair this without touching the (performance-sensitive) production threading model, the
 * current suite captures its live {@link RandomizedContext} in {@code @BeforeClass}
 * ({@link #capture()}). When a test analyzer is later built on an orphaned pool thread,
 * {@link #ensureContext()} reflectively (a) registers the captured context for the calling
 * thread's group and (b) seeds per-thread randomness for the calling thread, so
 * {@code RandomizedContext.current()} resolves to the live suite context.
 *
 * <p>Everything here is best-effort and fails open: if any reflection step throws, the original
 * behaviour is preserved (the analyzer construction proceeds and will fail exactly as before).
 * This class lives only in the test framework and has zero production / hot-path impact.
 */
public final class MockRandomizedContextAnchor {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static volatile RandomizedContext anchorContext;
  private static volatile Randomness anchorRandomness;

  // Reflection handles into com.carrotsearch.randomizedtesting internals, resolved once.
  private static final Object GLOBAL_LOCK;
  private static final Field CONTEXTS_FIELD;            // static IdentityHashMap<ThreadGroup,RandomizedContext>
  private static final Field PER_THREAD_RESOURCES_FIELD; // instance WeakHashMap<Thread,PerThreadResources>
  private static final Constructor<?> PER_THREAD_CTOR;   // PerThreadResources()
  private static final Field RANDOMNESSES_FIELD;         // ArrayDeque<Randomness>
  private static final Method RANDOMNESS_CLONE;          // Randomness.clone(Thread)
  private static final boolean AVAILABLE;

  static {
    Object globalLock = null;
    Field contexts = null, perThread = null, randomnesses = null;
    Constructor<?> ptrCtor = null;
    Method clone = null;
    boolean ok = false;
    try {
      Field gl = RandomizedContext.class.getDeclaredField("_globalLock");
      gl.setAccessible(true);
      globalLock = gl.get(null);

      contexts = RandomizedContext.class.getDeclaredField("contexts");
      contexts.setAccessible(true);

      perThread = RandomizedContext.class.getDeclaredField("perThreadResources");
      perThread.setAccessible(true);

      Class<?> ptrClass = Class.forName(
          "com.carrotsearch.randomizedtesting.RandomizedContext$PerThreadResources");
      ptrCtor = ptrClass.getDeclaredConstructor();
      ptrCtor.setAccessible(true);

      randomnesses = ptrClass.getDeclaredField("randomnesses");
      randomnesses.setAccessible(true);

      clone = Randomness.class.getDeclaredMethod("clone", Thread.class);
      clone.setAccessible(true);

      ok = true;
    } catch (Throwable t) {
      log.info("MockRandomizedContextAnchor disabled (reflection unavailable): {}", t.toString());
    }
    GLOBAL_LOCK = globalLock;
    CONTEXTS_FIELD = contexts;
    PER_THREAD_RESOURCES_FIELD = perThread;
    PER_THREAD_CTOR = ptrCtor;
    RANDOMNESSES_FIELD = randomnesses;
    RANDOMNESS_CLONE = clone;
    AVAILABLE = ok;
  }

  private MockRandomizedContextAnchor() {}

  /** Capture the live suite context. Call from a thread owned by the current suite (@BeforeClass). */
  public static void capture() {
    if (!AVAILABLE) return;
    try {
      RandomizedContext ctx = RandomizedContext.current();
      anchorContext = ctx;
      anchorRandomness = ctx.getRandomness();
      registerRootAnchor(ctx);
    } catch (Throwable t) {
      // No live context on this thread; leave any previous anchor in place.
    }
  }

  /**
   * Register the captured context against the JVM's top-most {@link ThreadGroup}.
   *
   * <p>{@code RandomizedContext.context(Thread)} resolves a thread's context by walking UP the
   * thread-group parent chain and, once a context is found, auto-creates per-thread randomness for
   * the calling thread (cloned from the runner's randomness). By anchoring the live suite context
   * at the root group, every orphaned shared-pool thread (recovery/IndexFetcher/ParWork/SolrQTP
   * workers whose group belongs to a long-finished test class) resolves cleanly instead of
   * throwing {@code "No context information for thread"}. This matters beyond MockTokenizer: e.g.
   * {@code BaseDirectoryWrapper.close()} runs {@code TestUtil.checkIndex} (which calls
   * {@code getRandom()}) BEFORE the real close, so a missing context aborts the close and leaks the
   * directory, breaking recovery's index swap.
   *
   * <p>Real per-class contexts stay keyed by their own {@code TGRP-<class>} group (a nearer
   * ancestor for the test's own threads), so this root anchor only catches threads that have no
   * closer registered context. The single root entry is overwritten on each class's capture(), so
   * it does not accumulate.
   */
  private static void registerRootAnchor(RandomizedContext ctx) {
    if (GLOBAL_LOCK == null) return;
    try {
      ThreadGroup root = Thread.currentThread().getThreadGroup();
      if (root == null) return;
      while (root.getParent() != null) {
        root = root.getParent();
      }
      synchronized (GLOBAL_LOCK) {
        @SuppressWarnings("unchecked")
        IdentityHashMap<ThreadGroup, RandomizedContext> contexts =
            (IdentityHashMap<ThreadGroup, RandomizedContext>) CONTEXTS_FIELD.get(null);
        contexts.put(root, ctx);
      }
    } catch (Throwable t) {
      log.debug("registerRootAnchor failed", t);
    }
  }

  /**
   * Ensure {@code RandomizedContext.current()} resolves on the calling thread. No-op if the thread
   * already has a context or if no anchor has been captured.
   */
  public static void ensureContext() {
    if (!AVAILABLE) return;
    try {
      RandomizedContext.current();
      return; // already resolvable
    } catch (Throwable expected) {
      // fall through and try to register
    }
    RandomizedContext ctx = anchorContext;
    Randomness rnd = anchorRandomness;
    if (ctx == null || rnd == null || GLOBAL_LOCK == null) return;
    try {
      Thread t = Thread.currentThread();
      ThreadGroup g = t.getThreadGroup();
      if (g == null) return;
      synchronized (GLOBAL_LOCK) {
        @SuppressWarnings("unchecked")
        IdentityHashMap<ThreadGroup, RandomizedContext> contexts =
            (IdentityHashMap<ThreadGroup, RandomizedContext>) CONTEXTS_FIELD.get(null);
        contexts.put(g, ctx);

        @SuppressWarnings("unchecked")
        Map<Thread, Object> perThread = (Map<Thread, Object>) PER_THREAD_RESOURCES_FIELD.get(ctx);
        if (perThread.get(t) == null) {
          Object res = PER_THREAD_CTOR.newInstance();
          @SuppressWarnings("unchecked")
          ArrayDeque<Randomness> deque = (ArrayDeque<Randomness>) RANDOMNESSES_FIELD.get(res);
          Randomness clone = (Randomness) RANDOMNESS_CLONE.invoke(rnd, t);
          deque.addFirst(clone);
          perThread.put(t, res);
        }
      }
    } catch (Throwable t) {
      // fail open — analyzer construction will proceed and fail exactly as it would have
      log.debug("ensureContext failed", t);
    }
  }
}
