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

import java.io.Serializable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Reentrant lock that can be used in a try-with-resources statement.
 *
 * <p>Typical usage:
 *
 * <pre>
 * try (AutoLock lock = this.lock.lock())
 * {
 *     // Something
 * }
 * </pre>
 */
public class AutoLock implements AutoCloseable, Serializable {
  private static final long serialVersionUID = 3300696774541816341L;

  private final ReentrantLock _lock = new ReentrantLock();

  /**
   * Acquires the lock.
   *
   * @return this AutoLock for unlocking
   */
  public AutoLock lock() {
    _lock.lock();
    return this;
  }

  /**
   * @see ReentrantLock#isHeldByCurrentThread()
   * @return whether this lock is held by the current thread
   */
  public boolean isHeldByCurrentThread() {
    return _lock.isHeldByCurrentThread();
  }

  /**
   * @return a {@link Condition} associated with this lock
   */
  public Condition newCondition() {
    return _lock.newCondition();
  }

  // Package-private for testing only.
  boolean isLocked() {
    return _lock.isLocked();
  }

  @Override
  public void close() {
    _lock.unlock();
  }

  /**
   * A reentrant lock with a condition that can be used in a try-with-resources statement.
   *
   * <p>Typical usage:
   *
   * <pre>
   * // Waiting
   * try (AutoLock lock = _lock.lock())
   * {
   *     lock.await();
   * }
   *
   * // Signaling
   * try (AutoLock lock = _lock.lock())
   * {
   *     lock.signalAll();
   * }
   * </pre>
   */
  public static class WithCondition extends AutoLock {
    private final Condition _condition = newCondition();

    @Override
    public AutoLock.WithCondition lock() {
      return (WithCondition) super.lock();
    }

    /**
     * @see Condition#signal()
     */
    public void signal() {
      _condition.signal();
    }

    /**
     * @see Condition#signalAll()
     */
    public void signalAll() {
      _condition.signalAll();
    }

    /**
     * @see Condition#await()
     * @throws InterruptedException if the current thread is interrupted
     */
    public void await() throws InterruptedException {
      _condition.await();
    }

    /**
     * @see Condition#await(long, TimeUnit)
     * @param time the time to wait
     * @param unit the time unit
     * @return false if the waiting time elapsed
     * @throws InterruptedException if the current thread is interrupted
     */
    public boolean await(long time, TimeUnit unit) throws InterruptedException {
      return _condition.await(time, unit);
    }
  }
}
