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

import java.lang.invoke.MethodHandles;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for retrying operations with various strategies.
 **/
public class RetryUtil {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /**
   * Interface for commands that can be retried and may throw exceptions.
   *
   * <p>Implementations should define the operation to be executed in the {@link #execute()} method.
   */
  public interface RetryCmd {
    void execute() throws Exception;
  }

  /**
   * Interface for commands that return a boolean result indicating success or failure.
   *
   * <p>Implementations should return {@code true} when the operation succeeds, or {@code false}
   * when it should be retried.
   */
  public interface BooleanRetryCmd {
    boolean execute();
  }

  /**
   * Retries a command when a specific exception type occurs, until successful or timeout is
   * reached.
   *
   * <p>This is a convenience method that delegates to {@link #retryOnException(Set, long, long,
   * RetryCmd)} with a singleton set containing the specified exception class.
   *
   * @param clazz the exception class to retry on
   * @param timeoutms maximum time to retry in milliseconds
   * @param intervalms wait interval between retries in milliseconds
   * @param cmd the command to execute
   * @throws Exception if the command fails with a different exception, or if timeout is reached
   * @throws InterruptedException if the thread is interrupted while sleeping between retries
   */
  public static void retryOnException(
      Class<? extends Exception> clazz, long timeoutms, long intervalms, RetryCmd cmd)
      throws Exception {
    retryOnException(Collections.singleton(clazz), timeoutms, intervalms, cmd);
  }

  /**
   * Retries a command when any of the specified exception types occur, until successful or timeout
   * is reached.
   *
   * <p>The command is executed repeatedly until it succeeds (completes without throwing an
   * exception) or the timeout is reached. If an exception matching one of the specified classes is
   * thrown and there is time remaining, the method will sleep for {@code intervalms} milliseconds
   * before retrying. If a different exception is thrown, or if the timeout is reached, the
   * exception is rethrown.
   *
   * @param classes set of exception classes to retry on
   * @param timeoutms maximum time to retry in milliseconds
   * @param intervalms wait interval between retries in milliseconds
   * @param cmd the command to execute
   * @throws Exception if the command fails with an exception not in the specified set, or if
   *     timeout is reached
   * @throws InterruptedException if the thread is interrupted while sleeping between retries
   */
  public static void retryOnException(
      Set<Class<? extends Exception>> classes, long timeoutms, long intervalms, RetryCmd cmd)
      throws Exception {
    long timeout =
        System.nanoTime() + TimeUnit.NANOSECONDS.convert(timeoutms, TimeUnit.MILLISECONDS);
    while (true) {
      try {
        cmd.execute();
      } catch (Exception t) {
        if (isInstanceOf(classes, t) && System.nanoTime() < timeout) {
          if (log.isInfoEnabled()) {
            log.info("Retry due to Exception, {} ", t.getClass().getName(), t);
          }
          Thread.sleep(intervalms);
          continue;
        }
        throw t;
      }
      // success
      break;
    }
  }

  private static boolean isInstanceOf(Set<Class<? extends Exception>> classes, Throwable t) {
    for (Class<? extends Exception> c : classes) {
      if (c.isInstance(t)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Retries a command until it returns {@code true} or the maximum number of retries is reached.
   *
   * <p>The command is executed up to {@code retries} times. After each failed attempt (when the
   * command returns {@code false}), the method pauses for the specified duration before retrying.
   * If all retries are exhausted without success, a {@link SolrException} is thrown with the
   * provided error message.
   *
   * @param errorMessage the error message to use if all retries are exhausted
   * @param retries maximum number of retry attempts
   * @param pauseTime duration to pause between retries
   * @param pauseUnit time unit for the pause duration
   * @param cmd the command to execute
   * @throws InterruptedException if the thread is interrupted while sleeping between retries
   * @throws SolrException if all retries are exhausted without success
   */
  public static void retryUntil(
      String errorMessage, int retries, long pauseTime, TimeUnit pauseUnit, BooleanRetryCmd cmd)
      throws InterruptedException {
    while (retries-- > 0) {
      if (cmd.execute()) return;
      pauseUnit.sleep(pauseTime);
    }
    throw new SolrException(ErrorCode.SERVER_ERROR, errorMessage);
  }

  /**
   * Retries a command until it returns {@code true} or the timeout is reached.
   *
   * <p>The command is executed repeatedly until it returns {@code true} or the timeout expires. If
   * the command returns {@code false} and there is time remaining, the method sleeps for {@code
   * intervalms} milliseconds before retrying. If the timeout is reached before the command
   * succeeds, a {@link SolrException} is thrown.
   *
   * @param timeoutms maximum time to retry in milliseconds
   * @param intervalms wait interval between retries in milliseconds
   * @param cmd the command to execute
   * @throws SolrException if the timeout is reached before the command succeeds, or if the thread
   *     is interrupted while sleeping between retries
   */
  public static void retryOnBoolean(long timeoutms, long intervalms, BooleanRetryCmd cmd) {
    long timeout =
        System.nanoTime() + TimeUnit.NANOSECONDS.convert(timeoutms, TimeUnit.MILLISECONDS);
    while (true) {
      boolean resp = cmd.execute();
      if (!resp && System.nanoTime() < timeout) {
        try {
          Thread.sleep(intervalms);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new SolrException(ErrorCode.SERVER_ERROR, "Interrupted while retrying operation");
        }
        continue;
      } else if (System.nanoTime() >= timeout) {
        throw new SolrException(ErrorCode.SERVER_ERROR, "Timed out while retrying operation");
      }

      // success
      break;
    }
  }
}
