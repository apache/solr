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

package org.apache.solr.shell.env;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.invoke.MethodHandles;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A reader/writer lock shared by every forked JVM of a test run, used to keep a test class that
 * needs the machine's whole process list to itself away from classes that are starting Solr nodes.
 *
 * <p>Most commands are already scoped: {@code solr stop --all} walks {@code SOLR_PID_DIR}, which is
 * per class, and {@code solr status -p} and {@code solr stop -p} name a port. The exception is
 * {@code solr status} with no port or URL, which asks the OS for every Solr process on the machine
 * — so under parallel forks it also sees other classes' nodes. A positive assertion ("my port is
 * listed") is fine; an exhaustive or negative one ("no nodes are running") is a race.
 *
 * <p>Classes that start a node take the lock {@linkplain #acquire shared}; a class that needs the
 * process list to itself takes it exclusively and therefore waits for the others to finish and
 * release. The lock lives in the run's shared output directory, which is what makes it work across
 * forks — and the OS drops it if a fork dies, so a crash cannot wedge the run.
 *
 * <p>Note {@link FileLock} is owned by the JVM rather than the thread. That is fine here because a
 * fork runs one test class at a time, but it does mean a class must release before the next one
 * acquires.
 */
public final class ProcessScopeLock implements AutoCloseable {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final String LOCK_FILE = ".process-scope.lock";

  /** How long to wait before concluding that a holder is stuck, rather than hanging the build. */
  private static final Duration ACQUIRE_TIMEOUT = Duration.ofMinutes(10);

  private final FileChannel channel;
  private final FileLock lock;
  private final boolean exclusive;

  private ProcessScopeLock(FileChannel channel, FileLock lock, boolean exclusive) {
    this.channel = channel;
    this.lock = lock;
    this.exclusive = exclusive;
  }

  /**
   * Takes the run's process-scope lock, waiting for conflicting holders.
   *
   * @param exclusive true to exclude every other class, false to run alongside other shared holders
   */
  public static ProcessScopeLock acquire(Path outputDir, boolean exclusive) {
    Path lockFile = outputDir.resolve(LOCK_FILE);
    FileChannel channel = null;
    try {
      Files.createDirectories(outputDir);
      channel =
          FileChannel.open(
              lockFile,
              StandardOpenOption.CREATE,
              StandardOpenOption.READ,
              StandardOpenOption.WRITE);
      FileLock acquired = waitForLock(channel, lockFile, exclusive);
      log.info("Holding {} process-scope lock on {}", exclusive ? "exclusive" : "shared", lockFile);
      return new ProcessScopeLock(channel, acquired, exclusive);
    } catch (IOException e) {
      closeQuietly(channel);
      throw new UncheckedIOException("Could not take the process-scope lock on " + lockFile, e);
    } catch (RuntimeException e) {
      closeQuietly(channel);
      throw e;
    }
  }

  private static FileLock waitForLock(FileChannel channel, Path lockFile, boolean exclusive)
      throws IOException {
    long deadline = System.nanoTime() + ACQUIRE_TIMEOUT.toNanos();
    while (true) {
      try {
        FileLock attempt = channel.tryLock(0L, Long.MAX_VALUE, !exclusive);
        if (attempt != null) {
          return attempt;
        }
      } catch (OverlappingFileLockException e) {
        // This JVM already holds it: a class failed to release. Worth failing loudly rather than
        // waiting out the timeout on ourselves.
        throw new IllegalStateException(
            "This JVM already holds the process-scope lock on "
                + lockFile
                + "; a previous test class did not release it",
            e);
      }
      if (System.nanoTime() >= deadline) {
        throw new IllegalStateException(
            "Waited "
                + ACQUIRE_TIMEOUT.toMinutes()
                + " minutes for the "
                + (exclusive ? "exclusive" : "shared")
                + " process-scope lock on "
                + lockFile
                + ". Another test class is probably wedged holding it.");
      }
      try {
        TimeUnit.MILLISECONDS.sleep(100);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException("Interrupted waiting for " + lockFile, e);
      }
    }
  }

  public boolean isExclusive() {
    return exclusive;
  }

  @Override
  public void close() {
    try {
      lock.release();
    } catch (IOException e) {
      log.warn("Could not release the process-scope lock", e);
    } finally {
      closeQuietly(channel);
    }
  }

  private static void closeQuietly(FileChannel channel) {
    if (channel != null) {
      try {
        channel.close();
      } catch (IOException e) {
        log.warn("Could not close the process-scope lock channel", e);
      }
    }
  }
}
