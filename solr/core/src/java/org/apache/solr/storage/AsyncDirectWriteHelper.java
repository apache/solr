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

package org.apache.solr.storage;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.Phaser;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.apache.solr.util.IOFunction;

public class AsyncDirectWriteHelper implements Closeable {

  private int populatingBuffer = 0;
  private int consumingBuffer = 1;
  private final int blockSize;
  private final boolean useDirectIO;
  private final Struct[] buffers = new Struct[2];
  private final AtomicReference<Future<?>> future = new AtomicReference<>();

  private enum Status {
    SYNC,
    ASYNC,
    FINISHED,
    FLUSH_ASYNC
  }

  private volatile Status status = Status.SYNC;
  private volatile int flushBufferIdx = -1;
  private static final Future<?> CLOSED = new CompletableFuture<>();
  private final long[] writePos = new long[] {-1};
  private final Path path;
  private final FileChannel[] channel = new FileChannel[1];

  private final DirectBufferPool bufferPool;

  public AsyncDirectWriteHelper(
      int blockSize, DirectBufferPool bufferPool, Path path, boolean useDirectIO) {
    this.blockSize = blockSize;
    this.bufferPool = bufferPool;
    this.path = path;
    this.useDirectIO = useDirectIO;
    Function<ByteBuffer, IOFunction<long[], long[]>> writeFunctionSupplier =
        (buffer) -> {
          return (writePos) -> {
            writePos[0] += channel[0].write(buffer, writePos[0]);
            return writePos;
          };
        };
    for (int i = 1; i >= 0; i--) {
      buffers[i] = new Struct(bufferPool, writeFunctionSupplier);
    }
  }

  public ByteBuffer init(long pos) throws IOException {
    writePos[0] = pos;
    assert populatingBuffer == 0;
    return buffers[populatingBuffer].buffer;
  }

  private ByteBuffer syncSwap(ByteBuffer populated) throws IOException {
    Struct sync = buffers[populatingBuffer];
    assert Objects.equals(sync.buffer, populated);
    sync.writeFunction.apply(writePos);
    return populated.clear();
  }

  private IOFunction<long[], long[]> swapConsume() {
    Struct releasing = buffers[consumingBuffer];
    Struct acquiring = buffers[consumingBuffer ^= 1];
    // mark previous buffer as ready to be written to
    releasing.write.arrive();
    // block on reaching the read phase for the new buffer
    acquiring.read.arriveAndAwaitAdvance();
    return acquiring.writeFunction;
  }

  public ByteBuffer write(ByteBuffer populated) throws IOException {
    switch (status) {
      case FINISHED:
      case FLUSH_ASYNC:
        throw new IllegalStateException();
      case SYNC:
        return syncSwap(populated);
      case ASYNC:
        break; // proceed
    }
    Struct releasing = buffers[populatingBuffer];
    Struct acquiring = buffers[populatingBuffer ^= 1];
    assert Objects.equals(releasing.buffer, populated);
    // mark previous buffer as ready to be read from
    releasing.read.arrive();
    // block on reaching the write phase for the new buffer
    acquiring.write.arriveAndAwaitAdvance();
    return acquiring.buffer.clear();
  }

  private Future<?> startWrite(ExecutorService exec) {
    return exec.submit(
        () -> {
          initChannel();
          IOFunction<long[], long[]> ioFunction = swapConsume();
          while (status == Status.ASYNC) {
            ioFunction.apply(writePos);
            ioFunction = swapConsume();
          }
          int adjust;
          if (consumingBuffer != flushBufferIdx) {
            // we've broken out of the loop. If we (consumer) were blocking waiting for input on the
            // final `flushBufferIdx`, then we do nothing. But if we (consumingBuffer idx) is _not_
            // the final flush buffer, we need to write our output and place our entry back in write
            // phase to signal the flush thread that it may proceed.
            ioFunction.apply(writePos);
            switch (status) {
              case FINISHED:
                buffers[consumingBuffer].write.arrive();
                return null;
              case FLUSH_ASYNC:
                status = Status.FINISHED;
                ioFunction = swapConsume();
                adjust = adjustFinalBuffer(buffers[consumingBuffer].buffer);
                break;
              default:
                throw new IllegalStateException();
            }
          } else if (status == Status.FLUSH_ASYNC) {
            status = Status.FINISHED;
            adjust = adjustFinalBuffer(buffers[consumingBuffer].buffer);
          } else {
            return null;
          }
          if (adjust != -1) {
            ioFunction.apply(writePos);
            if (adjust != 0) {
              channel[0].truncate(writePos[0] - adjust);
            }
          }
          return null;
        });
  }

  private void initChannel() throws IOException {
    if (useDirectIO) {
      channel[0] =
          FileChannel.open(
              path,
              StandardOpenOption.WRITE,
              StandardOpenOption.CREATE_NEW,
              CompressingDirectory.getDirectOpenOption());
    } else {
      channel[0] = FileChannel.open(path, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW);
    }
  }

  public void startSync() throws IOException {
    initChannel();
  }

  public void start(ExecutorService exec) {
    status = Status.ASYNC;
    Future<?> f = startWrite(exec);
    if (!future.compareAndSet(null, f)) {
      f.cancel(true);
      throw new IllegalStateException("started multiple times");
    }
  }

  public void flush(ByteBuffer populated, boolean synchronous) throws IOException {
    switch (status) {
      case FINISHED:
      case FLUSH_ASYNC:
        throw new IllegalStateException("flushed multiple times");
      case SYNC:
        status = Status.FINISHED;
        int adjust = adjustFinalBuffer(populated);
        if (adjust != -1) {
          syncSwap(populated);
          if (adjust != 0) {
            channel[0].truncate(writePos[0] - adjust);
          }
        }
        return;
      case ASYNC:
        flushBufferIdx = populatingBuffer;
        status = synchronous ? Status.FINISHED : Status.FLUSH_ASYNC;
        break; // proceed
    }
    Struct last = buffers[populatingBuffer];
    assert Objects.equals(last.buffer, populated);
    // first mark status as finished so that write thread may exit
    // mark the final buffer has ready to have its content read. The only practical reason
    // we must do this here is to unblock the write thread if it's waiting on this condition
    // (the write thread will exit though, as we write the last buffer synchronously)
    last.read.arrive();
    // wait for the other buffer to be writable. We will not actually populate it (as we
    // would in the case of `swapPopulate()`, but blocking on this condition indicates that
    // any data in this buffer has been flushed, and we may proceed to flush the last buffer.
    if (synchronous) {
      int adjust = adjustFinalBuffer(populated);
      buffers[populatingBuffer ^ 1].write.arriveAndAwaitAdvance();
      // finally, write the last buffer synchronously.
      if (adjust != -1) {
        last.writeFunction.apply(writePos);
        if (adjust != 0) {
          channel[0].truncate(writePos[0] - adjust);
        }
      }
    }
  }

  private int adjustFinalBuffer(ByteBuffer populated) {
    int remainingInBuffer = populated.position();
    int adjust;
    if (remainingInBuffer == 0) {
      populated.limit(0);
      adjust = -1;
    } else {
      // we need to rewind, as we have to write full blocks (we truncate file later):
      populated.rewind();
      int flushLimit = (((remainingInBuffer - 1) / blockSize) + 1) * blockSize;
      populated.limit(flushLimit);
      adjust = flushLimit - remainingInBuffer;
    }
    return adjust;
  }

  public int write(ByteBuffer src, long position) throws IOException {
    return channel[0].write(src, position);
  }

  @Override
  @SuppressWarnings("try")
  public void close() throws IOException {
    try {
      Future<?> f = future.getAndSet(CLOSED);
      if (f != null) {
        if (f == CLOSED) {
          throw new IllegalStateException("closed multiple times");
        }
        if (status == Status.ASYNC) {
          f.cancel(true);
        }
        try {
          f.get();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
          Throwable cause = e.getCause();
          if (cause instanceof IOException) {
            throw (IOException) cause;
          } else {
            throw new RuntimeException(e);
          }
        }
      }
    } finally {
      try (FileChannel ignored = channel[0]) {
        // ensure that FileChannel is closed, and return buffers to the pool
        bufferPool.release(buffers[0].buffer);
        bufferPool.release(buffers[1].buffer);
      }
    }
  }

  private static final class Struct {
    private final ByteBuffer buffer;
    private final Phaser read = new Phaser(2);
    private final Phaser write = new Phaser(2);
    private final IOFunction<long[], long[]> writeFunction;

    private Struct(
        DirectBufferPool bufferPool,
        Function<ByteBuffer, IOFunction<long[], long[]>> writeFunctionSupplier) {
      this.buffer = bufferPool.get();
      this.writeFunction = writeFunctionSupplier.apply(this.buffer);
    }
  }
}
