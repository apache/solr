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

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

public class DirectBufferPool {
  private final ConcurrentLinkedQueue<ByteBuffer> pool = new ConcurrentLinkedQueue<>();
  private final int size;
  private final int blockSize;
  private final int allocateChunkSize;

  private final LongAdder created = new LongAdder();
  private final LongAdder discarded = new LongAdder();
  private final AtomicLong poolSize = new AtomicLong();
  private final LongAdder hits = new LongAdder();
  private final AtomicInteger outstanding = new AtomicInteger();
  private final AtomicInteger outstandingHighWatermark = new AtomicInteger();

  private final int maxPoolSize;

  public DirectBufferPool(int size, int blockSize, int allocateChunkSize) {
    this.maxPoolSize =
        Math.min(
            8192, Math.toIntExact((Runtime.getRuntime().maxMemory() / (size + blockSize - 1)) / 4));
    this.size = size;
    this.blockSize = blockSize;
    this.allocateChunkSize = allocateChunkSize;
    if (size % blockSize != 0) {
      throw new IllegalArgumentException("block size incompatible with size");
    }
  }

  long[] getStats() {
    return new long[] {
      created.sum(),
      poolSize.get(),
      hits.sum(),
      discarded.sum(),
      outstanding.get(),
      outstandingHighWatermark.get()
    };
  }

  private void incrementOutstanding() {
    int newSize = outstanding.incrementAndGet();
    int extantHighWatermark = outstandingHighWatermark.get();
    if (newSize > extantHighWatermark) {
      do {
        extantHighWatermark =
            outstandingHighWatermark.compareAndExchange(extantHighWatermark, newSize);
      } while (extantHighWatermark < newSize);
    }
  }

  public ByteBuffer get() {
    ByteBuffer ret = pool.poll();
    if (ret != null) {
      poolSize.decrementAndGet();
      hits.increment();
      incrementOutstanding();
      return ret;
    } else {
      synchronized (pool) {
        ret = pool.poll();
        if (ret != null) {
          poolSize.decrementAndGet();
          hits.increment();
          incrementOutstanding();
          return ret;
        }
        ByteBuffer chunk =
            ByteBuffer.allocateDirect((size * allocateChunkSize) + blockSize - 1)
                .alignedSlice(blockSize);
        int pos = 0;
        int added = 0;
        try {
          for (int i = 1; i < allocateChunkSize; i++) {
            pool.add(chunk.position(pos).limit(pos += size).slice());
            poolSize.incrementAndGet();
            added++;
          }
        } finally {
          created.add(added);
        }
        ret = chunk.position(pos).limit(pos + size).slice();
        created.increment();
        incrementOutstanding();
        return ret;
      }
    }
  }

  public void release(ByteBuffer bb) {
    outstanding.decrementAndGet();
    bb.clear();
    if (poolSize.incrementAndGet() > maxPoolSize) {
      poolSize.decrementAndGet();
      discarded.increment();
    } else {
      pool.add(bb);
    }
  }
}
