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

public class DirectBufferPool {
  private final ConcurrentLinkedQueue<ByteBuffer> pool = new ConcurrentLinkedQueue<>();
  private final int size;
  private final int blockSize;
  private final int allocateChunkSize;

  public DirectBufferPool(int size, int blockSize, int allocatChunkSize) {
    this.size = size;
    this.blockSize = blockSize;
    this.allocateChunkSize = allocatChunkSize;
    if (size % blockSize != 0) {
      throw new IllegalArgumentException("block size incompatible with size");
    }
  }

  public ByteBuffer get() {
    ByteBuffer ret = pool.poll();
    if (ret != null) {
      return ret;
    } else {
      synchronized (pool) {
        ret = pool.poll();
        if (ret != null) {
          return ret;
        }
        ByteBuffer chunk =
            ByteBuffer.allocateDirect((size * allocateChunkSize) + blockSize - 1)
                .alignedSlice(blockSize);
        int pos = 0;
        for (int i = 1; i < allocateChunkSize; i++) {
          pool.add(chunk.position(pos).limit(pos += size).slice());
        }
        return chunk.position(pos).limit(pos + size).slice();
      }
    }
  }

  public void release(ByteBuffer bb) {
    bb.clear();
    pool.offer(bb);
  }
}
