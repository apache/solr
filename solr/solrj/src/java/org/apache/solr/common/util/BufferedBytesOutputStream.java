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

import java.io.IOException;
import java.io.OutputStream;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import org.eclipse.jetty.client.util.DeferredContentProvider;

public class BufferedBytesOutputStream extends OutputStream {
  private final DeferredContentProvider stream;

  protected ByteBuffer buf;

  protected int sz;

  public BufferedBytesOutputStream(ByteBuffer buffer, DeferredContentProvider stream) {
    buf = buffer;
    this.stream = stream;
  }

  /**
   * Writes the specified byte to this {@code ByteArrayOutputStream}.
   *
   * @param b the byte to be written.
   */
  public void write(int b) throws IOException {
    if (sz + 1 > buf.remaining()) {
      flush();
    }
    buf.put(sz++, (byte) b);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    if (len > buf.limit() - sz) {

      if (len > buf.capacity()) {
        flush();
        stream.offer(ByteBuffer.wrap(b, off, len));
        stream.flush();
        return;
      }
      flush();
    }

    buf.position(sz);
    try {
      buf.put(b, off, len);
    } catch (BufferOverflowException e) {
      throw new RuntimeException(
          "len:"
              + len
              + " sz:"
              + sz
              + " cap:"
              + buf.capacity()
              + " pos:"
              + buf.position()
              + " buflimit:"
              + buf.limit());
    }
    sz += len;
  }

  public void writeBytes(byte[] b) throws IOException {
    write(b, 0, b.length);
  }

  public void reset() {
    sz = 0;
    buf.clear();
  }

  public void flush() throws IOException {
    if (sz > 0) {
      buf.flip();
      stream.offer(buf);
      stream.flush();
      buf.clear();
      sz = 0;
    }
  }

  @Override
  public void close() throws IOException {
    flush();
    stream.close();
  }

}
