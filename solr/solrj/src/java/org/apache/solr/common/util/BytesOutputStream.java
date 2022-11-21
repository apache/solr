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

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;

public class BytesOutputStream extends OutputStream {
  private static final int MAX_ARRAY_SIZE = Integer.MAX_VALUE - 8;

  protected byte[] buf;

  protected int sz;

  public BytesOutputStream() {
    this(64);
  }

  public BytesOutputStream(int size) {
    if (size < 0) {
      throw new IllegalArgumentException("Size must be > 0: " + size);
    }
    buf = new byte[size];
  }

  public byte[] toBytes() {
    return Arrays.copyOf(buf, sz);
  }

  public Bytes bytes() {
    return new Bytes(buf, 0, sz);
  }

  public InputStream inputStream() {
    return new ByteArrayInputStream(buf);
  }

  private void ensureCapacity(int minCapacity) {
    if (minCapacity - buf.length > 0) expandBuf(minCapacity);
  }

  /** * Write a byte to the stream. */
  @Override
  public void write(int b) {

    try {
      buf[sz] = (byte) b;
      sz += 1;
    } catch (IndexOutOfBoundsException e) {
      ensureCapacity(sz + 1);
      buf[sz] = (byte) b;
      sz += 1;
    }
  }

  @Override
  public void write(byte[] b, int off, int len) {
    try {
      System.arraycopy(b, off, buf, sz, len);
      sz += len;
    } catch (IndexOutOfBoundsException e) {
      ensureCapacity(sz + len);
      System.arraycopy(b, off, buf, sz, len);
      sz += len;
    }
  }

  public void writeBytes(byte[] b) {
    write(b, 0, b.length);
  }

  public void reset() {
    sz = 0;
  }

  public int size() {
    return sz;
  }

  public String toString(String charset) {
    try {
      return new String(buf, 0, sz, charset);
    } catch (UnsupportedEncodingException e) {
      throw new IllegalArgumentException(e);
    }
  }

  private void expandBuf(int minCapacity) {
    int oldCapacity = buf.length;
    int newCapacity = oldCapacity << 1;
    if (newCapacity - minCapacity < 0) newCapacity = minCapacity;
    if (newCapacity - MAX_ARRAY_SIZE > 0) {
      if (minCapacity < 0)
        // overflow
        throw new OutOfMemoryError();
      newCapacity = (minCapacity > MAX_ARRAY_SIZE) ? Integer.MAX_VALUE : MAX_ARRAY_SIZE;
    }
    buf = Arrays.copyOf(buf, newCapacity);
  }

  @Override
  public void close() {
    // noop
  }

  public static class Bytes {

    public final byte[] bytes;
    public final int offset;
    public final int length;

    public Bytes(byte[] bytes, int offset, int length) {
      this.bytes = bytes;
      this.offset = offset;
      this.length = length;
    }
  }
}
