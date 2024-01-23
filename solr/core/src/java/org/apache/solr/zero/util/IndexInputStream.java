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
package org.apache.solr.zero.util;

import java.io.IOException;
import java.io.InputStream;
import org.apache.lucene.store.IndexInput;

/**
 * Wraps an {@link IndexInput} into an {@link InputStream}, while correctly converting the SIGNED
 * bytes returned by the {@link IndexInput} into the "unsigned bytes stored in an integer" expected
 * to be returned by {@link InputStream#read()}.
 *
 * <p>Class {@link org.apache.solr.util.PropertiesInputStream} does almost the same thing but {@link
 * org.apache.solr.util.PropertiesInputStream#read()} does not unsign the returned value, breaking
 * the contract of {@link InputStream#read()} as stated in its Javadoc "Returns: the next byte of
 * data, or -1 if the end of the stream is reached.".
 *
 * <p>Performance is likely lower using this class than doing direct file manipulations. To keep in
 * mind if we have streaming perf issues.
 */
public class IndexInputStream extends InputStream {

  private final IndexInput indexInput;

  private long remaining;
  private long mark;

  public IndexInputStream(IndexInput indexInput) {
    this.indexInput = indexInput;
    remaining = indexInput.length();
  }

  @Override
  public int read() throws IOException {
    if (remaining == 0) {
      return -1;
    } else {
      --remaining;
      return Byte.toUnsignedInt(indexInput.readByte());
    }
  }

  @Override
  public int available() {
    return (remaining > Integer.MAX_VALUE) ? Integer.MAX_VALUE : (int) remaining;
  }

  @Override
  public boolean markSupported() {
    return true;
  }

  @Override
  public void mark(int readLimit) {
    mark = indexInput.getFilePointer();
  }

  @Override
  public void reset() throws IOException {
    indexInput.seek(mark);
    remaining = indexInput.length() - mark;
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    if (remaining == 0) {
      return -1;
    }
    if (remaining < len) {
      len = (int) remaining;
    }
    indexInput.readBytes(b, off, len);
    remaining -= len;
    return len;
  }

  @Override
  public long skip(long n) throws IOException {
    if (remaining == 0) {
      return -1;
    }
    if (remaining < n) {
      n = remaining;
    }
    indexInput.seek(indexInput.getFilePointer() + n);
    remaining -= n;

    return n;
  }

  @Override
  public void close() throws IOException {
    indexInput.close();
  }
}
