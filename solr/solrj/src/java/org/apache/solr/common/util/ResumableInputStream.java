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
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An {@link InputStream} that can be resumed when the connection that is driving the input is
 * interrupted.
 */
public class ResumableInputStream extends InputStream {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private long bytesRead;
  private long markedBytesRead;
  private final Function<Long, InputStream> nextInputStreamSupplier;
  private InputStream delegate;

  /**
   * Create a new ResumableInputStream
   *
   * @param delegate The original {@link InputStream} that will be used as a delegate
   * @param nextInputStreamSupplier A function to create the next InputStream given the number of
   *     bytes already read. These inputs can, for example, be used to populate the <a
   *     href="https://www.rfc-editor.org/rfc/rfc9110.html#name-range">HTTP Range header</a>.
   */
  public ResumableInputStream(
      InputStream delegate, Function<Long, InputStream> nextInputStreamSupplier) {
    this.delegate = delegate;
    this.nextInputStreamSupplier = nextInputStreamSupplier;
    bytesRead = 0;
    markedBytesRead = 0;
  }

  @Override
  public int read() throws IOException {
    return read(false);
  }

  public int read(boolean isRetry) throws IOException {
    if (delegate == null) {
      delegate = nextInputStreamSupplier.apply(bytesRead);
    }
    int val;
    try {
      val = delegate.read();
      if (val >= 0) {
        bytesRead += 1;
      }
    } catch (IOException e) {
      // Only retry once on a single read
      if (isRetry) {
        throw e;
      }
      log.warn(
          "Exception thrown while consuming InputStream, retrying from byte: {}", bytesRead, e);
      resetInputStream();
      val = read(true);
    }
    return val;
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    return read(b, off, len, false);
  }

  public int read(byte[] b, int off, int len, boolean isRetry) throws IOException {
    if (delegate == null) {
      delegate = nextInputStreamSupplier.apply(bytesRead);
    }
    int readLen;
    try {
      readLen = delegate.read(b, off, len);
      if (len >= 0) {
        bytesRead += readLen;
      }
    } catch (IOException e) {
      // Only retry once on a single read
      if (isRetry) {
        throw e;
      }
      log.warn(
          "Exception thrown while consuming InputStream, retrying from byte: {}", bytesRead, e);
      resetInputStream();
      readLen = read(b, off, len, true);
    }
    return readLen;
  }

  @Override
  public boolean markSupported() {
    return true;
  }

  @Override
  public void mark(int readlimit) {
    markedBytesRead = bytesRead;
  }

  @Override
  public int available() throws IOException {
    return delegate.available();
  }

  @Override
  public void reset() {
    bytesRead = markedBytesRead;
    resetInputStream();
  }

  @Override
  public void close() throws IOException {
    if (delegate != null) {
      delegate.close();
    }
  }

  private void resetInputStream() {
    IOUtils.closeQuietly(delegate);
    delegate = null;
  }
}
