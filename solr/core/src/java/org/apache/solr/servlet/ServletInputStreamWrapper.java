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
package org.apache.solr.servlet;

import java.io.IOException;
import javax.servlet.ReadListener;
import javax.servlet.ServletInputStream;
import org.apache.solr.common.util.SuppressForbidden;

/**
 * Provides a convenient extension of the {@link ServletInputStream} class that can be subclassed by
 * developers wishing to adapt the behavior of a Stream. One such example may be to override {@link
 * #close()} to instead be a no-op as in SOLR-8933.
 *
 * <p>This class implements the Wrapper or Decorator pattern. Methods default to calling through to
 * the wrapped stream.
 */
@SuppressForbidden(reason = "delegate methods")
public class ServletInputStreamWrapper extends ServletInputStream {
  ServletInputStream stream;

  public ServletInputStreamWrapper(ServletInputStream stream) throws IOException {
    this.stream = stream;
  }

  @Override
  public int hashCode() {
    return stream.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    return stream.equals(obj);
  }

  @Override
  public int available() throws IOException {
    return stream.available();
  }

  @Override
  public void close() throws IOException {
    stream.close();
  }

  @Override
  public boolean isFinished() {
    return stream.isFinished();
  }

  @Override
  public boolean isReady() {
    return stream.isReady();
  }

  @Override
  public int read() throws IOException {
    return stream.read();
  }

  @Override
  public int read(byte[] b) throws IOException {
    return stream.read(b);
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    return stream.read(b, off, len);
  }

  @Override
  public void mark(int readlimit) {
    stream.mark(readlimit);
  }

  @Override
  public boolean markSupported() {
    return stream.markSupported();
  }

  @Override
  public int readLine(byte[] b, int off, int len) throws IOException {
    return stream.readLine(b, off, len);
  }

  @Override
  public void reset() throws IOException {
    stream.reset();
  }

  @Override
  public void setReadListener(ReadListener arg0) {
    stream.setReadListener(arg0);
  }

  @Override
  public long skip(long n) throws IOException {
    return stream.skip(n);
  }

  @Override
  public String toString() {
    return stream.toString();
  }
}
