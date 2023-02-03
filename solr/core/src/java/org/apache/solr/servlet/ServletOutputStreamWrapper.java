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
import javax.servlet.ServletOutputStream;
import javax.servlet.WriteListener;
import org.apache.solr.common.util.SuppressForbidden;

/**
 * Provides a convenient extension of the {@link ServletOutputStream} class that can be subclassed
 * by developers wishing to adapt the behavior of a Stream. One such example may be to override
 * {@link #close()} to instead be a no-op as in SOLR-8933.
 *
 * <p>This class implements the Wrapper or Decorator pattern. Methods default to calling through to
 * the wrapped stream.
 */
@SuppressForbidden(reason = "delegate methods")
public class ServletOutputStreamWrapper extends ServletOutputStream {
  ServletOutputStream stream;

  public ServletOutputStreamWrapper(ServletOutputStream stream) {
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
  public void flush() throws IOException {
    stream.flush();
  }

  @Override
  public void close() throws IOException {
    stream.close();
  }

  @Override
  public boolean isReady() {
    return stream.isReady();
  }

  @Override
  public void print(boolean arg0) throws IOException {
    stream.print(arg0);
  }

  @Override
  public void print(char c) throws IOException {
    stream.print(c);
  }

  @Override
  public void print(double d) throws IOException {
    stream.print(d);
  }

  @Override
  public void print(float f) throws IOException {
    stream.print(f);
  }

  @Override
  public void print(int i) throws IOException {
    stream.print(i);
  }

  @Override
  public void print(long l) throws IOException {
    stream.print(l);
  }

  @Override
  public void print(String arg0) throws IOException {
    stream.print(arg0);
  }

  @Override
  public void println() throws IOException {
    stream.println();
  }

  @Override
  public void println(boolean b) throws IOException {
    stream.println(b);
  }

  @Override
  public void println(char c) throws IOException {
    stream.println(c);
  }

  @Override
  public void println(double d) throws IOException {
    stream.println(d);
  }

  @Override
  public void println(float f) throws IOException {
    stream.println(f);
  }

  @Override
  public void println(int i) throws IOException {
    stream.println(i);
  }

  @Override
  public void println(long l) throws IOException {
    stream.println(l);
  }

  @Override
  public void println(String s) throws IOException {
    stream.println(s);
  }

  @Override
  public void setWriteListener(WriteListener arg0) {
    stream.setWriteListener(arg0);
  }

  @Override
  public void write(int b) throws IOException {
    stream.write(b);
  }

  @Override
  public void write(byte[] b) throws IOException {
    stream.write(b);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    stream.write(b, off, len);
  }

  @Override
  public String toString() {
    return stream.toString();
  }
}
