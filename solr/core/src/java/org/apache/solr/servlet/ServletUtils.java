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

import javax.servlet.ReadListener;
import javax.servlet.ServletInputStream;
import javax.servlet.ServletOutputStream;
import javax.servlet.WriteListener;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpServletResponseWrapper;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Various Util methods for interaction on servlet level, i.e. HttpServletRequest
 */
public abstract class ServletUtils {
  static String CLOSE_STREAM_MSG = "Attempted close of http request or response stream - in general you should not do this, "
      + "you may spoil connection reuse and possibly disrupt a client. If you must close without actually needing to close, "
      + "use a CloseShield*Stream. Closing or flushing the response stream commits the response and prevents us from modifying it. "
      + "Closing the request stream prevents us from guaranteeing ourselves that streams are fully read for proper connection reuse."
      + "Let the container manage the lifecycle of these streams when possible.";

  private ServletUtils() { /* only static methods in this class */ }

  /**
   * Use this to get the full path after context path "/solr", which is a combination of
   * servletPath and pathInfo.
   * @param request the HttpServletRequest object
   * @return String with path starting with a "/", or empty string if no path
   */
  public static String getPathAfterContext(HttpServletRequest request) {
    return request.getServletPath() + (request.getPathInfo() != null ? request.getPathInfo() : "");
  }

  /**
   * Wrap the request's input stream with a close shield. If this is a
   * retry, we will assume that the stream has already been wrapped and do nothing.
   *
   * Only the container should ever actually close the servlet output stream.
   *
   * @param request The request to wrap.
   * @param retry If this is an original request or a retry.
   * @return A request object with an {@link InputStream} that will ignore calls to close.
   */
  public static HttpServletRequest closeShield(HttpServletRequest request, boolean retry) {
    if (!retry) {
      return new HttpServletRequestWrapper(request) {

        @Override
        public ServletInputStream getInputStream() throws IOException {

          return new ServletInputStreamWrapper(super.getInputStream()) {
            @Override
            public void close() {
              // even though we skip closes, we let local tests know not to close so that a full understanding can take
              // place
              assert Thread.currentThread().getStackTrace()[2].getClassName().matches(
                  "org\\.apache\\.(?:solr|lucene).*") ? false : true : CLOSE_STREAM_MSG;
              this.stream = ClosedServletInputStream.CLOSED_SERVLET_INPUT_STREAM;
            }
          };

        }
      };
    } else {
      return request;
    }
  }

  /**
   * Wrap the response's output stream with a close shield. If this is a
   * retry, we will assume that the stream has already been wrapped and do nothing.
   *
   * Only the container should ever actually close the servlet request stream.
   *
   * @param response The response to wrap.
   * @param retry If this response corresponds to an original request or a retry.
   * @return A response object with an {@link OutputStream} that will ignore calls to close.
   */
  public static HttpServletResponse closeShield(HttpServletResponse response, boolean retry) {
    if (!retry) {
      return new HttpServletResponseWrapper(response) {

        @Override
        public ServletOutputStream getOutputStream() throws IOException {

          return new ServletOutputStreamWrapper(super.getOutputStream()) {
            @Override
            public void close() {
              // even though we skip closes, we let local tests know not to close so that a full understanding can take
              // place
              assert Thread.currentThread().getStackTrace()[2].getClassName().matches(
                  "org\\.apache\\.(?:solr|lucene).*") ? false
                      : true : CLOSE_STREAM_MSG;
              stream = ClosedServletOutputStream.CLOSED_SERVLET_OUTPUT_STREAM;
            }
          };
        }

      };
    } else {
      return response;
    }
  }

  public static class ClosedServletInputStream extends ServletInputStream {

    public static final ClosedServletInputStream CLOSED_SERVLET_INPUT_STREAM = new ClosedServletInputStream();

    @Override
    public int read() {
      return -1;
    }

    @Override
    public boolean isFinished() {
      return false;
    }

    @Override
    public boolean isReady() {
      return false;
    }

    @Override
    public void setReadListener(ReadListener arg0) {}
  }

  public static class ClosedServletOutputStream extends ServletOutputStream {

    public static final ClosedServletOutputStream CLOSED_SERVLET_OUTPUT_STREAM = new ClosedServletOutputStream();

    @Override
    public void write(final int b) throws IOException {
      throw new IOException("write(" + b + ") failed: stream is closed");
    }

    @Override
    public void flush() throws IOException {
      throw new IOException("flush() failed: stream is closed");
    }

    @Override
    public boolean isReady() {
      return false;
    }

    @Override
    public void setWriteListener(WriteListener arg0) {
      throw new RuntimeException("setWriteListener() failed: stream is closed");
    }
  }
}
