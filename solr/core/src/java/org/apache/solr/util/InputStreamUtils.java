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

package org.apache.solr.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;

public class InputStreamUtils {

  public static class BAOS extends ByteArrayOutputStream {
    public ByteBuffer getByteBuffer() {
      return ByteBuffer.wrap(super.buf, 0, super.count);
    }
  }

  public static ByteBuffer toByteArray(InputStream is) throws IOException {
    return toByteArray(is, Integer.MAX_VALUE);
  }

  /**
   * Reads an input stream into a byte array
   *
   * @param is the input stream
   * @return the byte array
   * @throws IOException If there is a low-level I/O error.
   */
  public static ByteBuffer toByteArray(InputStream is, long maxSize) throws IOException {
    try (BAOS bos = new BAOS()) {
      long sz = 0;
      int next = is.read();
      while (next > -1) {
        if (++sz > maxSize) {
          throw new BufferOverflowException();
        }
        bos.write(next);
        next = is.read();
      }
      bos.flush();
      return bos.getByteBuffer();
    }
  }
}
