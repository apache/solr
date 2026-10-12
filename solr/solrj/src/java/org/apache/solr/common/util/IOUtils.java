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

import java.io.UnsupportedEncodingException;
import java.lang.invoke.MethodHandles;
import java.nio.charset.Charset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IOUtils {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static void closeQuietly(Iterable<? extends AutoCloseable> closeables) {
    if (closeables == null) return;
    for (AutoCloseable closeable : closeables) closeQuietly(closeable);
  }

  public static void closeQuietly(AutoCloseable closeable) {
    try {
      if (closeable != null) {
        closeable.close();
      }
    } catch (Exception e) {
      log.error("Error while closing", e);
    }
  }

  /**
   * Resolves a charset name (typically from a Content-Type header) to a {@link Charset}. Unlike
   * {@link Charset#forName(String)}, an illegal or unsupported name results in a checked {@link
   * UnsupportedEncodingException}, matching the behavior of the legacy {@code String}-based JDK
   * charset APIs, so callers can treat a bad charset as an I/O error.
   */
  public static Charset charsetForName(String charsetName) throws UnsupportedEncodingException {
    try {
      return Charset.forName(charsetName);
    } catch (IllegalArgumentException e) {
      UnsupportedEncodingException uee =
          new UnsupportedEncodingException("Unsupported charset: " + charsetName);
      uee.initCause(e);
      throw uee;
    }
  }
}
