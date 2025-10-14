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
package org.apache.solr.handler.extraction;

import java.io.FilterReader;
import java.io.IOException;
import java.io.Reader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Filters out null character entities (&#0;, &#x0;, etc.) from XML content.
 *
 * <p>Removes numeric character entities that resolve to code point 0, such as <code>&#0;</code> or
 * <code>&#00;</code>. Everything else is passed through unchanged.
 */
final class XmlSanitizingReader extends FilterReader {
  private static final Pattern NULL_ENTITY_PATTERN =
      Pattern.compile("&#(0+|x0+);", Pattern.CASE_INSENSITIVE);
  private static final int BUFFER_SIZE = 8192;
  private static final int OVERLAP_SIZE = 16; // Max entity length: &#x00000000;

  private final char[] readBuffer = new char[BUFFER_SIZE + OVERLAP_SIZE];
  private final char[] buffer = new char[BUFFER_SIZE + OVERLAP_SIZE];
  private final StringBuilder sb = new StringBuilder(BUFFER_SIZE + OVERLAP_SIZE);
  private final StringBuffer result = new StringBuffer(BUFFER_SIZE + OVERLAP_SIZE);
  private int bufferPos = 0;
  private int bufferLimit = 0;
  private int overlapLen = 0;
  private boolean eof = false;

  XmlSanitizingReader(Reader in) {
    super(in);
  }

  @Override
  public int read() throws IOException {
    if (bufferPos < bufferLimit) {
      return buffer[bufferPos++];
    }
    if (fillBuffer() == -1) {
      return -1;
    }
    return buffer[bufferPos++];
  }

  @Override
  public int read(char[] cbuf, int off, int len) throws IOException {
    if (len == 0) return 0;
    int totalRead = 0;
    while (totalRead < len) {
      int available = bufferLimit - bufferPos;
      if (available > 0) {
        int toCopy = Math.min(available, len - totalRead);
        System.arraycopy(buffer, bufferPos, cbuf, off + totalRead, toCopy);
        bufferPos += toCopy;
        totalRead += toCopy;
      } else {
        if (fillBuffer() == -1) {
          return totalRead == 0 ? -1 : totalRead;
        }
      }
    }
    return totalRead;
  }

  private int fillBuffer() throws IOException {
    if (eof) return -1;

    // Copy overlap from end of previous buffer
    if (overlapLen > 0) {
      System.arraycopy(buffer, bufferLimit - overlapLen, readBuffer, 0, overlapLen);
    }

    // Read new data
    int read = in.read(readBuffer, overlapLen, BUFFER_SIZE);
    if (read == -1) {
      eof = true;
      if (overlapLen == 0) return -1;
      // Process remaining overlap at EOF
      read = 0;
    }

    // Sanitize without allocating a String
    sb.setLength(0);
    sb.append(readBuffer, 0, overlapLen + read);

    result.setLength(0);
    Matcher matcher = NULL_ENTITY_PATTERN.matcher(sb);
    while (matcher.find()) {
      matcher.appendReplacement(result, "");
    }
    matcher.appendTail(result);

    result.getChars(0, result.length(), buffer, 0);
    bufferLimit = result.length();
    bufferPos = overlapLen;

    // Edge case: if sanitization removed characters from overlap at EOF,
    // bufferPos might exceed bufferLimit
    if (bufferPos > bufferLimit) {
      bufferPos = bufferLimit;
    }

    // Keep last OVERLAP_SIZE chars for next iteration (unless EOF)
    overlapLen = eof ? 0 : Math.min(OVERLAP_SIZE, bufferLimit);

    return bufferLimit - bufferPos;
  }
}
