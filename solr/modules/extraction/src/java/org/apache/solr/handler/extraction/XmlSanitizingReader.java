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

import java.io.IOException;
import java.io.Reader;

/**
 * Minimal reader that drops only null numeric character entities from the stream.
 *
 * <p>Recognizes decimal and hexadecimal numeric entities that resolve to code point 0 (e.g. "&#0;",
 * "&#00;", "&#x0;", "&#x0000;") and removes them. If a null entity is unterminated (no ';'), it is
 * still removed when a non-entity character terminates the sequence. Everything else is passed
 * through unchanged.
 */
final class XmlSanitizingReader extends Reader {
  private final Reader in;
  // pushback buffer (stack) for lookahead and emitting literals
  private StringBuilder pushbackBuf = null;

  XmlSanitizingReader(Reader in) {
    this.in = in;
  }

  @Override
  public int read(char[] cbuf, int off, int len) throws IOException {
    if (len == 0) return 0;
    int written = 0;
    while (written < len) {
      int ch = read();
      if (ch == -1) break;
      cbuf[off + written++] = (char) ch;
    }
    return (written == 0) ? -1 : written;
  }

  @Override
  public int read() throws IOException {
    int c = nextChar();
    if (c == -1) return -1;
    if (c != '&') return c;

    // Possible entity
    int c2 = nextChar();
    if (c2 == -1) return '&';
    if (c2 != '#') {
      // Not numeric entity
      pushBackChar(c2);
      return '&';
    }

    // Start numeric entity: collect and decide if it's null
    int c3 = nextChar();
    if (c3 == -1) {
      // literal "&#" at EOF -> treat as '&' followed by EOF
      return '&';
    }

    boolean hex;
    boolean sawDigit = false;
    boolean nonZeroSeen = false;
    StringBuilder buf = new StringBuilder(8);
    buf.append("&#");

    if (c3 == 'x' || c3 == 'X') {
      hex = true;
      buf.append((char) c3);
    } else if (isDecDigit(c3)) {
      hex = false;
      sawDigit = true;
      if (c3 != '0') nonZeroSeen = true;
      buf.append((char) c3);
    } else {
      // Not a numeric entity actually, emit literal and push back c3
      pushBackChar(c3);
      return '&';
    }

    // Consume digits
    while (true) {
      int d = nextChar();
      if (d == -1) {
        // EOF terminates entity lexeme; if it's a null entity, drop it, else flush literal
        if (sawDigit && !nonZeroSeen) {
          return -1; // dropped; caller will get EOF next
        } else {
          // flush literal collected so far by returning first char and pushing back the rest
          return flushLiteral(buf);
        }
      }
      if (d == ';') {
        // Properly terminated entity
        buf.append(';');
        boolean isNull = sawDigit && !nonZeroSeen;
        if (isNull) {
          // drop the entire entity and continue reading next char
          return read();
        } else {
          // emit '&' and pushback the rest so that the sequence passes through unchanged
          return flushLiteral(buf);
        }
      }

      boolean validDigit = hex ? isHexDigit(d) : isDecDigit(d);
      if (validDigit) {
        sawDigit = true;
        if (d != '0') nonZeroSeen = true;
        buf.append((char) d);
        if (buf.length() > 16) {
          // cap buffering; treat as literal
          return flushLiteral(buf);
        }
        continue;
      }

      // Non-digit terminates the entity without semicolon
      boolean isNull = sawDigit && !nonZeroSeen;
      if (isNull) {
        // drop collected entity and push back the terminator
        pushBackChar(d);
        return read();
      } else {
        // Not a null entity; emit literal and then the terminator on next read
        pushBackChar(d);
        return flushLiteral(buf);
      }
    }
  }

  private int nextChar() throws IOException {
    if (pushbackBuf != null && !pushbackBuf.isEmpty()) {
      int lastIdx = pushbackBuf.length() - 1;
      char c = pushbackBuf.charAt(lastIdx);
      pushbackBuf.deleteCharAt(lastIdx);
      return c;
    }
    return in.read();
  }

  private void pushBackChar(int ch) {
    if (ch == -1) return;
    if (pushbackBuf == null) pushbackBuf = new StringBuilder();
    pushbackBuf.append((char) ch);
  }

  private void pushBackString(String s, int startIndex) {
    if (s == null) return;
    if (pushbackBuf == null) pushbackBuf = new StringBuilder();
    for (int i = s.length() - 1; i >= startIndex; i--) {
      pushbackBuf.append(s.charAt(i));
    }
  }

  private static boolean isDecDigit(int c) {
    return c >= '0' && c <= '9';
  }

  private static boolean isHexDigit(int c) {
    return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F');
  }

  // Emit the buffered literal sequence by returning '&' and pushing back the rest in reverse order
  private int flushLiteral(StringBuilder buf) {
    // push back all but the first char so subsequent reads emit them
    pushBackString(buf.toString(), 1);
    return buf.charAt(0);
  }

  @Override
  public void close() throws IOException {
    in.close();
  }
}
