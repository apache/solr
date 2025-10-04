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
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.Reader;
import java.io.Writer;
import java.nio.charset.StandardCharsets;

/**
 * Make sure the XHTML input is valid XML. Pipe text through this reader before passing it to an XML
 * parser. TODO: Warning: Most of this class is AI generated. Can be a lot smaller, only sanitize
 * '0x0'?
 */
final class XmlSanitizingReader extends java.io.Reader {
  private final java.io.Reader in;
  private final StringBuilder entityBuf = new StringBuilder();
  private boolean inEntity = false; // after reading '&'

  // For surrogate tracking to evaluate XML validity by code point
  private int pendingHighSurrogate = -1;

  public XmlSanitizingReader(java.io.Reader in) {
    this.in = in;
  }

  @Override
  public int read(char[] cbuf, int off, int len) throws java.io.IOException {
    int written = 0;
    while (written < len) {
      int ci = in.read();
      if (ci == -1) break;
      char ch = (char) ci;

      // Handle numeric entity stripping for &#0; and &#x0; variants
      if (!inEntity) {
        if (ch == '&') {
          inEntity = true;
          entityBuf.setLength(0);
          entityBuf.append(ch);
          continue; // don't write yet
        }
      } else {
        entityBuf.append(ch);
        // stop conditions for entity buffering
        if (ch == ';' || entityBuf.length() > 12) { // entities are short; cap length defensively
          String ent = entityBuf.toString();
          boolean drop = isNullNumericEntity(ent);
          inEntity = false;
          if (!drop) {
            // flush buffered entity to output
            for (int i = 0; i < ent.length() && written < len; i++) {
              cbuf[off + written++] = ent.charAt(i);
            }
          }
          continue;
        }
        // Keep buffering alphanumerics and '#', 'x'
        continue;
      }

      // Filter invalid XML 1.0 characters by code point
      if (Character.isHighSurrogate(ch)) {
        pendingHighSurrogate = ch;
        continue; // need next char to form code point
      }
      if (Character.isLowSurrogate(ch) && pendingHighSurrogate != -1) {
        int cp = Character.toCodePoint((char) pendingHighSurrogate, ch);
        pendingHighSurrogate = -1;
        if (isAllowedXmlChar(cp)) {
          // encode back as surrogate pair
          cbuf[off + written++] = Character.highSurrogate(cp);
          if (written < len) {
            cbuf[off + written++] = Character.lowSurrogate(cp);
          } else {
            // If no space for low surrogate, keep it pending (edge, unlikely with reasonable len)
            // Fallback: buffer low surrogate into a small one-char pushback by using a field
            // For simplicity, write only if space available; otherwise, return and next read
            // continues
            // But to avoid corruption, store it
            pushbackChar = Character.lowSurrogate(cp);
          }
        }
        continue;
      } else {
        // previous high surrogate without low surrogate -> invalid; drop it
        pendingHighSurrogate = -1;
      }

      int cp = ch;
      if (!Character.isSurrogate(ch) && isAllowedXmlChar(cp)) {
        cbuf[off + written++] = ch;
      }
    }
    return (written == 0) ? -1 : written;
  }

  private Character pushbackChar = null;

  @Override
  public boolean ready() throws java.io.IOException {
    return in.ready();
  }

  @Override
  public void close() throws java.io.IOException {
    in.close();
  }

  private static boolean isNullNumericEntity(String ent) {
    // Accept patterns like '&#0;', '&#00;', '&#x0;', '&#x0000;' (case-insensitive)
    if (ent == null) return false;
    if (!ent.startsWith("&#") || !ent.endsWith(";")) return false;
    String mid = ent.substring(2, ent.length() - 1);
    if (mid.isEmpty()) return false;
    if (mid.charAt(0) == 'x' || mid.charAt(0) == 'X') {
      // hex
      for (int i = 1; i < mid.length(); i++) {
        char c = mid.charAt(i);
        if (c != '0') return false;
      }
      return mid.length() > 1; // at least one zero after x
    } else {
      // decimal
      for (int i = 0; i < mid.length(); i++) {
        char c = mid.charAt(i);
        if (c != '0') return false;
      }
      return true; // one or more zeros
    }
  }

  private static boolean isAllowedXmlChar(int cp) {
    return cp == 0x9
        || cp == 0xA
        || cp == 0xD
        || (cp >= 0x20 && cp <= 0xD7FF)
        || (cp >= 0xE000 && cp <= 0xFFFD)
        || (cp >= 0x10000 && cp <= 0x10FFFF);
  }

  public static InputStream sanitize(InputStream in) throws IOException {
    PipedOutputStream out = new PipedOutputStream();
    PipedInputStream pipedIn = new PipedInputStream(out);

    Reader reader = new XmlSanitizingReader(new InputStreamReader(in, StandardCharsets.UTF_8));
    Writer writer = new OutputStreamWriter(out, StandardCharsets.UTF_8);

    Thread worker =
        new Thread(
            () -> {
              try (reader;
                  writer) {
                reader.transferTo(writer);
              } catch (IOException e) {
                try {
                  pipedIn.close();
                } catch (IOException ignored) {
                }
              }
            },
            "XmlSanitizingReaderWorker");
    worker.setDaemon(true);
    worker.start();

    return pipedIn;
  }
}
