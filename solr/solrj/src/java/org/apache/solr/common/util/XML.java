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
import java.io.Writer;
import java.util.Map;
import java.util.stream.Stream;

/** */
public class XML {

  public static void escapeCharData(String str, Writer out) throws IOException {
    escape(str, out);
  }

  public static void escapeAttributeValue(String str, Writer out) throws IOException {
    escape(str, out);
  }

  public static void escapeAttributeValue(char[] chars, int start, int length, Writer out)
      throws IOException {
    escape(chars, start, length, out);
  }

  /**
   * does NOT escape character data in val; it must already be valid XML. Attributes are always
   * escaped.
   */
  public static final void writeUnescapedXML(Writer out, String tag, String val, Object... attrs)
      throws IOException {
    writeXML(out, tag, (writer1) -> writer1.write(val), attrs);
  }

  /** escapes character data in val and attributes */
  public static final void writeXML(Writer out, String tag, String val, Object... attrs)
      throws IOException {
    final Writable writable = val != null ? (writer1) -> XML.escapeCharData(val, writer1) : null;
    writeXML(out, tag, writable, attrs);
  }

  /** escapes character data in val and attributes */
  public static void writeXML(Writer out, String tag, String val, Map<String, String> attrs)
      throws IOException {
    writeXML(
        out,
        tag,
        val,
        attrs.entrySet().stream()
            .flatMap((entry) -> Stream.of(entry.getKey(), entry.getValue()))
            .toArray());
  }

  /**
   * @lucene.internal
   */
  public static final void writeXML(Writer out, String tag, Writable valWritable, Object... attrs)
      throws IOException {
    out.write('<');
    out.write(tag);
    final int attrsLen = attrs == null ? 0 : attrs.length;
    for (int i = 0; i < attrsLen; i++) {
      out.write(' ');
      out.write(attrs[i++].toString());
      out.write('=');
      out.write('"');
      escapeAttributeValue(attrs[i].toString(), out);
      out.write('"');
    }
    if (valWritable == null) {
      out.write('/');
      out.write('>');
    } else {
      out.write('>');
      valWritable.write(out);
      out.write('<');
      out.write('/');
      out.write(tag);
      out.write('>');
    }
  }

  @FunctionalInterface
  public interface Writable {
    void write(Writer w) throws IOException;
  }

  private static void escape(char[] chars, int offset, int length, Writer out) throws IOException {
    for (int i = 0; i < length; i++) {
      writeEscaped(out, chars[offset + i]);
    }
  }

  private static void writeEscaped(Writer out, char ch) throws IOException {
    switch (ch) {
      case 0x09:
        out.write("&#09;");
        break;
      case 0x0a:
        out.write("&#0a;");
        break;
      case 0x0d:
        out.write("&#0d;");
        break;
      case '&':
        out.write("&amp;");
        break;
      case '"':
        out.write("&quot;");
        break;
      case '<':
        out.write("&lt;");
        break;
      case '>':
        out.write("&gt;");
        break;
      default:
        if (ch < 0x20) {
          throw new RuntimeException("Invalid character in XML attribute: " + "");
        }
        out.write(ch);
    }
  }

  private static void escape(String str, Writer out) throws IOException {
    for (int i = 0; i < str.length(); i++) {
      writeEscaped(out, str.charAt(i));
    }
  }
}
