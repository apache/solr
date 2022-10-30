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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.solr.SolrTestCaseJ4;

public class Utf8CharSequenceTest extends SolrTestCaseJ4 {

  public void testLargeString() throws IOException {
    ByteArrayUtf8CharSequence utf8 = new ByteArrayUtf8CharSequence("Hello World!".repeat(100));
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    byte[] buf = new byte[256];
    try (FastOutputStream fos = new FastOutputStream(baos, buf, 0)) {
      fos.writeUtf8CharSeq(utf8);
      fos.flush();
    }
    byte[] result = baos.toByteArray();
    ByteArrayUtf8CharSequence utf81 = new ByteArrayUtf8CharSequence(result, 0, result.length);
    assertEquals(utf81, utf8);
    baos.reset();
    utf8.write(baos);
    result = baos.toByteArray();
    utf81 = new ByteArrayUtf8CharSequence(result, 0, result.length);
    assertEquals(utf81, utf8);

    Map<String, Object> m0 = new HashMap<>();
    m0.put("str", utf8);
    baos.reset();
    try (JavaBinCodec jbc = new JavaBinCodec()) {
      jbc.marshal(m0, baos);
    }
    result = baos.toByteArray();
    try (JavaBinCodec jbc = new JavaBinCodec()) {
      Map<?, ?> m1 =
          (Map<?, ?>) jbc.setReadStringAsCharSeq(true).unmarshal(new ByteArrayInputStream(result));
      utf81 = (ByteArrayUtf8CharSequence) m1.get("str");
      assertEquals(utf81, utf8);
    }
  }

  public void testUnMarshal() throws IOException {
    NamedList<String> nl = new NamedList<>();
    String str = " The value!";
    for (int i = 0; i < 5; i++) {
      nl.add("key" + i, i + str.repeat(i));
    }
    StringBuilder sb = new StringBuilder();
    do {
      sb.append(str);
    } while (sb.length() <= 1024 * 4);
    nl.add("key_long", sb.toString());
    nl.add("key5", "5" + str);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (JavaBinCodec jbc = new JavaBinCodec()) {
      jbc.marshal(nl, baos);
    }
    byte[] bytes = baos.toByteArray();

    NamedList<?> nl1;
    try (JavaBinCodec jbc = new JavaBinCodec()) {
      nl1 =
          (NamedList<?>)
              jbc.setReadStringAsCharSeq(true)
                  .unmarshal(new ByteArrayInputStream(bytes, 0, bytes.length));
    }
    byte[] buf = ((ByteArrayUtf8CharSequence) nl1.getVal(0)).getBuf();
    ByteArrayUtf8CharSequence valLong = (ByteArrayUtf8CharSequence) nl1.get("key_long");
    assertNotSame(valLong.getBuf(), buf);

    for (int i = 1; i < 6; i++) {
      ByteArrayUtf8CharSequence val = (ByteArrayUtf8CharSequence) nl1.get("key" + i);
      assertEquals(buf, val.getBuf());
      String s = val.toString();
      assertTrue(s.startsWith("" + i));
      assertTrue(s, s.endsWith(str));
    }
  }
}
